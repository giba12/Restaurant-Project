"""
Post-generation checks for narrated findings.

The narrator's database role can only read causal_findings, so it can't see
data it shouldn't. That boundary does NOT stop a weak model from inventing
detail *within* the finding it was given -- the 0.5B CPU model used on the
Compose path was observed writing "which is 1.81% greater than the baseline
level of staff" for a finding that contains no percentage at all. Prompting
alone can't guarantee otherwise, so generated text is verified here before it
is stored, and replaced by a deterministic template if it can't be verified.

What is checked (all cheap and mechanical, no second model):
  - every number in the text traces back to the finding: the effect (rounded
    or truncated, or converted milliseconds -> seconds), the count of
    controlled confounders, or digits inside the finding's own identifiers;
  - a percent sign only appears when the effect really is in percent;
  - no statistical assertion the finding doesn't make (significance,
    confidence intervals, p-values);
  - the text doesn't contradict the sign of the effect ("decreased" for a
    positive effect, or the reverse);
  - it reads as prose: no raw column names, and no more than MAX_SENTENCES.

What is not checked: other qualitative claims made without numbers, direction
words or the statistical terms above. This narrows the invention surface; it
does not prove the prose is faithful. The stored text is always either
verified model output or the deterministic template, and the caller records
which.
"""
import re

_NUMBER = re.compile(r"\d[\d,]*(?:\.\d+)?")

_INCREASE = re.compile(
    r"\b(increase[sd]?|increasing|higher|greater|larger|longer|more|rise[sn]?|rose|rising)\b",
    re.IGNORECASE,
)
_DECREASE = re.compile(
    r"\b(decrease[sd]?|decreasing|lower|less|fewer|smaller|shorter|reduce[sd]?|reduction|"
    r"drop(?:s|ped)?|fall[s]?|fell|decline[sd]?|declining)\b",
    re.IGNORECASE,
)

_MS_UNITS = {"milliseconds", "millisecond", "ms"}

# Column-style names (to_go_container_used) belong to the data, not to prose. Seen
# from the live 0.5B model, which also echoed its own prompt back in these texts.
_RAW_IDENTIFIER = re.compile(r"\b[a-z0-9]+(?:_[a-z0-9]+)+\b")
# The prompt asks for 1-2 sentences; a little slack, but a paragraph is a sign the
# model is rambling or repeating its instructions.
MAX_SENTENCES = 3
_SENTENCE_END = re.compile(r"[.!?](?:\s|$)")

# Statistical assertions the finding never makes. The finding carries an effect,
# its unit, the controlled confounders and a pass/fail refutation flag -- nothing
# about significance, intervals or p-values -- so any of these is invented.
# (Seen from the live 0.5B model: "95% confidence interval", "p-value = 0.0002",
# "statistically significant".) "refutation" is deliberately NOT listed: the
# finding does carry refutation_passed.
_UNSUPPORTED_CLAIMS = re.compile(
    r"\b(significan(?:t|tly|ce)|confidence|intervals?|p-?values?|statistically|"
    r"certain(?:ly)?|proves?|proven|guarantee[sd]?)\b",
    re.IGNORECASE,
)


def format_number(value: float) -> str:
    """Two decimals, trailing zeros dropped: 7627.916 -> '7627.92', -90.2 -> '90.2' (absolute value)."""
    s = f"{abs(value):.2f}".rstrip("0").rstrip(".")
    return s or "0"


def _allowed_numbers(finding: dict) -> list[float]:
    estimate = abs(float(finding["effect_estimate"]))
    allowed = [estimate, float(len(finding.get("confounders_controlled") or []))]
    if (finding.get("effect_estimate_unit") or "").lower() in _MS_UNITS:
        allowed.append(estimate / 1000.0)

    # Digits that are part of the finding's own identifiers (e.g. 'table_01').
    for key in ("treatment_variable", "outcome_variable", "effect_estimate_unit"):
        allowed += [float(n.replace(",", "")) for n in _NUMBER.findall(str(finding.get(key) or ""))]
    for name in finding.get("confounders_controlled") or []:
        allowed += [float(n.replace(",", "")) for n in _NUMBER.findall(str(name))]
    return allowed


def _matches(token: str, allowed: list[float]) -> bool:
    text = token.replace(",", "")
    decimals = len(text.split(".")[1]) if "." in text else 0
    tolerance = 10 ** -decimals  # one unit in the last written place: covers rounding and truncation
    value = float(text)
    return any(abs(value - a) < tolerance for a in allowed)


def check_narration(text: str, finding: dict) -> list[str]:
    """Returns a list of problems; empty means the text passed every check."""
    problems: list[str] = []
    if not text or not text.strip():
        return ["empty text"]

    allowed = _allowed_numbers(finding)
    for token in _NUMBER.findall(text):
        if not _matches(token, allowed):
            problems.append(f"number {token} does not appear in the finding")

    identifier = _RAW_IDENTIFIER.search(text)
    if identifier:
        problems.append(f"uses the raw identifier '{identifier.group(0)}' instead of plain words")

    sentences = len(_SENTENCE_END.findall(text))
    if sentences > MAX_SENTENCES:
        problems.append(f"too long ({sentences} sentences, limit {MAX_SENTENCES})")

    claim = _UNSUPPORTED_CLAIMS.search(text)
    if claim:
        problems.append(f"makes a statistical claim not in the finding ('{claim.group(0)}')")

    if "%" in text and (finding.get("effect_estimate_unit") or "") != "percent_change":
        problems.append("percent sign used but the effect is not in percent")

    estimate = float(finding["effect_estimate"])
    if estimate > 0 and _DECREASE.search(text):
        problems.append(f"says '{_DECREASE.search(text).group(0)}' but the effect is positive")
    if estimate < 0 and _INCREASE.search(text):
        problems.append(f"says '{_INCREASE.search(text).group(0)}' but the effect is negative")
    return problems


def _phrase(name: str) -> str:
    return str(name).replace("_", " ")


def render_fallback(finding: dict) -> str:
    """Deterministic sentence built only from the finding's own fields."""
    estimate = float(finding["effect_estimate"])
    unit = finding.get("effect_estimate_unit")
    unit_text = "percent" if unit == "percent_change" else (unit or "")
    treatment = _phrase(finding["treatment_variable"])
    outcome = _phrase(finding["outcome_variable"])
    subject = treatment[:1].upper() + treatment[1:]

    if round(estimate, 2) == 0:
        effect = f"{subject} is estimated to have no measurable effect on {outcome}."
    else:
        direction = "increase" if estimate > 0 else "decrease"
        magnitude = f"{format_number(estimate)} {unit_text}".strip()
        effect = f"{subject} is estimated to {direction} {outcome} by {magnitude}."

    confounders = [_phrase(c) for c in (finding.get("confounders_controlled") or [])]
    controlled = (
        f" This controls for {', '.join(confounders)}."
        if confounders
        else " No confounders were controlled for."
    )
    refutation = (
        " The result passed the refutation check."
        if finding.get("refutation_passed")
        else " The result did not pass the refutation check."
    )
    return effect + controlled + refutation
