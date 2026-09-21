"""
Tests for narration_guard. Pure Python, no dependencies beyond pytest:

    python -m pytest test_narration_guard.py
"""
import pytest

import narration_guard as g


def finding(estimate=7627.916216216181, unit="milliseconds", confounders=("station_id",),
            treatment="staffing_level", outcome="pickup_delay_ms", refuted=True):
    return {
        "effect_estimate": estimate,
        "effect_estimate_unit": unit,
        "confounders_controlled": list(confounders),
        "treatment_variable": treatment,
        "outcome_variable": outcome,
        "refutation_passed": refuted,
    }


# The sentence the 0.5B model actually wrote for the finding above (found in the
# live narrated_findings table): the "1.81%" appears nowhere in the finding.
REAL_FABRICATION = (
    "The treatment variable of staff level has an effect of 7627.92 milliseconds on pickup delay, "
    "which is 1.81% greater than the baseline level of staff. The confounders of station_id were "
    "controlled for, and the refutation test passed."
)


def test_rejects_the_real_fabricated_percentage():
    problems = g.check_narration(REAL_FABRICATION, finding())
    assert any("1.81" in p for p in problems)
    assert any("percent sign" in p for p in problems)


def test_accepts_faithful_text_with_rounded_or_truncated_numbers():
    f = finding()
    for text in [
        "Higher staffing raised pickup delay by 7627.92 milliseconds, controlling for station.",
        "Pickup delay rose by about 7,628 milliseconds.",
        "Pickup delay rose by 7627.9 ms.",
    ]:
        assert g.check_narration(text, f) == [], text


def test_accepts_milliseconds_converted_to_seconds():
    assert g.check_narration("Pickup delay rose by roughly 7.6 seconds.", finding()) == []
    assert g.check_narration("Pickup delay rose by about 8 seconds.", finding()) == []


def test_rejects_a_number_that_is_not_in_the_finding():
    problems = g.check_narration("Pickup delay rose by 7627.92 ms across 14 shifts.", finding())
    assert problems == ["number 14 does not appear in the finding"]


def test_rejects_a_wrong_magnitude():
    assert g.check_narration("Pickup delay rose by 9000 milliseconds.", finding())
    assert g.check_narration("Pickup delay rose by 76.28 milliseconds.", finding())


def test_percent_sign_is_fine_only_for_percent_effects():
    pct = finding(estimate=4.5, unit="percent_change")
    assert g.check_narration("Waste rose by 4.5% after the change.", pct) == []
    assert g.check_narration("Waste rose by 4.5% after the change.", finding(estimate=4.5, unit="grams"))


def test_confounder_count_and_identifier_digits_are_allowed():
    f = finding(confounders=("portion_size_variant", "declared_dietary_restriction"))
    assert g.check_narration("Pickup delay rose by 7627.92 ms; 2 confounders were controlled for.", f) == []
    f = finding(treatment="table_12_open")
    assert g.check_narration("With table 12 open, pickup delay rose by 7627.92 ms.", f) == []


def test_direction_must_match_the_sign():
    assert g.check_narration("Pickup delay decreased by 7627.92 ms.", finding())
    assert g.check_narration("Waste increased by 90.2 grams.", finding(estimate=-90.2, unit="grams"))
    assert g.check_narration("Waste decreased by 90.2 grams.", finding(estimate=-90.2, unit="grams")) == []


def test_word_boundaries_avoid_false_positives():
    # 'regardless' contains 'less' but is not a direction word.
    assert g.check_narration("Regardless of the day, pickup delay rose by 7627.92 ms.", finding()) == []


@pytest.mark.parametrize("text", [
    "Pickup delay rose by 7627.92 ms, a statistically significant effect.",
    "The effect is significant at the 95% confidence level.",
    "Pickup delay rose by 7627.92 ms (p-value below the threshold).",
    "This proves that staffing raised pickup delay by 7627.92 ms.",
])
def test_rejects_unsupported_statistical_claims(text):
    assert any("statistical claim" in p for p in g.check_narration(text, finding())), text


def test_refutation_wording_is_allowed_because_the_finding_has_it():
    assert g.check_narration("Pickup delay rose by 7627.92 ms and the refutation test passed.", finding()) == []


# Texts the live 0.5B model wrote that passed the number/direction checks but are not
# fit to show: raw identifiers, and the prompt's own definitions parroted back.
LEAKY = [
    "The treatment of to_go_container_used caused the estimated_waste_grams to change by -89.53 grams, "
    "reflecting a reduction in waste volume due to the alteration in the container utilization.",
    "The treatment is to_go_container_used, which is the condition that was changed in the restaurant "
    "operations dashboard. The outcome is estimated_waste_grams, which is the value that was changed or "
    "affected by the treatment. According to the finding, the treatment resulted in a value of -89.48 grams, "
    "indicating a decrease in waste. The confounders were controlled for.",
]
GRAMS = dict(estimate=-89.5, unit="grams", treatment="to_go_container_used",
             outcome="estimated_waste_grams", confounders=("portion_size_variant", "declared_dietary_restriction"))


@pytest.mark.parametrize("text", LEAKY)
def test_rejects_raw_identifiers_and_rambling(text):
    assert g.check_narration(text, finding(**GRAMS))


def test_raw_identifier_and_length_problems_are_reported():
    problems = g.check_narration(LEAKY[1], finding(**GRAMS))
    assert any("raw identifier" in p for p in problems)
    assert any("too long" in p for p in problems)


def test_plain_prose_with_a_decimal_number_is_not_mistaken_for_many_sentences():
    text = "Using a to-go container decreased estimated waste by 89.5 grams. Portion size was controlled for."
    assert g.check_narration(text, finding(**GRAMS)) == []


def test_empty_text_is_rejected():
    assert g.check_narration("   ", finding()) == ["empty text"]


@pytest.mark.parametrize("f", [
    finding(),
    finding(estimate=-93.26, unit="grams", confounders=("portion_size_variant", "declared_dietary_restriction"),
            treatment="to_go_container_used", outcome="estimated_waste_grams"),
    finding(estimate=4.5, unit="percent_change"),
    finding(estimate=-0.4, unit="percent_change", confounders=()),
    finding(estimate=12.0, unit=None),
    finding(refuted=False),
])
def test_fallback_always_passes_its_own_guard(f):
    text = g.render_fallback(f)
    assert g.check_narration(text, f) == [], text


def test_fallback_wording():
    text = g.render_fallback(finding(estimate=-93.26, unit="grams", treatment="to_go_container_used",
                                     outcome="estimated_waste_grams",
                                     confounders=("portion_size_variant", "declared_dietary_restriction")))
    assert text == (
        "To go container used is estimated to decrease estimated waste grams by 93.26 grams. "
        "This controls for portion size variant, declared dietary restriction. "
        "The result passed the refutation check."
    )


def test_fallback_handles_a_zero_effect_and_missing_confounders():
    text = g.render_fallback(finding(estimate=0.001, confounders=()))
    assert "no measurable effect" in text and "No confounders were controlled for." in text


def test_format_number():
    assert g.format_number(7627.916216216181) == "7627.92"
    assert g.format_number(-90.2) == "90.2"
    assert g.format_number(3.0) == "3"
