"""
Repository-wide static checks: lint, committed secrets, and the version-1 /
version-2 boundary. No services, no network.

    pip install pytest ruff
    python -m pytest tests/static/test_repo_hygiene.py -v
"""
import os
import re
import shutil
import subprocess
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helpers import ROOT  # noqa: E402


def _tracked_files():
    """Tracked files when run inside a git checkout, otherwise every file on disk."""
    result = subprocess.run(["git", "ls-files", "-z"], capture_output=True, cwd=ROOT)
    if result.returncode == 0 and result.stdout:
        return [ROOT / p for p in result.stdout.decode().split("\0") if p]
    return [p for p in ROOT.rglob("*") if p.is_file() and "node_modules" not in p.parts and ".git" not in p.parts]


def _text(path):
    try:
        return path.read_text(encoding="utf-8")
    except (UnicodeDecodeError, OSError):
        return None


# ---------------------------------------------------------------- lint

@pytest.mark.skipif(shutil.which("ruff") is None and not os.environ.get("RUFF"), reason="ruff not installed")
def test_python_has_no_syntax_errors_undefined_names_or_dead_code():
    # E9/F63/F7/F82: syntax errors and undefined names (a crash waiting to happen).
    # F401/F811/F841: unused imports/redefinitions/variables (dead code).
    ruff = os.environ.get("RUFF") or "ruff"
    result = subprocess.run(
        [ruff, "check", "--select", "E9,F63,F7,F82,F401,F811,F841", "--no-cache",
         "--exclude", "node_modules", "--output-format", "concise", str(ROOT)],
        capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr


# ---------------------------------------------------------------- secrets

SECRET_PATTERNS = {
    "private key block": re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH |DSA |PGP )?PRIVATE KEY-----"),
    "AWS access key id": re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    "GitHub token": re.compile(r"\bgh[pousr]_[A-Za-z0-9]{36,}\b"),
    "Slack token": re.compile(r"\bxox[abprs]-[A-Za-z0-9-]{10,}\b"),
    "credential in a URL": re.compile(r"[a-z][a-z0-9+.-]*://[^\s:@/'\"]+:(?!\$|%|\{|<|changeme|REPLACE|password|pass\b|\.\.\.)[^\s:@/'\"]{6,}@[A-Za-z0-9.-]+"),
}
# Strings that look like a credential in a URL but are documented placeholders.
PLACEHOLDER_OK = ("changeme-local-dev-only", "REPLACE-AT-DEPLOY-TIME", "example", "user:pass")


def test_no_secrets_are_committed():
    findings = []
    for path in _tracked_files():
        if path.suffix in (".png", ".jpg", ".ico", ".svg", ".lock", ".uid") or path.name == "package-lock.json":
            continue
        text = _text(path)
        if text is None:
            continue
        for label, pattern in SECRET_PATTERNS.items():
            for match in pattern.finditer(text):
                if any(ok in match.group(0) for ok in PLACEHOLDER_OK):
                    continue
                findings.append(f"{path.relative_to(ROOT)}: {label}: {match.group(0)[:40]}...")
    assert not findings, "possible committed secrets:\n" + "\n".join(findings)


def test_the_real_credentials_directory_is_gitignored():
    # k8s/secrets/ holds the rotated, real credentials generated at deploy time.
    result = subprocess.run(
        ["git", "check-ignore", "-q", "k8s/secrets/anything.values.yaml"], cwd=ROOT, capture_output=True
    )
    if result.returncode == 128:
        pytest.skip("not a git checkout")
    assert result.returncode == 0, "k8s/secrets/ is not gitignored; generated credentials could be committed"


def test_no_environment_file_with_real_values_is_tracked():
    tracked = [p.name for p in _tracked_files()]
    assert ".env" not in tracked, "a real .env file is tracked; only .env.example may be"


# ---------------------------------------------------------------- version boundary

def test_version_1_never_references_version_2():
    """
    Runs the exact check documented in game/README.md (reading its exclusion
    list from there, so the doc and the test cannot drift apart): version 1
    must not depend on the game. It should find nothing.
    """
    readme = (ROOT / "game" / "README.md").read_text()
    exclusions = re.search(r'grep -v "([^"]+)"', readme)
    assert exclusions, "could not find the exclusion list in game/README.md"
    excluded = [re.compile(pattern.replace("\\.", r"\.")) for pattern in exclusions.group(1).split("\\|")]
    # Assembled from pieces so this file does not contain the strings it hunts
    # for (it would otherwise flag itself -- it is tracked, and not game code).
    needle = re.compile("|".join(re.escape(p) for p in ("game-" "bridge", "game/" "bridge", "game/" "client", "game/" "k8s")))
    offenders = []
    for path in _tracked_files():
        rel = str(path.relative_to(ROOT))
        if rel.startswith("game/") or "node_modules" in rel:
            continue
        text = _text(path)
        if text is None or not needle.search(text):
            continue
        if any(pattern.search(rel) for pattern in excluded):
            continue
        offenders.append(rel)
    assert not offenders, f"version-1 files reference version 2: {offenders}"
