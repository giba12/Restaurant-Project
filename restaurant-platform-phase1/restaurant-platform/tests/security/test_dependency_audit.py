"""
Supply-chain check: no pinned Python dependency has a published vulnerability.

Pinning exact versions (which this project does, on purpose) makes builds
reproducible but also freezes in whatever flaws those versions had on the day
they were pinned. This test asks the public advisory database (PyPI's, via
pip-audit) about every pin in every requirements.txt, so a newly disclosed
flaw turns CI red instead of sitting unnoticed.

Needs internet access, so it runs in the nightly workflow rather than on
every push (a new advisory is not caused by a commit).

    pip install pip-audit pytest
    python -m pytest tests/security -v
"""
import json
import os
import shutil
import subprocess
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helpers import ROOT  # noqa: E402

pytestmark = [pytest.mark.network, pytest.mark.skipif(shutil.which("pip-audit") is None, reason="pip-audit not installed")]

REQUIREMENTS = sorted(p for p in ROOT.rglob("requirements.txt") if "node_modules" not in p.parts)

# Advisories reviewed and accepted, each with the reason. Empty is the goal.
ACCEPTED = {
    # "PYSEC-XXXX-NNNN": "why this does not apply here",
}


def _pinned_lines(path):
    # pip-audit can only judge exact pins; the one deliberate range
    # (numpy>=1.26 in edge-simulators) is skipped here and checked for by
    # tests/static/test_dockerfiles.py's allow-list.
    lines = [line.split("#")[0].strip() for line in path.read_text().splitlines()]
    return [line for line in lines if "==" in line]


@pytest.mark.parametrize("requirements", REQUIREMENTS, ids=lambda p: str(p.relative_to(ROOT)))
def test_no_pinned_dependency_has_a_known_vulnerability(requirements, tmp_path):
    pinned = _pinned_lines(requirements)
    assert pinned, f"{requirements} has no pinned dependencies to audit"
    audit_input = tmp_path / "requirements.txt"
    audit_input.write_text("\n".join(pinned) + "\n")

    result = subprocess.run(
        ["pip-audit", "-r", str(audit_input), "--no-deps", "--disable-pip", "--progress-spinner", "off", "-f", "json"],
        capture_output=True, text=True, timeout=300,
    )
    assert result.stdout.strip(), f"pip-audit produced no output (is the network up?):\n{result.stderr}"
    findings = []
    for dependency in json.loads(result.stdout)["dependencies"]:
        for vuln in dependency.get("vulns", []):
            if vuln["id"] not in ACCEPTED:
                findings.append(f"{dependency['name']}=={dependency['version']}: {vuln['id']} (fixed in {vuln.get('fix_versions')})")
    assert not findings, f"{requirements.relative_to(ROOT)} pins vulnerable versions:\n  " + "\n  ".join(sorted(set(findings)))
