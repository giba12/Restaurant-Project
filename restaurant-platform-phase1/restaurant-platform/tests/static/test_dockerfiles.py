"""
Static checks on every Dockerfile in the repository: no build, no network.

These encode the container conventions this project settled on, so a new
image cannot quietly regress them: run as a non-root user, one Python minor
version everywhere, and fully pinned Python dependencies.

    pip install pytest
    python -m pytest tests/static/test_dockerfiles.py -v
"""
import os
import re
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helpers import ROOT  # noqa: E402

DOCKERFILES = sorted(p for p in ROOT.rglob("Dockerfile") if "node_modules" not in p.parts)

# The official nginx image starts its master process as root by design (it has
# to bind and then drops its workers to the unprivileged `nginx` user).
# Replacing that would mean forking the image's entrypoint for no real gain.
RUNS_AS_ROOT_ON_PURPOSE = {"services/dashboard-web/Dockerfile"}


def _rel(path):
    return str(path.relative_to(ROOT))


def _final_stage_lines(path):
    lines = path.read_text().splitlines()
    starts = [i for i, line in enumerate(lines) if re.match(r"^FROM\s", line, re.IGNORECASE)]
    return lines[starts[-1]:]


def test_dockerfiles_were_found():
    assert len(DOCKERFILES) >= 12, f"expected the project's ~14 Dockerfiles, found {len(DOCKERFILES)}"


@pytest.mark.parametrize("dockerfile", DOCKERFILES, ids=_rel)
def test_final_image_does_not_run_as_root(dockerfile):
    if _rel(dockerfile) in RUNS_AS_ROOT_ON_PURPOSE:
        pytest.skip("documented exception: official nginx image")
    users = [
        re.match(r"^USER\s+(\S+)", line, re.IGNORECASE).group(1)
        for line in _final_stage_lines(dockerfile)
        if re.match(r"^USER\s+\S+", line, re.IGNORECASE)
    ]
    assert users, f"{_rel(dockerfile)} never sets USER, so the container runs as root"
    final = users[-1].split(":")[0]
    assert final not in ("root", "0"), f"{_rel(dockerfile)} ends as USER {users[-1]}; it must drop root before ENTRYPOINT/CMD"


@pytest.mark.parametrize("dockerfile", DOCKERFILES, ids=_rel)
def test_python_images_use_the_one_project_wide_python_version(dockerfile):
    # Pinned to 3.11 everywhere: dowhy 0.11.1 does not support 3.12, and
    # kafka-python was verified only against this interpreter. Mixed versions
    # are how "works on my machine" bugs get in.
    for line in dockerfile.read_text().splitlines():
        match = re.match(r"^FROM\s+(\S*python\S*)", line, re.IGNORECASE)
        if match:
            assert match.group(1).endswith("python:3.11-slim"), f"{_rel(dockerfile)}: {match.group(1)}"


REQUIREMENTS = sorted(p for p in ROOT.rglob("requirements.txt") if "node_modules" not in p.parts)


@pytest.mark.parametrize("requirements", REQUIREMENTS, ids=_rel)
def test_requirements_are_pinned_exactly(requirements):
    # An unpinned transitive dependency is a time bomb: scipy 1.17 and
    # networkx 3.6 each broke causal-engine at startup (problem log item 47).
    # `numpy>=1.26` style ranges are allowed only where listed here.
    allowed_ranges = {("edge-simulators/requirements.txt", "numpy")}
    for line in requirements.read_text().splitlines():
        line = line.split("#")[0].strip()
        if not line:
            continue
        name = re.split(r"[=<>!~\[]", line)[0].strip()
        if "==" in line:
            continue
        assert (_rel(requirements), name) in allowed_ranges, f"{_rel(requirements)}: '{line}' is not pinned with =="
