"""
Guard: DoWhy 0.11.1 only works with pandas 2.x, and says so on every run.

DoWhy 0.11.1's regression estimator reads `model.params[0]`, indexing a pandas Series by position. pandas 2.1 deprecates that
(a FutureWarning, 5,757 of them in one statistical run) and pandas 3 removes it, which would break the causal engine at the first
estimate. Nothing breaks today because both are pinned exactly; this is what stops a routine "bump pandas" from doing it silently.

    python -m pytest tests/static/test_dowhy_pandas_pin.py -v
"""
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helpers import ROOT  # noqa: E402


def pins(path):
    found = {}
    for line in path.read_text().splitlines():
        line = line.split("#")[0].strip()
        if "==" in line:
            name, version = line.split("==", 1)
            found[name.strip().lower()] = version.strip()
    return found


def test_wherever_dowhy_0_11_is_pinned_pandas_is_pinned_to_the_2_x_line():
    checked = 0
    for path in sorted(ROOT.rglob("requirements*.txt")):
        if "node_modules" in path.parts:
            continue
        found = pins(path)
        if found.get("dowhy", "").startswith("0.11"):
            checked += 1
            assert "pandas" in found, f"{path.relative_to(ROOT)} pins dowhy 0.11 but does not pin pandas"
            assert re.match(r"2\.", found["pandas"]), (
                f"{path.relative_to(ROOT)}: pandas=={found['pandas']} with dowhy=={found['dowhy']}: DoWhy 0.11 indexes a Series by "
                "position, which pandas 3 removes, so the causal engine would fail at its first estimate")
    assert checked >= 1, "no requirements file pins dowhy 0.11, so this guard guards nothing (update it if DoWhy was upgraded)"


def test_the_statistical_runner_hides_that_one_warning_and_no_other():
    text = (ROOT / "tests" / "statistical" / "run_statistical_tests.sh").read_text()
    filters = re.findall(r'-W "([^"]+)"', text)
    assert filters == ["ignore:Series.__getitem__ treating keys as positions is deprecated:FutureWarning"], filters
