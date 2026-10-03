"""
Keeps TESTING.md honest: every test in the repository must be described in it.

A test catalogue that silently falls out of date is worse than none, because it
looks authoritative. So the catalogue is itself under test: adding a test
without documenting it (what it is, what it does, why it exists) fails CI.

    python -m pytest tests/static/test_testing_doc.py -v
"""
import ast
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helpers import ROOT  # noqa: E402

DOC = (ROOT / "TESTING.md").read_text()


def _all_test_functions():
    found = {}
    for path in sorted(ROOT.rglob("test_*.py")):
        if "node_modules" in path.parts:
            continue
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.FunctionDef) and node.name.startswith("test_"):
                found.setdefault(node.name, str(path.relative_to(ROOT)))
    return found


def test_every_test_function_is_documented_in_testing_md():
    missing = {name: file for name, file in _all_test_functions().items() if f"`{name}`" not in DOC}
    assert not missing, "tests missing from TESTING.md:\n  " + "\n  ".join(f"{n}  ({f})" for n, f in sorted(missing.items()))


def test_testing_md_does_not_describe_tests_that_no_longer_exist():
    import re

    documented = set(re.findall(r"`(test_[a-z0-9_]+)`", DOC))
    real = set(_all_test_functions())
    stale = documented - real
    assert not stale, f"TESTING.md describes tests that do not exist: {sorted(stale)}"
