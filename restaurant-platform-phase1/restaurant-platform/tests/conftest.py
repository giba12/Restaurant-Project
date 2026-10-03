"""
Shared pytest configuration for the cross-cutting test layers under tests/.

Per-service unit tests live next to the service they test (see TESTING.md);
everything here needs more than one service, a real database, or the whole
stack, so it lives in one place.
"""
import os
import sys

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
if TESTS_DIR not in sys.path:
    sys.path.insert(0, TESTS_DIR)


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: takes minutes (acceptance/soak); excluded from the default run")
    config.addinivalue_line("markers", "network: needs internet access (vulnerability databases)")


def pytest_addoption(parser):
    parser.addoption("--run-slow", action="store_true", default=False, help="also run tests marked slow")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--run-slow"):
        return
    import pytest

    skip_slow = pytest.mark.skip(reason="slow test; pass --run-slow to include it")
    for item in items:
        if "slow" in item.keywords:
            item.add_marker(skip_slow)
