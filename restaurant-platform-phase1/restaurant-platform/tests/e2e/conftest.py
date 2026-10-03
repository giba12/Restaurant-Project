import pytest

import stack_fixture


@pytest.fixture(scope="session", autouse=True)
def _stack_ready():
    stack_fixture.stack_ready_fixture()
