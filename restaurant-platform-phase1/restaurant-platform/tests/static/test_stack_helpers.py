"""
Tests of the stack-test helpers that need no stack.

The harness's own checks have been wrong before (a connector-health test that
read only the connector's state), so the parts that can be tested without
containers are tested here.

    python -m pytest tests/static/test_stack_helpers.py -v
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import stack_fixture  # noqa: E402


def connector(connector_state, *task_states):
    return {"status": {"connector": {"state": connector_state}, "tasks": [{"id": i, "state": s} for i, s in enumerate(task_states)]}}


def test_a_connector_is_running_only_if_it_and_all_its_tasks_are():
    payload = {"a": connector("RUNNING", "RUNNING"), "b": connector("RUNNING", "RUNNING", "RUNNING")}
    assert stack_fixture.summarise_connectors(payload) == {"a": "RUNNING", "b": "RUNNING"}


def test_a_failed_task_under_a_running_connector_is_reported_failed():
    # The 2026-10-03 failure: connectors RUNNING, every task FAILED, no data.
    payload = {"plate": connector("RUNNING", "FAILED"), "pos": connector("RUNNING", "RUNNING"), "twin": connector("RUNNING", "RUNNING", "FAILED")}
    assert stack_fixture.summarise_connectors(payload) == {"plate": "FAILED", "pos": "RUNNING", "twin": "FAILED"}


def test_a_connector_with_no_tasks_is_not_reported_running():
    assert stack_fixture.summarise_connectors({"a": connector("RUNNING")}) == {"a": "NO_TASKS"}


def test_a_connector_that_is_not_itself_running_is_reported_as_such():
    assert stack_fixture.summarise_connectors({"a": connector("PAUSED", "RUNNING")}) == {"a": "PAUSED"}
