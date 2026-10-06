"""
Tests of the stack-test helpers that need no stack.

The harness's own checks have been wrong before (a connector-health test that read only the
connector's state, and a container lookup that missed exited containers on Compose v2), so the
parts that can be tested without containers are tested here.

    python -m pytest tests/static/test_stack_helpers.py -v
"""
import os
import sys
import types

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import stack_fixture  # noqa: E402

SAMPLE = """# HELP bridge_mqtt_connected 1 while connected to the MQTT broker
# TYPE bridge_mqtt_connected gauge
bridge_mqtt_connected 1.0
# HELP bridge_messages_forwarded_total Messages confirmed by Kafka and acknowledged to MQTT
# TYPE bridge_messages_forwarded_total counter
bridge_messages_forwarded_total{kafka_topic="plate-waste-events"} 12.0
bridge_messages_forwarded_total{kafka_topic="pos-transaction-events"} 40.0
bridge_oldest_unconfirmed_seconds 0.25
"""


def test_metrics_text_is_parsed_into_series_with_their_labels_kept():
    metrics = stack_fixture.parse_metrics(SAMPLE)
    assert metrics["bridge_mqtt_connected"] == 1.0
    assert metrics['bridge_messages_forwarded_total{kafka_topic="plate-waste-events"}'] == 12.0
    assert metrics["bridge_oldest_unconfirmed_seconds"] == 0.25


def test_comments_blank_lines_and_unparseable_lines_are_ignored():
    assert stack_fixture.parse_metrics("# HELP x y\n\nnot a metric line\nx 3\n") == {"x": 3.0}


def test_an_empty_or_missing_metrics_body_gives_no_series_rather_than_an_error():
    assert stack_fixture.parse_metrics("") == {}


# ------------------------------------------------------------------ crash(): delivering a real SIGKILL

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "resilience"))
import test_failure_recovery as recovery  # noqa: E402


def fake_run_recording(calls, returncode=0, stderr=""):
    def fake_run(cmd, check=True, **kwargs):
        calls.append(cmd)
        return types.SimpleNamespace(returncode=returncode, stdout="", stderr=stderr)
    return fake_run


def test_a_process_the_host_user_owns_is_killed_directly_with_no_docker_or_sudo(monkeypatch):
    calls, signals = [], []
    monkeypatch.setattr(recovery, "inspect", lambda service: {"State": {"Pid": 4242}})
    monkeypatch.setattr(recovery.os, "kill", lambda pid, sig: signals.append((pid, sig)))
    monkeypatch.setattr(recovery, "run", fake_run_recording(calls))
    recovery.crash("storage-consumer")
    assert signals == [(4242, recovery.signal.SIGKILL)] and calls == []


def test_a_root_owned_process_is_killed_with_sudo_never_with_docker_kill(monkeypatch):
    # On GitHub's rootful Docker the runner (UID 1001) can signal the UID-1001 services directly but not Mosquitto.
    # `docker kill` there is a manual stop that the restart policy ignores: Mosquitto stayed down for the rest of
    # the run and four later tests failed (DEF-154).
    calls = []
    monkeypatch.setattr(recovery, "inspect", lambda service: {"State": {"Pid": 4242}})

    def denied(pid, sig):
        raise PermissionError(1, "Operation not permitted")

    monkeypatch.setattr(recovery.os, "kill", denied)
    monkeypatch.setattr(recovery, "run", fake_run_recording(calls))
    recovery.crash("mosquitto")
    assert calls == [["sudo", "-n", "kill", "-9", "4242"]]


def test_when_no_real_sigkill_can_be_delivered_the_test_fails_loudly_instead_of_using_docker_kill(monkeypatch):
    calls = []
    monkeypatch.setattr(recovery, "inspect", lambda service: {"State": {"Pid": 4242}})
    monkeypatch.setattr(recovery.os, "kill", lambda pid, sig: (_ for _ in ()).throw(PermissionError()))
    monkeypatch.setattr(recovery, "run", fake_run_recording(calls, returncode=1, stderr="sudo: a password is required"))
    with pytest.raises(RuntimeError, match="docker kill. is not an acceptable substitute"):
        recovery.crash("mosquitto")
    assert not any("docker" in part for call in calls for part in call), "it fell back to docker kill"
