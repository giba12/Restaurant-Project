"""
k8s/audit/test-sensor-alert.sh, run against a stand-in `kubectl` that behaves like a cluster whose sensor is stopped.

The script pauses a simulator on the live cluster to see the arrival alarm fire. It must (1) refuse to start when a test would prove
nothing, (2) restore the simulator on EVERY exit, because a paused sensor left paused is an outage, (3) fail when the alert does not
fire or does not get out, and (4) pass only when each hop did its job. None of that needs a cluster.

    pip install pytest
    python -m pytest tests/static/test_sensor_alert_script.py -v
"""
import os
import shutil
import subprocess
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helpers import ROOT  # noqa: E402

SCRIPT = ROOT / "k8s" / "audit" / "test-sensor-alert.sh"
pytestmark = pytest.mark.skipif(shutil.which("bash") is None or shutil.which("python3") is None, reason="bash and python3 are needed")

KUBECTL = r'''#!/usr/bin/env bash
# A stand-in kubectl. State is files in $STATE; every call is appended to $LOG. Behaviour switches come from the environment.
echo "kubectl $*" >> "$LOG"
count() { n=$(cat "$STATE/$1" 2>/dev/null || echo 0); echo $((n + 1)) > "$STATE/$1"; echo $((n + 1)); }
case "$1" in
  get) echo "${REPLICAS:-1}" ;;
  scale)
    case "$*" in
      *--replicas=0*) touch "$STATE/stopped"; rm -f "$STATE/restored" ;;
      *) touch "$STATE/restored" ;;
    esac ;;
  logs)  # the relay
    posts=0; [ -f "$STATE/fired" ] && [ -z "$RELAY_SILENT" ] && posts=1
    for i in $(seq 1 $posts); do echo 'alert-relay: "POST /alert HTTP/1.1" 200 -'; done
    [ -f "$STATE/fired" ] && [ -n "$RELAY_ERROR" ] && echo "alert-relay: error forwarding alert: boom"
    exit 0 ;;
  exec)
    target="$4"; url="${@: -1}"
    case "$target" in
      deploy/prometheus)
        if [ -n "$PROM_DIES_AFTER_STOP" ] && [ -f "$STATE/stopped" ]; then exit 1; fi
        case "$url" in
          */rules) [ -n "$RULE_MISSING" ] && echo '{"data":{"groups":[]}}' || echo '{"data":{"groups":[{"rules":[{"name":"SensorTopicSilent","health":"ok"}]}]}}' ;;
          *ALERTS*)
            if [ -n "$ALREADY_FIRING" ] && [ ! -f "$STATE/stopped" ]; then echo '{"data":{"result":[{"metric":{"kafka_topic":"staff-shift-events"}}]}}'; exit 0; fi
            if [ -f "$STATE/restored" ]; then
              n=$(count clear_polls)
              if [ "$n" -le "${CLEAR_AFTER:-1}" ]; then echo '{"data":{"result":[{"metric":{"kafka_topic":"pos-transaction-events"}}]}}'; else echo '{"data":{"result":[]}}'; fi
            elif [ -f "$STATE/stopped" ] && [ -z "$NEVER_FIRES" ]; then
              n=$(count fire_polls)
              if [ "$n" -ge "${FIRE_AFTER:-2}" ]; then
                touch "$STATE/fired"
                if [ -n "$ALSO_OTHER" ]; then echo '{"data":{"result":[{"metric":{"kafka_topic":"pos-transaction-events"}},{"metric":{"kafka_topic":"staff-shift-events"}}]}}'
                else echo '{"data":{"result":[{"metric":{"kafka_topic":"pos-transaction-events"}}]}}'; fi
              else echo '{"data":{"result":[]}}'; fi
            else echo '{"data":{"result":[]}}'; fi ;;
          *increase*) [ -n "$NOT_ARRIVING" ] && echo '{"data":{"result":[{"metric":{"kafka_topic":"pos-transaction-events"},"value":[0,"0"]}]}}' || echo '{"data":{"result":[{"metric":{"kafka_topic":"pos-transaction-events"},"value":[0,"42"]}]}}' ;;
        esac ;;
      deploy/alertmanager)
        [ -f "$STATE/fired" ] && [ -z "$AM_EMPTY" ] && echo '[{"labels":{"alertname":"SensorTopicSilent","kafka_topic":"pos-transaction-events"}}]' || echo '[]' ;;
    esac ;;
esac
'''


@pytest.fixture
def env(tmp_path):
    bin_dir, state = tmp_path / "bin", tmp_path / "state"
    bin_dir.mkdir()
    state.mkdir()
    (bin_dir / "kubectl").write_text(KUBECTL)
    (bin_dir / "kubectl").chmod(0o755)
    log = tmp_path / "calls.log"
    log.write_text("")
    base = {**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}", "STATE": str(state), "LOG": str(log), "POLL_SECONDS": "0.05", "RETRY_SLEEP": "0.05",
            "FIRE_TIMEOUT_MIN": "1", "CLEAR_TIMEOUT_MIN": "1"}
    return {"env": base, "state": state, "log": log}


def run(env, **extra):
    result = subprocess.run(["bash", str(SCRIPT)], env={**env["env"], **extra}, capture_output=True, text=True, timeout=120)
    return result, result.stdout + result.stderr


def scale_calls(env):
    return [line for line in env["log"].read_text().splitlines() if " scale " in line]


def test_when_every_hop_works_it_passes_and_the_sensor_is_started_again(env):
    result, out = run(env)
    assert result.returncode == 0, out
    for needle in ("PASS  SensorTopicSilent fired for pos-transaction-events", "PASS  it fired for no other topic", "PASS  Alertmanager holds the alert",
                   "PASS  the relay received it", "PASS  the relay logged no forwarding error", "PASS  the alert cleared"):
        assert needle in out, out
    calls = scale_calls(env)
    assert len(calls) == 2 and "--replicas=0" in calls[0] and "--replicas=1" in calls[1], calls


def test_an_alert_that_never_fires_fails_and_the_sensor_is_still_started_again(env):
    result, out = run(env, NEVER_FIRES="1", FIRE_TIMEOUT_MIN="0")
    assert result.returncode != 0 and "FAIL  it did not fire" in out
    assert (env["state"] / "restored").exists(), "the simulator was left stopped"


def test_an_alert_that_fires_for_another_topic_too_fails(env):
    result, out = run(env, ALSO_OTHER="1")
    assert result.returncode != 0 and "FAIL  it also fired for: staff-shift-events" in out
    assert (env["state"] / "restored").exists()


def test_an_alert_alertmanager_never_received_fails(env):
    result, out = run(env, AM_EMPTY="1")
    assert result.returncode != 0 and "FAIL  Alertmanager does not hold the alert" in out


def test_a_relay_that_received_nothing_fails(env):
    result, out = run(env, RELAY_SILENT="1")
    assert result.returncode != 0 and "FAIL  the relay received nothing" in out


def test_a_relay_forwarding_error_fails(env):
    result, out = run(env, RELAY_ERROR="1")
    assert result.returncode != 0 and "FAIL  the relay logged a forwarding error" in out


def test_an_alert_that_does_not_clear_after_the_sensor_returns_fails(env):
    result, out = run(env, CLEAR_AFTER="100000", CLEAR_TIMEOUT_MIN="0")
    assert result.returncode != 0 and "FAIL  the alert is still firing" in out


@pytest.mark.parametrize("extra, message", [({"RULE_MISSING": "1"}, "rule is not loaded"), ({"NOT_ARRIVING": "1"}, "has not arrived"),
                                            ({"ALREADY_FIRING": "1"}, "already firing"), ({"REPLICAS": "0"}, "is not running")])
def test_it_refuses_to_start_when_a_test_would_prove_nothing_and_stops_nothing(env, extra, message):
    result, out = run(env, **extra)
    assert result.returncode != 0 and message in out, out
    assert scale_calls(env) == [], "it scaled something although it refused to start"


def test_it_is_executable_parses_and_never_kills_by_name():
    text = SCRIPT.read_text()
    assert os.access(SCRIPT, os.X_OK) and subprocess.run(["bash", "-n", str(SCRIPT)], capture_output=True).returncode == 0
    assert "pkill" not in text and "killall" not in text
    assert "trap restore EXIT" in text, "the simulator must be restored on every exit"


def test_the_simulator_is_started_again_even_when_the_script_dies_half_way(env):
    # Prometheus becomes unreachable after the simulator is stopped: `set -e` ends the script mid-wait, and only the EXIT trap
    # stands between that and a sensor left stopped.
    result, out = run(env, PROM_DIES_AFTER_STOP="1")
    assert result.returncode != 0
    assert (env["state"] / "restored").exists(), "the script died with the simulator still stopped"
