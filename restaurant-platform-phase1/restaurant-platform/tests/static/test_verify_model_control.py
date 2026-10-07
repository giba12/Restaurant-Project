"""
k8s/audit/verify-model-control.sh, run against stand-ins for kubectl and the operator tool.

The script is what proves, on the live cluster, that the plate-waste node refuses a wrong signature and accepts the right one. It
handles the operator's login and the master secret, so it must report each outcome truthfully (a node that accepts a wrong
signature, or ignores a right one, must make it fail) and must never print a credential or put one on a command line.

    pip install pytest
    python -m pytest tests/static/test_verify_model_control.py -v
"""
import os
import re
import shutil
import subprocess
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helpers import ROOT  # noqa: E402

SCRIPT = ROOT / "k8s" / "audit" / "verify-model-control.sh"
pytestmark = pytest.mark.skipif(shutil.which("bash") is None, reason="bash is needed")

SECRETS = {"mqtt-edge-operator/username": "edge-operator", "mqtt-edge-operator/password": "operator-password-0123456789abcdef",
           "edge-control-master/key": "master-secret-0123456789abcdef0123456789", "mosquitto-tls/tls.crt": "-----fake ca-----"}

KUBECTL = r'''#!/usr/bin/env bash
echo "kubectl $*" >> "$LOG"
case "$1 $2" in
  "get secret")
    name="$3"; field=""
    for a in "$@"; do case "$a" in jsonpath=*) field="${a#jsonpath=\{.data.}"; field="${field%\}}"; field="${field//\\/}" ;; esac; done
    printf '%s' "$(cat "$SECRETS/$name/$field")" | base64 | tr -d '\n' ;;
  "exec -n") echo "${KEY_LENGTH:-64}" ;;
  "port-forward -n") echo "Forwarding from 127.0.0.1:18883 -> 8883"; exec sleep 60 ;;
  *) echo "stub kubectl: unexpected call: $*" >&2; exit 3 ;;
esac
'''

# A stand-in for `python -m control.edge_control`: a node that holds the right master secret and answers like the real one.
OPERATOR = r'''
import json, os, sys
state_file = os.environ["NODE_STATE"]
state = json.load(open(state_file)) if os.path.exists(state_file) else {"state": "ready", "reason": "listening", "version": "1.2.0"}
action = sys.argv[1]
if action == "status":
    print(f"sim-plate-cam-01: {state['state']} - {state['reason']}; running {state['version']} (ccbb7a351773)")
elif action == "rollout":
    right = os.environ["EDGE_CONTROL_MASTER_KEY"] == os.environ["EXPECTED_MASTER"]
    if os.environ.get("NODE_BEHAVIOUR") == "accepts-anything":
        right = True
    if os.environ.get("NODE_BEHAVIOUR") == "ignores-everything":
        sys.exit(0)
    state.update(state="unchanged" if right else "rejected", reason="already running this model" if right else "signature does not match")
    print("rolled out")
elif action == "clear":
    print("cleared")
json.dump(state, open(state_file, "w"))
'''


@pytest.fixture
def env(tmp_path):
    bin_dir, secrets, tools = tmp_path / "bin", tmp_path / "secrets", tmp_path / "tools"
    (tools / "control").mkdir(parents=True)
    bin_dir.mkdir()
    for key, value in SECRETS.items():
        name, field = key.split("/")
        (secrets / name).mkdir(parents=True, exist_ok=True)
        (secrets / name / field).write_text(value)
    (bin_dir / "kubectl").write_text(KUBECTL)
    (bin_dir / "kubectl").chmod(0o755)
    (tools / "control" / "__init__.py").write_text("")
    (tools / "control" / "edge_control.py").write_text(OPERATOR)
    log = tmp_path / "calls.log"
    log.write_text("")
    environment = {**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}", "SECRETS": str(secrets), "LOG": str(log), "EDGE_TOOLS": str(tools),
                   "NODE_STATE": str(tmp_path / "node.json"), "EXPECTED_MASTER": SECRETS["edge-control-master/key"], "PYTHON": sys.executable}
    return {"env": environment, "log": log}


def run(env, **extra):
    result = subprocess.run(["bash", str(SCRIPT)], env={**env["env"], **extra}, capture_output=True, text=True, timeout=60)
    return result, result.stdout + result.stderr


def test_a_node_that_refuses_a_wrong_signature_and_accepts_the_right_one_passes(env):
    result, out = run(env)
    assert result.returncode == 0, out
    assert "PASS  rejected: signature does not match" in out and "PASS  unchanged: already running this model" in out
    assert "cleared" in out


def test_a_node_that_accepts_a_wrongly_signed_command_fails_the_check(env):
    result, out = run(env, NODE_BEHAVIOUR="accepts-anything")
    assert result.returncode != 0 and "FAIL  a wrongly signed command was answered 'unchanged'" in out


def test_a_node_that_ignores_a_correctly_signed_command_fails_the_check(env):
    result, out = run(env, NODE_BEHAVIOUR="ignores-everything")
    assert result.returncode != 0 and "FAIL" in out


def test_a_node_with_no_key_is_reported_with_the_remedy(env):
    result, out = run(env, KEY_LENGTH="0")
    assert result.returncode != 0 and "controlKeySecret" in out


def test_a_python_without_paho_is_reported_with_the_remedy_before_anything_is_opened(env):
    # `false -c ...` stands in for an interpreter that cannot import paho.mqtt: the script must say what to do, not trace back.
    result, out = run(env, PYTHON="false")
    assert result.returncode == 2 and "paho" in out and "PYTHON=" in out
    assert "port-forward" not in env["log"].read_text(), "the port-forward was opened before the check"


def test_no_credential_is_printed_or_put_on_a_command_line(env):
    _, out = run(env)
    calls = env["log"].read_text()
    for name in ("mqtt-edge-operator/password", "edge-control-master/key"):
        assert SECRETS[name] not in out + calls, f"{name} leaked"


def test_the_port_forward_is_stopped_by_its_own_pid_and_nothing_is_killed_by_name():
    text = SCRIPT.read_text()
    assert 'kill "$forward_pid"' in text and "pkill" not in text and "killall" not in text


def test_it_is_executable_and_parses():
    assert os.access(SCRIPT, os.X_OK)
    assert subprocess.run(["bash", "-n", str(SCRIPT)], capture_output=True).returncode == 0
    assert re.search(r"^set -Eeuo pipefail", SCRIPT.read_text(), flags=re.M)
