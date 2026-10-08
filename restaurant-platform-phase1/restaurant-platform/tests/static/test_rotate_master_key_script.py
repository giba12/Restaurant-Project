"""
k8s/audit/rotate-master-key.sh, run against a stand-in cluster whose node behaves as a node holding the keys the script says it should.

The script rotates the secret that commands the edge nodes. Its safety is in the order it does things and in where it stops: it must
never delete the old keys unless it has just seen the old master REFUSED, never close the window unless both masters were accepted
inside it, never start from a state that is already broken or already mid-rotation, and never print or pass a credential on a command
line. The real rehearsal on Kubernetes is k8s/audit/test-rotation-in-scratch-namespace.sh; this pins the logic.

    pip install pytest
    python -m pytest tests/static/test_rotate_master_key_script.py -v
"""
import os
import re
import shutil
import subprocess
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helpers import ROOT  # noqa: E402

SCRIPT = ROOT / "k8s" / "audit" / "rotate-master-key.sh"
pytestmark = pytest.mark.skipif(shutil.which("bash") is None, reason="bash is needed")

KUBECTL = r'''#!/usr/bin/env bash
echo "kubectl $*" >> "$LOG"
count() { n=$(cat "$STATE/$1" 2>/dev/null || echo 0); echo $((n + 1)) > "$STATE/$1"; echo $((n + 1)); }
case "$1" in
  get)
    case "$2" in
      secret)
        name="$3"
        for a in "$@"; do case "$a" in jsonpath*) echo "SECRET VALUE READ" >> "$LOG.leak" ;; esac; done
        [ -f "$STATE/secrets/$name" ] && exit 0 || exit 1 ;;
      "deploy/edge-sim-plate-waste") echo "registry/edge-simulator:1.0" ;;
      pods)
        # `get pods` is how the script waits for the old pod to be gone: for the first POD_OVERLAP_POLLS after an upgrade there are two
        n=$(count pod_polls)
        if [ -f "$STATE/upgraded" ] && [ "$n" -le "${POD_OVERLAP_POLLS:-0}" ]; then printf '|Running\n2025-01-01T00:00:00Z|Running\n'; else printf '|Running\n'; fi ;;
      pod)
        echo Succeeded ;;
    esac ;;
  rollout) [ -n "$ROLLOUT_FAILS" ] && exit 1 || exit 0 ;;
  delete) exit 0 ;;
  apply)
    cat > "$STATE/manifest.last"
    name=$(sed -n 's/^metadata: {name: \(.*\)}/\1/p' "$STATE/manifest.last")
    cp "$STATE/manifest.last" "$STATE/pod-$name"
    echo "pods created: $name" >> "$LOG.pods"
    count pods_created >/dev/null ;;
  logs)
    pod="$2"; manifest="$STATE/pod-$pod"
    if grep -q "name: edge-control-master-previous, key: key" "$manifest" 2>/dev/null; then master=previous; else master=new; fi
    answer() { echo "sim-plate-cam-01: $1 - $2; running 1.2.0 (ccbb7a351773)"; }
    case "$pod" in
      rotation-before) [ -n "$CURRENT_BROKEN" ] && answer rejected "bad signature (not signed with a key this node holds)" || answer unchanged "already running this model" ;;
      rotation-clear) echo "cleared" ;;
      *)
        if [ "$master" = new ]; then
          [ -n "$NEW_REJECTED" ] && answer rejected "bad signature (not signed with a key this node holds)" || answer unchanged "already running this model"
        elif [ -f "$STATE/window_open" ]; then
          [ -n "$OLD_REJECTED_IN_WINDOW" ] && answer rejected "bad signature (not signed with a key this node holds)" || answer unchanged "already running this model"
        else
          [ -n "$OLD_STILL_ACCEPTED" ] && answer unchanged "already running this model" || answer rejected "bad signature (not signed with a key this node holds)"
        fi ;;
    esac ;;
esac
'''

HELM = r'''#!/usr/bin/env bash
echo "helm $*" >> "$LOG"
touch "$STATE/upgraded"; rm -f "$STATE/pod_polls"
case "$*" in
  *"controlKeyPreviousByNode.sim-plate-cam-01=edge-control-sim-plate-cam-01-previous"*) touch "$STATE/window_open" ;;
  *"controlKeyPreviousByNode.sim-plate-cam-01= "*|*"controlKeyPreviousByNode.sim-plate-cam-01=") rm -f "$STATE/window_open" ;;
esac
'''

PROVISION = r'''#!/usr/bin/env bash
echo "provision $*" >> "$LOG"
case "$1" in
  rotate-master) touch "$STATE/secrets/edge-control-master-previous" "$STATE/secrets/edge-control-sim-plate-cam-01-previous" ;;
  finish-rotation) rm -f "$STATE/secrets/edge-control-master-previous" "$STATE/secrets/edge-control-sim-plate-cam-01-previous" ;;
esac
'''


@pytest.fixture
def env(tmp_path):
    bin_dir, state = tmp_path / "bin", tmp_path / "state"
    (state / "secrets").mkdir(parents=True)
    bin_dir.mkdir()
    for name, body in (("kubectl", KUBECTL), ("helm", HELM), ("provision", PROVISION)):
        (bin_dir / name).write_text(body)
        (bin_dir / name).chmod(0o755)
    for secret in ("edge-control-master", "edge-control-sim-plate-cam-01", "mqtt-edge-operator", "mosquitto-tls"):
        (state / "secrets" / secret).write_text("")
    log = tmp_path / "calls.log"
    log.write_text("")
    environment = {**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}", "STATE": str(state), "LOG": str(log), "PROVISION_SCRIPT": str(bin_dir / "provision"),
                   "NS": "testns", "ROTATE_CONFIRM": "yes", "CHECK_SETTLE": "0", "WAIT_SECONDS": "20"}
    return {"env": environment, "state": state, "log": log}


def run(env, **extra):
    result = subprocess.run(["bash", str(SCRIPT)], env={**env["env"], **extra}, capture_output=True, text=True, timeout=120, stdin=subprocess.DEVNULL)
    return result, result.stdout + result.stderr


def calls(env, prefix):
    return [line for line in env["log"].read_text().splitlines() if line.startswith(prefix)]


def position(env, needle):
    lines = env["log"].read_text().splitlines()
    return next(i for i, line in enumerate(lines) if needle in line)


def test_a_rotation_in_which_every_check_passes_ends_with_the_old_keys_deleted_and_exits_zero(env):
    result, out = run(env)
    assert result.returncode == 0, out
    for needle in ("PASS  window-new-master", "PASS  window-old-master", "PASS  after-new-master", "PASS  after-old-master: the node answered 'rejected'", "PASS  no old key is left"):
        assert needle in out, out
    assert [c.split()[1] for c in calls(env, "provision")] == ["rotate-master", "finish-rotation"]
    assert len(calls(env, "helm upgrade")) == 2


def test_the_steps_happen_in_the_one_order_that_is_safe(env):
    run(env)
    rotate = position(env, "provision rotate-master")
    open_window = position(env, "controlKeyPreviousByNode.sim-plate-cam-01=edge-control-sim-plate-cam-01-previous")
    close_window = position(env, "controlKeyPreviousByNode.sim-plate-cam-01= ") if any("sim-plate-cam-01= " in c for c in calls(env, "helm")) else position(env, "controlKeyPreviousByNode.sim-plate-cam-01=--")
    finish = position(env, "provision finish-rotation")
    assert rotate < open_window < close_window < finish
    # the old master is tested for refusal AFTER the window is closed and BEFORE its Secret is deleted
    pods = [l for l in env["log"].read_text().splitlines() if l.startswith("kubectl apply")]
    assert len(pods) >= 6, "fewer operator checks than the five of the procedure and the clear"


def test_if_the_old_master_is_still_accepted_after_the_window_the_old_keys_are_not_deleted(env):
    result, out = run(env, OLD_STILL_ACCEPTED="1")
    assert result.returncode == 1 and "STILL ACCEPTS THE OLD MASTER" in out
    assert calls(env, "provision finish-rotation") == [], "the old keys were deleted although the old master still worked"


def test_if_the_node_does_not_accept_the_new_master_in_the_window_it_is_not_closed(env):
    result, out = run(env, NEW_REJECTED="1")
    assert result.returncode == 1 and "does not accept the NEW master" in out
    assert len(calls(env, "helm upgrade")) == 1 and calls(env, "provision finish-rotation") == []


def test_if_the_node_does_not_accept_the_old_master_in_the_window_it_is_not_closed(env):
    result, out = run(env, OLD_REJECTED_IN_WINDOW="1")
    assert result.returncode == 1 and "does not accept the OLD master during the window" in out
    assert len(calls(env, "helm upgrade")) == 1 and calls(env, "provision finish-rotation") == []


def test_it_will_not_start_from_a_state_that_is_already_broken(env):
    result, out = run(env, CURRENT_BROKEN="1")
    assert result.returncode == 1 and "CURRENT master does not work" in out
    assert calls(env, "provision") == [] and calls(env, "helm") == [], "it changed something before it knew the current master worked"


def test_it_will_not_start_a_second_rotation_on_top_of_an_unfinished_one(env):
    (env["state"] / "secrets" / "edge-control-master-previous").write_text("")
    result, out = run(env)
    assert result.returncode == 1 and "already in progress" in out
    assert calls(env, "provision") == [] and calls(env, "helm") == []


@pytest.mark.parametrize("missing", ["edge-control-master", "edge-control-sim-plate-cam-01", "mqtt-edge-operator", "mosquitto-tls"])
def test_it_will_not_start_when_a_secret_it_needs_is_missing(env, missing):
    (env["state"] / "secrets" / missing).unlink()
    result, out = run(env)
    assert result.returncode == 1 and missing in out
    assert calls(env, "provision") == [] and calls(env, "helm") == []


def test_it_asks_first_and_changes_nothing_without_an_answer(env):
    result, out = run(env, ROTATE_CONFIRM="")
    assert result.returncode == 2 and "ROTATE_CONFIRM=yes" in out
    assert calls(env, "provision") == [] and calls(env, "helm") == []


def test_it_waits_until_the_old_pod_is_gone_before_it_sends_a_single_command(env):
    # For the first polls after each upgrade there are two pods (the old one still terminating). A command sent then is answered by
    # whichever is connected, so no operator pod may be created until there is one.
    result, out = run(env, POD_OVERLAP_POLLS="3")
    assert result.returncode == 0, out
    lines = env["log"].read_text().splitlines()
    upgrades = [i for i, l in enumerate(lines) if l.startswith("helm upgrade")]
    for upgrade in upgrades:
        after = lines[upgrade:]
        next_apply = next(i for i, l in enumerate(after) if l.startswith("kubectl apply"))
        polls = [l for l in after[:next_apply] if l.startswith("kubectl get pods")]
        assert len(polls) >= 4, f"an operator pod was created after only {len(polls)} look(s) at the node's pods"


def test_no_credential_is_read_printed_or_put_on_a_command_line(env):
    result, out = run(env)
    assert not (env["log"].parent / "calls.log.leak").exists(), "the script read a Secret's value"
    for manifest in env["state"].glob("pod-*"):
        text = manifest.read_text()
        assert "secretKeyRef" in text and not re.search(r"(PASSWORD|MASTER_KEY)\s*,\s*value:", text), "a credential is a literal in a pod"
    assert "--from-literal" not in env["log"].read_text()


def test_it_is_executable_parses_and_kills_nothing_by_name():
    text = SCRIPT.read_text()
    assert os.access(SCRIPT, os.X_OK) and subprocess.run(["bash", "-n", str(SCRIPT)], capture_output=True).returncode == 0
    assert "pkill" not in text and "killall" not in text and re.search(r"^set -Eeuo pipefail", text, flags=re.M)
