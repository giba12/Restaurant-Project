"""
k8s/mosquitto/provision-mqtt-auth.sh, run against stand-ins for kubectl and the container engine that record every call.

The script handles every credential the broker's login needs, and it must be wrong in none of these ways: it must create the
right Secrets, keep ones that exist (so a re-run never changes a password), derive each node's key exactly as the Python tool
does, rotate a key without losing the old one, and never put a password or a key on a command line, where anyone on the
machine can read it. None of that needs a cluster, so none of it needs to wait for one.

    pip install pytest
    python -m pytest tests/static/test_mqtt_auth_provisioning.py -v
"""
import hmac
import hashlib
import os
import re
import shutil
import subprocess
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helpers import ROOT  # noqa: E402

SCRIPT = ROOT / "k8s" / "mosquitto" / "provision-mqtt-auth.sh"
USERS = ["sim-plate-cam-01", "sim-pos-01", "sim-ticket-timer-01", "sim-staffing-sensor-01", "rp-mqtt-kafka-bridge", "edge-operator"]

pytestmark = pytest.mark.skipif(shutil.which("bash") is None or shutil.which("openssl") is None or shutil.which("python3") is None,
                                reason="bash, openssl and python3 are needed")

KUBECTL = r'''#!/usr/bin/env bash
# A stand-in kubectl: secrets are files in $STATE; every call's arguments are appended to $LOG.
echo "kubectl $*" >> "$LOG"
case "$1 $2" in
  "get secret")
    name="$3"
    key=""
    for a in "$@"; do case "$a" in jsonpath=*|"jsonpath={.data."*) key="${a#*data.}"; key="${key%\}}" ;; esac; done
    if [ -n "$key" ]; then base64 < "$STATE/$name/$key" | tr -d '\n'; exit 0; fi
    [ -d "$STATE/$name" ] && exit 0 || exit 1 ;;
  "create secret")
    name="$4"; dry=0; files=()
    for a in "$@"; do case "$a" in --from-file=*) files+=("${a#--from-file=}") ;; --dry-run=client) dry=1 ;; esac; done
    target="$STATE/$name"; [ "$dry" = 1 ] && target="$STATE/.pending"
    if [ "$dry" = 0 ] && [ -d "$target" ]; then echo "AlreadyExists: $name" >&2; exit 1; fi
    rm -rf "$target"; mkdir -p "$target"
    for f in "${files[@]}"; do k="${f%%=*}"; p="${f#*=}"; cp "$p" "$target/$k"; done
    [ "$dry" = 1 ] && echo "pending $name" > "$STATE/.pending/.name" && echo "kind: Secret"
    exit 0 ;;
  "delete secret")
    rm -rf "$STATE/$3"; exit 0 ;;
  "apply -f")
    cat > /dev/null
    name="$(cat "$STATE/.pending/.name")"; rm -f "$STATE/.pending/.name"
    rm -rf "$STATE/${name#pending }"; mv "$STATE/.pending" "$STATE/${name#pending }"; exit 0 ;;
esac
echo "stub kubectl: unexpected call: $*" >&2; exit 3
'''

ENGINE = r'''#!/usr/bin/env bash
# A stand-in container engine: records its arguments, reads "user password" lines, writes a fake hashed password file.
echo "engine $*" >> "$LOG"
dir=""
while [ "$#" -gt 0 ]; do [ "$1" = "-v" ] && { dir="${2%%:*}"; }; shift; done
: > "$dir/passwd"
while read -r user password; do echo "$user:\$7\$fake\$$(printf '%s' "$password" | sha256sum | cut -c1-16)" >> "$dir/passwd"; done
'''


@pytest.fixture
def env(tmp_path):
    bin_dir, state = tmp_path / "bin", tmp_path / "state"
    bin_dir.mkdir()
    state.mkdir()
    (bin_dir / "kubectl").write_text(KUBECTL)
    (bin_dir / "engine").write_text(ENGINE)
    for stub in ("kubectl", "engine"):
        (bin_dir / stub).chmod(0o755)
    log = tmp_path / "calls.log"
    log.write_text("")
    environment = {**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}", "STATE": str(state), "LOG": str(log),
                   "CONTAINER_ENGINE": str(bin_dir / "engine"), "NS": "testns"}
    return {"env": environment, "state": state, "log": log}


def run(env, *args):
    result = subprocess.run(["bash", str(SCRIPT), *args], env=env["env"], capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, result.stdout + result.stderr
    return result


def secret(env, name, key):
    return (env["state"] / name / key).read_text()


def test_a_first_run_creates_every_secret_the_login_needs(env):
    run(env)
    names = {p.name for p in env["state"].iterdir()}
    expected = {f"mqtt-{u}" for u in USERS} | {"mosquitto-auth", "edge-control-master", "edge-control-sim-plate-cam-01",
                                               "edge-control-sim-plate-cam-01-generation"}
    assert expected <= names, f"missing: {sorted(expected - names)}"
    for user in USERS:
        assert secret(env, f"mqtt-{user}", "username") == user and len(secret(env, f"mqtt-{user}", "password")) == 48


def test_every_user_has_its_own_password(env):
    run(env)
    passwords = [secret(env, f"mqtt-{u}", "password") for u in USERS]
    assert len(set(passwords)) == len(USERS)


def test_the_brokers_password_file_holds_every_user_built_from_those_secrets(env):
    run(env)
    lines = secret(env, "mosquitto-auth", "passwd").splitlines()
    assert sorted(line.split(":")[0] for line in lines) == sorted(USERS)
    for line in lines:
        user = line.split(":")[0]
        expected = hashlib.sha256(secret(env, f"mqtt-{user}", "password").encode()).hexdigest()[:16]
        assert line.endswith(expected), f"{user}'s entry was not built from its own password"


def test_a_node_key_is_the_one_the_python_tool_derives_from_the_master(env):
    run(env)
    master = secret(env, "edge-control-master", "key")
    expected = hmac.new(master.encode(), b"edge-control/v1/sim-plate-cam-01/1", hashlib.sha256).hexdigest()
    assert secret(env, "edge-control-sim-plate-cam-01", "key") == expected


def test_the_shell_derivation_matches_the_known_answer_the_python_tests_also_pin(tmp_path):
    # The same literal is asserted against control.edge_control.derive_node_key in edge-simulators/test_edge_update.py.
    snippet = ('import hashlib, hmac, os, sys; print(hmac.new(os.environ["MASTER"].encode(), sys.argv[1].encode(), hashlib.sha256).hexdigest())')
    for generation, expected in ((1, "a148734f7efb5158df9f19510920ff38bcf07570c36c29324963375b1fd757fa"),
                                 (2, "d5c51e025da156674e36620045d53ec41080b955abf1335d0bf378816d32f5b4")):
        out = subprocess.run(["python3", "-c", snippet, f"edge-control/v1/known-node/{generation}"], env={**os.environ, "MASTER": "known-master"},
                             capture_output=True, text=True).stdout.strip()
        assert out == expected
    # And the script's own `derive` function is that snippet: read it back out of the script.
    assert snippet.split("print(")[1] in SCRIPT.read_text()


def test_a_second_run_keeps_every_existing_credential(env):
    run(env)
    before = {p.name: {f.name: f.read_text() for f in p.iterdir()} for p in env["state"].iterdir() if p.name != "mosquitto-auth"}
    again = run(env)
    after = {p.name: {f.name: f.read_text() for f in p.iterdir()} for p in env["state"].iterdir() if p.name != "mosquitto-auth"}
    assert before == after, "a re-run changed a credential"
    assert "created secret" not in again.stdout


def test_no_password_or_key_ever_appears_in_a_commands_arguments(env):
    run(env)
    secrets = []
    for path in env["state"].rglob("*"):
        if path.is_file() and path.name in ("password", "key", "passwd"):
            secrets += [value for value in re.split(r"\s+", path.read_text()) if len(value) >= 16]
    assert secrets, "no secrets were made, so this shows nothing"
    calls = env["log"].read_text()
    leaked = [s for s in secrets if s in calls]
    assert not leaked, "a credential was passed on a command line"


def test_the_script_never_prints_a_credential(env):
    out = run(env)
    for path in env["state"].rglob("*"):
        if path.is_file() and path.name in ("password", "key"):
            assert path.read_text() not in out.stdout + out.stderr


def test_a_dry_run_changes_nothing(env):
    out = run(env, "--dry-run")
    assert list(env["state"].iterdir()) == []
    assert "would create secret mqtt-sim-pos-01" in out.stdout and "would build mosquitto-auth" in out.stdout


def test_rotating_a_node_moves_its_key_to_previous_and_writes_the_next_generation(env):
    run(env)
    master = secret(env, "edge-control-master", "key")
    first = secret(env, "edge-control-sim-plate-cam-01", "key")
    run(env, "rotate-node", "sim-plate-cam-01")
    assert secret(env, "edge-control-sim-plate-cam-01-previous", "key") == first
    new = secret(env, "edge-control-sim-plate-cam-01", "key")
    assert new == hmac.new(master.encode(), b"edge-control/v1/sim-plate-cam-01/2", hashlib.sha256).hexdigest() and new != first
    assert secret(env, "edge-control-sim-plate-cam-01-generation", "generation") == "2"
    run(env, "rotate-node", "sim-plate-cam-01")  # and again
    assert secret(env, "edge-control-sim-plate-cam-01-generation", "generation") == "3"
    assert secret(env, "edge-control-sim-plate-cam-01-previous", "key") == new


def test_rotating_before_anything_is_provisioned_is_refused_with_a_reason(env):
    result = subprocess.run(["bash", str(SCRIPT), "rotate-node", "sim-plate-cam-01"], env=env["env"], capture_output=True, text=True)
    assert result.returncode != 0 and "run this script without arguments first" in result.stderr


def test_the_users_it_provisions_are_exactly_the_users_the_broker_acl_names():
    acl = (ROOT / "docker-compose" / "mosquitto" / "acl").read_text()
    in_acl = re.findall(r"^user (\S+)$", acl, flags=re.M)
    in_script = re.search(r"^USERS=\(([^)]*)\)", SCRIPT.read_text(), flags=re.M).group(1).split()
    entrypoint = (ROOT / "docker-compose" / "mosquitto" / "mosquitto-auth-entrypoint.sh").read_text()
    in_entrypoint = re.search(r"for user in ([^;]+); do", entrypoint).group(1).split()
    assert sorted(in_acl) == sorted(in_script) == sorted(in_entrypoint) == sorted(USERS)


def test_it_is_executable_and_parses():
    assert os.access(SCRIPT, os.X_OK)
    assert subprocess.run(["bash", "-n", str(SCRIPT)], capture_output=True).returncode == 0


def snapshot(env):
    return {p.name: {f.name: f.read_text() for f in p.iterdir()} for p in env["state"].iterdir()}


def derived(master, node, generation=1):
    return hmac.new(master.encode(), f"edge-control/v1/{node}/{generation}".encode(), hashlib.sha256).hexdigest()


def test_rotating_the_master_keeps_the_old_master_and_every_old_node_key_and_derives_the_new_keys_from_the_new_master(env):
    run(env)
    old_master, old_key = secret(env, "edge-control-master", "key"), secret(env, "edge-control-sim-plate-cam-01", "key")
    run(env, "rotate-master")
    new_master = secret(env, "edge-control-master", "key")
    assert new_master != old_master and len(new_master) == 64
    assert secret(env, "edge-control-master-previous", "key") == old_master
    assert secret(env, "edge-control-sim-plate-cam-01-previous", "key") == old_key
    new_key = secret(env, "edge-control-sim-plate-cam-01", "key")
    assert new_key == derived(new_master, "sim-plate-cam-01") and new_key != old_key
    # the generation does not move: the operator needs no flag, and the node's key differs because the master does
    assert secret(env, "edge-control-sim-plate-cam-01-generation", "generation") == "1"


def test_rotating_the_master_leaves_the_brokers_logins_alone(env):
    run(env)
    before = snapshot(env)
    run(env, "rotate-master")
    after = snapshot(env)
    for name in [f"mqtt-{u}" for u in USERS] + ["mosquitto-auth"]:
        assert before[name] == after[name], f"{name} changed: a master rotation is about control keys, not broker logins"


def test_a_second_master_rotation_is_refused_until_the_first_is_finished(env):
    run(env)
    run(env, "rotate-master")
    before = snapshot(env)
    result = subprocess.run(["bash", str(SCRIPT), "rotate-master"], env=env["env"], capture_output=True, text=True)
    assert result.returncode != 0 and "finish-rotation" in result.stderr
    assert snapshot(env) == before, "the refused rotation still changed something"


def test_finishing_a_rotation_deletes_every_old_key_and_keeps_the_new_ones(env):
    run(env)
    run(env, "rotate-master")
    new = {n: snapshot(env)[n] for n in ("edge-control-master", "edge-control-sim-plate-cam-01", "edge-control-sim-plate-cam-01-generation")}
    run(env, "finish-rotation")
    after = snapshot(env)
    assert "edge-control-master-previous" not in after and "edge-control-sim-plate-cam-01-previous" not in after
    assert {n: after[n] for n in new} == new
    run(env, "rotate-master")  # and a new rotation may begin
    assert "edge-control-master-previous" in snapshot(env)


def test_finishing_when_no_rotation_is_in_progress_changes_nothing(env):
    run(env)
    before = snapshot(env)
    run(env, "finish-rotation")
    assert snapshot(env) == before


def test_a_dry_run_of_a_master_rotation_changes_nothing(env):
    run(env)
    before = snapshot(env)
    out = run(env, "rotate-master", "--dry-run")
    assert snapshot(env) == before and "would replace secret edge-control-master" in out.stdout


def test_the_master_rotation_never_puts_a_key_on_a_command_line_or_prints_one(env):
    run(env)
    out = run(env, "rotate-master")
    secrets = [p.read_text() for p in env["state"].rglob("*") if p.is_file() and p.name == "key"]
    assert len(secrets) >= 4
    calls = env["log"].read_text()
    assert not [x for x in secrets if x in calls], "a key was passed on a command line"
    assert not [x for x in secrets if x in out.stdout + out.stderr], "a key was printed"
