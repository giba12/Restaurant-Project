"""
k8s/audit/test-broker-in-scratch-namespace.sh: the guard rails of a script that creates and deletes a whole namespace.

What it proves about the broker needs a cluster. What must be true wherever it runs is that it can never be pointed at a namespace
that holds anything real, that it always cleans up after itself, that it reads no real credential, and that it keeps its
credentials off command lines. Those are checked here.

    pip install pytest
    python -m pytest tests/static/test_scratch_trial_script.py -v
"""
import os
import re
import subprocess
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helpers import ROOT  # noqa: E402

SCRIPT = ROOT / "k8s" / "audit" / "test-broker-in-scratch-namespace.sh"
ROTATION = ROOT / "k8s" / "audit" / "test-rotation-in-scratch-namespace.sh"
SCRIPTS = [SCRIPT, ROTATION]


@pytest.mark.parametrize("script", SCRIPTS, ids=lambda p: p.name)
@pytest.mark.parametrize("namespace", ["kafka", "default", "kube-system", "kube-public", "restaurant-platform"])
def test_it_refuses_to_run_in_a_namespace_that_holds_anything_real(namespace, script, tmp_path):
    # `kubectl` here would fail the test loudly if the script got as far as calling it.
    stub = tmp_path / "kubectl"
    stub.write_text("#!/usr/bin/env bash\necho CALLED >> \"$LOG\"\nexit 1\n")
    stub.chmod(0o755)
    log = tmp_path / "log"
    result = subprocess.run(["bash", str(script)], env={**os.environ, "NS": namespace, "PATH": f"{tmp_path}:{os.environ['PATH']}", "LOG": str(log)},
                            capture_output=True, text=True, timeout=30)
    assert result.returncode == 2 and "refusing" in result.stderr
    assert not log.exists(), "it called kubectl before refusing"


def test_it_deletes_its_namespace_on_every_exit_and_only_its_own():
    text = SCRIPT.read_text()
    assert re.search(r"^trap cleanup EXIT$", text, flags=re.M)
    assert 'kubectl delete namespace "$NS"' in text
    deletes = re.findall(r"kubectl delete (\w+)", text)
    assert set(deletes) <= {"namespace", "pod", "pvc"}, f"it deletes something else: {deletes}"
    assert all("-n \"$NS\"" in line or "namespace" in line for line in text.splitlines() if "kubectl delete" in line and not line.lstrip().startswith("#")), \
        "a delete is not scoped to the scratch namespace"


def test_every_namespaced_command_is_scoped_and_it_reads_no_real_secret():
    text = SCRIPT.read_text()
    assert "-n kafka" not in text and "namespace kafka" not in text
    for line in text.splitlines():
        if line.lstrip().startswith("#"):
            continue
        assert not re.search(r"get secret .*(mqtt-edge-operator|edge-control-master)", line), line
    # the credentials it uses were made in the scratch namespace by the provisioning script, and the pods get them from their Secret
    assert 'NS="$NS" bash "$ROOT/k8s/mosquitto/provision-mqtt-auth.sh"' in text and "secretKeyRef" in text


def test_no_password_is_put_on_a_host_command_line():
    text = SCRIPT.read_text()
    # the password is expanded inside the pod ($MQTT_PW), never interpolated into a kubectl or docker argument here
    assert "-P \"$MQTT_PW\"" in text
    assert not re.search(r"kubectl [^\n]*(--from-literal|password=)", text)


def test_it_is_executable_and_parses():
    assert os.access(SCRIPT, os.X_OK)
    assert subprocess.run(["bash", "-n", str(SCRIPT)], capture_output=True).returncode == 0
    assert re.search(r"^set -Eeuo pipefail", SCRIPT.read_text(), flags=re.M)


def test_the_rotation_rehearsal_keeps_the_simulators_chart_out_of_the_real_namespace_and_deletes_its_own_on_every_exit():
    text = ROTATION.read_text()
    # the chart names its namespace in every template (value `namespace`, default kafka); the harness sets it and refuses if anything still says kafka
    assert '--set "namespace=$NS"' in text and 'grep -q "namespace: kafka"' in text
    assert re.search(r"^trap cleanup EXIT$", text, flags=re.M) and 'kubectl delete namespace "$NS"' in text
    assert 'HELM_EXTRA_ARGS="--set namespace=$NS"' in text, "the rotation's own chart upgrades would go to the real namespace"
    # it only ever names its own namespace: every Secret it reads or checks is one it made in the scratch namespace
    assert "-n kafka" not in text and "namespace kafka" not in text and "--namespace kafka" not in text
    assert all('-n "$NS"' in line for line in text.splitlines() if "kubectl get secret" in line or "kubectl create secret" in line), "a Secret command is not scoped to $NS"


def test_the_rotation_rehearsal_and_the_script_it_runs_are_executable_and_parse():
    for path in (ROTATION, ROOT / "k8s" / "audit" / "rotate-master-key.sh"):
        assert os.access(path, os.X_OK) and subprocess.run(["bash", "-n", str(path)], capture_output=True).returncode == 0, path.name
