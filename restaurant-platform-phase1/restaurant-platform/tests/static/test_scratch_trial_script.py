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


@pytest.mark.parametrize("namespace", ["kafka", "default", "kube-system", "kube-public", "restaurant-platform"])
def test_it_refuses_to_run_in_a_namespace_that_holds_anything_real(namespace, tmp_path):
    # `kubectl` here would fail the test loudly if the script got as far as calling it.
    stub = tmp_path / "kubectl"
    stub.write_text("#!/usr/bin/env bash\necho CALLED >> \"$LOG\"\nexit 1\n")
    stub.chmod(0o755)
    log = tmp_path / "log"
    result = subprocess.run(["bash", str(SCRIPT)], env={**os.environ, "NS": namespace, "PATH": f"{tmp_path}:{os.environ['PATH']}", "LOG": str(log)},
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
