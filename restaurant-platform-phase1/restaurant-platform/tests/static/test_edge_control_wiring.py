"""
Static checks that the edge control path (signed model commands from the cloud, edge_ai/updater.py) is wired
the way its safety rests on: a node takes commands only when it has a key, only the plate-waste node has one,
and the image carries the tools and the model store an operator needs. Nothing runs.

    pip install pyyaml pytest
    python -m pytest tests/static/test_edge_control_wiring.py -v
"""
import copy
import os
import re
import subprocess
import sys
import tempfile

import pytest
import yaml

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helpers import ROOT, have  # noqa: E402

needs_helm = pytest.mark.skipif(not have("helm"), reason="helm not installed")
EDGE_SERVICES = ["edge-sim-plate-waste", "edge-sim-pos-transaction", "edge-sim-service-timing", "edge-sim-staff-shift"]


def compose():
    return yaml.safe_load((ROOT / "docker-compose.yml").read_text())["services"]


def test_the_image_carries_the_control_tool_and_the_model_store():
    dockerfile = (ROOT / "edge-simulators" / "Dockerfile").read_text()
    for source, target in (("edge-simulators/control/", "/app/control/"), ("edge-simulators/model_store/", "/app/model_store/")):
        assert f"COPY {source} {target}" in dockerfile, f"the image does not copy {source}"
        assert (ROOT / source).is_dir() and any((ROOT / source).rglob("*")), f"{source} is empty or missing"
    assert (ROOT / "edge-simulators" / "control" / "__init__.py").is_file(), "`python -m control.edge_control` needs a package"


def test_only_the_plate_waste_node_is_given_a_control_key_in_compose():
    services = compose()
    with_key = [name for name in EDGE_SERVICES if "EDGE_CONTROL_KEY" in services[name]["environment"]]
    assert with_key == ["edge-sim-plate-waste"], f"the control key reached {with_key}"


def test_the_compose_key_can_be_set_from_the_environment_and_has_a_local_default():
    key = compose()["edge-sim-plate-waste"]["environment"]["EDGE_CONTROL_KEY"]
    assert re.fullmatch(r"\$\{EDGE_CONTROL_KEY:-[^}]+\}", key), key


def test_the_four_simulators_still_share_their_connection_settings():
    # The shared environment moved to a top-level extension field so the key stays on one node; nothing else may change.
    services = compose()
    for name in EDGE_SERVICES:
        env = services[name]["environment"]
        assert env["MQTT_HOST"] == "mosquitto" and env["MQTT_PORT"] == "8883" and env["MQTT_TLS_ENABLED"] == "true"
        assert env["SCENARIO_CONTROL_ENABLED"] == "true" and env["KAFKA_BOOTSTRAP_SERVERS"] == "kafka:9092"


def render(values_file=None):
    cmd = ["helm", "template", "t", str(ROOT / "k8s" / "edge-simulators"), "-n", "kafka"]
    if values_file:
        cmd += ["-f", values_file]
    result = subprocess.run(cmd, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    return [d for d in yaml.safe_load_all(result.stdout) if d and d["kind"] == "Deployment"]


@needs_helm
def test_the_chart_gives_only_the_plate_waste_node_its_own_key_from_the_secret_the_provisioning_script_makes():
    # Model control is on for the plate-waste node (2026-10-07) and for no other node: the others have no model to update.
    found = {}
    for deployment in render():
        for e in deployment["spec"]["template"]["spec"]["containers"][0]["env"]:
            if e["name"] == "EDGE_CONTROL_KEY":
                found[deployment["metadata"]["name"]] = e["valueFrom"]["secretKeyRef"]
    assert list(found) == ["edge-sim-plate-waste"], f"the key reached {sorted(found)}"
    assert found["edge-sim-plate-waste"] == {"name": "edge-control-sim-plate-cam-01", "key": "key"}
    # and it is the Secret provision-mqtt-auth.sh creates (it names a node's Secret edge-control-<node>)
    script = (ROOT / "k8s" / "mosquitto" / "provision-mqtt-auth.sh").read_text()
    assert 'NODES=(sim-plate-cam-01)' in script and '"edge-control-$node"' in script


@needs_helm
def test_naming_a_secret_gives_that_node_and_only_that_node_its_key_from_the_secret():
    values = yaml.safe_load((ROOT / "k8s" / "edge-simulators" / "values.yaml").read_text())
    sims = copy.deepcopy(values["simulators"])
    sims[0]["controlKeySecret"] = "edge-control"
    with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False) as f:
        yaml.safe_dump({"simulators": sims}, f)
    try:
        found = {}
        for deployment in render(f.name):
            for e in deployment["spec"]["template"]["spec"]["containers"][0]["env"]:
                if e["name"] == "EDGE_CONTROL_KEY":
                    found[deployment["metadata"]["name"]] = e
    finally:
        os.unlink(f.name)
    assert len(found) == 1, f"the key reached {sorted(found)}"
    (env,) = found.values()
    assert env["valueFrom"]["secretKeyRef"] == {"name": "edge-control", "key": "key"}, "the key must come from the Secret, not sit in the manifest"
    assert "value" not in env
