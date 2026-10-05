"""
Static checks that every simulator's ingest gate is wired to something real (DEF-148).

A simulator holds its events until the Kafka Connect connector that carries them
is running (edge-simulators/common/ingest_gate.py). That is only safe if the
gate asks about a connector that exists: a wrong name would leave the gate shut
for ever and the sensor publishing nothing, with every process healthy. These
checks make that kind of slip fail in CI instead of in production.

    pip install pyyaml pytest
    python -m pytest tests/static/test_ingest_gate_wiring.py -v
"""
import json
import os
import re
import subprocess
import sys
from urllib.parse import urlparse

import pytest
import yaml

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helpers import COMPOSE_FILE, ROOT, have  # noqa: E402

SERVICES = yaml.safe_load(COMPOSE_FILE.read_text())["services"]
SIMULATOR_SERVICES = {name: svc for name, svc in SERVICES.items() if name.startswith("edge-sim-")}
SENSOR_TYPES = {"plate-waste", "pos-transaction", "service-timing", "staff-shift"}


def _connector_names_in_compose():
    return {json.loads(p.read_text())["name"] for p in (ROOT / "docker-compose" / "kafka-connect" / "connectors").glob("*.json")}


def _connector_names_in_chart():
    return {yaml.safe_load(p.read_text())["metadata"]["name"] for p in (ROOT / "k8s" / "kafka-connect-mqtt" / "connectors").glob("*.yaml")}


def test_the_four_simulators_and_their_sensor_types_were_found():
    assert {svc["environment"]["SENSOR_TYPE"] for svc in SIMULATOR_SERVICES.values()} == SENSOR_TYPES


def test_every_sensor_type_has_a_connector_of_the_name_its_gate_asks_about_in_both_deployments():
    # The gate asks about f"{sensor_type}-source-connector" unless INGEST_GATE_CONNECTOR says otherwise.
    for name, svc in SIMULATOR_SERVICES.items():
        env = svc["environment"]
        asked = env.get("INGEST_GATE_CONNECTOR", f"{env['SENSOR_TYPE']}-source-connector")
        assert asked in _connector_names_in_compose(), f"{name}: no Compose connector named {asked}"
    for sensor in SENSOR_TYPES:
        assert f"{sensor}-source-connector" in _connector_names_in_chart(), f"no Kubernetes connector named {sensor}-source-connector"


def test_every_compose_simulator_gates_on_the_connect_rest_api():
    connect = SERVICES["kafka-connect"]
    assert connect is not None
    for name, svc in SIMULATOR_SERVICES.items():
        url = svc["environment"].get("INGEST_GATE_URL")
        assert url, f"{name} has no INGEST_GATE_URL: it would publish into a bridge that may not be there"
        parsed = urlparse(url)
        assert parsed.hostname == "kafka-connect" and parsed.port == 8083, f"{name}: {url} is not the Connect REST API"


def test_every_compose_simulator_has_a_kafka_address_for_its_gate_to_watch():
    # The gate also watches Kafka (Connect's REST status lies while Kafka is down). It reads the address
    # from KAFKA_BOOTSTRAP_SERVERS; without one it would silently watch nothing.
    for name, svc in SIMULATOR_SERVICES.items():
        assert re.fullmatch(r"[\w.-]+:\d+", svc["environment"].get("KAFKA_BOOTSTRAP_SERVERS", "")), name


@pytest.mark.skipif(not have("helm"), reason="helm not installed")
def test_the_simulator_chart_gives_every_gated_simulator_a_kafka_address_to_watch():
    result = subprocess.run(["helm", "template", "t", str(ROOT / "k8s" / "edge-simulators"), "-n", "kafka"], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    deployments = [d for d in yaml.safe_load_all(result.stdout) if d and d["kind"] == "Deployment"]
    assert len(deployments) == len(SENSOR_TYPES)
    for deployment in deployments:
        env = {e["name"]: e.get("value") for e in deployment["spec"]["template"]["spec"]["containers"][0]["env"]}
        assert re.fullmatch(r"[\w.-]+:\d+", env.get("KAFKA_BOOTSTRAP_SERVERS", "")), deployment["metadata"]["name"]


@pytest.mark.skipif(not have("helm"), reason="helm not installed")
def test_the_simulator_chart_gates_on_the_connect_api_and_lets_the_simulators_reach_it():
    result = subprocess.run(["helm", "template", "t", str(ROOT / "k8s" / "edge-simulators"), "-n", "kafka"], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    docs = [d for d in yaml.safe_load_all(result.stdout) if d]

    deployments = [d for d in docs if d["kind"] == "Deployment"]
    assert len(deployments) == len(SENSOR_TYPES)
    for deployment in deployments:
        env = {e["name"]: e.get("value") for e in deployment["spec"]["template"]["spec"]["containers"][0]["env"]}
        assert env.get("INGEST_GATE_URL") == "http://connect-cluster-connect-api.kafka.svc.cluster.local:8083", deployment["metadata"]["name"]
        assert float(env["INGEST_GATE_SETTLE_SECONDS"]) >= 10

    # Strimzi's own NetworkPolicy admits only Connect pods and the operator to 8083; this adds the simulators.
    policies = [d for d in docs if d["kind"] == "NetworkPolicy"]
    assert len(policies) == 1
    spec = policies[0]["spec"]
    assert spec["podSelector"]["matchLabels"]["strimzi.io/kind"] == "KafkaConnect"
    (rule,) = spec["ingress"]
    assert rule["from"] == [{"podSelector": {"matchLabels": {"component": "edge-simulator"}}}]
    assert [p["port"] for p in rule["ports"]] == [8083]
    assert all(d["spec"]["template"]["metadata"]["labels"]["component"] == "edge-simulator" for d in deployments)


@pytest.mark.skipif(not have("helm"), reason="helm not installed")
def test_turning_the_gate_off_in_the_chart_removes_both_the_variable_and_the_policy():
    result = subprocess.run(["helm", "template", "t", str(ROOT / "k8s" / "edge-simulators"), "-n", "kafka", "--set", "ingestGate.enabled=false"],
                            capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert "INGEST_GATE_URL" not in result.stdout and "kind: NetworkPolicy" not in result.stdout


REST_LOGGER = "org.apache.kafka.connect.runtime.rest.RestServer"


def test_polling_the_connect_api_does_not_flood_the_connect_log_in_the_compose_image():
    # Connect logs every REST request at INFO, and the gates make one a second per simulator.
    dockerfile = (ROOT / "docker-compose" / "kafka-connect" / "Dockerfile").read_text()
    assert REST_LOGGER in dockerfile and "level: WARN" in dockerfile


@pytest.mark.skipif(not have("helm"), reason="helm not installed")
def test_polling_the_connect_api_does_not_flood_the_connect_log_on_kubernetes():
    result = subprocess.run(["helm", "template", "t", str(ROOT / "k8s" / "kafka-connect-mqtt"), "-n", "kafka"], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    (connect,) = [d for d in yaml.safe_load_all(result.stdout) if d and d["kind"] == "KafkaConnect"]
    logging = connect["spec"]["logging"]
    assert logging["type"] == "inline"
    loggers = logging["loggers"]
    (ident,) = [key[len("logger."):-len(".name")] for key, value in loggers.items() if value == REST_LOGGER]
    assert loggers[f"logger.{ident}.level"] == "WARN"
