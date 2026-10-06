"""
Static checks that the MQTT-Kafka bridge is deployed the way its guarantee needs (DEF-152).

The bridge's guarantee (no sensor event lost to a restart of the bridge, Kafka or Mosquitto) rests on
settings spread over several files: a persistent MQTT session in the bridge's code, Mosquitto persisting
that session to a volume, one bridge at a time, and a restart that loses nothing. Each is easy to
change by accident and invisible until a restart happens, so each is checked here, nothing runs.

    pip install pyyaml pytest
    python -m pytest tests/static/test_bridge_wiring.py -v
"""
import os
import re
import subprocess
import sys

import pytest
import yaml

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helpers import ROOT, have  # noqa: E402

needs_helm = pytest.mark.skipif(not have("helm"), reason="helm not installed")


def render(chart):
    result = subprocess.run(["helm", "template", "t", str(ROOT / "k8s" / chart), "-n", "kafka"], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    return [doc for doc in yaml.safe_load_all(result.stdout) if doc]


def one(docs, kind, name=None):
    found = [d for d in docs if d["kind"] == kind and (name is None or d["metadata"]["name"] == name)]
    assert len(found) == 1, f"expected one {kind} {name or ''}, found {len(found)}"
    return found[0]


@needs_helm
def test_the_bridge_runs_as_one_replica_with_a_recreate_rollout():
    # Its MQTT client id names its persistent session: a second copy during a rolling update would take the
    # session over from the first, and the two would keep taking it from each other.
    deployment = one(render("mqtt-kafka-bridge"), "Deployment")
    assert deployment["spec"]["replicas"] == 1
    assert deployment["spec"]["strategy"]["type"] == "Recreate"


@needs_helm
def test_the_bridge_is_restarted_by_a_probe_when_it_is_up_but_not_connected():
    container = one(render("mqtt-kafka-bridge"), "Deployment")["spec"]["template"]["spec"]["containers"][0]
    for probe in ("readinessProbe", "livenessProbe"):
        assert container[probe]["exec"]["command"] == ["python", "bridge.py", "--check"], probe


@needs_helm
def test_the_bridge_reaches_both_brokers_over_tls_with_their_cas_mounted():
    spec = one(render("mqtt-kafka-bridge"), "Deployment")["spec"]["template"]["spec"]
    env = {e["name"]: e["value"] for e in spec["containers"][0]["env"]}
    assert env["MQTT_TLS_ENABLED"] == "true" and env["MQTT_PORT"] == "8883"
    assert env["KAFKA_SECURITY_PROTOCOL"] == "SSL" and env["KAFKA_BOOTSTRAP_SERVERS"].endswith(":9093")
    mounts = {m["mountPath"] for m in spec["containers"][0]["volumeMounts"]}
    assert {"/etc/kafka-tls", "/etc/mosquitto-tls"} <= mounts
    secrets = {v["secret"]["secretName"] for v in spec["volumes"]}
    assert secrets == {"restaurant-platform-kafka-cluster-ca-cert", "mosquitto-tls"}


@needs_helm
def test_mosquitto_on_kubernetes_persists_the_bridges_session_on_a_volume_and_never_runs_two_brokers():
    docs = render("mosquitto")
    conf = one(docs, "ConfigMap", "mosquitto-config")["data"]["mosquitto.conf"]
    assert re.search(r"^\s*persistence true\s*$", conf, flags=re.M)
    assert re.search(r"^\s*persistence_location /mosquitto/data/\s*$", conf, flags=re.M)
    assert int(re.search(r"^\s*max_queued_messages (\d+)\s*$", conf, flags=re.M).group(1)) >= 100_000

    deployment = one(docs, "Deployment", "mosquitto")
    # The old broker must stop before the new one starts (a ReadWriteOnce volume, one session store). Done with
    # maxSurge 0 / maxUnavailable 1 inside the existing RollingUpdate strategy, NOT by switching the type to
    # Recreate: on the live cluster that switch failed `helm upgrade` ("spec.strategy.rollingUpdate: Forbidden: may
    # not be specified when strategy type is Recreate") although a server-side dry run had passed.
    strategy = deployment["spec"]["strategy"]
    assert strategy["type"] == "RollingUpdate", "changing an existing Deployment's strategy type broke the live upgrade"
    assert str(strategy["rollingUpdate"]["maxSurge"]) == "0" and str(strategy["rollingUpdate"]["maxUnavailable"]) == "1"
    pod = deployment["spec"]["template"]["spec"]
    assert any(m["mountPath"] == "/mosquitto/data" for m in pod["containers"][0]["volumeMounts"])
    assert {"name": "data", "persistentVolumeClaim": {"claimName": "mosquitto-data"}} in pod["volumes"]
    assert one(docs, "PersistentVolumeClaim", "mosquitto-data")["spec"]["accessModes"] == ["ReadWriteOnce"]


def test_the_compose_and_kubernetes_mosquitto_configs_agree_on_persistence():
    compose = (ROOT / "docker-compose" / "mosquitto" / "mosquitto.conf").read_text()
    chart = (ROOT / "k8s" / "mosquitto" / "templates" / "configmap.yaml").read_text()
    for setting in ("persistence true", "persistence_location /mosquitto/data/", "autosave_interval 5", "max_queued_bytes 0"):
        assert re.search(rf"^\s*{re.escape(setting)}\s*$", compose, flags=re.M), f"Compose lacks {setting}"
        assert re.search(rf"^\s*{re.escape(setting)}\s*$", chart, flags=re.M), f"the chart lacks {setting}"


@needs_helm
def test_prometheus_scrapes_the_bridge_and_alerts_on_its_three_ways_of_not_forwarding():
    docs = render("observability")
    config = yaml.safe_load(one(docs, "ConfigMap", "prometheus-config")["data"]["prometheus.yml"])
    job = next(j for j in config["scrape_configs"] if j["job_name"] == "pipeline-health")
    keep = next(r for r in job["relabel_configs"] if r.get("action") == "keep")
    assert "mqtt-kafka-bridge" in keep["regex"]

    rules = yaml.safe_load(one(docs, "ConfigMap", "prometheus-rules")["data"]["alerts.yml"])
    alerts = {r["alert"] for g in rules["groups"] if g["name"] == "bridge" for r in g["rules"]}
    assert alerts == {"BridgeNotConnectedToMqtt", "BridgeKafkaErrors", "BridgeWaitingOnKafka"}

    kafka_jmx = next(j for j in config["scrape_configs"] if j["job_name"] == "kafka-jmx")
    assert "connect" not in str(kafka_jmx), "the retired Kafka Connect worker is still being scraped"


def test_kafka_connect_is_gone_from_both_deployments():
    # It was replaced, not supplemented: running both would deliver every message twice for ever.
    assert not (ROOT / "k8s" / "kafka-connect-mqtt").exists()
    assert not (ROOT / "docker-compose" / "kafka-connect").exists()
    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text())["services"]
    assert not [name for name in compose if "connect" in name], "a Kafka Connect service is still in docker-compose.yml"


def test_the_realign_script_tells_you_how_to_recover_when_a_step_fails_part_way():
    # It scales every database client to zero first. When `helm upgrade mosquitto` failed, the script stopped and
    # the clients stayed at zero with no message, because bash does not run an ERR trap inside a function (the
    # `run` helper) unless `set -E` is on. The hint existed and never printed.
    script = (ROOT / "k8s" / "realign" / "realign-live-cluster.sh").read_text()
    assert re.search(r"^set -[A-Za-z]*E[A-Za-z]* *\n?", script, flags=re.M) or "set -Eeuo pipefail" in script
    assert "trap restore_hint ERR" in script
    assert "kubectl scale deploy/$d -n $NS --replicas=1" in script


def test_the_realign_script_installs_the_bridge_before_it_retires_connect_and_imports_its_image_first():
    script = (ROOT / "k8s" / "realign" / "realign-live-cluster.sh").read_text()
    build = script.index("local/mqtt-kafka-bridge:1.0 -f services/mqtt-kafka-bridge/Dockerfile")
    persist = script.index("helm upgrade mosquitto k8s/mosquitto")
    imported = script.index("for img in storage-consumer")
    install = script.index("helm upgrade --install mqtt-kafka-bridge")
    retire = script.index("helm uninstall kafka-connect-mqtt")
    # Mosquitto's volume must exist before the bridge subscribes, the image must be in k3s before the pod is
    # created, and Connect may only go once the bridge is carrying messages (the other order leaves a gap).
    assert build < imported and persist < install and imported < install < retire
    assert "mqtt-kafka-bridge" in script[imported:script.index("\n", imported)], "the bridge image is not in the k3s import list"
