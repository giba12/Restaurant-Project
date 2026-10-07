"""
The SensorTopicSilent alert (k8s/observability), run through Prometheus's own rule tester (`promtool test rules`).

The alert is the only thing that notices a sensor that has stopped arriving while everything else is healthy. A rule like that is
easy to get subtly wrong in ways a read of the YAML will not show (it fires minutes early after a restart, it fires on every topic
during an outage that has its own alert, it never fires for a series that does not exist yet), so each of those is a case here,
with time-series input and the expected alerts. The rule is the one the chart renders, not a copy.

    pip install pyyaml pytest        # and docker or podman, helm, and network access for the Prometheus image
    python -m pytest tests/static/test_sensor_alert_rule.py -v
"""
import os
import subprocess
import sys

import pytest
import yaml

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helpers import ROOT, have  # noqa: E402

IMAGE = "docker.io/prom/prometheus:v2.55.1"  # the version the chart runs
ENGINE = "docker" if have("docker") else "podman"
TOPICS = ["plate-waste-events", "pos-transaction-events", "service-timing-events", "staff-shift-events"]
SUMMARY = "nothing has arrived for {} in 10m while the bridge is connected and forwarding: that sensor has stopped, or cannot publish"

pytestmark = pytest.mark.skipif(not (have("helm") and (have("docker") or have("podman"))), reason="helm and a container engine are needed")


def forwarded(topic, values):
    return {"series": f'bridge_messages_forwarded_total{{kafka_topic="{topic}"}}', "values": values}


def flowing(topic):
    return forwarded(topic, "0+10x60")


def gauge(name, values):
    return {"series": name, "values": values}


def alert_at(minutes, *silent_topics):
    return {"eval_time": f"{minutes}m", "alertname": "SensorTopicSilent",
            "exp_alerts": [{"exp_labels": {"severity": "warning", "kafka_topic": t}, "exp_annotations": {"summary": SUMMARY.format(t)}} for t in silent_topics]}


@pytest.fixture(scope="module")
def tester(tmp_path_factory):
    work = tmp_path_factory.mktemp("promtool")
    rendered = subprocess.run(["helm", "template", "t", str(ROOT / "k8s" / "observability"), "-n", "kafka"], capture_output=True, text=True)
    assert rendered.returncode == 0, rendered.stderr
    rules = next(d for d in yaml.safe_load_all(rendered.stdout) if d and d["kind"] == "ConfigMap" and d["metadata"]["name"] == "prometheus-rules")
    (work / "alerts.yml").write_text(rules["data"]["alerts.yml"])
    work.chmod(0o755)

    def run(name, input_series, alert_tests):
        spec = {"rule_files": ["alerts.yml"], "evaluation_interval": "1m",
                "tests": [{"name": name, "interval": "1m", "input_series": input_series, "alert_rule_test": alert_tests}]}
        (work / "unit.yml").write_text(yaml.safe_dump(spec))
        result = subprocess.run([ENGINE, "run", "--rm", "-v", f"{work}:/t", "--entrypoint", "promtool", IMAGE, "test", "rules", "/t/unit.yml"],
                                capture_output=True, text=True, timeout=180)
        assert result.returncode == 0, result.stdout + result.stderr
    return run


HEALTHY = [gauge("bridge_mqtt_connected", "1x60"), gauge("bridge_oldest_unconfirmed_seconds", "0x60")]


def test_a_topic_that_stops_arriving_is_reported_once_its_window_has_passed_and_only_that_topic(tester):
    silent = "pos-transaction-events"
    series = [forwarded(t, "5+0x60") if t == silent else flowing(t) for t in TOPICS] + HEALTHY
    # flat from the start: the window needs ten minutes of series, then the alert waits two more
    tester("a silent topic", series, [alert_at(9, ), alert_at(11, ), alert_at(13, silent), alert_at(40, silent)])


def test_a_topic_that_resumes_stops_being_reported(tester):
    silent = "staff-shift-events"
    resumed = "5x20 " + " ".join(f"{5 + 10 * i}" for i in range(1, 41))
    series = [forwarded(t, resumed) if t == silent else flowing(t) for t in TOPICS] + HEALTHY
    tester("a topic that resumes", series, [alert_at(15, silent), alert_at(35)])


def test_a_series_only_minutes_old_is_not_ten_silent_minutes(tester):
    # The bridge restarted: the topic's series is new, at 0. It has not been silent for ten minutes, it has been there for five.
    new = "pos-transaction-events"
    series = [forwarded(t, "_x20 0x40") if t == new else flowing(t) for t in TOPICS] + HEALTHY
    tester("a young series", series, [alert_at(25), alert_at(29), alert_at(33, new)])


def test_nothing_is_reported_while_every_topic_is_arriving(tester):
    tester("all flowing", [flowing(t) for t in TOPICS] + HEALTHY, [alert_at(m) for m in (5, 15, 30, 59)])


def test_nothing_is_reported_while_the_bridge_is_disconnected_because_that_has_its_own_alert(tester):
    series = [forwarded(t, "5+0x60") for t in TOPICS] + [gauge("bridge_mqtt_connected", "0x60"), gauge("bridge_oldest_unconfirmed_seconds", "0x60")]
    tester("bridge disconnected", series, [alert_at(m) for m in (15, 30, 59)])


def test_nothing_is_reported_while_kafka_is_holding_messages_up_because_that_has_its_own_alert(tester):
    series = [forwarded(t, "5+0x60") for t in TOPICS] + [gauge("bridge_mqtt_connected", "1x60"), gauge("bridge_oldest_unconfirmed_seconds", "120x60")]
    tester("kafka holding", series, [alert_at(m) for m in (15, 30, 59)])


def test_the_alert_is_named_in_the_bridge_alert_group_wiring_and_the_bridge_creates_each_series_at_start():
    text = (ROOT / "k8s" / "observability" / "templates" / "prometheus.yaml").read_text()
    assert "alert: SensorTopicSilent" in text
    bridge = (ROOT / "services" / "mqtt-kafka-bridge" / "bridge.py").read_text()
    assert "FORWARDED.labels(kafka_topic)" in bridge.split("def __init__")[1].split("# ------")[0]
