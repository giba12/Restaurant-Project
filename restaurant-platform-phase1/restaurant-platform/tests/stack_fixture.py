"""
Readiness gate shared by the stack-level test layers. The stack itself is
brought up and torn down by run_stack_tests.sh; these tests only verify it is
actually ready before anything is asserted about it.
"""
import functools
import urllib.request

import pytest

from helpers import DASHBOARD_PORT, compose, inspect, wait_for

# Everything in docker-compose.yml except the LLM trio (ollama, ollama-init,
# llm-narrator): pulling a model on every run would dominate the runtime for
# no pipeline coverage, since the narrator's logic is covered by its own tests.
STACK_SERVICES = [
    "mosquitto", "kafka", "mqtt-kafka-bridge", "timescaledb",
    "edge-sim-plate-waste", "edge-sim-pos-transaction", "edge-sim-service-timing", "edge-sim-staff-shift",
    "storage-consumer", "ticket-timing-aggregator", "anomaly-detector", "causal-engine", "finding-reviewer",
    "scenario-injection-controller", "digital-twin", "dashboard-api", "dashboard-web",
]
LONG_RUNNING = list(STACK_SERVICES)


def http_get(path, timeout=10):
    with urllib.request.urlopen(f"http://127.0.0.1:{DASHBOARD_PORT}{path}", timeout=timeout) as response:
        return response.status, response.read()


def parse_metrics(text: str) -> dict:
    """Prometheus text format -> {series: value}, labelled series keeping their labels in the key; comments skipped."""
    metrics = {}
    for line in text.splitlines():
        if line and not line.startswith("#"):
            name, _, value = line.rpartition(" ")
            try:
                metrics[name] = float(value)
            except ValueError:
                pass
    return metrics


def bridge_metrics():
    """The bridge's own Prometheus metrics, read from inside its container."""
    out = compose("exec", "-T", "mqtt-kafka-bridge", "python", "-c",
                  "import urllib.request; print(urllib.request.urlopen('http://localhost:8000/metrics', timeout=5).read().decode())",
                  timeout=30).stdout
    return parse_metrics(out)


def bridge_connected():
    """True while the bridge is connected to the MQTT broker, and so subscribed with its persistent session."""
    return bridge_metrics().get("bridge_mqtt_connected") == 1.0


def healthy(service):
    return inspect(service)["State"].get("Health", {}).get("Status") == "healthy"


@functools.lru_cache(maxsize=1)
def wait_until_ready():
    wait_for(lambda: healthy("kafka") and healthy("timescaledb"), 240, description="kafka and timescaledb to be healthy")
    wait_for(lambda: http_get("/api/health")[0] == 200, 180, description="the dashboard to answer")
    wait_for(bridge_connected, 240, description="the MQTT-Kafka bridge to be connected")
    return True


def stack_ready_fixture():
    try:
        wait_until_ready()
    except Exception as exc:
        pytest.exit(f"the test stack is not ready ({exc}). Run these layers via tests/run_stack_tests.sh", returncode=3)


KAFKA_BIN = "/opt/kafka/bin"


def end_offset(topic):
    """Total messages ever written to a topic (sum of every partition's end offset)."""
    out = compose("exec", "-T", "kafka", f"{KAFKA_BIN}/kafka-get-offsets.sh", "--bootstrap-server", "localhost:9092",
                  "--topic", topic, timeout=60).stdout
    return sum(int(line.rsplit(":", 1)[1]) for line in out.strip().splitlines() if line.strip())


def consumer_lag(group):
    """Messages the group has yet to process; None if the group has no committed offsets yet."""
    result = compose("exec", "-T", "kafka", f"{KAFKA_BIN}/kafka-consumer-groups.sh", "--bootstrap-server", "localhost:9092",
                     "--describe", "--group", group, check=False, timeout=60)
    lags = []
    for line in result.stdout.splitlines():
        parts = line.split()
        if len(parts) >= 6 and parts[0] == group:
            lags.append(parts[5])
    if not lags:
        return None
    return sum(int(x) for x in lags if x.isdigit()) + (10**9 if any(not x.isdigit() for x in lags) else 0)
