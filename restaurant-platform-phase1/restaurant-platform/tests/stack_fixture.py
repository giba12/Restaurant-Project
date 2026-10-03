"""
Readiness gate shared by the stack-level test layers. The stack itself is
brought up and torn down by run_stack_tests.sh; these tests only verify it is
actually ready before anything is asserted about it.
"""
import functools
import json
import urllib.request

import pytest

from helpers import DASHBOARD_PORT, compose, inspect, wait_for

# Everything in docker-compose.yml except the LLM trio (ollama, ollama-init,
# llm-narrator): pulling a model on every run would dominate the runtime for
# no pipeline coverage, since the narrator's logic is covered by its own tests.
STACK_SERVICES = [
    "mosquitto", "kafka", "kafka-connect", "kafka-connect-init", "timescaledb",
    "edge-sim-plate-waste", "edge-sim-pos-transaction", "edge-sim-service-timing", "edge-sim-staff-shift",
    "storage-consumer", "ticket-timing-aggregator", "anomaly-detector", "causal-engine", "finding-reviewer",
    "scenario-injection-controller", "digital-twin", "dashboard-api", "dashboard-web",
]
LONG_RUNNING = [s for s in STACK_SERVICES if s != "kafka-connect-init"]
CONNECTORS = ["plate-waste-source-connector", "pos-transaction-source-connector", "service-timing-source-connector", "staff-shift-source-connector"]


def http_get(path, timeout=10):
    with urllib.request.urlopen(f"http://127.0.0.1:{DASHBOARD_PORT}{path}", timeout=timeout) as response:
        return response.status, response.read()


def connector_states():
    out = compose("exec", "-T", "kafka-connect", "wget", "-qO-", "http://localhost:8083/connectors?expand=status", timeout=30).stdout
    return {name: info["status"]["connector"]["state"] for name, info in json.loads(out).items()}


def healthy(service):
    return inspect(service)["State"].get("Health", {}).get("Status") == "healthy"


@functools.lru_cache(maxsize=1)
def wait_until_ready():
    wait_for(lambda: healthy("kafka") and healthy("timescaledb"), 240, description="kafka and timescaledb to be healthy")
    wait_for(lambda: http_get("/api/health")[0] == 200, 180, description="the dashboard to answer")
    wait_for(lambda: set(connector_states()) >= set(CONNECTORS) and all(
        state == "RUNNING" for state in connector_states().values()), 240, description="all four connectors to be RUNNING")
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
