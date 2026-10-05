"""
End-to-end tests against the real, running Docker Compose stack.

Everything else in the regime tests a piece. These tests start from the
outside -- simulated sensors publishing over MQTT -- and follow the data all
the way to a browser-facing API, checking at every hop. If a hop is broken,
the first failing test names it.

    bash tests/run_stack_tests.sh e2e
"""
import json
import re

import pytest

import stack_fixture as stack
from helpers import compose, logs, restart_count, sql_int, wait_for

EVENT_TABLES = ["plate_waste_events", "pos_transaction_events", "service_timing_events", "staff_shift_events"]
PYTHON_SERVICES = ["storage-consumer", "ticket-timing-aggregator", "anomaly-detector", "causal-engine",
                   "finding-reviewer", "digital-twin", "dashboard-api"]


# ------------------------------------------------------------------ the stack is healthy

@pytest.mark.parametrize("service", stack.LONG_RUNNING)
def test_every_long_running_service_is_up(service):
    from helpers import inspect

    state = inspect(service)["State"]
    assert state["Running"], f"{service} is not running ({state.get('Status')})"


def test_the_one_shot_connector_registration_completed_successfully():
    from helpers import inspect

    state = inspect("kafka-connect-init")["State"]
    assert state["Status"] == "exited" and state["ExitCode"] == 0, state


def test_registering_the_connectors_again_corrects_one_whose_configuration_has_drifted():
    # The registration script used to POST, which Kafka Connect refuses for a connector that
    # already exists, and then exit 0 whichever way it had gone. So a changed config file never
    # reached a stack that had run before. It now PUTs, which creates or updates.
    name = "pos-transaction-source-connector"
    connect = f"http://kafka-connect:8083/connectors/{name}/config"

    def current_config():
        return json.loads(compose("exec", "-T", "kafka-connect", "wget", "-qO-", connect, timeout=30).stdout)

    original = current_config()
    drifted = {**original, "camel.kamelet.mqtt-source.clientId": "somebody-elses-client"}
    # busybox wget cannot PUT; the supervisor's container has Python.
    compose("exec", "-T", "kafka-connect-supervisor", "python", "-c",
            "import sys, urllib.request as u; "
            f"u.urlopen(u.Request('{connect}', data=sys.argv[1].encode(), method='PUT', headers={{'Content-Type': 'application/json'}}))",
            json.dumps(drifted), timeout=60)
    assert current_config() != original, "the test could not change the connector's configuration"

    compose("run", "--rm", "-T", "kafka-connect-init", timeout=300)  # exit code 0 or this raises

    assert current_config() == original, "registering again did not restore the connector's configuration"
    wait_for(lambda: all(state == "RUNNING" for state in stack.connector_states().values()), 120, interval=3,
             description="the connectors to be RUNNING after the configuration was restored")


def test_all_four_mqtt_connectors_are_running():
    # One connector failing was invisible once (problem log item 44): three of
    # four sensor topics silently stopped reaching Kafka.
    states = stack.connector_states()
    assert set(states) >= set(stack.CONNECTORS)
    assert all(state == "RUNNING" for state in states.values()), states


# ------------------------------------------------------------------ data flows, hop by hop

@pytest.mark.parametrize("table", EVENT_TABLES)
def test_every_sensor_type_reaches_the_database(table):
    # simulator -> MQTT -> Kafka Connect -> Kafka -> storage-consumer -> TimescaleDB
    wait_for(lambda: sql_int(f"SELECT count(*) FROM {table}") >= 5, 240, description=f"5 rows in {table}")


def test_the_edge_nodes_model_and_inference_reach_the_database_intact():
    # The plate-waste node runs a model on the node itself. What the database
    # holds must name the exact model that is committed in the repository (the
    # image was built from it), and the node's own trust flags must be present.
    # simulator (model inference) -> MQTT -> Kafka Connect -> Kafka -> consumer -> TimescaleDB
    from helpers import ROOT

    committed = json.load(open(ROOT / "edge-simulators" / "edge_ai" / "plate_waste_edge_model.json"))["weights_sha256"]
    wait_for(lambda: sql_int("SELECT count(*) FROM plate_waste_events WHERE raw_payload ? 'edge_inference'") >= 5, 240,
             description="5 plate-waste events carrying edge_inference")
    assert sql_int("SELECT count(*) FROM plate_waste_events WHERE raw_payload ? 'edge_inference' "
                   f"AND raw_payload #>> '{{edge_inference,model_sha256}}' <> '{committed}'") == 0, \
        "stored estimates name a model other than the one committed in the repository"
    assert sql_int("SELECT count(*) FROM plate_waste_events WHERE raw_payload ? 'edge_inference' AND schema_version <> '1.1.0'") == 0
    assert sql_int("SELECT count(*) FROM plate_waste_events WHERE raw_payload ? 'edge_inference' "
                   "AND (raw_payload #>> '{edge_inference,inference_latency_ms}')::float > 5") == 0, "inference exceeded its latency budget on the stack"


def test_the_dashboard_api_reports_the_edge_node_as_a_fleet():
    from helpers import ROOT

    committed = json.load(open(ROOT / "edge-simulators" / "edge_ai" / "plate_waste_edge_model.json"))["weights_sha256"]
    wait_for(lambda: sql_int("SELECT count(*) FROM plate_waste_events WHERE raw_payload ? 'edge_inference'") >= 5, 240,
             description="edge events")
    status, body = stack.http_get("/api/edge/plate-waste?minutes=60")
    assert status == 200
    nodes = json.loads(body)["nodes"]
    assert nodes and nodes[0]["readings"] >= 5
    assert nodes[0]["model_sha256"] == committed
    assert nodes[0]["out_of_distribution_rate"] < 0.05, "a clean simulated node should rarely distrust its own readings"


def test_ticket_timings_are_aggregated_into_complete_summaries():
    wait_for(lambda: sql_int("SELECT count(*) FROM ticket_timing_summaries WHERE is_complete") >= 10, 300,
             description="10 complete ticket summaries")


def test_completed_ticket_summaries_are_internally_consistent():
    wait_for(lambda: sql_int("SELECT count(*) FROM ticket_timing_summaries WHERE is_complete") >= 10, 300, description="summaries")
    broken = sql_int("""
        SELECT count(*) FROM ticket_timing_summaries WHERE is_complete AND (
               time_to_cook_start_ms IS NULL OR cook_duration_ms IS NULL OR pickup_delay_ms IS NULL
            OR service_delay_ms IS NULL OR total_ticket_duration_ms IS NULL
            OR time_to_cook_start_ms < 0 OR cook_duration_ms < 0 OR pickup_delay_ms < 0 OR service_delay_ms < 0
            OR abs(total_ticket_duration_ms - (time_to_cook_start_ms + cook_duration_ms + pickup_delay_ms + service_delay_ms)) > 5)
    """)
    assert broken == 0, f"{broken} complete summaries have missing, negative or non-additive durations"


def test_the_digital_twin_mirrors_the_restaurant():
    wait_for(lambda: sql_int("SELECT count(*) FROM twin_staff_state") >= 1 and sql_int("SELECT count(*) FROM twin_station_state") >= 1,
             240, description="twin staff and station rows")
    assert sql_int("SELECT count(*) FROM twin_station_state WHERE open_ticket_count < 0") == 0
    assert sql_int("SELECT count(*) FROM twin_staff_state WHERE status NOT IN ('on_shift','off_shift','on_break') OR status IS NULL") == 0


# ------------------------------------------------------------------ nothing is quietly failing

@pytest.mark.parametrize("table", EVENT_TABLES)
def test_no_event_was_stored_twice(table):
    assert sql_int(f"SELECT count(*) - count(DISTINCT event_id) FROM {table}") == 0


# Messages kafka-python logs at ERROR while a container is still starting up:
#   - "not found in cluster metadata": a consumer started before any producer
#     created its topic (topics are auto-created on first write);
#   - "DNS Resolution failure" / "Connection lost": the very first connection
#     attempt raced the container network's DNS becoming ready.
# Both are start-up races, not malfunctions: the client retries, every service
# recovers, and nothing restarts. Allowed -- but only as the one-offs they are
# (see the persistence test below), so a topic or host that genuinely never
# appears would still fail.
BENIGN_STARTUP_RACES = ("not found in cluster metadata", "DNS Resolution failure", "Connection lost")


def _is_benign(line):
    return any(marker in line for marker in BENIGN_STARTUP_RACES)


@pytest.mark.parametrize("service", PYTHON_SERVICES)
def test_no_python_service_has_logged_a_traceback_or_error(service):
    text = logs(service, tail=500)
    bad = [line for line in text.splitlines()
           if ("Traceback" in line or " ERROR " in line or "SCHEMA VIOLATION" in line) and not _is_benign(line)]
    assert not bad, f"{service} logged errors:\n" + "\n".join(bad[:5])


@pytest.mark.parametrize("service", PYTHON_SERVICES)
def test_the_startup_topic_race_happens_once_at_start_and_never_persists(service):
    # The client retries rapidly until the topic or host appears, so a race
    # produces a *burst* of lines (dozens in a couple of seconds), then silence.
    # What distinguishes it from a stuck pipeline is duration: every such line
    # must fall within a short window. A topic that never appears would keep
    # producing them for as long as the service runs.
    import datetime

    stamps = []
    for line in logs(service, tail=5000).splitlines():
        if _is_benign(line):
            match = re.search(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", line)
            if match:
                stamps.append(datetime.datetime.strptime(match.group(1), "%Y-%m-%d %H:%M:%S"))
    if not stamps:
        return
    span = (max(stamps) - min(stamps)).total_seconds()
    assert span <= 60, f"{service} kept logging start-up connection errors for {span:.0f}s; something is persistently unreachable"


@pytest.mark.parametrize("service", PYTHON_SERVICES)
def test_no_python_service_has_crashed_and_restarted(service):
    # A restart means a crash; the restart policy then hides it. Zero is the bar.
    assert restart_count(service) == 0, f"{service} has restarted {restart_count(service)} time(s)"


# ------------------------------------------------------------------ the part a user touches

def test_the_dashboard_page_is_served():
    status, body = stack.http_get("/")
    assert status == 200 and b'id="root"' in body


@pytest.mark.parametrize("path", ["/api/twin/tables", "/api/twin/staff", "/api/twin/stations", "/api/anomalies/summary", "/api/findings/narrated"])
def test_the_api_answers_through_the_web_proxy_without_the_browser_knowing_the_key(path):
    status, body = stack.http_get(path)
    assert status == 200
    assert isinstance(json.loads(body), list)


def test_the_dashboard_shows_live_station_state():
    wait_for(lambda: json.loads(stack.http_get("/api/twin/stations")[1]), 240, description="station state via the dashboard")
    stations = json.loads(stack.http_get("/api/twin/stations")[1])
    assert all(s["open_ticket_count"] >= 0 for s in stations)


# ------------------------------------------------------------------ observability

@pytest.mark.parametrize("service,metric", [
    ("anomaly-detector", "anomaly_detector_summaries_processed_total"),
    ("causal-engine", "causal_engine_anomalies_processed_total"),
    ("finding-reviewer", "causal_engine_findings_marked_ready_total"),
])
def test_pipeline_health_metrics_are_exposed(service, metric):
    script = "import urllib.request;print(urllib.request.urlopen('http://localhost:8000/metrics',timeout=5).read().decode())"
    text = compose("exec", "-T", service, "python", "-c", script, timeout=30).stdout
    assert metric in text, f"{service} does not expose {metric}"


def test_the_anomaly_detector_is_actually_consuming_summaries():
    script = "import urllib.request;print(urllib.request.urlopen('http://localhost:8000/metrics',timeout=5).read().decode())"

    def processed():
        text = compose("exec", "-T", "anomaly-detector", "python", "-c", script, timeout=30).stdout
        for line in text.splitlines():
            if line.startswith("anomaly_detector_summaries_processed_total"):
                return float(line.split()[-1]) > 0
        return False

    wait_for(processed, 300, description="the detector to process at least one summary")
