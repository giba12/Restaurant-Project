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
from helpers import ROOT, compose, logs, restart_count, sql, sql_int, wait_for

EVENT_TABLES = ["plate_waste_events", "pos_transaction_events", "service_timing_events", "staff_shift_events"]
PYTHON_SERVICES = ["storage-consumer", "ticket-timing-aggregator", "anomaly-detector", "causal-engine",
                   "finding-reviewer", "digital-twin", "dashboard-api"]


# ------------------------------------------------------------------ the stack is healthy

@pytest.mark.parametrize("service", stack.LONG_RUNNING)
def test_every_long_running_service_is_up(service):
    from helpers import inspect

    state = inspect(service)["State"]
    assert state["Running"], f"{service} is not running ({state.get('Status')})"


def test_the_bridge_is_connected_to_mqtt_and_subscribed():
    # A bridge that is up but not connected moves nothing, and the stack looks healthy: three of four
    # sensor topics once stopped reaching Kafka that way (problem log item 44).
    assert stack.bridge_connected()


def test_the_bridge_has_nothing_stuck_waiting_for_kafka():
    # Every message handed to Kafka is confirmed within seconds; an old unconfirmed one means Kafka is not taking them.
    metrics = stack.bridge_metrics()
    assert metrics["bridge_oldest_unconfirmed_seconds"] < 30, metrics
    assert metrics.get("bridge_kafka_errors_total", 0.0) == 0.0, metrics


def test_the_bridge_forwards_all_four_sensor_topics():
    forwarded = {name: value for name, value in stack.bridge_metrics().items() if name.startswith("bridge_messages_forwarded_total{")}
    assert len(forwarded) == 4 and all(value > 0 for value in forwarded.values()), forwarded


def test_mosquitto_keeps_the_bridges_session_on_disk():
    # The bridge's persistent session and the messages queued for it are what make "nothing is lost while the bridge
    # or Mosquitto is down" true; without persistence a Mosquitto restart discarded both (DEF-152).
    out = compose("exec", "-T", "mosquitto", "sh", "-c", "grep -E '^persistence ' /mosquitto/config/mosquitto.conf; ls /mosquitto/data").stdout
    assert "persistence true" in out, out


# ------------------------------------------------------------------ data flows, hop by hop

@pytest.mark.parametrize("table", EVENT_TABLES)
def test_every_sensor_type_reaches_the_database(table):
    # simulator -> MQTT -> mqtt-kafka-bridge -> Kafka -> storage-consumer -> TimescaleDB
    wait_for(lambda: sql_int(f"SELECT count(*) FROM {table}") >= 5, 240, description=f"5 rows in {table}")


def test_the_edge_nodes_model_and_inference_reach_the_database_intact():
    # The plate-waste node runs a model on the node itself. What the database
    # holds must name the exact model that is committed in the repository (the
    # image was built from it), and the node's own trust flags must be present.
    # simulator (model inference) -> MQTT -> mqtt-kafka-bridge -> Kafka -> consumer -> TimescaleDB
    committed = json.load(open(ROOT / "edge-simulators" / "edge_ai" / "plate_waste_edge_model.json"))["weights_sha256"]
    wait_for(lambda: sql_int("SELECT count(*) FROM plate_waste_events WHERE raw_payload ? 'edge_inference'") >= 5, 240,
             description="5 plate-waste events carrying edge_inference")
    assert sql_int("SELECT count(*) FROM plate_waste_events WHERE raw_payload ? 'edge_inference' "
                   f"AND raw_payload #>> '{{edge_inference,model_sha256}}' <> '{committed}'") == 0, \
        "stored estimates name a model other than the one committed in the repository"
    assert sql_int("SELECT count(*) FROM plate_waste_events WHERE raw_payload ? 'edge_inference' AND schema_version NOT IN ('1.1.0', '1.2.0', '1.3.0')") == 0
    # This catches a model that is slow in general, on a stack of seventeen containers that may be busy with a build: a
    # few scheduling hiccups are not a breach (it once demanded that no event took over 5 ms, then that under 1% did, and
    # failed at 1.1% on a loaded laptop). The p99 budget itself is checked under controlled conditions, at the chart's CPU
    # limit, by tests/load/test_edge_footprint.py.
    total = sql_int("SELECT count(*) FROM plate_waste_events WHERE raw_payload ? 'edge_inference'")
    over = sql_int("SELECT count(*) FROM plate_waste_events WHERE raw_payload ? 'edge_inference' "
                   "AND (raw_payload #>> '{edge_inference,inference_latency_ms}')::float > 5")
    assert over * 20 <= total, f"inference exceeded 5 ms on {over} of {total} events on the stack (more than 5%)"
    # Schema 1.3.0: every event the current node publishes carries the shift and flatline monitors' scores (null until
    # each 30-reading window fills, so the keys must exist but their values may not yet).
    assert sql_int("SELECT count(*) FROM plate_waste_events WHERE schema_version = '1.3.0' "
                   "AND NOT (raw_payload -> 'edge_inference' ? 'shift_score' AND raw_payload -> 'edge_inference' ? 'flatline_score')") == 0, \
        "a 1.3.0 event lacks shift_score or flatline_score"
    assert sql_int("SELECT count(*) FROM plate_waste_events WHERE schema_version = '1.3.0'") >= 5, "the node is not publishing schema 1.3.0"


def test_the_dashboard_api_reports_the_edge_node_as_a_fleet():
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


# ------------------------------------------------------------------ the model update path, on the live node
#
# Last in this file on purpose: it moves the running node to an older model and back, which leaves events from
# both models in the database, and the edge tests above assert that every stored estimate names the committed model.

BAKED = json.load(open(ROOT / "edge-simulators" / "edge_ai" / "plate_waste_edge_model.json"))["model_version"]  # the image's own model


def operator(*args, entrypoint=None):
    """Run the separate edge-operator service: it holds the operator's broker login and the master secret, which no node
    container has. (`compose run`, not `exec`: it is a tool run on demand, not part of the stack.)"""
    cmd = ["run", "--rm", "-T", "--no-deps"] + (["--entrypoint", entrypoint] if entrypoint else []) + ["edge-operator", *args]
    return compose(*cmd, timeout=120).stdout


def edge_control(*args):
    return operator(*args)


def operator_python(code: str) -> str:
    return operator("-c", code, entrypoint="python")


def edge_status():
    out = operator_python(
        "import json;from control import edge_control as c;l=c.Link();r=l.collect('edge/status/plate-waste/sim-plate-cam-01',3);l.close();"
        "print(next(iter(r.values())).decode() if r else '{}')").strip().splitlines()[-1]
    return json.loads(out or "{}")


PRELUDE = ("import copy,json,os,time;from control import edge_control as c;from edge_ai import updater;"
           "node='sim-plate-cam-01';key=c.derive_node_key(os.environ['EDGE_CONTROL_MASTER_KEY'],node,1);"
           f"store=c.ModelStore();good=store.load({BAKED!r})[0];link=c.Link();topic='control/edge/plate-waste/sim-plate-cam-01';")


def inline_publish(source: str):
    """Run a few lines of Python as the operator that publish a hand-made (bad) command to the node's topic."""
    operator_python(PRELUDE + source + "link.close()")


def events_from(version, since):
    return sql_int("SELECT count(*) FROM plate_waste_events WHERE raw_payload ? 'edge_inference' "
                   f"AND raw_payload #>> '{{edge_inference,model_version}}' = '{version}' AND ingested_at > '{since}'")


def db_now():
    return sql("SELECT now()")


def test_a_live_node_is_rolled_back_refuses_two_bad_models_and_is_rolled_forward_without_a_restart():
    started = db_now()
    wait_for(lambda: events_from(BAKED, started) >= 3, 180, description="events from the model baked into the image")

    # 1. A rollback to the older model (a version from before the shift monitor): applied at once, no shadow.
    before_rollback = db_now()
    edge_control("rollback", "--to", "1.0.0", "--nodes", "sim-plate-cam-01")
    wait_for(lambda: events_from("1.0.0", before_rollback) >= 3, 180, description="events from the rolled-back model")
    assert edge_status()["state"] == "applied"
    assert sql_int("SELECT count(*) FROM plate_waste_events WHERE raw_payload #>> '{edge_inference,model_version}' = '1.0.0' "
                   f"AND ingested_at > '{before_rollback}' AND raw_payload -> 'edge_inference' -> 'shift_score' <> 'null'::jsonb") == 0, \
        "a model from before the shift monitor reported a shift score"

    # 1b. A command signed with another node's key, and one addressed to another node, are refused: each node has its own
    #     key, and a command names its target. (Both are published by the operator login, which the broker allows.)
    inline_publish("other=c.derive_node_key(os.environ['EDGE_CONTROL_MASTER_KEY'],'sim-plate-cam-02',1);"
                   "cmd=c.command_from_artifact(good,other,node,shadow_readings=0,now=time.time());"
                   "link.publish(topic,json.dumps(cmd),True);")
    wait_for(lambda: "bad signature" in edge_status().get("reason", ""), 60, description="the wrong-key command to be refused")
    inline_publish("cmd=c.command_from_artifact(good,key,'sim-plate-cam-02',shadow_readings=0,now=time.time());"
                   "link.publish(topic,json.dumps(cmd),True);")
    wait_for(lambda: "addressed to 'sim-plate-cam-02'" in edge_status().get("reason", ""), 60, description="the misaddressed command to be refused")
    assert edge_status()["active"]["model_version"] == "1.0.0"

    # 2. A model whose artifact was changed after its hash was declared is refused, and estimates carry on unchanged.
    before_corrupt = db_now()
    inline_publish("cmd=c.command_from_artifact(good,key,node,shadow_readings=5);cmd['model']['artifact']=copy.deepcopy(good);"
                   "cmd['model']['artifact']['layers'][0]['bias'][0]+=0.5;cmd['issued_at']=time.time();"
                   "link.publish(topic,json.dumps(updater.sign(cmd,key)),True);")
    wait_for(lambda: "hash mismatch" in edge_status().get("reason", ""), 60, description="the corrupted model to be refused")
    assert edge_status()["state"] == "rejected" and edge_status()["active"]["model_version"] == "1.0.0"

    # 3. A model with a valid hash and answers that match the cloud's, but 30 g heavier on every estimate, passes every
    #    check except the one that compares it with the model in service on live readings, and is refused there.
    heavy = ("bad=copy.deepcopy(good);bad['layers'][-1]['bias'][0]+=0.3;bad['model_version']='9.9.9';"
             "bad['weights_sha256']=c.edge_model.behaviour_hash(bad);"
             "cmd=c.command_from_artifact(bad,key,node,shadow_readings=10,now=time.time());"
             "link.publish(topic,json.dumps(cmd),True);")
    inline_publish(heavy)
    wait_for(lambda: edge_status().get("state") == "rejected" and "shadow disagreement" in edge_status().get("reason", ""), 120,
             description="the heavy model to be refused after its shadow comparison")
    assert events_from("9.9.9", before_corrupt) == 0, "an estimate from the refused model was published"
    assert events_from(BAKED, before_corrupt) == 0, "the node left the older model without being told to"

    # 4. Rolled forward again, with the shadow comparison, to the model baked into the image: no restart involved.
    restarts = restart_count("edge-sim-plate-waste")
    before_forward = db_now()
    edge_control("rollout", "--version", BAKED, "--nodes", "sim-plate-cam-01", "--shadow-readings", "10")
    wait_for(lambda: events_from(BAKED, before_forward) >= 3, 180, description="events from the rolled-forward model")
    assert edge_status()["state"] == "applied"
    assert restart_count("edge-sim-plate-waste") == restarts, "the node restarted to change model"
    edge_control("clear", "--nodes", "sim-plate-cam-01")


# ------------------------------------------------------------------ the broker's login, on the live stack

def test_the_live_broker_refuses_anonymous_clients_and_wrong_passwords_and_accepts_the_right_ones():
    out = operator_python(
        "import json,time,paho.mqtt.client as m\n"
        "def attempt(user, pw):\n"
        "    r = {}\n"
        "    c = m.Client(client_id=user or 'anon-probe', protocol=m.MQTTv5, callback_api_version=m.CallbackAPIVersion.VERSION2)\n"
        "    c.tls_set(ca_certs='/etc/mosquitto-tls/tls.crt')\n"
        "    if user: c.username_pw_set(user, pw)\n"
        "    c.on_connect = lambda cl, u, f, rc, p=None: r.update(code=str(rc), failed=bool(rc.is_failure))\n"
        "    c.connect('mosquitto', 8883); c.loop_start(); time.sleep(1.5); c.loop_stop(); c.disconnect()\n"
        "    return r.get('failed')\n"
        "def plaintext_port_open():\n"
        "    import socket\n"
        "    try:\n"
        "        socket.create_connection(('mosquitto', 1883), timeout=3).close(); return True\n"
        "    except OSError:\n"
        "        return False\n"
        "print(json.dumps({'anonymous': attempt(None, None), 'wrong': attempt('sim-pos-01', 'not-the-password'), 'unknown': attempt('intruder', 'x'),\n"
        "                  'operator': attempt('edge-operator', 'changeme-local-dev-only'), 'plaintext_port_open': plaintext_port_open()}))\n")
    result = json.loads(out.strip().splitlines()[-1])
    assert result == {"anonymous": True, "wrong": True, "unknown": True, "operator": False, "plaintext_port_open": False}, result


# ------------------------------------------------------------------ a master-secret rotation, on the live stack

NODE = "sim-plate-cam-01"
ROTATED_MASTER = "rotated-master-for-the-e2e-test"


def derive(master, monkeypatch):
    monkeypatch.setenv("EDGE_CONTROL_MASTER_KEY", master)
    return operator("derive-key", "--node", NODE).strip().splitlines()[-1]


def recreate_plate_node():
    compose("up", "-d", "--no-deps", "--no-build", "--force-recreate", "edge-sim-plate-waste", timeout=180)
    wait_for(lambda: edge_status().get("state") in ("ready", "unchanged", "applied", "rejected"), 120, description="the recreated node to report a status")


def command_as(master, monkeypatch):
    """Roll the model the node already runs out as an operator holding `master`; returns the node's answer to exactly that command."""
    monkeypatch.setenv("EDGE_CONTROL_MASTER_KEY", master)
    request = re.search(r"request ([0-9a-f-]{36})", operator("rollout", "--version", BAKED, "--nodes", NODE)).group(1)
    wait_for(lambda: edge_status().get("request_id") == request, 60, description="the node to answer that command")
    return edge_status()


def test_a_live_node_is_rotated_to_a_new_master_obeys_both_during_the_window_and_refuses_the_old_one_afterwards(monkeypatch):
    # The procedure of provision-mqtt-auth.sh rotate-master, on the running stack: a real node container restarted with its key
    # under the NEW master as current and its key under the OLD master as previous, commanded by the real operator tool as each
    # master in turn; then restarted again without the previous key, which is what finish-rotation and the chart upgrade do.
    old_master = "dev-only-edge-control-master-key"
    old_key, new_key = derive(old_master, monkeypatch), derive(ROTATED_MASTER, monkeypatch)
    assert old_key != new_key
    try:
        monkeypatch.setenv("EDGE_CONTROL_MASTER_KEY", old_master)
        operator("clear", "--nodes", NODE)

        monkeypatch.setenv("EDGE_CONTROL_KEY", new_key)
        monkeypatch.setenv("EDGE_CONTROL_KEY_PREVIOUS", old_key)
        recreate_plate_node()
        assert command_as(old_master, monkeypatch)["state"] == "unchanged", "the window did not accept the old master"
        assert command_as(ROTATED_MASTER, monkeypatch)["state"] == "unchanged", "the window did not accept the new master"

        monkeypatch.delenv("EDGE_CONTROL_KEY_PREVIOUS")
        recreate_plate_node()
        refused = command_as(old_master, monkeypatch)
        assert refused["state"] == "rejected" and "bad signature" in refused["reason"], refused
        assert command_as(ROTATED_MASTER, monkeypatch)["state"] == "unchanged", "the new master stopped working"
    finally:
        monkeypatch.delenv("EDGE_CONTROL_KEY", raising=False)
        monkeypatch.delenv("EDGE_CONTROL_KEY_PREVIOUS", raising=False)
        monkeypatch.setenv("EDGE_CONTROL_MASTER_KEY", old_master)
        operator("clear", "--nodes", NODE)
        recreate_plate_node()
