"""
Tests for the simulators' store-and-forward behaviour (DEF-148).

A sensor holds its events while the Kafka Connect connector that carries them
into Kafka is not running, and sends them, in order, when it is. These tests
check the decision ("is the connector running?"), the holding and flushing, and
that nothing is sent twice or silently dropped. They do not need MQTT, Kafka or
Kafka Connect: the broker client and the Connect REST call are fakes. That the
events really arrive after a real Connect outage is checked against the real
stack by tests/resilience/test_failure_recovery.py.

    pip install jsonschema pytest
    cd edge-simulators && python -m pytest test_store_and_forward.py -v
"""
import collections
import json
import os
import sys
import time
import types

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
os.environ.setdefault("SCHEMA_DIR", os.path.join(os.path.dirname(HERE), "schemas"))

for _name in ("paho", "paho.mqtt", "paho.mqtt.client"):
    sys.modules.setdefault(_name, types.ModuleType(_name))
sys.modules["paho"].mqtt = sys.modules["paho.mqtt"]
sys.modules["paho.mqtt"].client = sys.modules["paho.mqtt.client"]

from common import ingest_gate, runtime  # noqa: E402
from common.ingest_gate import IngestGate, connector_is_running  # noqa: E402


def running(*task_states, connector="RUNNING"):
    return {"name": "c", "connector": {"state": connector}, "tasks": [{"id": i, "state": s} for i, s in enumerate(task_states)]}


# ------------------------------------------------------------------ the decision

@pytest.mark.parametrize("status, expected", [
    (running("RUNNING"), True),
    (running("RUNNING", "RUNNING"), True),
    (running("FAILED"), False),                         # the connector is up but its task is not
    (running("RUNNING", "FAILED"), False),
    (running("UNASSIGNED"), False),                     # what a broker restart left behind (DEF-142)
    (running("RUNNING", connector="PAUSED"), False),    # stopped on purpose: nothing is consuming
    (running("RUNNING", connector="UNASSIGNED"), False),
    (running(), False),                                 # registered, no task yet
    ({"name": "c", "tasks": []}, False),
    (None, False),                                      # unreachable or unknown connector
    ({}, False),
])
def test_only_a_running_connector_with_only_running_tasks_opens_the_gate(status, expected):
    assert connector_is_running(status) is expected


def test_a_gate_with_no_url_is_always_open_so_a_stand_alone_simulator_behaves_as_before():
    gate = IngestGate(None, "service-timing-source-connector")
    assert gate.is_open() and not gate.enabled


class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


def test_a_gate_stays_closed_until_the_connector_has_been_running_for_the_settle_period():
    # Kafka Connect says RUNNING before the task has subscribed (measured: about 3 s earlier),
    # and events sent in between are lost, so RUNNING alone must not open the gate.
    clock = Clock()
    gate = IngestGate("http://connect:8083", "c", settle_seconds=15, fetch=lambda url, name: running("RUNNING"), clock=clock)
    assert not gate.is_open()
    assert gate.poll_once() is False
    clock.now += 14.9
    assert gate.poll_once() is False
    clock.now += 0.2
    assert gate.poll_once() is True and gate.is_open()


def test_the_settle_period_starts_again_after_any_look_that_says_the_connector_is_not_running():
    clock = Clock()
    answers = {"now": running("RUNNING")}
    gate = IngestGate("http://connect:8083", "c", settle_seconds=10, fetch=lambda url, name: answers["now"], clock=clock)
    gate.poll_once()
    clock.now += 11
    assert gate.poll_once() is True

    answers["now"] = None                      # Connect went away
    assert gate.poll_once() is False           # closed at once, not after a delay
    answers["now"] = running("RUNNING")
    clock.now += 1
    assert gate.poll_once() is False           # back, but the clock has restarted
    clock.now += 9
    assert gate.poll_once() is False
    clock.now += 1.5
    assert gate.poll_once() is True


def test_a_task_that_flaps_never_gets_the_gate_open():
    clock = Clock()
    states = iter([running("RUNNING"), running("FAILED"), running("RUNNING"), running("UNASSIGNED")] * 3)
    gate = IngestGate("http://connect:8083", "c", settle_seconds=5, fetch=lambda url, name: next(states), clock=clock)
    for _ in range(12):
        clock.now += 3
        assert gate.poll_once() is False


def test_a_gate_closes_when_the_connect_api_cannot_be_reached_which_is_what_an_outage_looks_like():
    clock = Clock()
    answers = iter([running("RUNNING"), running("RUNNING"), None, None, running("RUNNING")])
    gate = IngestGate("http://connect:8083", "c", settle_seconds=0, fetch=lambda url, name: next(answers), clock=clock)
    assert [gate.poll_once() for _ in range(5)] == [True, True, False, False, True]


def test_the_gate_asks_about_its_own_connector_and_survives_a_dead_connect_api(monkeypatch):
    seen = []

    def refuse(request, timeout):
        seen.append(request if isinstance(request, str) else request.full_url)
        raise ingest_gate.urllib.error.URLError("connection refused")

    monkeypatch.setattr(ingest_gate.urllib.request, "urlopen", refuse)
    assert ingest_gate.fetch_connector_status("http://connect:8083/", "pos-transaction-source-connector") is None
    assert seen == ["http://connect:8083/connectors/pos-transaction-source-connector/status"]


def test_a_gate_whose_status_call_raises_something_unexpected_is_closed_and_keeps_polling():
    gate = IngestGate("http://connect:8083", "c", poll_seconds=0.01, settle_seconds=0, fetch=lambda url, name: 1 / 0)
    gate._open = True
    gate.start()
    try:
        deadline = time.time() + 2
        while gate.is_open() and time.time() < deadline:
            time.sleep(0.01)
        assert not gate.is_open()
        assert gate._thread.is_alive()
    finally:
        gate.stop()


# ------------------------------------------------------------------ holding and flushing

class FakeResult:
    def wait_for_publish(self, timeout=None):
        return None


class FakeClient:
    def __init__(self, *args, **kwargs):
        self.sent = []

    def publish(self, topic, payload, qos=0, retain=False):
        self.sent.append((topic, json.loads(payload)["event_id"], qos))
        return FakeResult()


class OpenGate:
    """A gate the test opens and closes by hand."""
    enabled = True

    def __init__(self, is_open=False):
        self.value = is_open

    def is_open(self):
        return self.value

    def start(self):
        pass


@pytest.fixture
def sim(monkeypatch):
    monkeypatch.setattr(runtime.mqtt, "Client", FakeClient, raising=False)
    monkeypatch.setattr(runtime.mqtt, "CallbackAPIVersion", types.SimpleNamespace(VERSION2=2), raising=False)
    simulator = runtime.Simulator("pos-transaction", "POSTransactionEvent.schema.json", "sensors/pos-transaction")
    simulator.gate = OpenGate(is_open=False)
    return simulator


def pos_event(n):
    from simulators import pos_transaction
    event = pos_transaction.generate_event()
    event["event_id"] = f"00000000-0000-4000-8000-{n:012d}"
    return event


def sent_ids(sim):
    return [event_id for _topic, event_id, _qos in sim.client.sent]


def test_events_are_held_while_the_gate_is_closed_and_sent_in_order_when_it_opens(sim):
    for n in range(5):
        sim.publish(pos_event(n))
    assert sim.client.sent == [], "events were sent while the bridge was not running"

    sim.gate.value = True
    sim.flush()
    assert sent_ids(sim) == [f"00000000-0000-4000-8000-{n:012d}" for n in range(5)]
    assert all(topic == "sensors/pos-transaction" and qos == 1 for topic, _id, qos in sim.client.sent)


def test_an_open_gate_sends_each_event_immediately(sim):
    sim.gate.value = True
    sim.publish(pos_event(1))
    assert sent_ids(sim) == ["00000000-0000-4000-8000-000000000001"]
    assert not sim._outbox


def test_a_flush_sends_every_held_event_exactly_once(sim):
    for n in range(3):
        sim.publish(pos_event(n))
    sim.gate.value = True
    sim.flush()
    sim.flush()
    sim.publish(pos_event(3))
    assert sent_ids(sim) == [f"00000000-0000-4000-8000-{n:012d}" for n in range(4)]


def test_the_gate_closing_again_midway_stops_sending_and_the_rest_waits(sim):
    for n in range(4):
        sim.publish(pos_event(n))

    class ClosesAfterTwo(OpenGate):
        def is_open(self):
            return len(sim.client.sent) < 2

    sim.gate = ClosesAfterTwo()
    sim.flush()
    assert len(sim.client.sent) == 2 and len(sim._outbox) == 2
    sim.gate = OpenGate(is_open=True)
    sim.flush()
    assert len(sim.client.sent) == 4


def test_an_invalid_event_is_still_fatal_and_is_never_queued(sim):
    import jsonschema
    bad = pos_event(1)
    del bad["transaction_id"]
    with pytest.raises(jsonschema.exceptions.ValidationError):
        sim.publish(bad)
    assert not sim._outbox


def test_a_full_outbox_drops_the_oldest_event_loudly_not_the_newest_and_not_silently(sim, monkeypatch, caplog):
    monkeypatch.setattr(sim, "_outbox", collections.deque(maxlen=3))
    with caplog.at_level("ERROR"):
        for n in range(5):
            sim.publish(pos_event(n))
    assert [json.loads(p)["event_id"][-1] for _t, _i, p in sim._outbox] == ["2", "3", "4"]
    assert sim._dropped == 2
    assert any("outbox full" in record.message for record in caplog.records)


def test_a_failure_to_send_one_event_is_not_retried_because_paho_resends_what_it_accepted(sim):
    class Flaky(FakeClient):
        def publish(self, topic, payload, qos=0, retain=False):
            self.sent.append((topic, json.loads(payload)["event_id"], qos))
            raise RuntimeError("publish timed out")

    sim.client = Flaky()
    sim.gate.value = True
    with pytest.raises(RuntimeError):
        sim.publish(pos_event(1))
    sim.gate.value = True
    sim.flush()
    assert len(sim.client.sent) == 1, "an event handed to the MQTT client was handed over a second time"


def test_the_gate_is_read_from_the_environment_with_the_connector_named_after_the_sensor(monkeypatch):
    monkeypatch.setattr(runtime.mqtt, "Client", FakeClient, raising=False)
    monkeypatch.setattr(runtime.mqtt, "CallbackAPIVersion", types.SimpleNamespace(VERSION2=2), raising=False)
    monkeypatch.setenv("INGEST_GATE_URL", "http://kafka-connect:8083")
    simulator = runtime.Simulator("staff-shift", "StaffShiftEvent.schema.json", "sensors/staff-shift")
    assert simulator.gate.enabled and simulator.gate.connector == "staff-shift-source-connector"
    assert simulator.gate.settle_seconds == 20 and simulator.gate.poll_seconds == 0.25
    assert not simulator.gate.is_open(), "closed until the connector has been seen running"

    monkeypatch.delenv("INGEST_GATE_URL")
    assert runtime.Simulator("staff-shift", "StaffShiftEvent.schema.json", "sensors/staff-shift").gate.is_open()


# ------------------------------------------------------------------ the simulator's own MQTT connection

def an_open_gate(clock, hold=30):
    gate = IngestGate("http://connect:8083", "c", settle_seconds=0, reconnect_hold_seconds=hold,
                      fetch=lambda url, name: running("RUNNING"), clock=clock)
    assert gate.poll_once() is True and gate.is_open()
    return gate


def test_losing_the_mqtt_connection_closes_the_gate_at_once():
    clock = Clock()
    gate = an_open_gate(clock)
    gate.broker_lost()
    assert not gate.is_open()


def test_the_first_connection_does_not_hold_but_a_reconnection_holds_while_the_connectors_follow():
    # After a broker restart this simulator is back within a second, but each connector reconnects
    # on its own backoff (measured 2, 4, 8 and 17 s) and loses what is sent before it has resubscribed.
    clock = Clock()
    gate = an_open_gate(clock, hold=30)
    gate.broker_back(reconnect=False)
    assert gate.is_open(), "an ordinary first connection must not delay anything"

    gate.broker_lost()
    gate.broker_back(reconnect=True)
    assert not gate.is_open()
    clock.now += 29.9
    assert not gate.is_open()
    clock.now += 0.2
    assert gate.is_open()


def test_the_hold_does_not_open_a_gate_whose_connector_is_not_running():
    clock = Clock()
    answers = {"now": running("RUNNING")}
    gate = IngestGate("http://connect:8083", "c", settle_seconds=0, reconnect_hold_seconds=5,
                      fetch=lambda url, name: answers["now"], clock=clock)
    gate.poll_once()
    gate.broker_lost()
    gate.broker_back(reconnect=True)
    answers["now"] = None
    gate.poll_once()
    clock.now += 60
    assert not gate.is_open()


def test_a_gate_with_no_url_ignores_the_mqtt_connection_so_a_stand_alone_simulator_is_unchanged():
    gate = IngestGate(None, "c")
    gate.broker_lost()
    assert gate.is_open()
    gate.broker_back(reconnect=True)
    assert gate.is_open()


def test_the_simulators_connect_and_disconnect_callbacks_drive_the_gate(monkeypatch):
    monkeypatch.setattr(runtime.mqtt, "Client", FakeClient, raising=False)
    monkeypatch.setattr(runtime.mqtt, "CallbackAPIVersion", types.SimpleNamespace(VERSION2=2), raising=False)
    simulator = runtime.Simulator("pos-transaction", "POSTransactionEvent.schema.json", "sensors/pos-transaction")
    clock = Clock()
    simulator.gate = an_open_gate(clock, hold=30)
    ok = types.SimpleNamespace(is_failure=False)
    client = types.SimpleNamespace(subscribe=lambda *a, **k: None)

    simulator._on_connect(client, None, None, ok)                  # the first connection
    assert simulator.gate.is_open()
    simulator._on_disconnect(client, None, None, None, None)       # the broker went away
    assert not simulator.gate.is_open()
    simulator._on_connect(client, None, None, ok)                  # and came back
    assert not simulator.gate.is_open()
    clock.now += 31
    assert simulator.gate.is_open()


# ------------------------------------------------------------------ Kafka itself

from common.ingest_gate import first_bootstrap_address  # noqa: E402


def a_gate_watching_kafka(clock, kafka, hold=45):
    gate = IngestGate("http://connect:8083", "c", settle_seconds=0, kafka_address=("kafka", 9092), kafka_poll_seconds=1,
                      kafka_hold_seconds=hold, fetch=lambda url, name: running("RUNNING"), kafka_check=lambda address: kafka["up"],
                      clock=clock)
    assert gate.poll_once() is True
    return gate


def test_the_gate_closes_as_soon_as_kafka_cannot_be_reached_even_though_connect_still_says_running():
    # Connect's status lives in a Kafka topic: with Kafka down REST keeps saying RUNNING while Connect
    # revokes its tasks and discards what they had buffered.
    clock, kafka = Clock(), {"up": True}
    gate = a_gate_watching_kafka(clock, kafka)
    kafka["up"] = False
    clock.now += 1
    assert gate.poll_once() is False and not gate.is_open()


def test_the_gate_stays_shut_for_the_recovery_time_after_kafka_returns():
    # Connect's tasks came back 19 to 24 s after Kafka was up again, and REST does not say so until then.
    clock, kafka = Clock(), {"up": True}
    gate = a_gate_watching_kafka(clock, kafka, hold=45)
    kafka["up"] = False
    clock.now += 1
    gate.poll_once()
    clock.now += 20                       # a twenty second outage
    gate.poll_once()
    assert not gate.is_open()
    kafka["up"] = True
    clock.now += 1
    gate.poll_once()
    assert not gate.is_open(), "reachable again is not recovered"
    clock.now += 44
    gate.poll_once()
    assert not gate.is_open()
    clock.now += 1.5
    assert gate.poll_once() is True and gate.is_open()


def test_kafka_is_asked_no_more_often_than_its_own_interval():
    clock = Clock()
    asked = []
    gate = IngestGate("http://connect:8083", "c", settle_seconds=0, kafka_address=("kafka", 9092), kafka_poll_seconds=1,
                      fetch=lambda url, name: running("RUNNING"), kafka_check=lambda address: asked.append(1) or True, clock=clock)
    for _ in range(8):                    # eight polls in two seconds, as the REST poll runs four times a second
        gate.poll_once()
        clock.now += 0.25
    assert len(asked) == 2


def test_a_gate_with_no_kafka_address_does_not_look_at_kafka():
    clock = Clock()
    gate = IngestGate("http://connect:8083", "c", settle_seconds=0, fetch=lambda url, name: running("RUNNING"),
                      kafka_check=lambda address: 1 / 0, clock=clock)
    assert gate.poll_once() is True


@pytest.mark.parametrize("servers, expected", [
    ("kafka:9092", ("kafka", 9092)),
    ("restaurant-platform-kafka-kafka-bootstrap.kafka.svc.cluster.local:9093", ("restaurant-platform-kafka-kafka-bootstrap.kafka.svc.cluster.local", 9093)),
    ("a:1,b:2", ("a", 1)),
    ("", None), (None, None), ("kafka", None), ("kafka:notaport", None),
])
def test_the_first_kafka_bootstrap_address_is_read_from_the_environment_value(servers, expected):
    assert first_bootstrap_address(servers) == expected


def test_the_simulator_watches_the_kafka_it_is_configured_with(monkeypatch):
    monkeypatch.setattr(runtime.mqtt, "Client", FakeClient, raising=False)
    monkeypatch.setattr(runtime.mqtt, "CallbackAPIVersion", types.SimpleNamespace(VERSION2=2), raising=False)
    monkeypatch.setenv("INGEST_GATE_URL", "http://kafka-connect:8083")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    simulator = runtime.Simulator("pos-transaction", "POSTransactionEvent.schema.json", "sensors/pos-transaction")
    assert simulator.gate.kafka_address == ("kafka", 9092) and simulator.gate.kafka_hold_seconds == 45
    assert simulator.gate.reconnect_hold_seconds == 30


# ------------------------------------------------------------------ the Kafka reachability check, on real sockets

import shutil  # noqa: E402
import socket  # noqa: E402
import ssl  # noqa: E402
import subprocess  # noqa: E402
import threading  # noqa: E402

from common.ingest_gate import kafka_is_reachable  # noqa: E402


def listening_socket():
    server = socket.socket()
    server.bind(("127.0.0.1", 0))
    server.listen(5)
    return server


def test_a_plaintext_listener_is_reachable_and_a_closed_port_is_not():
    server = listening_socket()
    address = server.getsockname()
    try:
        assert kafka_is_reachable(address) is True
    finally:
        server.close()
    assert kafka_is_reachable(address) is False


@pytest.fixture
def tls_server(tmp_path):
    if shutil.which("openssl") is None:
        pytest.skip("no openssl to make a certificate with")
    cert, key = tmp_path / "tls.crt", tmp_path / "tls.key"
    subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", str(key), "-out", str(cert), "-days", "1",
                    "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost"], check=True, capture_output=True)
    server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_context.load_cert_chain(str(cert), str(key))
    server = listening_socket()
    stop = threading.Event()

    def serve():
        server.settimeout(0.2)
        while not stop.is_set():
            try:
                conn, _ = server.accept()
            except OSError:
                continue
            try:
                with server_context.wrap_socket(conn, server_side=True):
                    pass
            except (ssl.SSLError, OSError):
                pass

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    yield ("localhost", server.getsockname()[1]), cert
    stop.set()
    thread.join(2)
    server.close()


def test_a_tls_listener_is_reachable_through_a_real_handshake(tls_server):
    address, cert = tls_server
    assert kafka_is_reachable(address, ssl_context=ssl.create_default_context(cafile=str(cert))) is True


def test_a_tls_error_still_means_kafka_is_up_so_a_bad_certificate_cannot_shut_the_gate_for_ever(tls_server):
    address, _cert = tls_server
    untrusting = ssl.create_default_context()  # does not trust the server's self-signed certificate
    assert kafka_is_reachable(address, ssl_context=untrusting) is True


def test_a_tls_listener_that_is_down_is_not_reachable(tls_server):
    address, cert = tls_server
    context = ssl.create_default_context(cafile=str(cert))
    assert kafka_is_reachable(("127.0.0.1", 1), ssl_context=context) is False
