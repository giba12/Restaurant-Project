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
