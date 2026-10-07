"""
Tests for mqtt-kafka-bridge: the guarantee that a message is acknowledged to MQTT only once
Kafka has it, and that anything unrecoverable ends the process instead of losing a message.

No MQTT broker, no Kafka: the paho client and the Kafka producer are fakes that record what the
bridge does to them. That real messages survive real Kafka, Mosquitto and bridge outages is checked
on the real stack by tests/resilience/test_failure_recovery.py.

    pip install paho-mqtt prometheus-client pytest
    cd services/mqtt-kafka-bridge && python -m pytest test_bridge.py -v
"""
import os
import sys
import threading
import time
import types

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import bridge  # noqa: E402

ROUTES = {"sensors/a": "a-events", "sensors/b": "b-events"}


class FakeFuture:
    def __init__(self):
        self.callbacks, self.errbacks = [], []

    def add_callback(self, fn):
        self.callbacks.append(fn)

    def add_errback(self, fn):
        self.errbacks.append(fn)

    def confirm(self):
        for fn in self.callbacks:
            fn(types.SimpleNamespace(offset=1))

    def fail(self, exc=None):
        for fn in self.errbacks:
            fn(exc or RuntimeError("kafka said no"))


class FakeProducer:
    def __init__(self, raises=None):
        self.sent, self.futures, self.raises, self.flushed = [], [], raises, False

    def send(self, topic, value=None):
        if self.raises:
            raise self.raises
        self.sent.append((topic, value))
        future = FakeFuture()
        self.futures.append(future)
        return future

    def flush(self, timeout=None):
        self.flushed = True


class FakeClient:
    def __init__(self, ack_raises=None):
        self.subscribed, self.acked, self.ack_raises = [], [], ack_raises

    def subscribe(self, topic, qos=0):
        self.subscribed.append((topic, qos))

    def ack(self, mid, qos):
        if self.ack_raises:
            raise self.ack_raises
        self.acked.append((mid, qos))


def message(topic="sensors/a", payload=b'{"event_id": "x"}', mid=1, qos=1):
    return types.SimpleNamespace(topic=topic, payload=payload, mid=mid, qos=qos)


def a_bridge(producer=None, **kwargs):
    producer = producer or FakeProducer()
    exits = []
    b = bridge.Bridge(ROUTES, lambda: producer, exit_fn=exits.append, sleep=lambda s: None, **kwargs)
    b.client = FakeClient()
    return b, producer, exits


# ------------------------------------------------------------------ routes

def test_the_default_routes_carry_the_four_sensor_topics_to_the_four_kafka_topics():
    assert bridge.parse_routes(bridge.DEFAULT_ROUTES) == {
        "sensors/plate-waste": "plate-waste-events",
        "sensors/pos-transaction": "pos-transaction-events",
        "sensors/service-timing": "service-timing-events",
        "sensors/staff-shift": "staff-shift-events",
    }


@pytest.mark.parametrize("spec", ["", "  ,  ", "sensors/a", "sensors/a=", "=a-events", "sensors/a=a-events,oops"])
def test_a_malformed_or_empty_route_list_is_an_error_not_a_silent_gap(spec):
    with pytest.raises(ValueError):
        bridge.parse_routes(spec)


def test_whitespace_around_routes_is_ignored():
    assert bridge.parse_routes(" sensors/a = a-events , sensors/b=b-events ") == ROUTES


# ------------------------------------------------------------------ connecting

def test_connecting_subscribes_to_every_route_at_qos_1():
    b, _, _ = a_bridge()
    client = FakeClient()
    b.on_connect(client, None, None, types.SimpleNamespace(is_failure=False))
    assert sorted(client.subscribed) == [("sensors/a", 1), ("sensors/b", 1)]


def test_a_refused_connection_subscribes_to_nothing():
    b, _, _ = a_bridge()
    client = FakeClient()
    b.on_connect(client, None, None, types.SimpleNamespace(is_failure=True))
    assert client.subscribed == []


# ------------------------------------------------------------------ the guarantee

def test_a_message_is_not_acknowledged_until_kafka_has_confirmed_it():
    b, producer, _ = a_bridge()
    m = message(mid=7)
    b.forward(m)
    assert producer.sent == [("a-events", b'{"event_id": "x"}')]
    assert b.client.acked == [], "acknowledged before Kafka confirmed: a crash now would lose the message"

    producer.futures[0].confirm()
    assert b.client.acked == [(7, 1)]


def test_a_confirmed_message_is_acknowledged_exactly_once():
    b, producer, _ = a_bridge()
    b.forward(message(mid=3))
    producer.futures[0].confirm()
    producer.futures[0].confirm()  # kafka-python never does this, but a double callback must not double-ack
    assert b.client.acked == [(3, 1)]


def test_payloads_and_topics_pass_through_untouched_and_in_arrival_order():
    b, producer, _ = a_bridge()
    payloads = [b"\x00\x01binary", b'{"a": 1}', b"", "ünïcode".encode()]
    for n, payload in enumerate(payloads):
        b.forward(message(topic="sensors/b" if n % 2 else "sensors/a", payload=payload, mid=n))
    assert [value for _, value in producer.sent] == payloads
    assert [topic for topic, _ in producer.sent] == ["a-events", "b-events", "a-events", "b-events"]


def test_messages_confirmed_out_of_order_are_each_acknowledged_by_their_own_id():
    b, producer, _ = a_bridge()
    for mid in (10, 11, 12):
        b.forward(message(mid=mid))
    producer.futures[2].confirm()
    producer.futures[0].confirm()
    assert b.client.acked == [(12, 1), (10, 1)]
    assert len(b._unconfirmed) == 1, "message 11 is still waiting for Kafka"


def test_a_message_on_a_topic_with_no_route_is_acknowledged_and_dropped_so_it_cannot_block_the_rest():
    b, producer, _ = a_bridge()
    b.forward(message(topic="sensors/unknown", mid=5))
    assert producer.sent == []
    assert b.client.acked == [(5, 1)], "left unacknowledged it would fill Mosquitto's in-flight window for ever"


# ------------------------------------------------------------------ failure

def test_when_kafka_reports_a_failure_the_process_exits_and_the_message_is_not_acknowledged():
    b, producer, exits = a_bridge()
    b.forward(message(mid=9))
    producer.futures[0].fail()
    assert exits == [1]
    assert b.client.acked == []


def test_when_the_producer_cannot_even_accept_a_message_the_process_exits_without_acknowledging():
    b, _, exits = a_bridge(producer=FakeProducer(raises=TimeoutError("no metadata")))
    b.forward(message(mid=2))
    assert exits == [1]
    assert b.client.acked == []


def test_a_failed_acknowledgement_is_survivable_because_mosquitto_will_deliver_the_message_again():
    b, producer, exits = a_bridge()
    b.client = FakeClient(ack_raises=RuntimeError("not connected"))
    b.forward(message(mid=4))
    producer.futures[0].confirm()
    assert exits == [], "a lost acknowledgement is the at-least-once rule working, not a reason to die"


# ------------------------------------------------------------------ the watchdog

def test_the_watchdog_ends_a_process_whose_oldest_message_has_waited_too_long_for_kafka():
    now = {"t": 1000.0}
    b, producer, exits = a_bridge(watchdog_seconds=60, clock=lambda: now["t"])
    b.forward(message(mid=1))
    now["t"] += 59
    b.watchdog_once()
    assert exits == []
    now["t"] += 2
    b.watchdog_once()
    assert exits == [1]


def test_the_watchdog_is_quiet_when_nothing_is_waiting_or_everything_was_confirmed():
    now = {"t": 1000.0}
    b, producer, exits = a_bridge(watchdog_seconds=60, clock=lambda: now["t"])
    now["t"] += 10_000
    b.watchdog_once()
    b.forward(message(mid=1))
    producer.futures[0].confirm()
    now["t"] += 10_000
    b.watchdog_once()
    assert exits == []


# ------------------------------------------------------------------ start-up and shutdown

def test_a_kafka_that_is_not_ready_is_retried_and_no_message_is_lost_meanwhile():
    attempts = []
    producer = FakeProducer()

    def make_producer():
        attempts.append(1)
        if len(attempts) < 3:
            raise ConnectionError("no brokers")
        return producer

    exits, pauses = [], []
    b = bridge.Bridge(ROUTES, make_producer, exit_fn=exits.append, sleep=pauses.append)
    b.client = FakeClient()
    b.forward(message(mid=1))
    assert len(attempts) == 3 and pauses == [1.0, 2.0]
    assert producer.sent == [("a-events", b'{"event_id": "x"}')] and exits == []


def test_the_forward_loop_hands_queued_messages_to_kafka_and_stops_when_asked():
    b, producer, _ = a_bridge()
    thread = threading.Thread(target=b.forward_loop, daemon=True)
    thread.start()
    for mid in range(3):
        b.on_message(None, None, message(mid=mid))
    deadline = time.time() + 5
    while len(producer.sent) < 3 and time.time() < deadline:
        time.sleep(0.01)
    assert len(producer.sent) == 3
    b.stop()
    thread.join(5)
    assert not thread.is_alive()


def test_stopping_lets_kafka_settle_what_it_has_and_leaves_the_rest_with_the_broker():
    b, producer, _ = a_bridge()
    b.forward(message(mid=1))
    b.stop()
    assert producer.flushed
    assert b.client.acked == [], "nothing was confirmed, so nothing may be acknowledged"


# ------------------------------------------------------------------ the MQTT client's configuration

def test_the_mqtt_client_has_a_persistent_session_manual_acknowledgement_and_a_fixed_client_id(monkeypatch):
    seen = {}

    class Recorder:
        def __init__(self, **kwargs):
            seen.update(kwargs)
            self.tls = None

        def tls_set(self, ca_certs=None):
            self.tls = ca_certs

        def reconnect_delay_set(self, min_delay, max_delay):
            pass

    monkeypatch.setattr(bridge.mqtt, "Client", Recorder)
    monkeypatch.delenv("MQTT_TLS_ENABLED", raising=False)
    monkeypatch.delenv("BRIDGE_CLIENT_ID", raising=False)
    b = bridge.Bridge(ROUTES, lambda: None)
    client, host, port = bridge.build_client(b)
    assert seen["clean_session"] is False, "a clean session discards everything published while the bridge is away"
    assert seen["manual_ack"] is True, "without manual acknowledgement a message is acknowledged on arrival, before Kafka has it"
    assert seen["client_id"] == "rp-mqtt-kafka-bridge"
    assert client.tls is None and b.client is client


def test_tls_is_switched_on_by_the_environment_with_the_mounted_ca(monkeypatch):
    class Recorder:
        def __init__(self, **kwargs):
            self.tls = None

        def tls_set(self, ca_certs=None):
            self.tls = ca_certs

        def reconnect_delay_set(self, min_delay, max_delay):
            pass

    monkeypatch.setattr(bridge.mqtt, "Client", Recorder)
    monkeypatch.setenv("MQTT_TLS_ENABLED", "true")
    monkeypatch.setenv("MQTT_PORT", "8883")
    client, _host, port = bridge.build_client(bridge.Bridge(ROUTES, lambda: None))
    assert client.tls == "/etc/mosquitto-tls/tls.crt" and port == 8883


def recorder(login_calls):
    class Recorder:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def tls_set(self, ca_certs=None):
            pass

        def username_pw_set(self, username, password=None):
            login_calls.append((username, password))

        def reconnect_delay_set(self, min_delay, max_delay):
            pass

    return Recorder


def test_the_bridge_logs_in_to_the_broker_with_the_username_and_password_from_its_environment(monkeypatch):
    calls = []
    monkeypatch.setattr(bridge.mqtt, "Client", recorder(calls))
    monkeypatch.setenv("MQTT_USERNAME", "rp-mqtt-kafka-bridge")
    monkeypatch.setenv("MQTT_PASSWORD", "from-a-secret")
    bridge.build_client(bridge.Bridge(ROUTES, lambda: None))
    assert calls == [("rp-mqtt-kafka-bridge", "from-a-secret")]


def test_with_no_username_set_the_bridge_sends_no_login_at_all(monkeypatch):
    # An anonymous connection, which only a broker that still allows anonymous clients accepts (the staged cutover).
    calls = []
    monkeypatch.setattr(bridge.mqtt, "Client", recorder(calls))
    monkeypatch.delenv("MQTT_USERNAME", raising=False)
    monkeypatch.delenv("MQTT_PASSWORD", raising=False)
    bridge.build_client(bridge.Bridge(ROUTES, lambda: None))
    assert calls == []


def test_a_subscription_the_broker_refuses_takes_the_bridge_out_of_the_connected_state_so_the_probe_and_alert_see_it():
    # Mosquitto does not report a refused subscription (it grants it and delivers nothing; see
    # tests/integration/test_mosquitto_auth.py), but a broker that does must not leave the bridge looking healthy.
    b = bridge.Bridge(ROUTES, lambda: None)
    bridge.MQTT_CONNECTED.set(1)
    granted = types.SimpleNamespace(is_failure=False)
    refused = types.SimpleNamespace(is_failure=True)
    b.on_subscribe(None, None, 1, [granted, granted])
    assert bridge.MQTT_CONNECTED._value.get() == 1
    b.on_subscribe(None, None, 2, [granted, refused])
    assert bridge.MQTT_CONNECTED._value.get() == 0


def test_the_client_is_given_the_subscription_handler(monkeypatch):
    seen = {}

    class Recorder:
        def __init__(self, **kwargs):
            pass

        def reconnect_delay_set(self, min_delay, max_delay):
            pass

        def __setattr__(self, name, value):
            seen[name] = value

    monkeypatch.setattr(bridge.mqtt, "Client", Recorder)
    monkeypatch.delenv("MQTT_USERNAME", raising=False)
    monkeypatch.delenv("MQTT_TLS_ENABLED", raising=False)
    b = bridge.Bridge(ROUTES, lambda: None)
    bridge.build_client(b)
    assert seen["on_subscribe"] == b.on_subscribe


# ------------------------------------------------------------------ the health probe

def test_the_check_passes_only_when_the_running_bridge_reports_it_is_connected(monkeypatch, capsys):
    class Body:
        def __init__(self, text):
            self.text = text

        def read(self):
            return self.text.encode()

    for text, expected in [("bridge_mqtt_connected 1.0\n", 0), ("bridge_mqtt_connected 0.0\n", 1), ("", 1)]:
        monkeypatch.setattr("urllib.request.urlopen", lambda url, timeout=0, t=text: Body(t))
        assert bridge.check() == expected, text


def test_the_check_fails_rather_than_raises_when_the_metrics_cannot_be_reached(monkeypatch):
    def refuse(url, timeout=0):
        raise OSError("connection refused")

    monkeypatch.setattr("urllib.request.urlopen", refuse)
    assert bridge.check() == 1


# ------------------------------------------------------------------ the real producer's configuration

def test_the_real_kafka_producer_accepts_the_bridges_settings():
    # The tests above fake the producer, so a settings mistake would only show on a real stack: kafka-python
    # rejects delivery_timeout_ms <= linger_ms + request_timeout_ms, and the bridge sat in its "Kafka is not
    # ready" retry loop forwarding nothing until a real run showed it. Building the real producer (it does not
    # connect when the api_version is given) checks every setting for real.
    kafka = pytest.importorskip("kafka")
    os.environ["KAFKA_BOOTSTRAP_SERVERS"] = "127.0.0.1:1"
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
    producer = bridge.build_producer()
    try:
        assert isinstance(producer, kafka.KafkaProducer)
        assert producer.config["acks"] == -1 or producer.config["acks"] == "all"
    finally:
        producer.close(timeout=0)
