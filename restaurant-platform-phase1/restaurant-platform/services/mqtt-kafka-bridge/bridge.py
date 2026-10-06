"""
mqtt-kafka-bridge: carries every sensor event from MQTT into Kafka without losing one.

Replaces the four Apache Camel MQTT source connectors that ran inside Kafka Connect.
Those subscribed with clean sessions, so whatever the sensors published while the bridge
was down, restarting or not yet subscribed was gone, and nothing could be done about it
from the outside: their status lied (Connect kept saying RUNNING while Kafka was down and
it had revoked every task), a persistent session deadlocked their start-up, and the
simulator-side workarounds that were built around them (DEF-148, DEF-151) could only
shrink the loss, never remove it. See docs/quality/06-defect-log.md, DEF-148 to DEF-152.

The guarantee here is at-least-once, from the sensor's broker to Kafka, and it rests on
three things working together:

  1. A persistent MQTT session (clean_session=False, a fixed client id) and QoS 1. While
     this process is down, restarting or stuck, Mosquitto keeps every message published
     to its topics and delivers them when the session returns.
  2. Manual acknowledgement. A message is acknowledged to Mosquitto only after Kafka has
     confirmed the write (acks=all). Until then Mosquitto still owns it, and will deliver
     it again after any disconnect. Mosquitto's in-flight window (max_inflight_messages,
     20) therefore also bounds how many messages this process ever holds at once.
  3. Crash on anything unrecoverable. If Kafka cannot take a message (the producer gave
     up) or the oldest unconfirmed message is too old, the process exits and its
     supervisor (Compose's restart policy, Kubernetes) starts it again. Nothing is lost by
     exiting: the unconfirmed messages were never acknowledged.

What it does not promise is exactly-once. A crash between Kafka's confirmation and the
acknowledgement makes Mosquitto deliver that message again, so it can reach Kafka twice;
the storage consumer's event_id uniqueness makes the second copy harmless (NFR-REL-04).

Payloads pass through untouched (the storage consumer validates them against the schemas).
"""
import logging
import os
import queue
import signal
import sys
import threading
import time

import paho.mqtt.client as mqtt
from prometheus_client import Counter, Gauge, start_http_server

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
log = logging.getLogger("mqtt-kafka-bridge")

DEFAULT_ROUTES = (
    "sensors/plate-waste=plate-waste-events,"
    "sensors/pos-transaction=pos-transaction-events,"
    "sensors/service-timing=service-timing-events,"
    "sensors/staff-shift=staff-shift-events"
)

FORWARDED = Counter("bridge_messages_forwarded_total", "Messages confirmed by Kafka and acknowledged to MQTT", ["kafka_topic"])
KAFKA_ERRORS = Counter("bridge_kafka_errors_total", "Messages Kafka could not take (each one ends the process)")
UNKNOWN_TOPIC = Counter("bridge_unrouted_messages_total", "Messages on a topic with no route (acknowledged and dropped)")
MQTT_CONNECTED = Gauge("bridge_mqtt_connected", "1 while connected to the MQTT broker")
UNCONFIRMED = Gauge("bridge_unconfirmed_messages", "Messages handed to Kafka and not yet confirmed")
OLDEST_UNCONFIRMED = Gauge("bridge_oldest_unconfirmed_seconds", "Age of the oldest message Kafka has not confirmed")


def parse_routes(spec: str) -> dict[str, str]:
    """'mqtt/topic=kafka-topic,...' -> {'mqtt/topic': 'kafka-topic'}; anything malformed is an error, not a silent gap."""
    routes = {}
    for item in filter(None, (part.strip() for part in spec.split(","))):
        mqtt_topic, sep, kafka_topic = item.partition("=")
        if not sep or not mqtt_topic.strip() or not kafka_topic.strip():
            raise ValueError(f"bad route {item!r}: expected <mqtt topic>=<kafka topic>")
        routes[mqtt_topic.strip()] = kafka_topic.strip()
    if not routes:
        raise ValueError("no routes configured: the bridge would carry nothing")
    return routes


class Bridge:
    """
    The forwarding logic, with the MQTT client and the Kafka producer passed in so it can be
    tested without either. `make_producer` is called, with retries, on the worker thread, so
    the MQTT connection (and its subscription, which starts Mosquitto queueing for us) comes up
    first even when Kafka is not ready yet.
    """

    def __init__(self, routes, make_producer, watchdog_seconds=300.0, exit_fn=None, clock=time.monotonic, sleep=time.sleep):
        self.routes = routes
        self._make_producer = make_producer
        self._producer = None
        self.watchdog_seconds = watchdog_seconds
        self._exit = exit_fn or self._default_exit
        self._clock = clock
        self._sleep = sleep
        self.client = None  # set by main(): the paho client, needed to acknowledge
        self.inbox = queue.Queue()
        self._lock = threading.Lock()
        self._unconfirmed = {}  # id(message) -> time handed to Kafka
        self._stopping = threading.Event()

    # ------------------------------------------------------------------ MQTT callbacks (the network thread)

    def on_connect(self, client, userdata, flags, reason_code, properties=None):
        if reason_code.is_failure:
            log.error("MQTT connection refused: %s", reason_code)
            return
        # The subscription is part of the persistent session and survives a reconnect; repeating it
        # is harmless and covers a session the broker lost (a restart without persistence).
        for topic in self.routes:
            client.subscribe(topic, qos=1)
        MQTT_CONNECTED.set(1)
        log.info("connected to MQTT; subscribed (QoS 1, persistent session) to %s", ", ".join(self.routes))

    def on_disconnect(self, client, userdata, flags, reason_code, properties=None):
        MQTT_CONNECTED.set(0)
        log.warning("disconnected from MQTT (%s); Mosquitto keeps everything not yet acknowledged", reason_code)

    def on_message(self, client, userdata, message):
        # Hand off at once: the network thread must keep answering keepalives however slow Kafka is.
        self.inbox.put(message)

    # ------------------------------------------------------------------ the worker thread

    def get_producer(self):
        delay = 1.0
        while self._producer is None and not self._stopping.is_set():
            try:
                self._producer = self._make_producer()
                log.info("connected to Kafka")
            except Exception as exc:
                log.warning("Kafka is not ready (%s); retrying in %.0fs. Mosquitto is holding the messages.", exc, delay)
                self._sleep(delay)
                delay = min(delay * 2, 15.0)
        return self._producer

    def forward_loop(self):
        while not self._stopping.is_set():
            try:
                message = self.inbox.get(timeout=0.5)
            except queue.Empty:
                continue
            self.forward(message)

    def forward(self, message):
        kafka_topic = self.routes.get(message.topic)
        if kafka_topic is None:
            # Acknowledge, or it would sit in the in-flight window for ever and starve the real topics.
            UNKNOWN_TOPIC.inc()
            log.error("no route for MQTT topic %r; dropping the message", message.topic)
            self._acknowledge(message)
            return
        producer = self.get_producer()
        if producer is None:  # shutting down
            return
        with self._lock:
            self._unconfirmed[id(message)] = self._clock()
            UNCONFIRMED.set(len(self._unconfirmed))
        try:
            future = producer.send(kafka_topic, value=bytes(message.payload))
        except Exception as exc:  # the producer could not even accept it (metadata timeout, buffer, closed)
            self._failed(message, exc)
            return
        future.add_callback(lambda _metadata, m=message, t=kafka_topic: self._confirmed(m, t))
        future.add_errback(lambda exc, m=message: self._failed(m, exc))

    def _confirmed(self, message, kafka_topic):
        """Kafka has the message on every in-sync replica: now, and only now, tell Mosquitto it can forget it."""
        with self._lock:
            known = self._unconfirmed.pop(id(message), None) is not None
            UNCONFIRMED.set(len(self._unconfirmed))
        if not known:  # already confirmed: acknowledge once
            return
        self._acknowledge(message)
        FORWARDED.labels(kafka_topic).inc()

    def _failed(self, message, exc):
        KAFKA_ERRORS.inc()
        log.error("Kafka did not take a message from %r (%s: %s); exiting so nothing is acknowledged that was not stored",
                  message.topic, type(exc).__name__, exc)
        self.fatal()

    def _acknowledge(self, message):
        try:
            self.client.ack(message.mid, message.qos)
        except Exception as exc:
            # The connection this message arrived on is gone; Mosquitto will deliver it again. That is the
            # at-least-once rule working, not a fault.
            log.warning("could not acknowledge message %s (%s); it will be delivered again", message.mid, exc)

    # ------------------------------------------------------------------ supervision

    def oldest_unconfirmed_age(self):
        with self._lock:
            if not self._unconfirmed:
                return 0.0
            return self._clock() - min(self._unconfirmed.values())

    def watchdog_once(self):
        age = self.oldest_unconfirmed_age()
        OLDEST_UNCONFIRMED.set(age)
        if age > self.watchdog_seconds:
            log.error("the oldest message has waited %.0fs for Kafka (limit %.0fs); exiting to start afresh", age, self.watchdog_seconds)
            self.fatal()

    def watchdog_loop(self):
        while not self._stopping.wait(5.0):
            self.watchdog_once()

    def fatal(self):
        self._exit(1)

    @staticmethod
    def _default_exit(code):
        logging.shutdown()
        os._exit(code)

    def stop(self):
        """Orderly shutdown: stop forwarding, let Kafka settle what it has, leave the rest queued at the broker."""
        self._stopping.set()
        if self._producer is not None:
            try:
                self._producer.flush(timeout=5)
            except Exception as exc:
                log.warning("flush on shutdown failed (%s); unconfirmed messages will be delivered again", exc)


# ---------------------------------------------------------------------- wiring

def build_producer():
    from kafka import KafkaProducer

    import phase5_common as common

    return KafkaProducer(
        bootstrap_servers=common.KAFKA_BOOTSTRAP_SERVERS,
        api_version=common.KAFKA_API_VERSION,
        **common.KAFKA_TLS_KWARGS,
        acks="all",                              # confirmed only once every in-sync replica has it
        retries=2147483647,                      # retriable errors are retried until the batch times out
        max_in_flight_requests_per_connection=1,  # a retry must not reorder messages
        request_timeout_ms=int(os.environ.get("BRIDGE_KAFKA_REQUEST_TIMEOUT_MS", "30000")),
        # How long a message may wait for Kafka, retrying, before its send fails and the process exits to start
        # afresh. kafka-python requires it to exceed linger_ms + request_timeout_ms.
        delivery_timeout_ms=int(os.environ.get("BRIDGE_KAFKA_DELIVERY_TIMEOUT_MS", "120000")),
        max_block_ms=int(os.environ.get("BRIDGE_KAFKA_MAX_BLOCK_MS", "60000")),
        linger_ms=5,
    )


def build_client(bridge: Bridge):
    host = os.environ.get("MQTT_HOST", "mosquitto.kafka.svc.cluster.local")
    port = int(os.environ.get("MQTT_PORT", "1883"))
    client = mqtt.Client(
        client_id=os.environ.get("BRIDGE_CLIENT_ID", "rp-mqtt-kafka-bridge"),  # fixed: it names the persistent session
        clean_session=False,
        manual_ack=True,
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
    )
    if os.environ.get("MQTT_TLS_ENABLED", "false").lower() == "true":
        client.tls_set(ca_certs=os.environ.get("MQTT_TLS_CA_FILE", "/etc/mosquitto-tls/tls.crt"))
    client.on_connect = bridge.on_connect
    client.on_disconnect = bridge.on_disconnect
    client.on_message = bridge.on_message
    client.reconnect_delay_set(min_delay=1, max_delay=10)
    bridge.client = client
    return client, host, port


def check() -> int:
    """`python bridge.py --check`: exit 0 only if the running bridge is connected to MQTT (a Compose/Kubernetes health probe)."""
    import urllib.request

    try:
        body = urllib.request.urlopen(f"http://localhost:{os.environ.get('BRIDGE_METRICS_PORT', '8000')}/metrics", timeout=3).read().decode()
    except OSError as exc:
        print(f"not healthy: metrics unreachable ({exc})")
        return 1
    healthy = "bridge_mqtt_connected 1.0" in body
    print("healthy" if healthy else "not healthy: not connected to MQTT")
    return 0 if healthy else 1


def main():
    if "--check" in sys.argv[1:]:
        return check()
    routes = parse_routes(os.environ.get("BRIDGE_ROUTES", DEFAULT_ROUTES))
    bridge = Bridge(routes, build_producer, watchdog_seconds=float(os.environ.get("BRIDGE_WATCHDOG_SECONDS", "300")))
    client, host, port = build_client(bridge)
    start_http_server(int(os.environ.get("BRIDGE_METRICS_PORT", "8000")))

    def shutdown(signum, frame):
        log.info("signal %s: shutting down", signum)
        bridge.stop()
        client.disconnect()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    threading.Thread(target=bridge.forward_loop, name="forward", daemon=True).start()
    threading.Thread(target=bridge.watchdog_loop, name="watchdog", daemon=True).start()

    log.info("connecting to MQTT at %s:%s", host, port)
    # Retries the first connection too: the broker may not be up yet, and the session (and Mosquitto's
    # queue) only exists once this has connected once.
    client.connect_async(host, port, keepalive=30)
    client.loop_forever(retry_first_connection=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
