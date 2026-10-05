"""
Shared simulator runtime.

Responsibilities kept deliberately narrow and identical across all four
simulators:
  1. Load this sensor type's JSON Schema once at startup.
  2. Validate every generated event against that schema BEFORE publishing.
     A validation failure is a bug in the generator, not a data problem,
     so it is fatal (raises) rather than logged-and-skipped -- silently
     dropping invalid events would hide the bug and could let a
     non-conformant message reach Kafka on a future code change.
  3. Publish conformant events to MQTT at a non-uniform (Poisson process)
     rate, matching the Phase 1 requirement that edge devices do not
     emit on a fixed clock tick.
  4. Log every publish so `kubectl logs` gives a visible, auditable event
     stream independent of the Kafka side.
  5. Hold events while the bridge into Kafka is not running, and send them in
     order when it is (common/ingest_gate.py, opt-in via INGEST_GATE_URL).
"""

import collections
import json
import logging
import os
import random
import sys
import threading
import time

import jsonschema
import paho.mqtt.client as mqtt

from common.ingest_gate import IngestGate

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)


# How long events may be held while the bridge is down before the oldest are dropped
# (about 8 MB of events at the default size; hours at the real rates). Dropping is
# logged: a bounded buffer that fails loudly beats an unbounded one that takes the pod down.
OUTBOX_MAX_EVENTS = int(os.environ.get("OUTBOX_MAX_EVENTS", "5000"))
FLUSH_INTERVAL_SECONDS = 0.5


class Simulator:
    def __init__(self, sensor_type: str, schema_filename: str, mqtt_topic: str):
        self.sensor_type = sensor_type
        self.log = logging.getLogger(sensor_type)

        self.mqtt_host = os.environ.get("MQTT_HOST", "mosquitto.kafka.svc.cluster.local")
        self.mqtt_port = int(os.environ.get("MQTT_PORT", "1883"))
        self.mqtt_topic = os.environ.get("MQTT_TOPIC", mqtt_topic)
        # Opt-in TLS: defaults to today's exact plaintext behavior (no
        # tls_set() call at all) so Compose needs no changes -- same pattern
        # as services/phase5_common.py's KAFKA_TLS_KWARGS. A k8s chart
        # switches this simulator over by setting MQTT_TLS_ENABLED=true,
        # pointing MQTT_PORT at 8883, and mounting Mosquitto's self-signed
        # cert (k8s/mosquitto-tls/) at MQTT_TLS_CA_FILE's path.
        self.mqtt_tls_enabled = os.environ.get("MQTT_TLS_ENABLED", "false").lower() == "true"
        self.mqtt_tls_ca_file = os.environ.get("MQTT_TLS_CA_FILE", "/etc/mosquitto-tls/tls.crt")
        self.source_id = os.environ.get("SOURCE_ID", f"sim-{sensor_type}-01")

        # events per minute (Poisson rate parameter lambda); interval between
        # events is drawn from an exponential distribution with mean 60/rate
        self.rate_per_minute = float(os.environ.get("EVENT_RATE_PER_MIN", "6"))

        # Store-and-forward: events wait here while this sensor's connector is not running.
        self._outbox: collections.deque = collections.deque(maxlen=OUTBOX_MAX_EVENTS)
        self._dropped = 0
        self.gate = IngestGate(
            os.environ.get("INGEST_GATE_URL"),
            os.environ.get("INGEST_GATE_CONNECTOR", f"{sensor_type}-source-connector"),
            poll_seconds=float(os.environ.get("INGEST_GATE_POLL_SECONDS", "0.25")),
            settle_seconds=float(os.environ.get("INGEST_GATE_SETTLE_SECONDS", "20")),
        )

        schema_dir = os.environ.get("SCHEMA_DIR", "/app/schemas")
        schema_path = os.path.join(schema_dir, schema_filename)
        with open(schema_path, "r") as f:
            self.schema = json.load(f)
        self.validator = jsonschema.Draft202012Validator(self.schema)

        self.client = mqtt.Client(
            client_id=self.source_id,
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        )
        if self.mqtt_tls_enabled:
            self.client.tls_set(ca_certs=self.mqtt_tls_ca_file)
        # client.connect() only opens the socket and sends the CONNECT packet
        # -- it does not wait for the broker's CONNACK, which is only read
        # once loop_start()'s background thread is running. Without this,
        # connect() below could return "connected" and let run_forever()
        # start publishing before the MQTT-level handshake had actually
        # finished. Plaintext's round trip was fast enough this never
        # showed up in practice; TLS's extra handshake latency was enough to
        # expose it live (publish failing with "client is not currently
        # connected" in a tight, never-crashing, never-recovering loop,
        # since nothing was waiting for or checking CONNACK at all).
        self._connected_event = threading.Event()
        self._handlers = {}
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message

    def _on_connect(self, client, userdata, flags, reason_code, properties=None):
        if reason_code.is_failure:
            self.log.error("mqtt CONNACK failure: %s", reason_code)
        else:
            # Subscriptions do not survive a reconnect with a clean session, so
            # they are re-established on every successful connect.
            for topic in self._handlers:
                client.subscribe(topic, qos=1)
            self._connected_event.set()

    def subscribe(self, topic: str, handler) -> None:
        """Call `handler(payload_bytes)` for every message on `topic`, across reconnects."""
        self._handlers[topic] = handler
        if self._connected_event.is_set():
            self.client.subscribe(topic, qos=1)

    def _on_message(self, client, userdata, message):
        handler = self._handlers.get(message.topic)
        if handler is not None:
            try:
                handler(message.payload)
            except Exception:
                self.log.exception("handler for %s failed", message.topic)

    def publish_state(self, topic: str, payload: str) -> None:
        """Publish shared simulated-world state: retained, so a late subscriber sees it at once."""
        self.client.publish(topic, payload, qos=1, retain=True)

    def _on_disconnect(self, client, userdata, flags, reason_code, properties=None):
        self._connected_event.clear()

    def connect(self):
        self.log.info(
            "connecting to mqtt broker %s:%s topic=%s",
            self.mqtt_host, self.mqtt_port, self.mqtt_topic,
        )
        # Retry loop: the broker pod may not be ready yet on cluster
        # (re)start, and this is a long-running pod, not a one-shot job --
        # crashing on the first failed connection would just cause a
        # CrashLoopBackOff for no reason when the fix is "wait a few seconds".
        backoff = 2
        while True:
            try:
                self._connected_event.clear()
                self.client.connect(self.mqtt_host, self.mqtt_port, keepalive=60)
                self.client.loop_start()
                if not self._connected_event.wait(timeout=10):
                    raise TimeoutError("no CONNACK received within 10s")
                self.log.info("connected")
                return
            except Exception as exc:
                self.client.loop_stop()
                self.log.warning("mqtt connect failed (%s), retrying in %ss", exc, backoff)
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)

    def validate(self, event: dict) -> None:
        errors = sorted(self.validator.iter_errors(event), key=lambda e: e.path)
        if errors:
            for err in errors:
                self.log.error("SCHEMA VIOLATION at %s: %s", list(err.path), err.message)
            raise jsonschema.exceptions.ValidationError(
                f"{len(errors)} schema violation(s) generating {self.sensor_type} event; refusing to publish"
            )

    def publish(self, event: dict) -> None:
        """Validate the event and queue it; it is sent now if the bridge is running, otherwise when it is."""
        self.validate(event)
        if len(self._outbox) == self._outbox.maxlen:
            self._dropped += 1
            if self._dropped == 1 or self._dropped % 100 == 0:
                self.log.error("outbox full (%d events) while the bridge is down: dropped %d event(s), oldest first",
                               len(self._outbox), self._dropped)
        self._outbox.append((event["event_type"], event["event_id"], json.dumps(event)))
        self.flush()

    def flush(self) -> None:
        """Send held events, oldest first, for as long as the bridge is running."""
        while self._outbox and self.gate.is_open():
            event_type, event_id, payload = self._outbox.popleft()
            # Out of the outbox before the broker has acknowledged: paho keeps an unacknowledged
            # QoS 1 message and resends it itself after a reconnect, so sending it again here would
            # put it into Kafka twice.
            result = self.client.publish(self.mqtt_topic, payload, qos=1)
            result.wait_for_publish(timeout=5)
            self.log.info("published %s event_id=%s", event_type, event_id)

    def sleep_poisson_interval(self) -> None:
        mean_seconds = 60.0 / max(self.rate_per_minute, 0.01)
        interval = random.expovariate(1.0 / mean_seconds)
        # floor at 0.5s so a very small draw doesn't hammer the broker
        deadline = time.monotonic() + max(interval, 0.5)
        while True:
            # Held events go out as soon as the bridge is back, not at the next event's turn.
            try:
                self.flush()
            except Exception:
                self.log.exception("unexpected error sending held events, will retry")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            time.sleep(min(remaining, FLUSH_INTERVAL_SECONDS))

    def run_forever(self, generate_event_fn):
        """generate_event_fn: () -> dict, called once per loop iteration."""
        self.connect()
        self.gate.start()
        self.log.info(
            "starting event loop: sensor_type=%s rate=%.2f/min",
            self.sensor_type, self.rate_per_minute,
        )
        while True:
            try:
                event = generate_event_fn()
                self.publish(event)
            except jsonschema.exceptions.ValidationError:
                # Fatal: a generator bug producing non-conformant events
                # must stop the pod (visible CrashLoopBackOff) rather than
                # keep silently emitting bad data.
                self.log.exception("fatal schema violation, exiting")
                sys.exit(1)
            except Exception:
                # Transient publish/connection errors: log and keep going,
                # this is a long-running simulator, not a batch job.
                self.log.exception("unexpected error publishing event, continuing")
            self.sleep_poisson_interval()
