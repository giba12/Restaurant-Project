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
"""

import json
import logging
import os
import random
import signal
import sys
import threading
import time

import jsonschema
import paho.mqtt.client as mqtt

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)


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

        schema_dir = os.environ.get("SCHEMA_DIR", "/app/schemas")
        schema_path = os.path.join(schema_dir, schema_filename)
        with open(schema_path, "r") as f:
            self.schema = json.load(f)
        self.validator = jsonschema.Draft202012Validator(self.schema)

        # MQTT 5, not 3.1.1, so the broker can say "not authorized": with 3.1.1 a publish the broker's access rules refuse
        # is acknowledged as a success and silently dropped, and this simulator would log an event as published that
        # never left it (the loss ledger, tests/resilience, trusts that log line).
        self.client = mqtt.Client(
            client_id=self.source_id,
            protocol=mqtt.MQTTv5,
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        )
        self._refused_publishes = {}
        if self.mqtt_tls_enabled:
            self.client.tls_set(ca_certs=self.mqtt_tls_ca_file)
        # Broker credentials (the broker refuses anonymous clients and limits each user to its own topics). Unset
        # means an anonymous connection, which only a broker still allowing anonymous clients will accept.
        if os.environ.get("MQTT_USERNAME"):
            self.client.username_pw_set(os.environ["MQTT_USERNAME"], os.environ.get("MQTT_PASSWORD", ""))
        self._connected_callbacks = []
        self._stopping = threading.Event()
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
        self.client.on_publish = self._on_publish

    def _on_connect(self, client, userdata, flags, reason_code, properties=None):
        if reason_code.is_failure:
            self.log.error("mqtt CONNACK failure: %s", reason_code)
        else:
            # Subscriptions do not survive a reconnect with a clean session, so
            # they are re-established on every successful connect.
            for topic in self._handlers:
                client.subscribe(topic, qos=1)
            self._connected_event.set()
            for callback in self._connected_callbacks:
                try:
                    callback()
                except Exception:
                    self.log.exception("a connected callback failed")

    def on_connected(self, callback) -> None:
        """Call `callback()` after every successful connect (including reconnects), on the network thread."""
        self._connected_callbacks.append(callback)

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

    def _on_publish(self, client, userdata, mid, reason_code, properties=None):
        if reason_code.is_failure:
            self._refused_publishes[mid] = reason_code

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
        self.validate(event)
        payload = json.dumps(event)
        result = self.client.publish(self.mqtt_topic, payload, qos=1)
        result.wait_for_publish(timeout=5)
        refused = self._refused_publishes.pop(result.mid, None)
        if refused is not None:
            # Not logged as "published": that line is the record of what reached the broker.
            raise PermissionError(f"the broker refused {event['event_type']} event_id={event['event_id']} on {self.mqtt_topic}: {refused}")
        self.log.info("published %s event_id=%s", event["event_type"], event["event_id"])

    def sleep_poisson_interval(self) -> None:
        mean_seconds = 60.0 / max(self.rate_per_minute, 0.01)
        interval = random.expovariate(1.0 / mean_seconds)
        # floor at 0.5s so a very small draw doesn't hammer the broker
        # wait(), not sleep(): a stop request ends the wait at once
        self._stopping.wait(max(interval, 0.5))

    def request_stop(self, *_signal_args) -> None:
        """Ask the event loop to finish: it stops publishing, tells the broker it is leaving, and run_forever returns."""
        self._stopping.set()

    def run_forever(self, generate_event_fn):
        """generate_event_fn: () -> dict, called once per loop iteration."""
        self.connect()
        self.log.info(
            "starting event loop: sensor_type=%s rate=%.2f/min",
            self.sensor_type, self.rate_per_minute,
        )
        # A container's first process ignores SIGTERM unless it handles it, so without this a simulator being replaced kept running,
        # connected, for Kubernetes's whole 30 s grace period. Every simulator connects to the broker as its SOURCE_ID and the
        # broker lets a second connection under the same id take the first one over, so the old pod and its replacement kicked each
        # other off the broker for those 30 s, and a command to the node was answered by whichever was connected, the old one
        # (holding the old key) included. Now: stop at once and say goodbye to the broker. Only from the main thread (where signal
        # handlers can be set); anything else that runs the loop in a thread asks with request_stop().
        previous = {}
        if threading.current_thread() is threading.main_thread():
            for signum in (signal.SIGTERM, signal.SIGINT):
                previous[signum] = signal.signal(signum, self.request_stop)
        try:
            self._event_loop(generate_event_fn)
        finally:
            for signum, handler in previous.items():
                signal.signal(signum, handler)
        self.log.info("stopping: leaving the broker")
        self.client.loop_stop()
        self.client.disconnect()

    def _event_loop(self, generate_event_fn):
        while not self._stopping.is_set():
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
