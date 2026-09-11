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
import sys
import time

import jsonschema
import paho.mqtt.client as mqtt

from common.ids import new_event_id, now_iso  # re-exported for generators

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
        self.source_id = os.environ.get("SOURCE_ID", f"sim-{sensor_type}-01")

        # events per minute (Poisson rate parameter lambda); interval between
        # events is drawn from an exponential distribution with mean 60/rate
        self.rate_per_minute = float(os.environ.get("EVENT_RATE_PER_MIN", "6"))

        schema_dir = os.environ.get("SCHEMA_DIR", "/app/schemas")
        schema_path = os.path.join(schema_dir, schema_filename)
        with open(schema_path, "r") as f:
            self.schema = json.load(f)
        self.validator = jsonschema.Draft202012Validator(self.schema)

        self.client = mqtt.Client(
            client_id=self.source_id,
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        )

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
                self.client.connect(self.mqtt_host, self.mqtt_port, keepalive=60)
                self.client.loop_start()
                self.log.info("connected")
                return
            except Exception as exc:
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
        self.log.info("published %s event_id=%s", event["event_type"], event["event_id"])

    def sleep_poisson_interval(self) -> None:
        mean_seconds = 60.0 / max(self.rate_per_minute, 0.01)
        interval = random.expovariate(1.0 / mean_seconds)
        # floor at 0.5s so a very small draw doesn't hammer the broker
        time.sleep(max(interval, 0.5))

    def run_forever(self, generate_event_fn):
        """generate_event_fn: () -> dict, called once per loop iteration."""
        self.connect()
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
