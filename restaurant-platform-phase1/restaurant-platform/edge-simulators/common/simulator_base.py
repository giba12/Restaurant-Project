import json
import time
import uuid
import logging
from datetime import datetime, timezone

import numpy as np 

import paho.mqtt.client as mqtt
from jsonSchema import validate, ValidationError

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s %(message)s")
log = logging.getLogger("edge-simulator")

def load_schema(schema_path: str) -> dict:
    with open(schema_path, "r") as f:
        return json.load(f)


def run_simulator(
    sensor_name: str,
    mqtt_topic: str,
    schema_path: str,
    mqtt_broker: str,
    mqtt_port: int,
    generate_event_fn,
    mean_interval_seconds: float,
):
    """
    Generic non-uniform (Poisson-process) event simulator loop.
    generate_event_fn: callable() -> dict, producing one schema-conformant event payload
    (minus event_id/schema_version/source_id/timestamp/source_kind, filled in here).
    """

    schema = load_schema(schema_path)

    client = mqtt.Client(client_id=f"sim-{sensor_name}-{uuid.uuid4().hex[:8]}")
    client.connect(mqtt_broker, mqtt_port, keepalive=60)
    client.loop_start()

    log.info(
        "Starting %s simulator -> mqtt topic '%s' (mean interval %.1fs)",
        sensor_name,
        mqtt_topic,
        mean_interval_seconds,
    )

    published_count = 0
    rejected_count = 0

    try:
        while True:
            event = generate_event_fn()
            event["event_id"] = str(uuid.uuid4())
            event.setdefault("schema_version", "1.0.0")
            event.setdefault("source_id", f"sim-{sensor_name}-01")
            event.setdefault("source_kind", "simulated")
            event["timestamp"] = datetime.now(timezone.utc).isoformat()

            try:
                validate(instance=event, schema=schema)
            except ValidationError as e:
                rejected_count += 1
                log.error("SCHEMA VIOLATION (not published): %s", e.message)
            else:
                client.publish(mqtt_topic, json.dumps(event), qos=1)
                published_count += 1
                log.info("Published #%d: %s", published_count, event["event_id"])

            if published_count and published_count % 10 == 0:
                log.info("Stats: published=%d rejected=%d", published_count, rejected_count)

            time.sleep(max(0.1, np.random.exponential(mean_interval_seconds)))
    except KeyboardInterrupt:
        pass
    finally:
        client.loop_stop()
        client.disconnect()