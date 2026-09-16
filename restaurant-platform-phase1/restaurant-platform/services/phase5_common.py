"""
Shared helpers for Phase 5 services (ticket-timing-aggregator,
anomaly-detector, scenario-injection-controller, causal-engine).

Deliberately NOT the same module as edge-simulators/common/*.py -- that
package's contents (world.py, ids.py, runtime.py) have never been uploaded
to this project's chat history, so this file does not assume its internals.
RESTAURANT_ID and SCHEMA_VERSION are read from env vars here instead of a
shared world.py constant; reconcile with the real common/world.py values
once that file is available, rather than assuming they already match.
"""
import datetime
import json
import os
import uuid

RESTAURANT_ID = os.environ.get("RESTAURANT_ID", "restaurant-01")
SCHEMA_VERSION = os.environ.get("SCHEMA_VERSION", "1.0.0")

KAFKA_BOOTSTRAP_SERVERS = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS",
    # Strimzi convention: <Kafka CR name>-kafka-bootstrap. Cluster name
    # inferred from the broker pod name seen in prior sessions
    # (restaurant-platform-kafka-dev-pool-0) -- NOT independently confirmed
    # against the actual Kafka CR/Service name. Verify with
    # `kubectl get svc -n kafka` before relying on this default.
    "restaurant-platform-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092",
)
KAFKA_API_VERSION = (2, 8, 0)  # pinned -- automatic negotiation fails against Kafka 4.3.1

SCHEMA_DIR = os.environ.get("SCHEMA_DIR", "/app/schemas")

PG_HOST = os.environ.get("PGHOST", "timescaledb.kafka.svc.cluster.local")  # verify actual Service name
PG_PORT = int(os.environ.get("PGPORT", "5432"))
PG_DATABASE = os.environ.get("PGDATABASE", "restaurant_platform")
PG_USER = os.environ.get("PGUSER", "restaurant_app")
PG_PASSWORD = os.environ.get("PGPASSWORD", "")


def new_event_id() -> str:
    return str(uuid.uuid4())


def now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def load_schema(filename: str) -> dict:
    path = os.path.join(SCHEMA_DIR, filename)
    with open(path, "r") as f:
        return json.load(f)


def pg_connect():
    import psycopg2

    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DATABASE, user=PG_USER, password=PG_PASSWORD
    )
