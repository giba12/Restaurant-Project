"""
Shared helpers for the Phase 5/6/7 Python services (ticket-timing-aggregator,
anomaly-detector, scenario-injection-controller, causal-engine, digital-twin,
finding-narrator, dashboard-api): configuration from environment variables,
the Kafka and Postgres connection settings, and a few small utilities.

Deliberately separate from edge-simulators/common/ -- that package is the
simulators' own runtime and is not shared with these services. The one thing
both sides must agree on is the restaurant id, which comes from the events
themselves (the simulators use "rest-001"); RESTAURANT_ID below is only a
fallback and a default for command-line tools, and matches that value.
"""
import datetime
import json
import os
import uuid

RESTAURANT_ID = os.environ.get("RESTAURANT_ID", "rest-001")
SCHEMA_VERSION = os.environ.get("SCHEMA_VERSION", "1.0.0")

KAFKA_BOOTSTRAP_SERVERS = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS",
    # Strimzi names the bootstrap Service <Kafka CR name>-kafka-bootstrap.
    "restaurant-platform-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092",
)
KAFKA_API_VERSION = (2, 8, 0)  # pinned -- automatic negotiation fails against Kafka 4.3.1

SCHEMA_DIR = os.environ.get("SCHEMA_DIR", "/app/schemas")

PG_HOST = os.environ.get("PGHOST", "timescaledb.kafka.svc.cluster.local")
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
