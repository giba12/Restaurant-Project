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
import ssl
import uuid

RESTAURANT_ID = os.environ.get("RESTAURANT_ID", "rest-001")
SCHEMA_VERSION = os.environ.get("SCHEMA_VERSION", "1.0.0")

KAFKA_BOOTSTRAP_SERVERS = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS",
    # Strimzi names the bootstrap Service <Kafka CR name>-kafka-bootstrap.
    # Dead in practice, not just in theory: every k3s chart sets this env
    # var explicitly (all on :9093 as of 2026-09-25, since the plaintext
    # "plain" listener this default used to point at was removed from
    # k8s/kafka-strimzi once every consumer had migrated), and Compose sets
    # its own unrelated value (a different, non-Strimzi broker). Kept
    # pointing at a real, reachable address anyway rather than a stale one,
    # on the general principle that an unreachable code path describing
    # infrastructure that no longer exists is a landmine for whoever next
    # runs this file standalone.
    "restaurant-platform-kafka-kafka-bootstrap.kafka.svc.cluster.local:9093",
)
KAFKA_API_VERSION = (2, 8, 0)  # pinned -- automatic negotiation fails against Kafka 4.3.1

# Opt-in TLS: defaults to today's exact plaintext behavior (empty dict, safe
# to **-spread into any KafkaConsumer/KafkaProducer call) so Compose needs no
# changes at all. A k8s chart switches a service over by setting
# KAFKA_SECURITY_PROTOCOL=SSL (and usually also pointing KAFKA_BOOTSTRAP_SERVERS
# at the ":9093" listener -- see k8s/kafka-strimzi/templates/kafka-cluster.yaml)
# and mounting Strimzi's auto-generated <cluster>-cluster-ca-cert Secret's
# ca.crt at KAFKA_SSL_CAFILE's path. No SASL/client cert here -- this is
# encryption in transit, not client authentication (a separate, larger scope).
KAFKA_SECURITY_PROTOCOL = os.environ.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
KAFKA_SSL_CAFILE = os.environ.get("KAFKA_SSL_CAFILE", "/etc/kafka-tls/ca.crt")
# ssl_context, not ssl_cafile: kafka-python 2.0.2 (2020, unmaintained -- the
# same library KAFKA_API_VERSION above already has to work around for a
# different reason) builds its own ssl.SSLContext internally when only
# ssl_cafile is given, and that internal construction fails the TLS
# handshake against this broker outright ("SSL handshake failed" server
# side) for reasons that don't trace to the cert, the hostname, or the
# network path -- confirmed live by hand-rolling the identical handshake
# with plain ssl.create_default_context() in the same container, which
# negotiates TLSv1.3 successfully. Passing a pre-built context via
# ssl_context sidesteps kafka-python's own context construction entirely
# (its documented behavior: "If provided, all other ssl_* configurations
# will be ignored").
KAFKA_TLS_KWARGS = (
    {"security_protocol": KAFKA_SECURITY_PROTOCOL, "ssl_context": ssl.create_default_context(cafile=KAFKA_SSL_CAFILE)}
    if KAFKA_SECURITY_PROTOCOL != "PLAINTEXT"
    else {}
)

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


def start_metrics_server(default_port: int) -> None:
    """
    Starts a prometheus_client HTTP server in a background thread for the
    calling service's own pipeline-health counters -- separate from (and a
    deliberate follow-up to) k8s/observability's existing alerting, which
    only ever covered pod/infrastructure health (is it up, is it
    restarting), not whether the pipeline these services run is actually
    doing anything. METRICS_PORT, not a hardcoded one, so a chart can avoid
    a collision if it ever needs to.

    Lazy import, matching pg_connect() just above: a service that never
    calls this (digital-twin, dashboard-api, finding-narrator) shouldn't
    need prometheus_client installed at all.
    """
    from prometheus_client import start_http_server

    start_http_server(int(os.environ.get("METRICS_PORT", str(default_port))))
