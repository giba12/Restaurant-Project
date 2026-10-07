"""
Fixtures for the database integration tests. These talk to a real TimescaleDB
whose address comes from TEST_PG_DSN (set by run_db_tests.sh).
"""
import os
import sys
import time
import types
from urllib.parse import urlparse

import psycopg2
import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))

DSN = os.environ.get("TEST_PG_DSN")

# The service modules read their connection settings from the environment at
# import time, so point them at the test database before anything imports them.
if DSN:
    parsed = urlparse(DSN)
    os.environ.update({
        "TIMESCALE_DSN": DSN, "PGHOST": parsed.hostname, "PGPORT": str(parsed.port),
        "PGDATABASE": parsed.path.lstrip("/"), "PGUSER": parsed.username, "PGPASSWORD": parsed.password,
    })
os.environ.setdefault("TIMESCALE_DSN", "postgresql://unset@localhost/unset")
os.environ["SCHEMA_DIR"] = os.path.join(ROOT, "schemas")
os.environ["API_KEY"] = "integration-test-key"

# The simulators import paho.mqtt at module level for a class these tests never
# construct. Stub it only if the real package is not installed.
try:
    import paho.mqtt.client  # noqa: F401
except ImportError:
    paho, mqtt, client = types.ModuleType("paho"), types.ModuleType("paho.mqtt"), types.ModuleType("paho.mqtt.client")
    client.Client = None
    mqtt.client, paho.mqtt = client, mqtt
    sys.modules.update({"paho": paho, "paho.mqtt": mqtt, "paho.mqtt.client": client})

for path in ("edge-simulators", "services", "services/digital-twin", "services/ticket-timing-aggregator",
             "services/anomaly-detector", "services/causal-engine", "services/dashboard-api", "storage/consumer"):
    sys.path.insert(0, os.path.join(ROOT, path))


def pytest_configure(config):
    config.addinivalue_line("markers", "no_db: needs a container of its own (the broker) but not the database, so it is not skipped without TEST_PG_DSN")


def pytest_collection_modifyitems(config, items):
    if DSN:
        return
    if os.environ.get("REQUIRE_DB"):
        raise pytest.UsageError("REQUIRE_DB is set but TEST_PG_DSN is not -- refusing to skip the database tests")
    skip = pytest.mark.skip(reason="TEST_PG_DSN not set; run via tests/integration/run_db_tests.sh")
    for item in items:
        if "no_db" not in item.keywords:
            item.add_marker(skip)


def _connect(dsn, attempts=30):
    last = None
    for _ in range(attempts):
        try:
            return psycopg2.connect(dsn)
        except psycopg2.OperationalError as exc:
            last = exc
            time.sleep(1)
    raise last


@pytest.fixture(scope="session")
def dsn():
    return DSN


@pytest.fixture()
def conn(dsn):
    """A connection to a database emptied of data (the schema stays)."""
    connection = _connect(dsn)
    with connection.cursor() as cur:
        cur.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
        tables = [row[0] for row in cur.fetchall()]
        cur.execute("TRUNCATE " + ", ".join(f'"{t}"' for t in tables) + " RESTART IDENTITY CASCADE")
    connection.commit()
    yield connection
    connection.rollback()
    connection.close()


def scalar(connection, query, params=None):
    with connection.cursor() as cur:
        cur.execute(query, params)
        row = cur.fetchone()
    connection.commit()
    return row[0] if row else None


def rows(connection, query, params=None):
    with connection.cursor() as cur:
        cur.execute(query, params)
        result = cur.fetchall()
    connection.commit()
    return result
