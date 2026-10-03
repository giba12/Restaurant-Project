"""
Integration tests: the LLM narrator's database permission boundary.

This is the platform's headline safety claim: the narrator is "structurally
incapable of inventing a claim from data it can't see", because it connects as
a database role that can read causal findings and nothing else. A claim like
that is only worth anything if it is tested against the real database with
that real role -- so these tests log in as narrator_app and try everything it
must not be able to do.
"""
import os

import psycopg2
import pytest

import factory
from conftest import DSN, rows

NARRATOR_DSN = os.environ.get("TEST_PG_NARRATOR_DSN")
pytestmark = pytest.mark.skipif(not (DSN and NARRATOR_DSN), reason="needs TEST_PG_NARRATOR_DSN (run_db_tests.sh sets it)")

# Everything in the platform except what the narrator is meant to see.
FORBIDDEN_TO_READ = [
    "plate_waste_events", "pos_transaction_events", "pos_transaction_line_items", "service_timing_events",
    "staff_shift_events", "ticket_timing_summaries", "anomaly_events",
    "twin_table_state", "twin_staff_state", "twin_station_state",
]


@pytest.fixture()
def narrator():
    connection = psycopg2.connect(NARRATOR_DSN)
    connection.autocommit = True  # so a denied statement does not poison the next one
    yield connection
    connection.close()


def attempt(connection, statement, params=None):
    with connection.cursor() as cur:
        cur.execute(statement, params)
        return cur.fetchall() if cur.description else None


@pytest.mark.parametrize("table", FORBIDDEN_TO_READ)
def test_narrator_cannot_read_any_table_except_findings(narrator, table):
    with pytest.raises(psycopg2.errors.InsufficientPrivilege):
        attempt(narrator, f"SELECT * FROM {table} LIMIT 1")


def test_narrator_can_read_causal_findings(narrator):
    attempt(narrator, "SELECT * FROM causal_findings LIMIT 1")  # must not raise


@pytest.mark.parametrize("statement", [
    "UPDATE causal_findings SET narrative_ready = true",
    "DELETE FROM causal_findings",
    "INSERT INTO causal_findings (finding_id) VALUES (gen_random_uuid())",
    "TRUNCATE causal_findings",
])
def test_narrator_cannot_modify_findings(narrator, statement):
    # It must not be able to promote a finding past the review gate, rewrite
    # the evidence it is narrating, or erase it.
    with pytest.raises(psycopg2.errors.InsufficientPrivilege):
        attempt(narrator, statement)


def test_narrator_can_write_only_its_own_output_table(narrator, conn):
    finding = factory.service_timing_event()["event_id"]
    attempt(narrator, "INSERT INTO narrated_findings (finding_id, restaurant_id, narrative_text, model_used) VALUES (%s, 'rest-001', 'text', 'template-fallback')", (finding,))
    assert rows(conn, "SELECT model_used FROM narrated_findings") == [("template-fallback",)]


@pytest.mark.parametrize("statement", [
    "CREATE TABLE narrator_scratch (x int)",
    "DROP TABLE narrated_findings",
    "ALTER TABLE narrated_findings ADD COLUMN x int",
    "CREATE ROLE intruder LOGIN",
    "GRANT SELECT ON plate_waste_events TO narrator_app",
])
def test_narrator_cannot_change_the_schema_or_its_own_privileges(narrator, statement):
    with pytest.raises(psycopg2.Error):
        attempt(narrator, statement)


def test_narrator_is_not_a_superuser(narrator):
    assert attempt(narrator, "SELECT rolsuper, rolcreaterole, rolcreatedb FROM pg_roles WHERE rolname = current_user") == [(False, False, False)]
