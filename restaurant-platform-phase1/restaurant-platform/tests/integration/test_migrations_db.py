"""
Integration tests: the migrations themselves.

run_db_tests.sh already proves a fresh database comes up from the six
migrations. These tests check what that produced, and that re-running every
migration is safe -- which is exactly what happens on every `helm upgrade`
(the schema-init Job re-applies all six against a live database).
"""
import os
import subprocess

import pytest

from conftest import rows, scalar

MIGRATIONS = ["001_hypertables.sql", "002_phase5_hypertables.sql", "003_phase6.sql", "004_player_source_kind.sql", "005_ticket_origin.sql", "006_twin_open_tickets.sql"]
CONTAINER = os.environ.get("TEST_PG_CONTAINER")

EXPECTED_TABLES = {
    "plate_waste_events", "pos_transaction_events", "pos_transaction_line_items", "service_timing_events",
    "staff_shift_events", "ticket_timing_summaries", "anomaly_events", "causal_findings", "narrated_findings",
    "twin_table_state", "twin_staff_state", "twin_station_state", "twin_open_tickets",
}
EXPECTED_HYPERTABLES = {"plate_waste_events", "pos_transaction_events", "pos_transaction_line_items", "service_timing_events", "staff_shift_events", "anomaly_events", "causal_findings"}


def test_every_expected_table_exists(conn):
    present = {r[0] for r in rows(conn, "SELECT tablename FROM pg_tables WHERE schemaname = 'public'")}
    assert EXPECTED_TABLES <= present, f"missing tables: {EXPECTED_TABLES - present}"


def test_the_time_series_tables_are_real_hypertables(conn):
    hypertables = {r[0] for r in rows(conn, "SELECT hypertable_name FROM timescaledb_information.hypertables")}
    assert EXPECTED_HYPERTABLES <= hypertables, f"not hypertables: {EXPECTED_HYPERTABLES - hypertables}"


def test_the_narrator_role_exists_and_can_log_in(conn):
    assert scalar(conn, "SELECT rolcanlogin FROM pg_roles WHERE rolname = 'narrator_app'") is True


@pytest.mark.skipif(not CONTAINER, reason="needs the database container name (run_db_tests.sh sets TEST_PG_CONTAINER)")
@pytest.mark.parametrize("migration", MIGRATIONS)
def test_re_running_a_migration_against_a_live_database_succeeds(conn, migration):
    result = subprocess.run(
        ["docker", "exec", "-e", f"NARRATOR_PGPASSWORD={os.environ['TEST_PG_NARRATOR_PASSWORD']}", CONTAINER,
         "psql", "-U", "restaurant_app", "-d", "restaurant_platform", "-v", "ON_ERROR_STOP=1",
         "-f", f"/docker-entrypoint-initdb.d/{migration}"],
        capture_output=True, text=True,
    )
    assert result.returncode == 0, f"{migration} is not idempotent:\n{result.stderr}"
