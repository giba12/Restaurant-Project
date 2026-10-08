"""
Static guard: no code that writes to the database can be added without saying what happens when it writes the same thing again.

Kafka delivers at least once, so every writer will one day be run twice on the same input. The real-database tests
(tests/integration/test_idempotent_writes_db.py) show that today's writers cope; this is what stops tomorrow's from silently not.
Nothing runs.

    python -m pytest tests/static/test_idempotent_writes.py -v
"""
import re
import sys
import os

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helpers import ROOT  # noqa: E402

APPLICATION_DIRS = ["services", "storage", "game/bridge"]


def application_python_files():
    for directory in APPLICATION_DIRS:
        for path in sorted((ROOT / directory).rglob("*.py")):
            if "node_modules" in path.parts or path.name.startswith("test_") or "tests" in path.parts or path.name == "conftest.py":
                continue
            yield path


def inserts():
    """(file, table, the SQL text of the statement) for every INSERT INTO in the application's own code."""
    found = []
    for path in application_python_files():
        text = path.read_text()
        for match in re.finditer(r"INSERT INTO\s+(\w+)", text):
            statement = text[match.start():match.start() + 4000]
            end = statement.find('"""')
            found.append((path.relative_to(ROOT).as_posix(), match.group(1), statement[:end] if end > 0 else statement))
    return found


def test_the_guard_sees_the_writers_it_is_meant_to_guard():
    tables = {table for _, table, _ in inserts()}
    assert {"plate_waste_events", "pos_transaction_events", "pos_transaction_line_items", "staff_shift_events", "service_timing_events",
            "ticket_timing_summaries", "anomaly_events", "causal_findings", "narrated_findings", "twin_table_state", "twin_station_state",
            "twin_staff_state", "twin_open_tickets"} <= tables, f"a writer is not being scanned: {sorted(tables)}"


@pytest.mark.parametrize("path, table, statement", inserts(), ids=lambda v: v if isinstance(v, str) and len(v) < 60 else "")
def test_every_insert_says_what_happens_on_a_conflict(path, table, statement):
    assert "ON CONFLICT" in statement, f"{path}: INSERT INTO {table} has no ON CONFLICT clause, so writing the same thing twice is not defined"


def test_every_table_the_migrations_create_has_a_primary_key():
    # ON CONFLICT needs a key to conflict on; a table with none cannot be written idempotently by anything.
    missing = []
    for path in sorted((ROOT / "storage" / "schema").glob("*.sql")):
        text = path.read_text()
        for match in re.finditer(r"CREATE TABLE IF NOT EXISTS\s+(\w+)\s*\((.*?)\n\);", text, flags=re.S):
            if "PRIMARY KEY" not in match.group(2):
                missing.append(f"{path.name}: {match.group(1)}")
    assert not missing, f"tables with no primary key: {missing}"


def test_the_derived_rows_take_their_ids_from_what_they_are_about_not_from_the_clock_or_chance():
    detector = (ROOT / "services" / "anomaly-detector" / "detector.py").read_text()
    builders = detector[detector.index("def build_control_limit_event"):detector.index("def insert_anomaly")]
    assert re.findall(r'"anomaly_id":\s*(\S+)', builders) == ["_anomaly_id(summary,", "_anomaly_id(summary,"], "an anomaly id is made some other way"
    assert "new_event_id" not in builders, "a builder uses a random id"
    engine = (ROOT / "services" / "causal-engine" / "causal_engine.py").read_text()
    assert '"finding_id": finding_id or finding_id_for(' in engine
    assert 'common.stable_id("finding"' in engine
