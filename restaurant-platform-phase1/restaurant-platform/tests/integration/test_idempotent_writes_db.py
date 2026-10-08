"""
Integration tests: every write the platform makes to the database is idempotent, against a real TimescaleDB with the real migrations.

Kafka delivers at least once. A consumer that stores a message and crashes before committing sees it again, and the effect of
seeing it twice must be the effect of seeing it once. The sensor events have always been safe (the event carries its own id and the
tables are keyed on it); the rows the platform DERIVES (an anomaly from a ticket summary, a finding from an anomaly) were not, because
each pass invented a fresh random id. These tests run each writer twice and compare what is in the table, not just how many rows.

    bash tests/integration/run_db_tests.sh tests/integration/test_idempotent_writes_db.py
"""
import copy
import os
import sys
import types

import pytest

import aggregator
import causal_engine
import consumer
import detector
import factory
import phase5_common as common
import twin
from conftest import ROOT, rows, scalar

try:
    import requests  # noqa: F401  (the narrator imports it; the database tests do not otherwise need it)
except ImportError:
    sys.modules["requests"] = types.ModuleType("requests")
sys.path.insert(0, os.path.join(ROOT, "services", "finding-narrator"))
import narrator  # noqa: E402


def contents(conn, table):
    """Every row of `table` as JSON, without the columns the database stamps itself (ingested_at, updated_at), in a fixed order."""
    return [r[0] for r in rows(conn, f"SELECT to_jsonb(t) - 'ingested_at' - 'updated_at' FROM {table} t ORDER BY 1::text")]


def store(conn, topic, event):
    consumer.write_with_retry(conn, consumer.TOPIC_INSERT_FN[topic], event)


def summary(ticket_id="t1", **over):
    value = {
        "summary_id": common.new_event_id(), "event_type": "TicketTimingSummary", "schema_version": "1.0.0", "source_id": "timing-aggregator-01",
        "computed_at": "2026-10-01T12:30:00.000Z", "restaurant_id": "rest-001", "ticket_id": ticket_id, "station_id": "station-grill",
        "table_id": "table-01", "origin": "simulated", "order_time": "2026-10-01T12:00:00.000Z", "cook_started_time": "2026-10-01T12:01:00.000Z",
        "plated_time": "2026-10-01T12:06:00.000Z", "picked_up_time": "2026-10-01T12:07:00.000Z", "delivered_time": "2026-10-01T12:08:00.000Z",
        "time_to_cook_start_ms": 60000, "cook_duration_ms": 300000, "pickup_delay_ms": 60000, "service_delay_ms": 60000,
        "total_ticket_duration_ms": 480000, "is_complete": True,
    }
    value.update(over)
    return value


# ------------------------------------------------------------------ the sensor events

EVENT_TABLES = {"plate-waste-events": ("plate_waste_events",), "pos-transaction-events": ("pos_transaction_events", "pos_transaction_line_items"),
                "service-timing-events": ("service_timing_events",), "staff-shift-events": ("staff_shift_events",)}
KIND_BY_TOPIC = {"plate-waste-events": "plate_waste", "pos-transaction-events": "pos_transaction", "service-timing-events": "service_timing",
                 "staff-shift-events": "staff_shift"}


@pytest.mark.parametrize("topic", sorted(EVENT_TABLES))
def test_storing_a_sensor_event_a_second_time_leaves_every_table_exactly_as_it_was(conn, topic):
    event = factory.simulator_events_of_every_kind()[KIND_BY_TOPIC[topic]]
    store(conn, topic, event)
    first = {table: contents(conn, table) for table in EVENT_TABLES[topic]}
    assert first[EVENT_TABLES[topic][0]], "nothing was stored, so this shows nothing"
    store(conn, topic, copy.deepcopy(event))
    store(conn, topic, copy.deepcopy(event))
    assert {table: contents(conn, table) for table in EVENT_TABLES[topic]} == first


def test_a_point_of_sale_event_with_several_line_items_is_stored_once_with_each_line_once(conn):
    event = factory.pos_transaction_event()
    assert len(event["line_items"]) >= 1
    store(conn, "pos-transaction-events", event)
    store(conn, "pos-transaction-events", copy.deepcopy(event))
    assert scalar(conn, "SELECT count(*) FROM pos_transaction_line_items") == len(event["line_items"])
    assert scalar(conn, "SELECT count(*) FROM pos_transaction_events") == 1


# ------------------------------------------------------------------ the digital twin

def test_the_twin_told_the_same_ticket_events_again_ends_in_the_same_state(conn):
    opened = factory.service_timing_event("order_fired", ticket_id="t1", table_id="table-07", station_id="station-grill")
    delivered = factory.service_timing_event("delivered", ticket_id="t1", table_id="table-07", station_id="station-grill")
    twin.handle_service_timing(conn, opened)
    snapshot_open = {t: contents(conn, t) for t in ("twin_table_state", "twin_station_state", "twin_open_tickets")}
    twin.handle_service_timing(conn, copy.deepcopy(opened))
    assert {t: contents(conn, t) for t in snapshot_open} == snapshot_open, "an opening told twice changed the twin"
    twin.handle_service_timing(conn, delivered)
    snapshot_done = {t: contents(conn, t) for t in snapshot_open}
    twin.handle_service_timing(conn, copy.deepcopy(delivered))
    assert {t: contents(conn, t) for t in snapshot_open} == snapshot_done, "a delivery told twice changed the twin"


def test_the_twin_told_the_same_staff_event_again_ends_in_the_same_state(conn):
    event = factory.staff_shift_event("clock_in", staff_id="staff-001")
    twin.handle_staff_shift(conn, event)
    first = contents(conn, "twin_staff_state")
    twin.handle_staff_shift(conn, copy.deepcopy(event))
    assert contents(conn, "twin_staff_state") == first


# ------------------------------------------------------------------ ticket summaries

def test_a_summary_that_is_older_than_the_stored_one_does_not_overwrite_it(conn):
    # A replay from the start of the topic, or an old message redelivered, must not turn a completed ticket back into an unfinished one.
    aggregator.upsert_summary(conn, summary("t1", computed_at="2026-10-01T12:30:00.000Z"))
    aggregator.upsert_summary(conn, summary("t1", computed_at="2026-10-01T12:05:00.000Z", is_complete=False, delivered_time=None,
                                            picked_up_time=None, total_ticket_duration_ms=None, service_delay_ms=None, pickup_delay_ms=None))
    complete, total = rows(conn, "SELECT is_complete, total_ticket_duration_ms FROM ticket_timing_summaries")[0]
    assert complete is True and total == 480000


def test_a_newer_summary_still_replaces_an_older_one_and_the_same_one_again_changes_nothing(conn):
    aggregator.upsert_summary(conn, summary("t1", computed_at="2026-10-01T12:05:00.000Z", is_complete=False, delivered_time=None))
    newest = summary("t1", computed_at="2026-10-01T12:30:00.000Z")
    aggregator.upsert_summary(conn, newest)
    first = contents(conn, "ticket_timing_summaries")
    assert rows(conn, "SELECT is_complete FROM ticket_timing_summaries")[0][0] is True
    aggregator.upsert_summary(conn, copy.deepcopy(newest))
    assert contents(conn, "ticket_timing_summaries") == first


# ------------------------------------------------------------------ anomalies

def test_scoring_the_same_completed_ticket_again_stores_one_anomaly_and_returns_the_first_one_stored(conn, monkeypatch):
    ticket = summary("t1")
    stamps = iter(["2026-10-01T12:31:00.000Z", "2026-10-01T12:31:45.000Z"])  # the second pass happens later, as a redelivery does
    monkeypatch.setattr(common, "now_iso", lambda: next(stamps))
    first = detector.build_control_limit_event("total_ticket_duration_ms", ticket, 480000.0, (100000.0, 300000.0))
    second = detector.build_control_limit_event("total_ticket_duration_ms", copy.deepcopy(ticket), 480000.0, (100000.0, 300000.0))
    assert first["anomaly_id"] == second["anomaly_id"] and first["detected_at"] != second["detected_at"]
    stored_first = detector.insert_anomaly(conn, first)
    stored_second = detector.insert_anomaly(conn, second)
    assert scalar(conn, "SELECT count(*) FROM anomaly_events") == 1
    assert stored_first["detected_at"] == stored_second["detected_at"] == "2026-10-01T12:31:00.000Z", "what is published again must be what is stored"


def test_one_ticket_gives_a_different_anomaly_for_each_metric_and_method_and_each_ticket_its_own(conn):
    a, b = summary("t1"), summary("t2")
    ids = {detector.build_control_limit_event("total_ticket_duration_ms", a, 1.0, (0.0, 0.5))["anomaly_id"],
           detector.build_control_limit_event("pickup_delay_ms", a, 1.0, (0.0, 0.5))["anomaly_id"],
           detector.build_isolation_forest_event(a, -0.2)["anomaly_id"],
           detector.build_control_limit_event("total_ticket_duration_ms", b, 1.0, (0.0, 0.5))["anomaly_id"]}
    assert len(ids) == 4


# ------------------------------------------------------------------ causal findings

SPEC = causal_engine.TREATMENT_MAP["estimated_waste_grams"]
RESULT = {"effect_estimate": -93.26, "confidence_interval": {"lower": -120.0, "upper": -60.0, "confidence_level": 0.95},
          "method": "backdoor.linear_regression", "refutation_passed": True, "effect_p_value": 0.01, "placebo_p_value": 0.7}


def test_the_same_anomaly_analysed_again_has_the_same_finding_id_and_the_first_estimate_is_kept(conn):
    anomaly_id = common.new_event_id()
    first = causal_engine._build_finding(SPEC, RESULT, "rest-001", anomaly_id, None)
    second = causal_engine._build_finding(SPEC, {**RESULT, "effect_estimate": -80.0}, "rest-001", anomaly_id, None)
    assert first["finding_id"] == second["finding_id"] and first["effect_estimate"] != second["effect_estimate"]
    causal_engine.insert_finding(conn, first)
    returned = causal_engine.insert_finding(conn, second)
    assert scalar(conn, "SELECT count(*) FROM causal_findings") == 1
    assert returned["effect_estimate"] == pytest.approx(-93.26), "the re-estimate replaced the stored estimate"
    assert float(scalar(conn, "SELECT effect_estimate FROM causal_findings")) == pytest.approx(-93.26)


def test_analysing_a_redelivered_anomaly_does_not_estimate_again_and_publishes_the_stored_finding(conn, monkeypatch):
    estimates = []

    def estimate(df, treatment, outcome, confounders):
        estimates.append(1)
        return {**RESULT, "effect_estimate": -93.26 * len(estimates)}  # a different number every time it is computed

    monkeypatch.setattr(causal_engine, "_load_data", lambda *args: None)
    monkeypatch.setattr(causal_engine, "_run_dowhy", estimate)
    sent = []
    producer = types.SimpleNamespace(send=lambda topic, value: sent.append(value), flush=lambda: None)
    anomaly = {"anomaly_id": common.new_event_id(), "metric_name": "estimated_waste_grams", "restaurant_id": "rest-001",
               "window_start": "2026-10-01T00:00:00Z", "window_end": "2026-10-02T00:00:00Z"}
    first = causal_engine.process_anomaly(conn, producer, {}, copy.deepcopy(anomaly))
    second = causal_engine.process_anomaly(conn, producer, {}, copy.deepcopy(anomaly))
    assert len(estimates) == 1, "the anomaly was estimated twice"
    assert scalar(conn, "SELECT count(*) FROM causal_findings") == 1
    assert len(sent) == 2 and sent[0] == sent[1] == first == second, "downstream did not get the same finding twice"


def test_an_analysis_with_no_anomaly_behind_it_is_not_deduplicated(conn):
    # The one-shot scenario path is run on purpose, possibly again on more data: each run is its own finding.
    one = causal_engine._build_finding(SPEC, RESULT, "rest-001", None, "scenario-1")
    two = causal_engine._build_finding(SPEC, RESULT, "rest-001", None, "scenario-1")
    assert one["finding_id"] != two["finding_id"]
    causal_engine.insert_finding(conn, one)
    causal_engine.insert_finding(conn, two)
    assert scalar(conn, "SELECT count(*) FROM causal_findings") == 2


# ------------------------------------------------------------------ narrations

def test_a_finding_narrated_twice_keeps_the_first_narration_and_is_seen_as_narrated(conn):
    finding_id = common.new_event_id()
    assert narrator.narration_exists(conn, finding_id) is False
    narrator.insert_narration(conn, finding_id, "rest-001", "the first narration", "model-a")
    assert narrator.narration_exists(conn, finding_id) is True
    narrator.insert_narration(conn, finding_id, "rest-001", "a second narration", "model-b")
    assert rows(conn, "SELECT narrative_text, model_used FROM narrated_findings") == [("the first narration", "model-a")]
