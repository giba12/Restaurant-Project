"""
Integration tests: the derived-data writers (aggregator, anomaly detector,
causal engine) and the SQL the causal engine uses to read its inputs,
against a real database.

The analysis queries deserve the most scrutiny of anything here. They decide
which rows feed a causal estimate; if one quietly includes a human player's
session, or miscounts staffing, every finding built on it is wrong and nothing
raises an error.
"""

import pytest

import aggregator
import causal_engine
import consumer
import detector
import factory
from conftest import rows, scalar

WINDOW = {"window_start": "2026-10-01T00:00:00Z", "window_end": "2026-10-02T00:00:00Z"}


def full_summary(ticket_id, **over):
    summary = {
        "summary_id": factory.service_timing_event()["event_id"], "event_type": "TicketTimingSummary",
        "schema_version": "1.0.0", "source_id": "timing-aggregator-01", "computed_at": "2026-10-01T12:30:00.000Z",
        "restaurant_id": "rest-001", "ticket_id": ticket_id, "station_id": "station-grill", "table_id": "table-01",
        "origin": "simulated", "order_time": "2026-10-01T12:00:00.000Z", "cook_started_time": "2026-10-01T12:01:00.000Z",
        "plated_time": "2026-10-01T12:06:00.000Z", "picked_up_time": "2026-10-01T12:07:00.000Z",
        "delivered_time": "2026-10-01T12:08:00.000Z", "time_to_cook_start_ms": 60000, "cook_duration_ms": 300000,
        "pickup_delay_ms": 60000, "service_delay_ms": 60000, "total_ticket_duration_ms": 480000, "is_complete": True,
    }
    summary.update(over)
    return summary


# ------------------------------------------------------------------ aggregator writer

def test_a_ticket_summary_is_updated_in_place_as_the_ticket_progresses(conn):
    # One row per ticket, rewritten at every stage -- not one row per stage.
    state = aggregator.TicketState()
    stages = ["order_fired", "cook_started", "plated", "picked_up_by_server", "delivered"]
    for i, stage in enumerate(stages):
        summary = state.apply(factory.service_timing_event(stage, ticket_id="t1", timestamp=factory.ts(i * 60)))
        aggregator.upsert_summary(conn, summary)
        assert scalar(conn, "SELECT count(*) FROM ticket_timing_summaries") == 1
    complete, total = rows(conn, "SELECT is_complete, total_ticket_duration_ms FROM ticket_timing_summaries")[0]
    assert complete is True and total == 240_000


def test_replaying_a_summary_is_harmless(conn):
    summary = full_summary("t1")
    for _ in range(3):
        aggregator.upsert_summary(conn, summary)
    assert scalar(conn, "SELECT count(*) FROM ticket_timing_summaries") == 1


def test_the_ticket_origin_survives_the_round_trip(conn):
    aggregator.upsert_summary(conn, full_summary("t1", origin="interactive"))
    assert scalar(conn, "SELECT origin FROM ticket_timing_summaries") == "interactive"


# ------------------------------------------------------------------ detector and causal writers

@pytest.mark.parametrize("build", [
    lambda: detector.build_control_limit_event("pickup_delay_ms", {**full_summary("t1")}, 99999.0, (1000.0, 2000.0)),
    lambda: detector.build_isolation_forest_event({**full_summary("t1")}, -0.3),
])
def test_anomaly_events_of_both_methods_are_stored(conn, build):
    event = build()
    detector.insert_anomaly(conn, event)
    method, station, severity = rows(conn, "SELECT detection_method, station_id, severity FROM anomaly_events")[0]
    assert (method, station) == (event["detection_method"], "station-grill")
    assert severity == event["severity"]


def test_a_causal_finding_is_stored_with_its_gate_and_refutation_flags(conn):
    spec = causal_engine.TREATMENT_MAP["estimated_waste_grams"]
    result = {"effect_estimate": -93.26, "confidence_interval": None, "method": "backdoor.linear_regression", "refutation_passed": True}
    finding = causal_engine._build_finding(spec, result, "rest-001", None, "scenario-1")
    causal_engine.insert_finding(conn, finding)
    ready, refuted, effect, scenario = rows(
        conn, "SELECT narrative_ready, refutation_passed, effect_estimate, scenario_injection_id FROM causal_findings"
    )[0]
    assert ready is False, "a freshly stored finding must wait for the reviewer"
    assert refuted is True and scenario == "scenario-1"
    assert float(effect) == pytest.approx(-93.26)


# ------------------------------------------------------------------ the analysis queries

def run_query(conn, metric):
    return rows(conn, causal_engine.TREATMENT_MAP[metric]["query"], WINDOW)


def test_the_waste_query_runs_against_the_real_schema_and_returns_the_confounders(conn):
    store = lambda **kw: consumer.insert_plate_waste(conn.cursor(), factory.plate_waste_event(**kw))  # noqa: E731
    store(**{"confounder_flags.to_go_container_used": True, "confounder_flags.portion_size_variant": "large",
             "confounder_flags.declared_dietary_restriction": False, "timestamp": "2026-10-01T12:00:00Z"})
    conn.commit()
    result = run_query(conn, "estimated_waste_grams")
    assert len(result) == 1
    to_go, grams, portion, dietary = result[0]
    assert (to_go, portion, dietary) == (True, "large", False)
    assert grams is not None


def test_the_waste_query_excludes_human_player_sessions(conn):
    cur = conn.cursor()
    consumer.insert_plate_waste(cur, factory.plate_waste_event(source_kind="simulated", timestamp="2026-10-01T12:00:00Z"))
    consumer.insert_plate_waste(cur, factory.plate_waste_event(source_kind="player", timestamp="2026-10-01T12:00:01Z"))
    conn.commit()
    assert len(run_query(conn, "estimated_waste_grams")) == 1


def test_the_waste_query_respects_the_time_window(conn):
    cur = conn.cursor()
    consumer.insert_plate_waste(cur, factory.plate_waste_event(timestamp="2026-10-01T12:00:00Z"))
    consumer.insert_plate_waste(cur, factory.plate_waste_event(timestamp="2026-09-01T12:00:00Z"))  # a month earlier
    conn.commit()
    assert len(run_query(conn, "estimated_waste_grams")) == 1


def seed_staffing_scenario(conn):
    """Two staff on shift, one has clocked out, one is a player, then a ticket is picked up."""
    cur = conn.cursor()
    shifts = [
        ("clock_in", "cook-a", "simulated", "2026-10-01T11:00:00Z"),
        ("clock_in", "cook-b", "simulated", "2026-10-01T11:00:00Z"),
        ("clock_out", "cook-b", "simulated", "2026-10-01T11:30:00Z"),
        ("clock_in", "player-1", "player", "2026-10-01T11:00:00Z"),
    ]
    for action, staff, kind, when in shifts:
        consumer.insert_staff_shift(cur, factory.staff_shift_event(action, staff_id=staff, source_kind=kind, timestamp=when))
    conn.commit()
    aggregator.upsert_summary(conn, full_summary("t1", picked_up_time="2026-10-01T12:07:00.000Z"))


def test_staffing_level_counts_only_staff_actually_on_shift_and_never_players(conn):
    seed_staffing_scenario(conn)
    result = run_query(conn, "pickup_delay_ms")
    assert len(result) == 1
    pickup_delay, station, staffing = result[0]
    assert pickup_delay == 60000 and station == "station-grill"
    # cook-a is on shift; cook-b clocked out before pickup; player-1 is excluded.
    assert staffing == 1


def test_the_pickup_query_excludes_interactive_tickets(conn):
    seed_staffing_scenario(conn)
    aggregator.upsert_summary(conn, full_summary("t2", origin="interactive"))
    assert len(run_query(conn, "pickup_delay_ms")) == 1


def test_the_pickup_query_excludes_tickets_with_no_pickup_delay(conn):
    seed_staffing_scenario(conn)
    aggregator.upsert_summary(conn, full_summary("t2", pickup_delay_ms=None, picked_up_time=None, delivered_time=None, is_complete=False))
    assert len(run_query(conn, "pickup_delay_ms")) == 1
