"""
Tests for the aggregator's per-ticket state machine: the arithmetic of the
durations it publishes, what it does with incomplete or interleaved tickets,
and that it never holds memory for a ticket that is finished. No Kafka or
database: the kafka import is stubbed and only TicketState is used.

The durations are the raw material of every downstream anomaly and causal
finding, so an off-by-one-stage mistake here would corrupt the whole platform
without raising a single error.

    pip install jsonschema pytest
    cd services/ticket-timing-aggregator && python -m pytest test_aggregator.py -v
"""
import json
import os
import sys
import types

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
sys.path.insert(0, HERE)
SCHEMAS = os.path.join(os.path.dirname(os.path.dirname(HERE)), "schemas")
os.environ.setdefault("SCHEMA_DIR", SCHEMAS)

sys.modules.setdefault("kafka", types.SimpleNamespace(KafkaConsumer=None, KafkaProducer=None))
sys.modules.setdefault("psycopg2", types.SimpleNamespace())

import jsonschema
import pytest

import aggregator

STAGES = ["order_fired", "cook_started", "plated", "picked_up_by_server", "delivered"]
SUMMARY_SCHEMA = json.load(open(os.path.join(SCHEMAS, "TicketTimingSummary.schema.json")))


def at(seconds):
    return f"2026-10-01T12:{seconds // 60:02d}:{seconds % 60:02d}.000Z"


def event(ticket, stage, seconds, **over):
    e = {
        "ticket_id": ticket, "stage": stage, "source_kind": "simulated", "station_id": "station-grill",
        "table_id": "table-01", "restaurant_id": "rest-001", "timestamp": at(seconds),
    }
    e.update(over)
    return e


def run_ticket(state, ticket="t1", offsets=(0, 60, 360, 390, 435), upto=5):
    summary = None
    for stage, seconds in list(zip(STAGES, offsets))[:upto]:
        summary = state.apply(event(ticket, stage, seconds))
    return summary


def test_durations_are_computed_between_the_right_pairs_of_stages():
    summary = run_ticket(aggregator.TicketState())
    assert summary["time_to_cook_start_ms"] == 60_000  # order_fired   -> cook_started
    assert summary["cook_duration_ms"] == 300_000      # cook_started  -> plated
    assert summary["pickup_delay_ms"] == 30_000        # plated        -> picked_up
    assert summary["service_delay_ms"] == 45_000       # picked_up     -> delivered
    assert summary["total_ticket_duration_ms"] == 435_000
    assert summary["is_complete"] is True


def test_an_in_flight_ticket_reports_only_the_durations_it_has_so_far():
    state = aggregator.TicketState()
    summary = run_ticket(state, upto=3)  # fired, cooking, plated -- not yet picked up
    assert summary["is_complete"] is False
    assert summary["time_to_cook_start_ms"] == 60_000
    assert summary["cook_duration_ms"] == 300_000
    assert summary["pickup_delay_ms"] is None
    assert summary["service_delay_ms"] is None


@pytest.mark.parametrize("upto", [1, 2, 3, 4, 5])
def test_every_intermediate_summary_satisfies_the_published_contract(upto):
    # The aggregator validates its own output before publishing and treats a
    # failure as fatal, so a shape bug at any stage would crash-loop the pod.
    summary = run_ticket(aggregator.TicketState(), upto=upto)
    jsonschema.validate(summary, SUMMARY_SCHEMA)


def test_a_finished_ticket_is_dropped_from_memory():
    # Without this the process would leak one dict per ticket forever.
    state = aggregator.TicketState()
    run_ticket(state)
    assert state.tickets == {}


def test_an_unfinished_ticket_stays_tracked_until_it_finishes():
    state = aggregator.TicketState()
    run_ticket(state, upto=2)
    assert "t1" in state.tickets
    state.apply(event("t1", "plated", 360))
    assert len(state.tickets) == 1


def test_a_ticket_already_mid_sequence_at_restart_publishes_nothing_and_does_not_leak():
    # The documented recovery gap (problem log item 43): after a restart the
    # first event seen for a ticket may not be order_fired. There is no valid
    # summary to publish (order_time is required); the old code crashed here.
    state = aggregator.TicketState()
    assert state.apply(event("orphan", "plated", 360)) is None
    assert state.apply(event("orphan", "picked_up_by_server", 390)) is None
    assert "orphan" in state.tickets
    assert state.apply(event("orphan", "delivered", 435)) is None
    assert "orphan" not in state.tickets, "an orphaned ticket must not accumulate forever"


def test_interleaved_tickets_never_contaminate_each_other():
    state = aggregator.TicketState()
    results = {}
    schedule = [
        ("a", "order_fired", 0), ("b", "order_fired", 10), ("a", "cook_started", 30), ("b", "cook_started", 100),
        ("a", "plated", 90), ("b", "plated", 400), ("a", "picked_up_by_server", 100), ("b", "picked_up_by_server", 500),
        ("a", "delivered", 120), ("b", "delivered", 600),
    ]
    for ticket, stage, seconds in schedule:
        summary = state.apply(event(ticket, stage, seconds))
        if stage == "delivered":
            results[ticket] = summary
    assert results["a"]["total_ticket_duration_ms"] == 120_000
    assert results["b"]["total_ticket_duration_ms"] == 590_000
    assert results["a"]["cook_duration_ms"] == 60_000
    assert results["b"]["cook_duration_ms"] == 300_000


def test_the_latest_station_wins_when_a_ticket_is_reassigned():
    state = aggregator.TicketState()
    state.apply(event("t1", "order_fired", 0, station_id="station-grill"))
    summary = state.apply(event("t1", "cook_started", 30, station_id="station-fry"))
    assert summary["station_id"] == "station-fry"


def test_a_missing_station_on_a_later_event_does_not_erase_a_known_one():
    state = aggregator.TicketState()
    state.apply(event("t1", "order_fired", 0, station_id="station-grill"))
    summary = state.apply(event("t1", "cook_started", 30, station_id=None))
    assert summary["station_id"] == "station-grill"


def test_a_duplicate_delivery_does_not_crash():
    # Kafka can redeliver across a rebalance. The documented behaviour is a
    # fresh (orphan) state, not an exception that would crash-loop the pod.
    state = aggregator.TicketState()
    run_ticket(state)
    assert state.apply(event("t1", "delivered", 435)) is None


@pytest.mark.parametrize(
    "start,end,expected",
    [
        ("2026-10-01T12:00:00Z", "2026-10-01T12:00:01Z", 1000),
        ("2026-10-01T12:00:00.000Z", "2026-10-01T12:00:00.250Z", 250),
        ("2026-10-01T12:00:00+00:00", "2026-10-01T12:01:00+00:00", 60_000),
        ("2026-10-01T12:00:00+02:00", "2026-10-01T10:00:30Z", 30_000),  # same instant +30s across offsets
    ],
)
def test_duration_arithmetic_handles_formats_and_offsets(start, end, expected):
    assert aggregator._duration_ms(start, end) == expected


@pytest.mark.parametrize("start,end", [(None, "2026-10-01T12:00:00Z"), ("2026-10-01T12:00:00Z", None), ("garbage", "2026-10-01T12:00:00Z")])
def test_duration_arithmetic_returns_none_instead_of_raising(start, end):
    assert aggregator._duration_ms(start, end) is None


# ------------------------------------------------------------------ a clock that steps backwards

def at_ms(seconds, millis):
    return f"2026-10-01T12:{seconds // 60:02d}:{seconds % 60:02d}.{millis:03d}Z"


def test_a_clock_that_steps_backwards_gives_an_unknown_duration_not_a_negative_one():
    # Producers stamp events with their own wall clock. Wall clocks step
    # backwards (NTP, or WSL2 resynchronising a busy host: the simulator's own
    # log showed six steps of 15-549 ms in one 11-minute load run), and real
    # edge devices are never perfectly synchronised. A negative duration is
    # impossible, so the honest value is "unknown".
    assert aggregator._duration_ms(at_ms(10, 100), at_ms(10, 16)) is None
    assert aggregator._duration_ms(at_ms(10, 100), at_ms(10, 100)) == 0  # simultaneous is fine
    assert aggregator._duration_ms(at_ms(10, 100), at_ms(10, 101)) == 1


def test_a_backwards_step_between_two_stages_does_not_make_the_summary_invalid():
    # Before the fix this produced time_to_cook_start_ms = -84, which failed the
    # published schema's minimum of 0; the aggregator treats its own schema
    # failure as fatal, so it crashed, restarted, and lost every ticket in flight.
    state = aggregator.TicketState()
    state.apply(event("t1", "order_fired", 0, timestamp=at_ms(10, 100)))
    summary = state.apply(event("t1", "cook_started", 0, timestamp=at_ms(10, 16)))  # 84 ms before the order
    jsonschema.validate(summary, SUMMARY_SCHEMA)
    assert summary["time_to_cook_start_ms"] is None

    # The rest of the ticket carries on normally.
    for stage, millis in (("plated", 40), ("picked_up_by_server", 50), ("delivered", 60)):
        summary = state.apply(event("t1", stage, 0, timestamp=at_ms(11, millis)))
        jsonschema.validate(summary, SUMMARY_SCHEMA)
    assert summary["is_complete"] is True
    assert summary["cook_duration_ms"] == 1_024  # plated 11.040 minus cook_started 10.016
    assert summary["total_ticket_duration_ms"] == 960  # delivered 11.060 minus order 10.100


def test_a_backwards_step_in_one_ticket_does_not_affect_another():
    state = aggregator.TicketState()
    state.apply(event("bad", "order_fired", 0, timestamp=at_ms(10, 500)))
    state.apply(event("good", "order_fired", 10))
    assert state.apply(event("bad", "cook_started", 0, timestamp=at_ms(10, 100)))["time_to_cook_start_ms"] is None
    assert state.apply(event("good", "cook_started", 70))["time_to_cook_start_ms"] == 60_000
