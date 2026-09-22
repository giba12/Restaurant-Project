"""
Tests for the ticket `origin` the aggregator stamps on every summary. No Kafka
or database: the kafka import is stubbed and only TicketState.apply is used.

    pip install jsonschema pytest
    cd services/ticket-timing-aggregator && python -m pytest test_origin.py
"""
import os
import sys
import types

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
sys.path.insert(0, HERE)
os.environ.setdefault("SCHEMA_DIR", os.path.join(os.path.dirname(os.path.dirname(HERE)), "schemas"))

sys.modules.setdefault("kafka", types.SimpleNamespace(KafkaConsumer=None, KafkaProducer=None))
sys.modules.setdefault("psycopg2", types.SimpleNamespace())

import jsonschema
import pytest

import aggregator
import phase5_common as common

STAGES = ["order_fired", "cook_started", "plated", "picked_up_by_server", "delivered"]


def event(ticket, stage, kind, second):
    return {
        "ticket_id": ticket, "stage": stage, "source_kind": kind, "station_id": "station-grill",
        "table_id": "table-01", "restaurant_id": "rest-001",
        "timestamp": f"2026-09-21T12:00:{second:02d}.000Z",
    }


def run(kinds, ticket="t1"):
    """Feed one ticket through all five stages; kinds[i] is the source_kind of stage i."""
    state = aggregator.TicketState()
    summary = None
    for i, (stage, kind) in enumerate(zip(STAGES, kinds)):
        summary = state.apply(event(ticket, stage, kind, i * 2))
    return summary


def test_all_simulated_stays_simulated():
    assert run(["simulated"] * 5)["origin"] == "simulated"


def test_a_ticket_the_crew_fires_is_interactive_from_its_first_event():
    state = aggregator.TicketState()
    first = state.apply(event("t1", "order_fired", "crew", 0))
    assert first["origin"] == "interactive" and first["is_complete"] is False


def test_any_player_or_crew_event_makes_the_ticket_interactive():
    assert run(["simulated", "player", "simulated", "simulated", "simulated"])["origin"] == "interactive"
    assert run(["simulated", "simulated", "simulated", "simulated", "crew"])["origin"] == "interactive"
    assert run(["crew", "player", "player", "crew", "crew"])["origin"] == "interactive"


def test_origin_is_sticky_once_interactive():
    state = aggregator.TicketState()
    assert state.apply(event("t1", "order_fired", "crew", 0))["origin"] == "interactive"
    assert state.apply(event("t1", "cook_started", "simulated", 2))["origin"] == "interactive"


def test_vendor_integration_is_its_own_origin_not_interactive():
    assert run(["vendor_integration"] * 5)["origin"] == "vendor_integration"


def test_an_event_with_no_source_kind_counts_as_simulated():
    e = event("t1", "order_fired", "simulated", 0)
    del e["source_kind"]
    assert aggregator.TicketState().apply(e)["origin"] == "simulated"


def test_tickets_do_not_leak_origin_into_each_other():
    state = aggregator.TicketState()
    state.apply(event("interactive-one", "order_fired", "crew", 0))
    assert state.apply(event("sim", "order_fired", "simulated", 1))["origin"] == "simulated"


@pytest.mark.parametrize("kinds", [["simulated"] * 5, ["crew", "player", "player", "crew", "crew"]])
def test_summaries_validate_against_the_schema(kinds):
    schema = common.load_schema("TicketTimingSummary.schema.json")
    jsonschema.validate(instance=run(kinds), schema=schema)
