"""
Tests for the game-bridge state machines and envelope handling. No Kafka needed:
_publish is replaced with a recorder. Run from this directory:

    pip install -r requirements.txt pytest httpx
    python -m pytest test_bridge.py
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
os.environ.setdefault("SCHEMA_DIR", os.path.join(ROOT, "schemas"))
sys.path.insert(0, os.path.join(ROOT, "edge-simulators", "common"))  # world.py, as in the image
sys.path.insert(0, HERE)

import pytest
from fastapi.testclient import TestClient

import main


@pytest.fixture()
def client(monkeypatch):
    main._open_tickets.clear()
    main._clocked_in.clear()
    published = []
    monkeypatch.setattr(main, "_publish", lambda topic, event: published.append((topic, event)))
    c = TestClient(main.app)
    c.published = published
    return c


def fire(client, **over):
    body = {"player_id": "ana", "stage": "order_fired", "table_id": "table-03", "station_id": "station-grill"}
    body.update(over)
    return client.post("/api/service-timing", json=body)


def test_full_ticket_lifecycle(client):
    r = fire(client)
    assert r.status_code == 200
    ticket_id = r.json()["ticket_id"]
    assert r.json()["elapsed_since_previous_stage_ms"] is None

    for stage in ["cook_started", "plated", "picked_up_by_server", "delivered"]:
        r = client.post("/api/service-timing", json={"player_id": "ana", "stage": stage, "ticket_id": ticket_id})
        assert r.status_code == 200, r.text
        assert isinstance(r.json()["elapsed_since_previous_stage_ms"], int)
        assert r.json()["table_id"] == "table-03" and r.json()["station_id"] == "station-grill"

    assert [e["stage"] for _, e in client.published] == main.STAGES
    assert {t for t, _ in client.published} == {"service-timing-events"}
    assert ticket_id not in main._open_tickets


def test_envelope_is_owned_by_the_bridge(client):
    event = fire(client).json()
    assert event["source_kind"] == "player"
    assert event["source_id"] == "game-ana"
    assert event["event_type"] == "ServiceTimingEvent"
    assert event["restaurant_id"] == main.world.RESTAURANT_ID


def test_stage_out_of_order_is_rejected_and_does_not_publish(client):
    ticket_id = fire(client).json()["ticket_id"]
    n = len(client.published)
    r = client.post("/api/service-timing", json={"player_id": "ana", "stage": "plated", "ticket_id": ticket_id})
    assert r.status_code == 409 and "cook_started" in r.text
    assert len(client.published) == n


def test_unknown_ticket_is_404(client):
    r = client.post("/api/service-timing", json={"player_id": "ana", "stage": "cook_started", "ticket_id": "nope"})
    assert r.status_code == 404


def test_order_fired_requires_known_table_and_station(client):
    assert fire(client, table_id="table-99").status_code == 422
    assert fire(client, station_id="station-moon").status_code == 422
    assert fire(client, table_id=None).status_code == 422


def test_duplicate_ticket_id_is_409(client):
    assert fire(client, ticket_id="t-1").status_code == 200
    assert fire(client, ticket_id="t-1").status_code == 409


def test_bad_player_id_and_stage_are_422(client):
    assert fire(client, player_id="Ana Smith!").status_code == 422
    assert fire(client, stage="teleported").status_code == 422


def test_failed_publish_does_not_advance_ticket(client, monkeypatch):
    ticket_id = fire(client).json()["ticket_id"]

    def boom(topic, event):
        raise main.HTTPException(status_code=503, detail="kafka down")

    monkeypatch.setattr(main, "_publish", boom)
    r = client.post("/api/service-timing", json={"player_id": "ana", "stage": "cook_started", "ticket_id": ticket_id})
    assert r.status_code == 503
    assert main._open_tickets[ticket_id]["stage_index"] == 1  # still waiting for cook_started


def shift(client, action, **over):
    body = {"player_id": "ana", "role": "line_cook", "shift_action": action}
    body.update(over)
    return client.post("/api/staff-shift", json=body)


def test_shift_sequence(client):
    assert shift(client, "break_start").status_code == 409  # not clocked in
    r = shift(client, "clock_in")
    assert r.status_code == 200 and r.json()["staff_id"] == "player-ana"
    assert shift(client, "clock_in").status_code == 409
    assert shift(client, "break_end").status_code == 409
    assert shift(client, "break_start").status_code == 200
    assert shift(client, "break_start").status_code == 409
    assert shift(client, "break_end").status_code == 200
    assert shift(client, "station_reassign", station_id="station-saute").status_code == 200
    assert shift(client, "clock_out").status_code == 200
    assert shift(client, "clock_out").status_code == 409
    assert {t for t, _ in client.published} == {"staff-shift-events"}


def test_station_reassign_needs_a_known_station(client):
    shift(client, "clock_in")
    assert shift(client, "station_reassign").status_code == 422
    assert shift(client, "station_reassign", station_id="station-moon").status_code == 422


def test_schema_rejects_invalid_role(client):
    r = shift(client, "clock_in", role="astronaut")
    assert r.status_code == 422 and "role" in r.text


def test_world_endpoint_matches_schema_enums(client):
    w = client.get("/api/world").json()
    assert w["stages"][0] == "order_fired" and w["stages"][-1] == "delivered"
    assert "line_cook" in w["roles"] and "clock_in" in w["shift_actions"]
    assert "station-grill" in w["stations"] and "table-01" in w["tables"]
