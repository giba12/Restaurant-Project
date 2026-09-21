"""
Tests for the game-bridge rules: envelope handling, ticket and shift state
machines, role gating, and the crew/director. No Kafka and no real time:
_publish is replaced with a recorder and _clock with a controllable clock, so
the crew's delays are exercised exactly. Run from this directory:

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


class FakeClock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t

    def advance(self, seconds):
        self.t += seconds


@pytest.fixture()
def client(monkeypatch):
    main._open_tickets.clear()
    main._clocked_in.clear()
    published = []
    clock = FakeClock()
    monkeypatch.setattr(main, "_publish", lambda topic, event: published.append((topic, event)))
    monkeypatch.setattr(main, "_clock", clock)
    # The dining room is off by default (no spawn for a very long time), so tests about
    # the crew see only the tickets they create. spawner_on() turns it on.
    monkeypatch.setattr(main, "_last_spawn", clock.t)
    monkeypatch.setattr(main, "SPAWN_SECONDS", 1e9)
    monkeypatch.setattr(main, "MAX_OPEN_TICKETS", 5)
    monkeypatch.setattr(main, "CREW_MIN_SECONDS", 5.0)
    monkeypatch.setattr(main, "CREW_MAX_SECONDS", 5.0)  # min == max: the crew delay is exactly 5 s
    c = TestClient(main.app)  # no `with`: the background director thread does not start
    c.published = published
    c.clock = clock
    c.monkeypatch = monkeypatch
    return c


def spawner_on(client):
    client.monkeypatch.setattr(main, "SPAWN_SECONDS", 8.0)
    client.monkeypatch.setattr(main, "_last_spawn", float("-inf"))


def shift(client, action, player="ana", role="line_cook", **over):
    body = {"player_id": player, "role": role, "shift_action": action}
    body.update(over)
    return client.post("/api/staff-shift", json=body)


def on_shift(client, player="ana", role="line_cook", station="station-grill"):
    assert shift(client, "clock_in", player, role).status_code == 200
    if role == "line_cook":
        assert shift(client, "station_reassign", player, role, station_id=station).status_code == 200


def stage(client, stage_name, ticket_id, player="ana"):
    return client.post("/api/service-timing", json={"player_id": player, "stage": stage_name, "ticket_id": ticket_id})


def fire(client, player="ana", **over):
    body = {"player_id": player, "stage": "order_fired", "table_id": "table-03", "station_id": "station-grill"}
    body.update(over)
    return client.post("/api/service-timing", json=body)


def service_events(client):
    return [e for t, e in client.published if t == "service-timing-events"]


def tick(client, seconds=0.0):
    client.clock.advance(seconds)
    main.director_tick()


# ------------------------------------------------ envelope and ticket state machine

def test_a_cook_takes_a_ticket_to_plated_then_the_crew_finishes_it(client):
    on_shift(client)
    ticket_id = fire(client).json()["ticket_id"]
    for name in ["cook_started", "plated"]:
        client.clock.advance(3)
        r = stage(client, name, ticket_id)
        assert r.status_code == 200, r.text
        assert r.json()["elapsed_since_previous_stage_ms"] == 3000
        assert r.json()["table_id"] == "table-03" and r.json()["station_id"] == "station-grill"

    tick(client)          # crew notices nobody can pick it up and starts its 5 s delay
    tick(client, 4.9)
    assert ticket_id in main._open_tickets
    tick(client, 0.2)     # picked_up_by_server
    tick(client)          # restarts the delay for the next stage
    tick(client, 5.0)     # delivered
    assert [e["stage"] for e in service_events(client)] == main.STAGES
    assert ticket_id not in main._open_tickets


def test_envelope_is_owned_by_the_bridge(client):
    on_shift(client)
    event = fire(client).json()
    assert event["source_kind"] == "player"
    assert event["source_id"] == "game-ana"
    assert event["event_type"] == "ServiceTimingEvent"
    assert event["restaurant_id"] == main.world.RESTAURANT_ID


def test_stage_out_of_order_is_rejected_and_does_not_publish(client):
    on_shift(client)
    ticket_id = fire(client).json()["ticket_id"]
    n = len(client.published)
    r = stage(client, "plated", ticket_id)
    assert r.status_code == 409 and "cook_started" in r.text
    assert len(client.published) == n


def test_unknown_ticket_is_404(client):
    on_shift(client)
    assert stage(client, "cook_started", "nope").status_code == 404


def test_order_fired_requires_known_table_and_station(client):
    on_shift(client)
    assert fire(client, table_id="table-99").status_code == 422
    assert fire(client, station_id="station-moon").status_code == 422
    assert fire(client, table_id=None).status_code == 422


def test_duplicate_ticket_id_is_409(client):
    on_shift(client)
    assert fire(client, ticket_id="t-1").status_code == 200
    assert fire(client, ticket_id="t-1").status_code == 409


def test_bad_player_id_and_stage_are_422(client):
    assert fire(client, player_id="Ana Smith!").status_code == 422
    on_shift(client)
    assert fire(client, stage="teleported").status_code == 422


def test_failed_publish_does_not_advance_ticket(client, monkeypatch):
    on_shift(client)
    ticket_id = fire(client).json()["ticket_id"]

    def boom(topic, event):
        raise main.HTTPException(status_code=503, detail="kafka down")

    monkeypatch.setattr(main, "_publish", boom)
    assert stage(client, "cook_started", ticket_id).status_code == 503
    assert main._open_tickets[ticket_id]["stage_index"] == 1  # still waiting for cook_started


# ------------------------------------------------ shifts

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


def test_role_is_fixed_for_the_shift(client):
    shift(client, "clock_in", role="line_cook")
    r = shift(client, "break_start", role="server")
    assert r.status_code == 409 and "line_cook" in r.text


def test_world_endpoint_matches_schema_enums(client):
    w = client.get("/api/world").json()
    assert w["stages"][0] == "order_fired" and w["stages"][-1] == "delivered"
    assert "line_cook" in w["roles"] and "clock_in" in w["shift_actions"]
    assert "station-grill" in w["stations"] and "table-01" in w["tables"]
    assert w["playable_roles"]["line_cook"] == ["cook_started", "plated"]
    assert w["playable_roles"]["server"] == ["picked_up_by_server", "delivered"]
    assert set(w["playable_stations"]) <= set(w["stations"])


# ------------------------------------------------ roles

def test_roles_only_perform_their_own_stages(client):
    on_shift(client, "ana", "line_cook", "station-grill")
    on_shift(client, "bo", "server")
    ticket_id = fire(client).json()["ticket_id"]

    r = stage(client, "cook_started", ticket_id, player="bo")
    assert r.status_code == 403 and "server" in r.text
    assert stage(client, "cook_started", ticket_id, player="ana").status_code == 200
    assert stage(client, "plated", ticket_id, player="ana").status_code == 200
    r = stage(client, "picked_up_by_server", ticket_id, player="ana")
    assert r.status_code == 403 and "line_cook" in r.text
    assert stage(client, "picked_up_by_server", ticket_id, player="bo").status_code == 200
    assert stage(client, "delivered", ticket_id, player="bo").status_code == 200
    assert [e["source_id"] for e in service_events(client)] == \
        ["game-ana", "game-ana", "game-ana", "game-bo", "game-bo"]


def test_a_cook_only_works_their_own_station(client):
    on_shift(client, "ana", "line_cook", "station-saute")
    ticket_id = fire(client, station_id="station-grill").json()["ticket_id"]
    r = stage(client, "cook_started", ticket_id)
    assert r.status_code == 403 and "station-grill" in r.text


def test_players_must_be_clocked_in_and_off_break(client):
    assert fire(client).status_code == 409
    on_shift(client)
    ticket_id = fire(client).json()["ticket_id"]
    assert stage(client, "cook_started", ticket_id, player="ghost").status_code == 409
    shift(client, "break_start")
    assert stage(client, "cook_started", ticket_id).status_code == 409
    assert fire(client).status_code == 409


# ------------------------------------------------ the crew

def test_crew_does_not_touch_a_stage_a_player_can_do(client):
    on_shift(client)
    ticket_id = fire(client).json()["ticket_id"]
    tick(client)
    tick(client, 60)  # far past any crew delay
    assert [e["stage"] for e in service_events(client)] == ["order_fired"]
    assert main._open_tickets[ticket_id]["stage_index"] == 1


def test_crew_cooks_when_no_cook_is_at_that_station(client):
    spawner_on(client)
    on_shift(client, "bo", "server")   # only a server is on shift
    tick(client)                        # spawn (crew fires the ticket), crew starts its delay
    (ticket_id,) = main._open_tickets
    tick(client, 5.0)                   # cook_started
    tick(client)
    tick(client, 5.0)                   # plated
    mine = [e["stage"] for e in service_events(client) if e["ticket_id"] == ticket_id]
    assert mine == ["order_fired", "cook_started", "plated"]  # (the dining room fires others meanwhile)
    # now a player can act: the server takes over and the crew stands back
    board = {r["ticket_id"]: r for r in client.get("/api/tickets", params={"player_id": "bo"}).json()}
    assert board[ticket_id]["can_act"] is True
    tick(client, 60)
    assert main._open_tickets[ticket_id]["stage_index"] == main.STAGES.index("picked_up_by_server")


def test_crew_events_are_tagged_simulated(client):
    spawner_on(client)
    on_shift(client, "bo", "server")
    tick(client)
    tick(client)
    tick(client, 5.0)
    events = service_events(client)
    assert len(events) >= 2
    assert {(e["source_kind"], e["source_id"]) for e in events} == {("simulated", "game-crew")}


def test_crew_covers_while_the_player_is_on_break(client):
    on_shift(client)
    ticket_id = fire(client).json()["ticket_id"]
    shift(client, "break_start")
    tick(client)
    tick(client, 5.0)  # crew starts the cook_started delay only now; check it fires
    tick(client)
    tick(client, 5.0)
    assert [e["stage"] for e in service_events(client)][:2] == ["order_fired", "cook_started"]
    assert service_events(client)[1]["source_id"] == "game-crew"
    shift(client, "break_end")
    n = len(client.published)
    tick(client, 60)
    assert len(client.published) == n  # the player has plated to do; the crew stands back


def test_crew_finishes_tickets_after_the_player_clocks_out(client):
    on_shift(client)
    ticket_id = fire(client).json()["ticket_id"]
    shift(client, "clock_out")
    for _ in range(12):
        tick(client, 5.0)
    assert [e["stage"] for e in service_events(client)] == main.STAGES
    assert ticket_id not in main._open_tickets


def test_a_failed_crew_publish_backs_off_and_retries(client, monkeypatch):
    spawner_on(client)
    on_shift(client, "bo", "server")
    tick(client)
    (ticket_id,) = main._open_tickets
    tick(client)  # delay starts
    good = main._publish

    def boom(topic, event):
        raise main.HTTPException(status_code=503, detail="kafka down")

    monkeypatch.setattr(main, "_publish", boom)
    tick(client, 5.0)
    assert main._open_tickets[ticket_id]["stage_index"] == 1  # unchanged
    monkeypatch.setattr(main, "_publish", good)
    tick(client, 5.0)
    assert main._open_tickets[ticket_id]["stage_index"] == 2  # retried and advanced


# ------------------------------------------------ the dining room

def test_no_tickets_are_fired_with_nobody_on_shift(client):
    spawner_on(client)
    tick(client, 100)
    assert client.get("/api/tickets").json() == []


def test_tickets_spawn_at_the_cooks_station_on_a_timer_up_to_the_cap(client, monkeypatch):
    monkeypatch.setattr(main, "CREW_MIN_SECONDS", 10_000.0)  # keep the crew out of this test
    monkeypatch.setattr(main, "CREW_MAX_SECONDS", 10_000.0)
    spawner_on(client)
    on_shift(client, "ana", "line_cook", "station-salad")
    tick(client)
    assert len(main._open_tickets) == 1
    assert {t["station_id"] for t in main._open_tickets.values()} == {"station-salad"}
    tick(client, 7.9)
    assert len(main._open_tickets) == 1          # not yet
    tick(client, 0.2)
    assert len(main._open_tickets) == 2
    for _ in range(10):
        tick(client, 8.0)
    assert len(main._open_tickets) == main.MAX_OPEN_TICKETS


def test_a_failed_spawn_does_not_retry_every_tick(client, monkeypatch):
    calls = []

    def boom(topic, event):
        calls.append(event)
        raise main.HTTPException(status_code=503, detail="kafka down")

    spawner_on(client)
    on_shift(client)
    monkeypatch.setattr(main, "_publish", boom)
    tick(client)
    tick(client, 0.5)
    tick(client, 0.5)
    assert len(calls) == 1
    assert main._open_tickets == {}


def test_a_backed_up_station_does_not_starve_a_cook_at_another(client, monkeypatch):
    monkeypatch.setattr(main, "CREW_MIN_SECONDS", 10_000.0)
    monkeypatch.setattr(main, "CREW_MAX_SECONDS", 10_000.0)
    spawner_on(client)
    on_shift(client, "ana", "line_cook", "station-saute")
    for _ in range(10):
        tick(client, 8.0)
    assert len(main._open_tickets) == main.MAX_OPEN_TICKETS  # saute is full
    on_shift(client, "cy", "line_cook", "station-grill")
    tick(client, 8.0)
    stations = sorted(t["station_id"] for t in main._open_tickets.values())
    assert stations.count("station-grill") == 1  # the new cook still gets tickets
    assert stations.count("station-saute") == main.MAX_OPEN_TICKETS


# ------------------------------------------------ the ticket board

def test_ticket_board_says_who_each_ticket_waits_on(client):
    on_shift(client, "ana", "line_cook", "station-grill")
    on_shift(client, "bo", "server")
    t1 = fire(client, station_id="station-grill").json()["ticket_id"]
    t2 = fire(client, station_id="station-saute").json()["ticket_id"]
    client.clock.advance(4)
    stage(client, "cook_started", t1)
    stage(client, "plated", t1)

    def board(player):
        return {r["ticket_id"]: r for r in client.get("/api/tickets", params={"player_id": player}).json()}

    ana, bo = board("ana"), board("bo")
    assert ana[t1]["waiting_on"] == "another player" and not ana[t1]["can_act"]   # the server has it
    assert bo[t1]["waiting_on"] == "you" and bo[t1]["can_act"]
    assert ana[t2]["waiting_on"] == "crew"                                          # nobody cooks at saute
    assert bo[t2]["waiting_on"] == "crew"
    assert ana[t1]["last_stage"] == "plated" and ana[t1]["next_stage"] == "picked_up_by_server"
    assert client.get("/api/tickets", params={"player_id": "Bad Id"}).status_code == 422
