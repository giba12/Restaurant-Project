"""
game-bridge (version 2 groundwork)

HTTP-to-Kafka bridge for a human-driven game client. A player's actions
become the same schema-validated events the edge simulators emit, published
to the same Kafka topics -- so storage, anomaly detection, causal
inference, the digital twin and the narrator run completely unmodified.
The contract additions are source_kind "player" (all four event schemas) and "crew"
(ServiceTimingEvent only), plus the ticket `origin` the aggregator derives from them.

Why a bridge instead of a Kafka client inside the game engine: game engines
have no first-class Kafka client, and this keeps validation in exactly one
place. The game sends only domain fields (which stage, which table, which
station); the bridge owns the envelope (event_id, timestamp, source_id,
source_kind, schema_version, restaurant_id) and everything derived
(elapsed_since_previous_stage_ms), so a client can't produce a malformed or
self-inconsistent event even by accident.

Roles and the crew. A player clocks in as a line_cook (owns cook_started, at
their own station), an expo (owns plated, the whole floor -- expo works the
pass for every station, not one) or a server (owns picked_up_by_server and
delivered, the whole floor). Every stage a player does not own is done by the crew: a
background "director" thread advances it after a random delay, tagged
source_kind "crew" / source_id "game-crew" so the pipeline can tell the crew
from the player -- and so the ticket-timing aggregator can mark every ticket
in an interactive session (which the crew fires) as such and keep it out of the
simulators' anomaly baseline. A stage is crew-owned whenever no clocked-in, not-on-break
player can do it. The director also fires new tickets (the dining room) while
anyone is on shift, up to MAX_OPEN_TICKETS per staffed station. So the player's speed moves the
whole chain: a slow cook delays the server, a slow server delays the guest.

Guests. A guest is not staff: their events are tagged "player" like a staff
player's, but they never clock in, hold no role, and are gated by nothing
StaffShiftEvent's role enum defines (it has no "guest"). POST /api/guest/order
picks a table and a menu (world.MENU, the same one PlateWaste/POS simulators
use), fires the kitchen ticket exactly as order_fired does, and records the
order; the kitchen decides which station cooks it, the same as the dining
room's own tickets. GET /api/guest/status reports the ticket's progress and
the running total; POST /api/guest/pay is only accepted once the ticket has
been delivered, and publishes a POSTransactionEvent (source_kind "player",
already an allowed value on that schema) for the same line items. This closes
the loop the schemas already supported: a guest's order and payment are real
data alongside a staff player's, quarantined from the simulators' baseline
the same way (see ticket-timing-aggregator's origin_of()).

Per-ticket and per-player state is in memory and resets on restart, same as
the simulators' TicketLifecycle/ShiftState. A ticket left open across a
bridge restart gets a 404 on its next stage; the client re-reads GET
/api/tickets and simply stops seeing it.

Limits: no authentication -- local demo only, like dashboard-api's open CORS.
The director holds the state lock while it publishes, as the API does, so a
slow Kafka delays both.
"""
import json
import logging
import os
import random
import threading
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import jsonschema
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

import world  # copied from edge-simulators/common/world.py at image build

KAFKA_BOOTSTRAP_SERVERS = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS",
    "restaurant-platform-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092",
)
SCHEMA_DIR = os.environ.get("SCHEMA_DIR", "/app/schemas")
# A ticket a player abandons mid-lifecycle would otherwise sit in memory forever.
MAX_TICKET_AGE_SECONDS = int(os.environ.get("MAX_TICKET_AGE_SECONDS", "3600"))

# Game pacing. Environment-tunable so tests (and a slower or faster game) need no code change.
SPAWN_SECONDS = float(os.environ.get("SPAWN_SECONDS", "8"))          # gap between new tickets
MAX_OPEN_TICKETS = int(os.environ.get("MAX_OPEN_TICKETS", "5"))      # per staffed station
CREW_MIN_SECONDS = float(os.environ.get("CREW_MIN_SECONDS", "6"))    # crew delay per stage
CREW_MAX_SECONDS = float(os.environ.get("CREW_MAX_SECONDS", "14"))
DIRECTOR_TICK_SECONDS = float(os.environ.get("DIRECTOR_TICK_SECONDS", "0.5"))
DIRECTOR_ENABLED = os.environ.get("GAME_DIRECTOR", "1") != "0"

log = logging.getLogger("uvicorn.error")

SERVICE_TIMING_TOPIC = "service-timing-events"
STAFF_SHIFT_TOPIC = "staff-shift-events"
POS_TRANSACTION_TOPIC = "pos-transaction-events"

MAX_ORDER_ITEMS = 10
DEFAULT_PAYMENT_METHOD = "card"

PLAYER_ID_PATTERN = r"^[a-z0-9][a-z0-9-]{0,31}$"


def _load_validator(filename: str) -> tuple[dict, jsonschema.Draft202012Validator]:
    with open(os.path.join(SCHEMA_DIR, filename), "r") as f:
        schema = json.load(f)
    return schema, jsonschema.Draft202012Validator(schema)


TIMING_SCHEMA, TIMING_VALIDATOR = _load_validator("ServiceTimingEvent.schema.json")
SHIFT_SCHEMA, SHIFT_VALIDATOR = _load_validator("StaffShiftEvent.schema.json")
POS_SCHEMA, POS_VALIDATOR = _load_validator("POSTransactionEvent.schema.json")
PAYMENT_METHODS: list[str] = POS_SCHEMA["properties"]["payment_method"]["enum"]

# Enum values come from the schemas, not from a second copy in this file --
# contract-first: if a schema's stage list changes, this follows.
STAGES: list[str] = TIMING_SCHEMA["properties"]["stage"]["enum"]
SHIFT_ACTIONS: list[str] = SHIFT_SCHEMA["properties"]["shift_action"]["enum"]
ROLES: list[str] = SHIFT_SCHEMA["properties"]["role"]["enum"]

# Game rules (not contract): which stages each playable role performs, and the
# stations a line cook can stand at. Checked against the schema and world at
# import so a renamed stage or station fails loudly here, not mid-game.
# order_fired -> cook_started (line_cook, at their station) -> plated (expo, whole floor,
# since expo works the pass for every station) -> picked_up_by_server -> delivered (server,
# whole floor). Only line_cook is station-scoped (see _can_act); real kitchens don't scope
# expo or server to one station.
PLAYABLE_ROLES: dict[str, list[str]] = {
    "line_cook": ["cook_started"],
    "expo": ["plated"],
    "server": ["picked_up_by_server", "delivered"],
}
# Cooking stations only -- where a line cook stands and a ticket is fired. station-expo is
# deliberately not here: it is the pass expo works, not a place a ticket is cooked, so a
# ticket is never fired "at" it. (It used to be, before the expo role existed; fixed 2026-09-22.)
PLAYABLE_STATIONS: list[str] = ["station-grill", "station-saute", "station-salad"]
for _role, _stages in PLAYABLE_ROLES.items():
    if _role not in ROLES or any(st not in STAGES for st in _stages):
        raise ValueError(f"PLAYABLE_ROLES[{_role!r}] does not match the schemas' roles/stages")
if any(st not in world.STATIONS for st in PLAYABLE_STATIONS):
    raise ValueError("PLAYABLE_STATIONS does not match world.STATIONS")

# Who an event is attributed to: (source_kind, source_id).
CREW_ACTOR = ("crew", "game-crew")

@asynccontextmanager
async def lifespan(_app: FastAPI):
    stop = threading.Event()

    def loop() -> None:
        while not stop.wait(DIRECTOR_TICK_SECONDS):
            try:
                director_tick()
            except Exception:  # never let one bad tick kill the game
                log.exception("director tick failed")

    if DIRECTOR_ENABLED:
        threading.Thread(target=loop, name="director", daemon=True).start()
    yield
    stop.set()


app = FastAPI(title="restaurant-platform game-bridge", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # local demo only; needed if the game is exported to the web
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

_lock = threading.Lock()
_producer = None  # kafka.KafkaProducer, created on first publish
# ticket_id -> {"stage_index", "last_stage_ts", "table_id", "station_id", "crew_due"}
# ("stage_index" is the NEXT stage to perform; "crew_due" is when the crew will do it, if it is theirs)
_open_tickets: dict[str, dict] = {}
# staff_id -> {"on_break": bool, "role": str, "station_id": str | None}; presence means clocked in
_clocked_in: dict[str, dict] = {}
_last_spawn = float("-inf")
# A guest is not staff -- no clock-in, no StaffShiftEvent (its role enum has no "guest"). Keyed
# by the guest's own player_id (not staff's "player-<name>"), so the two never collide.
# player_id -> {"table_id", "ticket_id", "items": [{"menu_item_id","quantity"}], "delivered": bool}
_guests: dict[str, dict] = {}
# ticket_id -> player_id, cleared when the ticket is delivered (see _publish_stage). Lets
# _publish_stage mark a guest's order delivered without every caller needing to know about guests.
_ticket_to_guest: dict[str, str] = {}


def _clock() -> float:
    return time.monotonic()  # one place to fake in tests


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _get_producer():
    global _producer
    if _producer is None:
        # Deferred import: kafka-python 2.0.2 only imports on Python <= 3.11, and
        # nothing else here (or in the tests) needs it.
        from kafka import KafkaProducer
        _producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            api_version=(2, 8, 0),  # pinned -- automatic negotiation fails against Kafka 4.3.1
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
    return _producer


def _publish(topic: str, event: dict) -> None:
    """Blocks until the broker acknowledges, so a 200 means the event is on the topic."""
    try:
        _get_producer().send(topic, event).get(timeout=10)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"could not publish to Kafka: {exc}")


def _validated(validator: jsonschema.Draft202012Validator, event: dict) -> dict:
    errors = sorted(validator.iter_errors(event), key=lambda e: list(e.path))
    if errors:
        raise HTTPException(
            status_code=422,
            detail=[f"{'/'.join(str(p) for p in e.path) or '(event)'}: {e.message}" for e in errors],
        )
    return event


def _player_actor(player_id: str) -> tuple[str, str]:
    return ("player", f"game-{player_id}")


def _envelope(event_type: str, actor: tuple[str, str]) -> dict:
    source_kind, source_id = actor
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "schema_version": world.SCHEMA_VERSION,
        "source_id": source_id,
        "source_kind": source_kind,
        "timestamp": _now_iso(),
        "restaurant_id": world.RESTAURANT_ID,
    }


def _require_known(value: str | None, allowed: list[str], field: str) -> None:
    # The JSON Schema only requires a string here; the closed ID spaces live in
    # world.py, and staying inside them is what keeps player events joinable.
    if value not in allowed:
        raise HTTPException(status_code=422, detail=f"{field} must be one of {allowed}, got {value!r}")


def _evict_stale_tickets() -> None:
    cutoff = _clock() - MAX_TICKET_AGE_SECONDS
    for tid in [t for t, v in _open_tickets.items() if v["last_stage_ts"] < cutoff]:
        del _open_tickets[tid]


class ServiceTimingRequest(BaseModel):
    player_id: str = Field(pattern=PLAYER_ID_PATTERN)
    stage: str
    ticket_id: str | None = Field(default=None, description="Omit on order_fired to have one minted.")
    table_id: str | None = Field(default=None, description="Required on order_fired; inherited afterwards.")
    station_id: str | None = Field(default=None, description="Required on order_fired; inherited afterwards.")


class StaffShiftRequest(BaseModel):
    player_id: str = Field(pattern=PLAYER_ID_PATTERN)
    role: str
    shift_action: str
    station_id: str | None = None
    scheduled_vs_actual: str | None = None


class GuestOrderItem(BaseModel):
    menu_item_id: str
    quantity: int = Field(default=1, ge=1, le=10)


class GuestOrderRequest(BaseModel):
    player_id: str = Field(pattern=PLAYER_ID_PATTERN)
    table_id: str
    items: list[GuestOrderItem] = Field(min_length=1, max_length=MAX_ORDER_ITEMS)


class GuestPayRequest(BaseModel):
    player_id: str = Field(pattern=PLAYER_ID_PATTERN)
    payment_method: str = DEFAULT_PAYMENT_METHOD


@app.get("/api/health")
def health():
    return {"status": "ok"}


@app.get("/api/world")
def world_info():
    """What a client may send -- lets the game populate its UI without hardcoding IDs."""
    return {
        "restaurant_id": world.RESTAURANT_ID,
        "stations": world.STATIONS,
        "tables": world.TABLES,
        "stages": STAGES,
        "roles": ROLES,
        "shift_actions": SHIFT_ACTIONS,
        "playable_roles": PLAYABLE_ROLES,  # role -> the stages that role performs
        "playable_stations": PLAYABLE_STATIONS,
        "menu": world.MENU,  # menu_item_id -> unit_price_cents
        "payment_methods": PAYMENT_METHODS,
    }


def _can_act(state: dict | None, stage: str, station_id: str | None) -> bool:
    """Can this clocked-in player perform `stage` on a ticket at `station_id` right now?"""
    if state is None or state["on_break"]:
        return False
    if stage not in PLAYABLE_ROLES.get(state["role"], ()):
        return False
    # A line cook works their own station; a server works the whole floor.
    return state["role"] != "line_cook" or state["station_id"] == station_id


def _player_who_can(stage: str, station_id: str | None) -> str | None:
    for staff_id, state in _clocked_in.items():
        if _can_act(state, stage, station_id):
            return staff_id
    return None


def _publish_stage(actor: tuple[str, str], stage: str, ticket_id: str,
                   table_id: str, station_id: str) -> dict:
    """Validate, publish and only then advance the ticket. Caller holds _lock and has
    already checked the stage is the ticket's next one."""
    if stage == STAGES[0]:
        elapsed_ms = None
    else:
        elapsed_ms = int((_clock() - _open_tickets[ticket_id]["last_stage_ts"]) * 1000)

    event = _envelope("ServiceTimingEvent", actor)
    event.update(
        ticket_id=ticket_id,
        table_id=table_id,
        station_id=station_id,
        stage=stage,
        elapsed_since_previous_stage_ms=elapsed_ms,
    )
    _validated(TIMING_VALIDATOR, event)
    _publish(SERVICE_TIMING_TOPIC, event)

    # Only advance state once the event is actually on the topic.
    if stage == STAGES[-1]:
        _open_tickets.pop(ticket_id, None)
        guest_id = _ticket_to_guest.pop(ticket_id, None)
        if guest_id is not None and guest_id in _guests:
            _guests[guest_id]["delivered"] = True
    else:
        _open_tickets[ticket_id] = {
            "stage_index": STAGES.index(stage) + 1,
            "last_stage_ts": _clock(),
            "table_id": table_id,
            "station_id": station_id,
            "crew_due": None,
        }
    return event


@app.post("/api/service-timing")
def post_service_timing(req: ServiceTimingRequest):
    with _lock:
        _evict_stale_tickets()
        if req.stage not in STAGES:
            raise HTTPException(status_code=422, detail=f"stage must be one of {STAGES}, got {req.stage!r}")
        staff_id = f"player-{req.player_id}"
        state = _clocked_in.get(staff_id)

        if req.stage == STAGES[0]:
            # PLAYABLE_STATIONS, not world.STATIONS: a ticket is only ever cooked at a real
            # cooking station. Firing one at, say, station-expo or station-bar would create a
            # ticket no line cook could ever be staffed at, so the crew would silently cook
            # every one of them forever.
            _require_known(req.station_id, PLAYABLE_STATIONS, "station_id")
            _require_known(req.table_id, world.TABLES, "table_id")
            if state is None or state["on_break"]:
                raise HTTPException(status_code=409, detail=f"{staff_id} must be clocked in (and not on break) to fire a ticket")
            ticket_id = req.ticket_id or str(uuid.uuid4())
            if ticket_id in _open_tickets:
                raise HTTPException(status_code=409, detail=f"ticket {ticket_id} is already open")
            table_id, station_id = req.table_id, req.station_id
        else:
            if not req.ticket_id:
                raise HTTPException(status_code=422, detail="ticket_id is required after order_fired")
            ticket = _open_tickets.get(req.ticket_id)
            if ticket is None:
                raise HTTPException(status_code=404, detail=f"no open ticket {req.ticket_id}")
            expected = STAGES[ticket["stage_index"]]
            if req.stage != expected:
                raise HTTPException(
                    status_code=409,
                    detail=f"ticket {req.ticket_id} is waiting for stage {expected!r}, got {req.stage!r}",
                )
            if state is None:
                raise HTTPException(status_code=409, detail=f"{staff_id} is not clocked in")
            if state["on_break"]:
                raise HTTPException(status_code=409, detail=f"{staff_id} is on break")
            if req.stage not in PLAYABLE_ROLES.get(state["role"], ()):
                raise HTTPException(status_code=403, detail=f"a {state['role']} does not perform {req.stage!r}")
            if not _can_act(state, req.stage, ticket["station_id"]):
                raise HTTPException(
                    status_code=403,
                    detail=f"that ticket is at {ticket['station_id']}; you are at {state['station_id']}",
                )
            ticket_id = req.ticket_id
            table_id, station_id = ticket["table_id"], ticket["station_id"]

        return _publish_stage(_player_actor(req.player_id), req.stage, ticket_id, table_id, station_id)


@app.get("/api/tickets")
def list_tickets(player_id: str | None = Query(default=None, pattern=PLAYER_ID_PATTERN)):
    """Open tickets, oldest first, and who each is waiting on. With player_id, `can_act`
    says whether that player may perform the ticket's next stage right now."""
    with _lock:
        _evict_stale_tickets()
        now = _clock()
        state = _clocked_in.get(f"player-{player_id}") if player_id else None
        rows = []
        for ticket_id, t in _open_tickets.items():
            stage = STAGES[t["stage_index"]]
            can_act = _can_act(state, stage, t["station_id"])
            if can_act:
                waiting_on = "you"
            elif _player_who_can(stage, t["station_id"]) is not None:
                waiting_on = "another player"
            else:
                waiting_on = "crew"
            rows.append({
                "ticket_id": ticket_id,
                "table_id": t["table_id"],
                "station_id": t["station_id"],
                "last_stage": STAGES[t["stage_index"] - 1],
                "next_stage": stage,
                "seconds_in_stage": round(now - t["last_stage_ts"], 1),
                "waiting_on": waiting_on,
                "can_act": can_act,
            })
        rows.sort(key=lambda r: -r["seconds_in_stage"])
        return rows


# ---------------------------------------------------------------- guests (ordering, eating, paying)

def _guest_status_locked(player_id: str) -> dict:
    """Caller holds _lock. Raises 404 if player_id has no open order."""
    guest = _guests.get(player_id)
    if guest is None:
        raise HTTPException(status_code=404, detail=f"no open order for {player_id!r}")
    total_cents = sum(world.MENU[i["menu_item_id"]] * i["quantity"] for i in guest["items"])
    row = {
        "table_id": guest["table_id"],
        "ticket_id": guest["ticket_id"],
        "items": guest["items"],
        "total_amount_cents": total_cents,
        "delivered": guest["delivered"],
    }
    ticket = _open_tickets.get(guest["ticket_id"])
    if ticket is not None:
        row["next_stage"] = STAGES[ticket["stage_index"]]
        row["seconds_in_stage"] = round(_clock() - ticket["last_stage_ts"], 1)
    return row


@app.post("/api/guest/order")
def post_guest_order(req: GuestOrderRequest):
    """A guest is not staff: no clock-in, and no role gates what they may do. Ordering fires
    the kitchen ticket exactly as an order_fired stage does, at whichever cooking station has
    room -- the guest does not pick one, matching how a kitchen actually assigns tickets."""
    with _lock:
        _require_known(req.table_id, world.TABLES, "table_id")
        if req.player_id in _guests:
            raise HTTPException(status_code=409, detail=f"{req.player_id} already has an open order; pay before ordering again")
        for item in req.items:
            if item.menu_item_id not in world.MENU:
                raise HTTPException(status_code=422, detail=f"menu_item_id must be one of {world.MENU_ITEM_IDS}, got {item.menu_item_id!r}")
        eligible = _eligible_stations()
        if not eligible:
            raise HTTPException(status_code=503, detail="the kitchen has no room right now -- try again shortly")
        station_id = random.choice(eligible)
        ticket_id = str(uuid.uuid4())
        _publish_stage(_player_actor(req.player_id), STAGES[0], ticket_id, req.table_id, station_id)
        _guests[req.player_id] = {
            "table_id": req.table_id,
            "ticket_id": ticket_id,
            "items": [item.model_dump() for item in req.items],
            "delivered": False,
        }
        _ticket_to_guest[ticket_id] = req.player_id
        return _guest_status_locked(req.player_id)


@app.get("/api/guest/status")
def get_guest_status(player_id: str = Query(pattern=PLAYER_ID_PATTERN)):
    with _lock:
        return _guest_status_locked(player_id)


@app.post("/api/guest/pay")
def post_guest_pay(req: GuestPayRequest):
    with _lock:
        guest = _guests.get(req.player_id)
        if guest is None:
            raise HTTPException(status_code=404, detail=f"no open order for {req.player_id!r}")
        if not guest["delivered"]:
            raise HTTPException(status_code=409, detail="the order has not been delivered yet")
        if req.payment_method not in PAYMENT_METHODS:
            raise HTTPException(status_code=422, detail=f"payment_method must be one of {PAYMENT_METHODS}, got {req.payment_method!r}")

        line_items = [
            {
                "menu_item_id": i["menu_item_id"],
                "quantity": i["quantity"],
                "unit_price_cents": world.MENU[i["menu_item_id"]],
                "modifiers": [],
                "voided": False,
            }
            for i in guest["items"]
        ]
        event = _envelope("POSTransactionEvent", _player_actor(req.player_id))
        event.update(
            transaction_id=str(uuid.uuid4()),
            table_id=guest["table_id"],
            server_staff_id=None,
            line_items=line_items,
            total_amount_cents=sum(li["quantity"] * li["unit_price_cents"] for li in line_items),
            currency="USD",
            payment_method=req.payment_method,
            discount_applied_cents=0,
        )
        _validated(POS_VALIDATOR, event)
        _publish(POS_TRANSACTION_TOPIC, event)
        del _guests[req.player_id]
        return event


@app.post("/api/staff-shift")
def post_staff_shift(req: StaffShiftRequest):
    staff_id = f"player-{req.player_id}"
    with _lock:
        state = _clocked_in.get(staff_id)
        action = req.shift_action

        if action == "clock_in":
            if state is not None:
                raise HTTPException(status_code=409, detail=f"{staff_id} is already clocked in")
        elif state is None:
            raise HTTPException(status_code=409, detail=f"{staff_id} is not clocked in")
        elif req.role != state["role"]:
            raise HTTPException(status_code=409, detail=f"{staff_id} clocked in as {state['role']}, not {req.role}")
        elif action == "break_start" and state["on_break"]:
            raise HTTPException(status_code=409, detail=f"{staff_id} is already on break")
        elif action == "break_end" and not state["on_break"]:
            raise HTTPException(status_code=409, detail=f"{staff_id} is not on break")

        if action == "station_reassign" or req.station_id is not None:
            _require_known(req.station_id, world.STATIONS, "station_id")

        event = _envelope("StaffShiftEvent", _player_actor(req.player_id))
        event.update(staff_id=staff_id, role=req.role, shift_action=action, station_id=req.station_id)
        if req.scheduled_vs_actual is not None:
            event["scheduled_vs_actual"] = req.scheduled_vs_actual
        _validated(SHIFT_VALIDATOR, event)
        _publish(STAFF_SHIFT_TOPIC, event)

        if action == "clock_in":
            _clocked_in[staff_id] = {"on_break": False, "role": req.role, "station_id": req.station_id}
        elif action == "clock_out":
            del _clocked_in[staff_id]
        elif action == "break_start":
            state["on_break"] = True
        elif action == "break_end":
            state["on_break"] = False
        elif action == "station_reassign":
            state["station_id"] = req.station_id
        return event


# ---------------------------------------------------------------- the director

def _eligible_stations() -> list[str]:
    """Cooking stations with room for one more open ticket. The cap is per station a cook is
    working, so a backed-up station never starves a cook who just arrived at another; with no
    cook on shift (only servers, or a guest ordering with nobody clocked in at all) it falls
    back to every cooking station, capped as a whole, so food can still go out."""
    cook_stations = {
        s["station_id"] for s in _clocked_in.values()
        if not s["on_break"] and s["role"] == "line_cook" and s["station_id"]
    }
    open_at = lambda st: sum(1 for t in _open_tickets.values() if t["station_id"] == st)
    if cook_stations:
        return sorted(st for st in cook_stations if open_at(st) < MAX_OPEN_TICKETS)
    return PLAYABLE_STATIONS if len(_open_tickets) < MAX_OPEN_TICKETS else []


def _spawn_ticket(now: float) -> None:
    """The dining room: while anyone playable is on shift, fire a ticket every SPAWN_SECONDS
    (at one of the cooks' stations, or anywhere in the kitchen if only servers are working)."""
    global _last_spawn
    active = [s for s in _clocked_in.values() if not s["on_break"] and s["role"] in PLAYABLE_ROLES]
    if not active or now - _last_spawn < SPAWN_SECONDS:
        return
    eligible = _eligible_stations()
    if not eligible:
        return
    _last_spawn = now  # set first: a failed publish then retries after SPAWN_SECONDS, not every tick
    station_id = random.choice(eligible)
    table_id = random.choice(world.TABLES)
    try:
        _publish_stage(CREW_ACTOR, STAGES[0], str(uuid.uuid4()), table_id, station_id)
    except HTTPException as exc:
        log.warning("director could not fire a ticket: %s", exc.detail)


def director_tick() -> None:
    """One step of the crew: maybe fire a ticket, and advance every ticket whose next
    stage no player can do, once its random delay has passed. Called by a background
    thread every DIRECTOR_TICK_SECONDS; tests call it directly with a fake _clock."""
    with _lock:
        now = _clock()
        _evict_stale_tickets()
        _spawn_ticket(now)
        for ticket_id, t in list(_open_tickets.items()):
            stage = STAGES[t["stage_index"]]
            if _player_who_can(stage, t["station_id"]) is not None:
                t["crew_due"] = None  # a player has it; the crew stands back
            elif t["crew_due"] is None:
                t["crew_due"] = now + random.uniform(CREW_MIN_SECONDS, CREW_MAX_SECONDS)
            elif now >= t["crew_due"]:
                try:
                    _publish_stage(CREW_ACTOR, stage, ticket_id, t["table_id"], t["station_id"])
                except HTTPException as exc:
                    t["crew_due"] = now + CREW_MIN_SECONDS  # back off, do not hammer a down Kafka
                    log.warning("crew could not advance %s to %s: %s", ticket_id, stage, exc.detail)
