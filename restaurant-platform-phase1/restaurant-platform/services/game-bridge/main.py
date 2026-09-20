"""
game-bridge (version 2 groundwork)

HTTP-to-Kafka bridge for a human-driven game client. A player's actions
become the same schema-validated events the edge simulators emit, published
to the same Kafka topics -- so storage, anomaly detection, causal
inference, the digital twin and the narrator run completely unmodified.
The only contract change is source_kind="player" (schemas/*Event.schema.json).

Why a bridge instead of a Kafka client inside the game engine: game engines
have no first-class Kafka client, and this keeps validation in exactly one
place. The game sends only domain fields (which stage, which table, which
station); the bridge owns the envelope (event_id, timestamp, source_id,
source_kind, schema_version, restaurant_id) and everything derived
(elapsed_since_previous_stage_ms), so a client can't produce a malformed or
self-inconsistent event even by accident.

Per-ticket and per-player state is in memory and resets on restart, same as
the simulators' TicketLifecycle/ShiftState. A ticket left open across a
bridge restart gets a 404 on its next stage; the client should start a new
ticket.

Limits: no authentication -- local demo only, like dashboard-api's open CORS.
"""
import json
import os
import threading
import time
import uuid
from datetime import datetime, timezone

import jsonschema
from fastapi import FastAPI, HTTPException
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

SERVICE_TIMING_TOPIC = "service-timing-events"
STAFF_SHIFT_TOPIC = "staff-shift-events"

PLAYER_ID_PATTERN = r"^[a-z0-9][a-z0-9-]{0,31}$"


def _load_validator(filename: str) -> tuple[dict, jsonschema.Draft202012Validator]:
    with open(os.path.join(SCHEMA_DIR, filename), "r") as f:
        schema = json.load(f)
    return schema, jsonschema.Draft202012Validator(schema)


TIMING_SCHEMA, TIMING_VALIDATOR = _load_validator("ServiceTimingEvent.schema.json")
SHIFT_SCHEMA, SHIFT_VALIDATOR = _load_validator("StaffShiftEvent.schema.json")

# Enum values come from the schemas, not from a second copy in this file --
# contract-first: if a schema's stage list changes, this follows.
STAGES: list[str] = TIMING_SCHEMA["properties"]["stage"]["enum"]
SHIFT_ACTIONS: list[str] = SHIFT_SCHEMA["properties"]["shift_action"]["enum"]
ROLES: list[str] = SHIFT_SCHEMA["properties"]["role"]["enum"]

app = FastAPI(title="restaurant-platform game-bridge")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # local demo only; needed if the game is exported to the web
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

_lock = threading.Lock()
_producer = None  # kafka.KafkaProducer, created on first publish
# ticket_id -> {"stage_index", "last_stage_ts" (monotonic), "table_id", "station_id"}
_open_tickets: dict[str, dict] = {}
# staff_id -> {"on_break": bool}; presence means clocked in
_clocked_in: dict[str, dict] = {}


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


def _envelope(event_type: str, player_id: str) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "schema_version": world.SCHEMA_VERSION,
        "source_id": f"game-{player_id}",
        "source_kind": "player",
        "timestamp": _now_iso(),
        "restaurant_id": world.RESTAURANT_ID,
    }


def _require_known(value: str | None, allowed: list[str], field: str) -> None:
    # The JSON Schema only requires a string here; the closed ID spaces live in
    # world.py, and staying inside them is what keeps player events joinable.
    if value not in allowed:
        raise HTTPException(status_code=422, detail=f"{field} must be one of {allowed}, got {value!r}")


def _evict_stale_tickets() -> None:
    cutoff = time.monotonic() - MAX_TICKET_AGE_SECONDS
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
    }


@app.post("/api/service-timing")
def post_service_timing(req: ServiceTimingRequest):
    with _lock:
        _evict_stale_tickets()
        if req.stage not in STAGES:
            raise HTTPException(status_code=422, detail=f"stage must be one of {STAGES}, got {req.stage!r}")

        if req.stage == STAGES[0]:
            _require_known(req.station_id, world.STATIONS, "station_id")
            _require_known(req.table_id, world.TABLES, "table_id")
            ticket_id = req.ticket_id or str(uuid.uuid4())
            if ticket_id in _open_tickets:
                raise HTTPException(status_code=409, detail=f"ticket {ticket_id} is already open")
            table_id, station_id, elapsed_ms = req.table_id, req.station_id, None
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
            ticket_id = req.ticket_id
            table_id, station_id = ticket["table_id"], ticket["station_id"]
            elapsed_ms = int((time.monotonic() - ticket["last_stage_ts"]) * 1000)

        event = _envelope("ServiceTimingEvent", req.player_id)
        event.update(
            ticket_id=ticket_id,
            table_id=table_id,
            station_id=station_id,
            stage=req.stage,
            elapsed_since_previous_stage_ms=elapsed_ms,
        )
        _validated(TIMING_VALIDATOR, event)
        _publish(SERVICE_TIMING_TOPIC, event)

        # Only advance state once the event is actually on the topic.
        if req.stage == STAGES[-1]:
            _open_tickets.pop(ticket_id, None)
        else:
            _open_tickets[ticket_id] = {
                "stage_index": STAGES.index(req.stage) + 1,
                "last_stage_ts": time.monotonic(),
                "table_id": table_id,
                "station_id": station_id,
            }
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
        elif action == "break_start" and state["on_break"]:
            raise HTTPException(status_code=409, detail=f"{staff_id} is already on break")
        elif action == "break_end" and not state["on_break"]:
            raise HTTPException(status_code=409, detail=f"{staff_id} is not on break")

        if action == "station_reassign" or req.station_id is not None:
            _require_known(req.station_id, world.STATIONS, "station_id")

        event = _envelope("StaffShiftEvent", req.player_id)
        event.update(staff_id=staff_id, role=req.role, shift_action=action, station_id=req.station_id)
        if req.scheduled_vs_actual is not None:
            event["scheduled_vs_actual"] = req.scheduled_vs_actual
        _validated(SHIFT_VALIDATOR, event)
        _publish(STAFF_SHIFT_TOPIC, event)

        if action == "clock_in":
            _clocked_in[staff_id] = {"on_break": False}
        elif action == "clock_out":
            del _clocked_in[staff_id]
        elif action == "break_start":
            state["on_break"] = True
        elif action == "break_end":
            state["on_break"] = False
        return event
