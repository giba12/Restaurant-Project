# Version 2: the game

A human-playable front end for the platform. You clock in at a kitchen station, tickets arrive, and you move each one through the stages. Every action becomes a real event on the same Kafka topics the simulators use, so storage, anomaly detection, causal inference, the digital twin and the narrator all run on your play without being changed.

Everything for version 2 lives in this directory. Version 1 (the simulated platform, everything outside `game/`) runs completely without it.

```
game/
  bridge/                  HTTP-to-Kafka service (FastAPI): validates a player's actions and publishes events
  client/                  the Godot 4.5 project (the game itself)
  docker-compose.game.yml  Compose overlay that adds the bridge to the base stack
  README.md                this file
```

## The boundary between version 1 and version 2

**Rule: version 1 never references version 2.** Nothing outside `game/` imports, builds, starts or names anything inside it. The dependency runs one way only, from the game onto the platform.

The game depends on the platform in exactly these places, and nowhere else:

| Dependency | Where it lives | Why it is in version 1's tree |
|---|---|---|
| `source_kind: "player"` on the four event schemas | `schemas/*.schema.json` | It is a contract change, and the contract must be one shared definition. Simulators and vendor integrations ignore it. |
| Migration `004`, which lets the database accept `player` | `storage/schema/004_player_source_kind.sql` (and its copy under `k8s/timescaledb/files/`) | It has to reach every database, including ones created before the game existed, or player events would fail a `CHECK`. It is idempotent and harmless without the game. |
| The simulated world's ids (stations, tables, `RESTAURANT_ID`) | `edge-simulators/common/world.py`, copied into the bridge image at build time | So player events join with simulated ones. The bridge reads it; nothing reads the bridge. |
| A running Kafka and TimescaleDB | the base Compose stack | The bridge publishes to the same topics. |

To confirm the rule still holds, this should print nothing:

```bash
grep -rIl "game-bridge\|game/bridge\|game/client" --exclude-dir=game --exclude-dir=node_modules --exclude-dir=.git . \
  | grep -v "CODEBASE-GUIDE.md\|restaurant-platform-implementation-status.md\|restaurant-platform-project-notes.md"
```

(The excluded files are documentation that describes the game; they do not run it.)

## Run it

Start the platform with the game bridge added, from the `restaurant-platform` directory:

```bash
docker compose -f docker-compose.yml -f game/docker-compose.game.yml up -d --build
```

The base stack on its own (`docker compose up --build`) does not start the bridge. The bridge listens on `localhost:8001`, and the dashboard on `localhost:8080` (the game's findings panel reads from it).

Then open `game/client` in Godot 4.5 and press Play, or from a terminal:

```bash
godot4 --path game/client
```

Override the endpoints with `BRIDGE_URL` (default `http://127.0.0.1:8001`) and `DASHBOARD_URL` (default `http://127.0.0.1:8080`). If the bridge isn't up, the game says so and retries every few seconds.

If you are reusing a database volume that predates the game, apply migration 004 once (fresh volumes get it automatically):

```bash
docker compose exec -T timescaledb psql -U restaurant_app -d restaurant_platform \
  < storage/schema/004_player_source_kind.sql
```

## The bridge API

`game/bridge/main.py` accepts a player's actions and publishes them as schema-validated events tagged `source_kind: "player"`. It has no authentication, so it is bound to localhost only.

```bash
curl -s localhost:8001/api/world     # valid stations, tables, stages, roles, actions

curl -s -H 'content-type: application/json' localhost:8001/api/staff-shift \
  -d '{"player_id":"ana","role":"line_cook","shift_action":"clock_in"}'

# order_fired mints a ticket_id; pass it on each later stage, in order
curl -s -H 'content-type: application/json' localhost:8001/api/service-timing \
  -d '{"player_id":"ana","stage":"order_fired","table_id":"table-07","station_id":"station-grill"}'
```

Out-of-order stages, unknown tables or stations, and bad clock-in sequences are rejected with 409, 422 or 404. The player then appears in the dashboard's staff panel and in `twin_staff_state`.

## How the game maps onto the platform

| In the game | Event | Notes |
|---|---|---|
| Clock in, take a station, clock out | `StaffShiftEvent` | `staff_id` is `player-<name>`, so players never collide with the simulators' roster |
| A ticket arrives | `ServiceTimingEvent` `order_fired` | table is random, station is yours |
| Each button press | `ServiceTimingEvent` for the next stage | the bridge computes `elapsed_since_previous_stage_ms` |

The bridge owns the rules (stage order, clock-in sequencing) and rejects anything invalid; the game just reports what it said.

## Tests

```bash
# Bridge state machines, no Kafka needed
cd game/bridge && pip install -r requirements.txt pytest httpx && python -m pytest test_bridge.py

# Godot client and main scene, headless, against a live bridge
godot4 --headless --path game/client -s res://tests/smoke_test.gd
```

The smoke test uses the fixed player id `smoke-test`, so repeated runs reuse one staff row in the digital twin.

## Known limits

- Placeholder UI on Godot's default theme; no art, audio, or export presets yet.
- One player does every stage, including the server's pickup and delivery. Splitting those into separate roles is a design decision for later.
- A human clearing a ticket in a few seconds will look like a fast outlier next to the simulators' ~30 s baselines. Whether player tickets share the anomaly baseline or get their own is undecided.
- Bridge state (open tickets, clocked-in players) is in memory. If the bridge restarts mid-shift the game drops the tickets it lost and you need to clock in again.
- The bridge is not deployed on Kubernetes.
