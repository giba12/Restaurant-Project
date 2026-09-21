# Version 2: the game

A human-playable front end for the platform. You clock in as a **line cook** (cook and plate the tickets at your station) or a **server** (pick up and deliver), and a crew inside the bridge does every stage you don't own, so your speed moves the whole chain. Every action becomes a real event on the same Kafka topics the simulators use, so storage, anomaly detection, causal inference, the digital twin and the narrator all run on your play without being changed.

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

**Rootless Podman on WSL2** (the setup this project was built on): after every WSL restart the Podman socket is off, and `docker compose` fails with `FileNotFoundError ... No such file or directory` and a wall of Python traceback. Start it and point Compose at it, in the same terminal, before the command above:

```bash
systemctl --user start podman.socket
export DOCKER_HOST=unix:///run/user/$(id -u)/podman/podman.sock
```

Containers do not survive a WSL restart either, so the `up -d` above is needed again each time. Named volumes (your data) do survive. Docker Desktop and native Docker need none of this.

Override the endpoints with `BRIDGE_URL` (default `http://127.0.0.1:8001`) and `DASHBOARD_URL` (default `http://127.0.0.1:8080`). If the bridge isn't up, the game says so and retries every few seconds.

If you are reusing a database volume that predates the game, apply migration 004 once (fresh volumes get it automatically):

```bash
docker compose exec -T timescaledb psql -U restaurant_app -d restaurant_platform \
  < storage/schema/004_player_source_kind.sql
```

## The bridge API

`game/bridge/main.py` accepts a player's actions and publishes them as schema-validated events tagged `source_kind: "player"`. It has no authentication, so it is bound to localhost only.

```bash
curl -s localhost:8001/api/world     # stations, tables, stages, roles, and which stages each playable role performs

curl -s -H 'content-type: application/json' localhost:8001/api/staff-shift \
  -d '{"player_id":"ana","role":"line_cook","shift_action":"clock_in"}'
curl -s -H 'content-type: application/json' localhost:8001/api/staff-shift \
  -d '{"player_id":"ana","role":"line_cook","shift_action":"station_reassign","station_id":"station-grill"}'

curl -s 'localhost:8001/api/tickets?player_id=ana'   # the board: who each open ticket is waiting on

# Tickets normally arrive by themselves while you are on shift. You can also fire one by hand;
# order_fired mints a ticket_id, which you pass on each later stage, in order:
curl -s -H 'content-type: application/json' localhost:8001/api/service-timing \
  -d '{"player_id":"ana","stage":"order_fired","table_id":"table-07","station_id":"station-grill"}'
```

Out-of-order stages, unknown tables or stations, and bad clock-in sequences are rejected with 409, 422 or 404. A stage outside your role, or at another cook's station, is 403. The player then appears in the dashboard's staff panel and in `twin_staff_state`.

## Roles and the crew

| Role | Performs | Works |
|---|---|---|
| `line_cook` | `cook_started`, `plated` | tickets at the one station they took |
| `server` | `picked_up_by_server`, `delivered` | the whole floor |

Every stage a player can't do is done by the **crew**, a background "director" thread in the bridge. A stage is crew-owned whenever no clocked-in player *who is not on a break* can do it (for a cook: at that ticket's station). The crew acts after a random delay (6 to 14 s by default), so if you cook slowly the server waits, and if you serve slowly the guest waits. Clock out or take a break and the crew covers your stages. The director also fires new tickets (the dining room) every few seconds while someone is on shift: at the cooks' stations, or anywhere in the kitchen if only servers are working.

Pacing is set by environment variables on the bridge (defaults in brackets): `SPAWN_SECONDS` [8], `MAX_OPEN_TICKETS` [5, per staffed station], `CREW_MIN_SECONDS` [6], `CREW_MAX_SECONDS` [14], `DIRECTOR_TICK_SECONDS` [0.5], `GAME_DIRECTOR=0` to turn the director off.

## How the game maps onto the platform

| In the game | Event | Tagged |
|---|---|---|
| Clock in, take a station, break, clock out | `StaffShiftEvent` | `player`, `source_id = game-<name>`; `staff_id` is `player-<name>` so players never collide with the simulators' roster |
| A ticket arrives | `ServiceTimingEvent` `order_fired` | `simulated`, `game-crew` (the dining room); table is random, station is a cook's |
| Your button press | `ServiceTimingEvent` for the next stage | `player`, `game-<name>`; the bridge computes `elapsed_since_previous_stage_ms` |
| The crew's stages | `ServiceTimingEvent` | `simulated`, `game-crew` |

Crew events are `simulated`, not `player`, so anything downstream can tell a human's actions from the bridge's. The bridge owns the rules (stage order, roles, stations, clock-in sequencing) and the timing of everything the crew does; the game only polls `GET /api/tickets`, presents it, and reports what the bridge rejected.

## Tests

```bash
# Bridge rules, roles and the crew: no Kafka and no waiting (a fake clock drives the director)
cd game/bridge && pip install -r requirements.txt pytest httpx && python -m pytest test_bridge.py

# Godot client and main scene, headless, against a live bridge (about a minute: it waits for the crew)
godot4 --headless --path game/client -s res://tests/smoke_test.gd
```

The smoke test uses the fixed player id `smoke-test`, so repeated runs reuse one staff row in the digital twin.

## Known limits

- Placeholder UI on Godot's default theme; no art, audio, or export presets yet.
- Godot prints a few harmless lines at start-up under WSL2/WSLg: `xkbcommon ... unrecognized keysym "dead_hamza"` (the Godot snap's bundled keyboard table has a symbol its own library does not know; it only affects Arabic compose-key sequences) and `Could not set V-Sync mode` (the software OpenGL driver, Mesa llvmpipe, does not support it). Neither can be fixed from the project, and neither affects the game.
- Only two roles are playable (line cook, server). Expo, host, bartender and dishwasher exist in the schema but have no game rules yet, and the guest side (ordering, eating, paying) is not built.
- Crew and player timings are game-paced (seconds), against the simulators' ~30 s baselines, so game tickets can look like fast outliers to the anomaly detector. Whether they share the baseline or get their own is undecided.
- Two players in the same role share the tickets first come, first served; there is no queue or seating.
- Bridge state (open tickets, clocked-in players) is in memory. If the bridge restarts mid-shift the board empties and you need to clock in again.
- The director holds the bridge's state lock while it publishes, so a slow Kafka delays both the crew and the API.
- The bridge is not deployed on Kubernetes.
