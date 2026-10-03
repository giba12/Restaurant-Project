# Version 2: the game

A human-playable front end for the platform. Work a shift as a **line cook**, **expo** or **server**, with a crew covering every stage nobody is playing so your speed moves the whole chain -- or sit down as a **guest**, order from the menu, and pay once your food arrives. Every action becomes a real event on the same Kafka topics the simulators use, so storage, anomaly detection, causal inference, the digital twin and the narrator all run on your play without being changed.

Everything for version 2 lives in this directory. Version 1 (the simulated platform, everything outside `game/`) runs completely without it.

```
game/
  bridge/                  HTTP-to-Kafka service (FastAPI): validates a player's actions and publishes events
  client/                  the Godot 4.5 project (the game itself)
  k8s/bridge/              Helm chart that deploys the bridge to k3s (see "Deploying to Kubernetes")
  docker-compose.game.yml  Compose overlay that adds the bridge to the base stack
  README.md                this file
```

## The boundary between version 1 and version 2

**Rule: version 1 never references version 2.** Nothing outside `game/` imports, builds, starts or names anything inside it. The dependency runs one way only, from the game onto the platform.

The game depends on the platform in exactly these places, and nowhere else:

| Dependency | Where it lives | Why it is in version 1's tree |
|---|---|---|
| `source_kind: "player"` on the four event schemas, and `"crew"` on `ServiceTimingEvent` | `schemas/*.schema.json` | It is a contract change, and the contract must be one shared definition. Simulators and vendor integrations ignore it. |
| Migrations `004` and `005`, which let the database accept `player` and `crew`, and add the ticket `origin` column | `storage/schema/004_player_source_kind.sql`, `005_ticket_origin.sql` (and their copies under `k8s/timescaledb/files/`) | It has to reach every database, including ones created before the game existed, or player events would fail a `CHECK`. It is idempotent and harmless without the game. |
| The simulated world's ids (stations, tables, `RESTAURANT_ID`) | `edge-simulators/common/world.py`, copied into the bridge image at build time | So player events join with simulated ones. The bridge reads it; nothing reads the bridge. |
| A running Kafka and TimescaleDB | the base Compose stack | The bridge publishes to the same topics. |

To confirm the rule still holds, this should print nothing:

```bash
grep -rIl "game-bridge\|game/bridge\|game/client\|game/k8s" --exclude-dir=game --exclude-dir=node_modules --exclude-dir=.git . \
  | grep -v "CODEBASE-GUIDE.md\|restaurant-platform-implementation-status.md\|restaurant-platform-project-notes.md\|TESTING.md\|k8s/harden/harden-live-cluster.sh\|k8s/timescaledb-backup/README.md\|k8s/kafka-tls/\|k8s/web-tls/\|k8s/audit/\|\.env\.example"
```

(The excluded files are documentation that describes the game, or operations tooling that names `game-bridge-credentials`/`BRIDGE_API_KEY`/`game-bridge-tls` while rotating secrets, cutting TLS over, or auditing both versions' charts -- neither is version 1's own runtime code depending on version 2. `k8s/kafka-tls/` and `k8s/web-tls/` were added to this list 2026-09-28, correcting a gap from when those scripts were first written: they already named `game-bridge` as one of the services they migrate, and were never added here. `k8s/audit/` added 2026-10-01 for the same reason, found doing this exact cleanup pass: `audit-live-cluster.sh` checks `game-bridge`'s chart alongside every other one, and was never added when it was written days after the exclusion list's last update. `TESTING.md` added 2026-10-02: it documents the whole test regime, including the version-boundary test that runs this very check. That test, `tests/static/test_repo_hygiene.py`, reads this exclusion list straight out of this file, so the two cannot drift apart.)

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

`game/bridge/main.py` accepts a player's or a guest's actions and publishes them as schema-validated events tagged `source_kind: "player"`. Every route except `/api/health` requires an `X-API-Key` header (added 2026-09-23); on Compose it defaults to `changeme-local-dev-only` like the platform's other dev credentials (`export BRIDGE_API_KEY=changeme-local-dev-only` before running the client), and on k3s a real one is generated by `k8s/harden/harden-live-cluster.sh`. It is bound to localhost only on Compose (or reached by port-forward on k3s; see "Deploying to Kubernetes").

```bash
export KEY=changeme-local-dev-only   # or your real BRIDGE_API_KEY on k3s

curl -s -H "X-API-Key: $KEY" localhost:8001/api/world     # stations, tables, stages, roles, and which stages each playable role performs

curl -s -H "X-API-Key: $KEY" -H 'content-type: application/json' localhost:8001/api/staff-shift \
  -d '{"player_id":"ana","role":"line_cook","shift_action":"clock_in"}'
curl -s -H "X-API-Key: $KEY" -H 'content-type: application/json' localhost:8001/api/staff-shift \
  -d '{"player_id":"ana","role":"line_cook","shift_action":"station_reassign","station_id":"station-grill"}'

curl -s -H "X-API-Key: $KEY" 'localhost:8001/api/tickets?player_id=ana'   # the board: who each open ticket is waiting on

# Tickets normally arrive by themselves while you are on shift. You can also fire one by hand;
# order_fired mints a ticket_id, which you pass on each later stage, in order:
curl -s -H "X-API-Key: $KEY" -H 'content-type: application/json' localhost:8001/api/service-timing \
  -d '{"player_id":"ana","stage":"order_fired","table_id":"table-07","station_id":"station-grill"}'
```

Out-of-order stages, unknown tables or stations, and bad clock-in sequences are rejected with 409, 422 or 404. A stage outside your role, or at another cook's station, is 403. The player then appears in the dashboard's staff panel and in `twin_staff_state`.

## Roles and the crew

| Role | Performs | Works |
|---|---|---|
| `line_cook` | `cook_started` | tickets at the one station they took |
| `expo` | `plated` | the whole floor -- expo works the pass for every station, not one |
| `server` | `picked_up_by_server`, `delivered` | the whole floor |

Only `line_cook` is scoped to a station; a ticket is only ever fired at a real cooking station (`station-grill`, `station-saute`, `station-salad`). `station-expo` is a real station (staff can clock in there) but not a cooking one, so no ticket is fired at it and no line cook can pick it as their station.

Every stage a player can't do is done by the **crew**, a background "director" thread in the bridge. A stage is crew-owned whenever no clocked-in player *who is not on a break* can do it (for a cook: at that ticket's station). The crew acts after a random delay (6 to 14 s by default), so if you cook slowly the server waits, and if you serve slowly the guest waits. Clock out or take a break and the crew covers your stages. The director also fires new tickets (the dining room) every few seconds while someone is on shift: at the cooks' stations, or anywhere in the kitchen if only servers are working.

Pacing is set by environment variables on the bridge (defaults in brackets): `SPAWN_SECONDS` [8], `MAX_OPEN_TICKETS` [5, per staffed station], `CREW_MIN_SECONDS` [6], `CREW_MAX_SECONDS` [14], `DIRECTOR_TICK_SECONDS` [0.5], `GAME_DIRECTOR=0` to turn the director off. The Compose overlay passes the first four through from your shell, so `SPAWN_SECONDS=1 CREW_MIN_SECONDS=1 CREW_MAX_SECONDS=2 docker compose -f docker-compose.yml -f game/docker-compose.game.yml up -d game-bridge` gives a fast test run.

## Guests

A guest is not staff: no clock-in, no role, none of the rules above apply. Sit at a table, order from the menu (the same `world.MENU` the POS and plate-waste simulators use), and the order fires a kitchen ticket exactly as `order_fired` does -- the kitchen decides which cooking station, same as any other ticket. Pay once it is delivered, and a real `POSTransactionEvent` is published for the items ordered, tagged `player` (already an allowed value on that schema).

```bash
curl -s -H "X-API-Key: $KEY" -H 'content-type: application/json' localhost:8001/api/guest/order \
  -d '{"player_id":"gwen","table_id":"table-05","items":[{"menu_item_id":"menu-burger-classic","quantity":2}]}'

curl -s -H "X-API-Key: $KEY" 'localhost:8001/api/guest/status?player_id=gwen'   # progress and running total

curl -s -H "X-API-Key: $KEY" -H 'content-type: application/json' localhost:8001/api/guest/pay \
  -d '{"player_id":"gwen","payment_method":"cash"}'   # 409 until delivered
```

A guest can only have one open order at a time (409 to order again before paying), and the kitchen can say no if every cooking station is at its ticket cap (503). In the client, "guest" is a fourth option in the role picker; picking it swaps the ticket board for a menu and a Place order button, and skips the staff clock-in entirely.

## Quarantine and comparison

Your play must not change what the platform treats as normal for everyone else. Measured on real data, a station worked at game pace fills the detector's 200-ticket rolling window in about half an hour (the simulators complete around 5 tickets an hour per station; a played station up to about 450), and with a window that game-dominated, 82% of ordinary simulated tickets would be flagged. So:

- **Flag.** Every ticket in an interactive session gets `origin = interactive` in `ticket_timing_summaries`, from its first event (the crew fires the ticket, so the crew's `crew` source kind is what marks it, before any player acts). Simulated tickets stay `simulated`.
- **Quarantine.** The anomaly detector skips interactive tickets: they are never evaluated and never enter a baseline window. The causal engine's staffing-level query leaves out interactive tickets and staff events from human-driven sources. The tickets are still stored, so nothing is lost.
- **Compare.** `GET /api/comparison` on the dashboard API (the game and the dashboard both use it) reports two things: **you against the crew** over the same window and clock, and **game tickets against the simulated restaurant's**, per stage, as medians and 90th percentiles. Clocking out shows this as a shift report in the client; the dashboard has a matching panel.

Read the comparison as pace, not a score: player and crew usually do different stages, the crew's times are largely set by `CREW_MIN_SECONDS`/`CREW_MAX_SECONDS`, and the game and the simulators run on different clocks.

If you are reusing a database that recorded game tickets before this change, apply migration `005` and then the one-off backfill (it re-tags the earlier crew events and marks their tickets interactive; idempotent):

```bash
docker compose exec -T timescaledb psql -U restaurant_app -d restaurant_platform -v ON_ERROR_STOP=1 \
  < storage/schema/005_ticket_origin.sql
docker compose exec -T timescaledb psql -U restaurant_app -d restaurant_platform \
  < game/bridge/backfill-crew-origin.sql
```

Migration `005` changes tables the pipeline services are reading. If it appears to hang, a session is holding a lock (see the codebase guide, section 11); stop the services that read the database, or terminate the idle session it names, then re-run it. A fresh volume applies it automatically.

## How the game maps onto the platform

| In the game | Event | Tagged |
|---|---|---|
| Clock in, take a station, break, clock out | `StaffShiftEvent` | `player`, `source_id = game-<name>`; `staff_id` is `player-<name>` so players never collide with the simulators' roster |
| A ticket arrives | `ServiceTimingEvent` `order_fired` | `crew`, `game-crew` (the dining room); table is random, station is a cook's |
| Your button press | `ServiceTimingEvent` for the next stage | `player`, `game-<name>`; the bridge computes `elapsed_since_previous_stage_ms` |
| The crew's stages | `ServiceTimingEvent` | `crew`, `game-crew` |
| A guest orders | `ServiceTimingEvent` `order_fired` | `player`, `game-<guest>`; same ticket lifecycle as a staff-fired one |
| A guest pays | `POSTransactionEvent` | `player`, `game-<guest>`; `line_items` are exactly what they ordered |

Crew events are `crew`, not `player` or `simulated`, so anything downstream can tell a human's actions, the bridge's automated staff and the simulators apart. The bridge owns the rules (stage order, roles, stations, clock-in sequencing) and the timing of everything the crew does; the game only polls `GET /api/tickets`, presents it, and reports what the bridge rejected.

## Tests

```bash
# Bridge rules, roles and the crew: no Kafka and no waiting (a fake clock drives the director)
cd game/bridge && pip install -r requirements.txt pytest httpx && python -m pytest test_bridge.py

# Godot client and main scene, headless, against a live bridge (a couple of minutes: it waits
# for the crew, and for a guest's own order to be cooked with nobody staffed)
godot4 --headless --path game/client -s res://tests/smoke_test.gd
```

The smoke test uses the fixed player id `smoke-test`, so repeated runs reuse one staff row in the digital twin.

## Known limits

- Placeholder UI on Godot's default theme; no art, audio, or export presets yet.
- Godot prints a few harmless lines at start-up under WSL2/WSLg: `xkbcommon ... unrecognized keysym "dead_hamza"` (the Godot snap's bundled keyboard table has a symbol its own library does not know; it only affects Arabic compose-key sequences) and `Could not set V-Sync mode` (the software OpenGL driver, Mesa llvmpipe, does not support it). Neither can be fixed from the project, and neither affects the game.
- Only three staff roles are playable (line cook, expo, server). Host, bartender and dishwasher exist in the schema but have no game rules yet.
- One open ticket per table: ordering, a staff `order_fired` and the dining room all check the same thing before seating one, so a guest, the crew and a staff order can never land on the same table together. `POST /api/guest/leave` cancels an order before paying (the table it used stays occupied until the ticket the kitchen already started actually closes -- leaving frees the guest's name and claim on it, not the food itself); a guest who never leaves or pays is freed automatically `MAX_GUEST_AGE_SECONDS` after delivery, or immediately if their ticket was itself evicted first.
- Game timings are on a shorter clock (seconds per stage) than the simulators' (about 20 s by median), so they are quarantined from the anomaly baseline and compared instead (next section). Interactive tickets therefore produce no anomalies or causal findings of their own; the shift report is the player's feedback.
- Two players in the same role share the tickets first come, first served; there is no queue or seating.
- Bridge state (open tickets, clocked-in players) is in memory. If the bridge restarts mid-shift the board empties and you need to clock in again.
- The director holds the bridge's state lock while it publishes, so a slow Kafka delays both the crew and the API.

## Deploying to Kubernetes

The bridge has its own chart, `game/k8s/bridge/`, kept out of the shared `k8s/` directory on purpose: that directory is version 1, and this chart exists only for the game. `game/k8s/deploy-bridge.sh` builds the image, imports it into k3s and runs `helm upgrade --install` in one step (`--dry-run` to see the steps first); it needs `sudo` (importing into containerd is root-only).

```bash
bash game/k8s/deploy-bridge.sh --dry-run
bash game/k8s/deploy-bridge.sh
```

It deploys as a `ClusterIP` Service, never exposed outside the cluster, with the same defaults as the Compose overlay, overridable in `game/k8s/bridge/values.yaml`. It also needs the `game-bridge-credentials` Secret (`k8s/harden/harden-live-cluster.sh` creates it) before it will accept any request. Reach it the same way the dashboard and Grafana are reached on k3s:

```bash
kubectl port-forward svc/game-bridge -n kafka 8001:8001
BRIDGE_URL=http://127.0.0.1:8001 godot4 --path game/client
```

**TLS (optional, added 2026-09-28):** `bash k8s/web-tls/cutover-web-tls.sh game-bridge` switches the bridge to HTTPS -- a single uvicorn process serves either plain HTTP or TLS, not both, so this is a straight cutover, not an additive listener; `... rollback game-bridge` reverts it. Once switched, fetch its self-signed cert once and point the client at both the new scheme and the cert:

```bash
kubectl get secret game-bridge-tls -n kafka -o jsonpath='{.data.tls\.crt}' | base64 -d > /tmp/game-bridge.crt
kubectl port-forward svc/game-bridge -n kafka 8001:8001
BRIDGE_URL=https://127.0.0.1:8001 BRIDGE_TLS_CERT_PATH=/tmp/game-bridge.crt godot4 --path game/client
```

The client trusts this cert specifically (`TLSOptions.client(cert)` in `bridge_client.gd`), the same real cert-pinning every other TLS piece in this project uses, not a blanket bypass -- confirmed live that Godot 4.5's `HTTPRequest` rejects a self-signed cert by default, same as a browser, and accepts it once pinned this way. Without `BRIDGE_TLS_CERT_PATH` set, an `https://` `BRIDGE_URL` fails the same way. The same mechanism and `DASHBOARD_TLS_CERT_PATH` cover the dashboard's own read endpoints this client also calls (`DASHBOARD_URL`); see `k8s/web-tls/cutover-web-tls.sh dashboard`.

The bridge bakes its schemas into the image at build time, so unlike the Phase 5-7 services it needs no ConfigMap. It is unaffected by `k8s/realign/realign-live-cluster.sh`, which only touches the shared platform; a schema or migration change that affects the bridge (as `005` did) still needs that script run first.
