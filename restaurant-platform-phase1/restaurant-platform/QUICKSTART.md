# Quickstart (portable path)

This runs the whole pipeline — simulated restaurant sensors, Kafka, storage, anomaly detection, causal inference, and an LLM-narrated findings feed — with a single command, on any machine with Docker installed. No Kubernetes, no GPU required.

This is a second, deliberately simpler deployment path alongside the project's primary k3s/Helm setup (see `restaurant-platform-implementation-status.md`), which demonstrates real Kubernetes depth but assumes a dev-configured cluster. This path trades some of that depth for "clone and run."

## Requirements

- Docker (or Podman with the `docker` compatibility shim) and Docker Compose v2.
- ~4GB of free RAM, a few GB of disk for images and the LLM model.
- No GPU needed — the portable path uses a small CPU-only model (`qwen2.5:0.5b-instruct`) instead of the k3s path's larger GPU-accelerated one.

## Run it

```bash
cd restaurant-platform-phase1/restaurant-platform
docker compose up --build
```

First run builds ~13 images and pulls the LLM model — expect several minutes. Subsequent runs are fast (everything's cached).

## What to look at while it's running

Open **http://localhost:8080** for the live dashboard — station/staff state, anomaly counts, and the narrated-findings feed, all pulled from the running pipeline.

```bash
# Real ticket flow accumulating
docker compose exec timescaledb psql -U restaurant_app -d restaurant_platform \
  -c "SELECT count(*), count(*) FILTER (WHERE is_complete) FROM ticket_timing_summaries;"

# Anomalies detected organically
docker compose exec timescaledb psql -U restaurant_app -d restaurant_platform \
  -c "SELECT count(*), detection_method FROM anomaly_events GROUP BY detection_method;"

# Narrated findings -- the actual point of the whole pipeline
docker compose exec timescaledb psql -U restaurant_app -d restaurant_platform \
  -c "SELECT narrative_text, narrated_at FROM narrated_findings ORDER BY narrated_at DESC LIMIT 5;"

# Live restaurant state (digital twin)
docker compose exec timescaledb psql -U restaurant_app -d restaurant_platform \
  -c "SELECT * FROM twin_station_state;"
```

Findings take a little while to appear organically (the anomaly detector needs ~30 completed tickets per station before it has a baseline). To see one immediately instead of waiting:

```bash
docker compose exec causal-engine python causal_engine.py \
  --scenario-injection-id manual-test \
  --metric-name estimated_waste_grams \
  --window-start 2020-01-01T00:00:00Z --window-end 2030-01-01T00:00:00Z \
  --restaurant-id restaurant-01
```

Then check `narrated_findings` again after a few seconds.

## Game bridge (version 2 groundwork)

`game-bridge` accepts a human player's actions over HTTP and publishes them as the same events the simulators emit, tagged `source_kind: "player"`. It listens on `localhost:8001` only (no authentication).

```bash
curl -s localhost:8001/api/world     # valid stations, tables, stages, roles

curl -s -H 'content-type: application/json' localhost:8001/api/staff-shift \
  -d '{"player_id":"ana","role":"line_cook","shift_action":"clock_in"}'

# order_fired mints a ticket_id; pass it on each later stage, in order
curl -s -H 'content-type: application/json' localhost:8001/api/service-timing \
  -d '{"player_id":"ana","stage":"order_fired","table_id":"table-07","station_id":"station-grill"}'
```

Out-of-order stages, unknown tables/stations and bad clock-in sequences are rejected with 409/422. The player then shows up in the dashboard's staff panel and in `twin_staff_state`.

If you're reusing a database volume from before this feature existed, apply the migration once (fresh volumes get it automatically):

```bash
docker compose exec -T timescaledb psql -U restaurant_app -d restaurant_platform \
  < storage/schema/004_player_source_kind.sql
```

## Stopping / resetting

```bash
docker compose down          # stop, keep data (Kafka topics, DB, pulled model)
docker compose down -v       # stop and wipe all data -- start completely fresh next time
```

## How this differs from the k3s deployment

| | k3s path (`k8s/`) | Portable path (this file) |
|---|---|---|
| Orchestration | Strimzi-managed Kafka, Helm charts | Plain Kafka (KRaft), Docker Compose |
| LLM narrator | `qwen2.5:3b-instruct`, Ollama native on host, GPU-accelerated | `qwen2.5:0.5b-instruct`, Ollama containerized, CPU-only |
| MinIO | Deployed (unused placeholder for a future stretch goal) | Not included — no reason to add startup weight for zero current use |
| Purpose | Demonstrates real Kubernetes/Strimzi operational depth | Demonstrates the same application/data pipeline, runnable anywhere |
