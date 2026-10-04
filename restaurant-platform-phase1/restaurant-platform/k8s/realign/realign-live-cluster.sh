#!/usr/bin/env bash
# Bring the k3s deployment back in line with the repo: database schema
# (migrations 001 to 006), the phase5-schemas ConfigMap, the Kafka Connect
# worker setting, and every locally built image that has changed. Safe to re-run whenever the repo has moved on.
# Run from the repo root:
#
#   bash k8s/realign/realign-live-cluster.sh           # do it
#   bash k8s/realign/realign-live-cluster.sh --dry-run # print the steps only
#
# 2026-10-04 update. It now also carries: migration 006 (the digital twin's
# open-ticket set), the digital-twin, edge-simulator and scenario-controller
# images (the edge-AI plate-waste node, staffing as a driver of the simulated
# kitchen), and the Kafka Connect `scheduled.rebalance.max.delay.ms=0` setting
# (DEF-142) applied by upgrading the kafka-connect-mqtt release, which makes
# Strimzi roll the Connect pod.
#
# History. It first repaired a database built before the raw-table
# corrections; since then it also carries later changes (ticket origin, the
# interactive-session quarantine). Why it must be one step: the live database still has
# the original layout of the raw event tables, and the storage-consumer /
# narrator / dashboard images in k3s are older builds. Importing a new
# storage-consumer alone would crash on inserts (it writes the corrected
# columns); running `helm upgrade` on timescaledb alone would fail (the
# current 001 indexes a column the old table lacks). So, in order:
#
#   1. build the changed images
#   2. stop every workload that talks to the database (a DROP TABLE cannot
#      get its lock while any of them holds a transaction open)
#   3. drop the raw tables if they are in the legacy layout
#   4. helm upgrade timescaledb  -> its schema-init Job applies 001..006 (it
#      recreates dropped tables; 005 adds ticket_timing_summaries.origin and the
#      'crew' source kind; 006 adds twin_open_tickets), phase5-schemas -> the
#      updated JSON Schemas, and kafka-connect-mqtt -> the Connect setting
#   5. import the images into k3s (needs sudo: containerd is root-only)
#   6. start the workloads again on the new images; check line items are landing
#
# The dropped tables hold simulated data. Kafka offsets are already
# committed, so the dropped rows are not replayed; history starts fresh.
# Safe to re-run: step 3 does nothing once the layout is current, and steps 1,
# 4 and 5 just repeat.
#
# Needs: sudo, kubectl, helm, and a container CLI. Under rootless Podman on
# WSL2:  export DOCKER_HOST=unix:///run/user/$(id -u)/podman/podman.sock
set -euo pipefail

NS=kafka
# Every deployment that connects to TimescaleDB. All run one replica.
DB_CLIENTS="storage-consumer ticket-timing-aggregator anomaly-detector causal-engine finding-reviewer digital-twin llm-narrator dashboard-api"
DRY=0
[ "${1:-}" = "--dry-run" ] && DRY=1

run() {
  echo "+ $*"
  [ "$DRY" = 1 ] || "$@"
}

restore_hint() {
  echo >&2
  echo "Stopped part-way. The database clients may still be scaled to 0. To bring them back:" >&2
  for d in $DB_CLIENTS; do echo "  kubectl scale deploy/$d -n $NS --replicas=1" >&2; done
  echo "The script is safe to re-run from the top." >&2
}
trap restore_hint ERR INT TERM

[ -f k8s/realign/drop-legacy-raw-tables.sql ] || { echo "run this from the repo root" >&2; exit 1; }

if [ "$DRY" = 0 ]; then
  sudo -v   # ask for the password once, up front, not halfway through
  kubectl get ns "$NS" >/dev/null
  docker version >/dev/null
fi

echo "== 1. build images"
run docker build --network=host -t local/storage-consumer:1.0 -f storage/consumer/Dockerfile .
run docker build --network=host -t local/finding-narrator:1.0 -f services/finding-narrator/Dockerfile services
run docker build --network=host -t local/dashboard-web:1.0 services/dashboard-web
# The four Phase 5 to 7 services below build from the services/ directory, like the narrator.
for svc in ticket-timing-aggregator anomaly-detector causal-engine dashboard-api digital-twin scenario-injection-controller; do
  run docker build --network=host -t "local/$svc:1.0" -f "services/$svc/Dockerfile" services
done
# The edge simulators build from the repo root (they bundle schemas/ and the edge model).
run docker build --network=host -t local/edge-simulator:1.0 -f edge-simulators/Dockerfile .

echo "== 2. stop every database client"
for d in $DB_CLIENTS; do run kubectl scale "deploy/$d" -n "$NS" --replicas=0; done
# Let the pods finish terminating; step 3 also disconnects any straggler.
[ "$DRY" = 1 ] || sleep 20

echo "== 3. drop raw tables if in the legacy layout"
if [ "$DRY" = 1 ]; then
  echo "+ kubectl exec -i -n $NS timescaledb-0 -- psql ... < k8s/realign/drop-legacy-raw-tables.sql"
else
  kubectl exec -i -n "$NS" timescaledb-0 -- \
    sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1' \
    < k8s/realign/drop-legacy-raw-tables.sql
fi

echo "== 4. helm upgrade timescaledb (schema-init applies 001..006), phase5-schemas, kafka-connect-mqtt"
run helm upgrade --install timescaledb k8s/timescaledb -n "$NS" --wait --timeout 10m
run helm upgrade --install phase5-schemas k8s/phase5-schemas -n "$NS" --wait
# --reset-then-reuse-values: the release's own values (TLS settings) are kept, and any
# chart default added since is picked up (a plain --reuse-values once left a committed fix
# undeployed for ten hours, DEF-090). Strimzi rolls the Connect pod for the new worker config.
run helm upgrade kafka-connect-mqtt k8s/kafka-connect-mqtt -n "$NS" --reset-then-reuse-values

echo "== 5. import images into k3s"
for img in storage-consumer finding-narrator dashboard-web ticket-timing-aggregator anomaly-detector causal-engine dashboard-api digital-twin scenario-injection-controller edge-simulator; do
  echo "+ docker save local/$img:1.0 | sudo k3s ctr images import -"
  [ "$DRY" = 1 ] || docker save "local/$img:1.0" | sudo k3s ctr images import -
done

echo "== 6. restart workloads on the new images"
for d in $DB_CLIENTS; do run kubectl scale "deploy/$d" -n "$NS" --replicas=1; done
# Not database clients, but they run the rebuilt images: the dashboard, the four edge
# simulators (the plate-waste node now runs a model; staff and kitchen are coupled) and
# the scenario controller.
EXTRAS="dashboard-web edge-sim-plate-waste edge-sim-pos-transaction edge-sim-service-timing edge-sim-staff-shift scenario-injection-controller"
for d in $EXTRAS; do run kubectl rollout restart "deploy/$d" -n "$NS"; done
for d in $DB_CLIENTS $EXTRAS; do run kubectl rollout status "deploy/$d" -n "$NS" --timeout=180s; done
run kubectl wait --for=condition=Ready pod/connect-cluster-connect-0 -n "$NS" --timeout=300s

echo "== 7. check"
if [ "$DRY" = 0 ]; then
  echo "waiting 60s for events to flow..."
  sleep 60
  kubectl exec -i -n "$NS" timescaledb-0 -- \
    sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"' <<'SQL'
SELECT (SELECT count(*) FROM twin_open_tickets)          AS twin_open_tickets,
       (SELECT count(*) FROM plate_waste_events WHERE raw_payload ? 'edge_inference' AND "timestamp" > now() - interval '5 minutes') AS edge_events_5m,
       (SELECT count(*) FROM pos_transaction_events)     AS pos_parents,
       (SELECT count(*) FROM pos_transaction_line_items) AS pos_items,
       (SELECT count(*) FROM plate_waste_events)         AS plate_waste,
       (SELECT count(*) FROM staff_shift_events)         AS staff_shift,
       (SELECT count(*) FROM service_timing_events)      AS service_timing;
SQL
  echo "expect every count above to be > 0 and growing (twin_open_tickets can be 0 for a moment). Then: kubectl logs deploy/storage-consumer -n $NS --tail=20"
  kubectl get kafkaconnector -n "$NS" 2>/dev/null || true
fi
echo "done"
