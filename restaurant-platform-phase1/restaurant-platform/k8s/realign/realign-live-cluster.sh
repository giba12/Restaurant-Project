#!/usr/bin/env bash
# One-time: bring a k3s deployment that was built before the raw-table
# corrections back in line with the repo. Run from the repo root:
#
#   bash k8s/realign/realign-live-cluster.sh           # do it
#   bash k8s/realign/realign-live-cluster.sh --dry-run # print the steps only
#
# Why this exists and why it must be one step: the live database still has
# the original layout of the raw event tables, and the storage-consumer /
# narrator / dashboard images in k3s are older builds. Importing a new
# storage-consumer alone would crash on inserts (it writes the corrected
# columns); running `helm upgrade` on timescaledb alone would fail (the
# current 001 indexes a column the old table lacks). So, in order:
#
#   1. build the three changed images
#   2. stop every workload that talks to the database (a DROP TABLE cannot
#      get its lock while any of them holds a transaction open)
#   3. drop the raw tables if they are in the legacy layout
#   4. helm upgrade timescaledb  -> its schema-init Job recreates them from 001..004
#   5. import the images into k3s (needs sudo: containerd is root-only)
#   6. start the workloads again on the new images; check line items are landing
#
# The dropped tables hold simulated data. Kafka offsets are already
# committed, so the dropped rows are not replayed; history starts fresh.
# Safe to re-run: step 3 does nothing once the layout is current.
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

echo "== 4. helm upgrade timescaledb (schema-init recreates the tables)"
run helm upgrade --install timescaledb k8s/timescaledb -n "$NS" --wait --timeout 10m

echo "== 5. import images into k3s"
for img in storage-consumer finding-narrator dashboard-web; do
  echo "+ docker save local/$img:1.0 | sudo k3s ctr images import -"
  [ "$DRY" = 1 ] || docker save "local/$img:1.0" | sudo k3s ctr images import -
done

echo "== 6. restart workloads on the new images"
for d in $DB_CLIENTS; do run kubectl scale "deploy/$d" -n "$NS" --replicas=1; done
run kubectl rollout restart deploy/dashboard-web -n "$NS"
for d in $DB_CLIENTS dashboard-web; do run kubectl rollout status "deploy/$d" -n "$NS" --timeout=180s; done

echo "== 7. check"
if [ "$DRY" = 0 ]; then
  echo "waiting 60s for events to flow..."
  sleep 60
  kubectl exec -i -n "$NS" timescaledb-0 -- \
    sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"' <<'SQL'
SELECT (SELECT count(*) FROM pos_transaction_events)     AS pos_parents,
       (SELECT count(*) FROM pos_transaction_line_items) AS pos_items,
       (SELECT count(*) FROM plate_waste_events)         AS plate_waste,
       (SELECT count(*) FROM staff_shift_events)         AS staff_shift,
       (SELECT count(*) FROM service_timing_events)      AS service_timing;
SQL
  echo "expect every count above to be > 0 and growing. Then: kubectl logs deploy/storage-consumer -n $NS --tail=20"
fi
echo "done"
