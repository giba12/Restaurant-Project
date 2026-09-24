#!/usr/bin/env bash
# Proves the backup is actually restorable, not just present. Applies
# templates/restore-drill-job.yaml (rendered via `helm template`, since this
# Job is deliberately NOT part of the chart's own install/upgrade -- an
# on-demand drill, not something that runs on every deploy), waits for it,
# prints its output, then deletes it. Run from the repo root:
#
#   bash k8s/timescaledb-backup/restore-drill.sh
#
# What it does NOT prove: that the restored data matches production
# byte-for-byte, or that a restore under real time pressure during an actual
# incident goes this smoothly. It proves the backup repo is reachable, the
# credentials work, and pgBackRest can turn what's in it back into a
# database with real rows and a real max(timestamp) in each table -- the
# specific things a backup that has never been test-restored cannot promise.
#
# Needs: sudo is NOT required (no image import; the restore uses the same
# timescaledb-ha image already in k3s), but this DOES read from the live
# cluster and run a Job. Run bootstrap.sh first -- this expects a stanza and
# at least one completed backup to already exist.
set -euo pipefail

NS=kafka
JOB=timescaledb-restore-drill

[ -f k8s/timescaledb-backup/Chart.yaml ] || { echo "run this from the repo root" >&2; exit 1; }

echo "== applying the restore-drill Job =="
# A Job's pod template is immutable -- if a previous drill run is still
# sitting there (failed or otherwise), `kubectl apply` on a changed spec
# would error instead of updating it. Deleting first makes every run of
# this script safe to retry.
kubectl delete job "$JOB" -n "$NS" --ignore-not-found
helm template restore-drill k8s/timescaledb-backup --show-only templates/restore-drill-job.yaml \
  --set restoreDrill.enabled=true \
  | kubectl apply -n "$NS" -f -

echo "== waiting for it to finish (up to 5 minutes) =="
kubectl wait --for=condition=complete "job/$JOB" -n "$NS" --timeout=5m 2>&1 || {
  echo "did not complete in time -- printing whatever logs exist, then leaving the Job for you to inspect:" >&2
  kubectl logs -n "$NS" "job/$JOB" 2>&1 || true
  exit 1
}

echo "== output =="
kubectl logs -n "$NS" "job/$JOB"

echo "== cleaning up =="
kubectl delete job "$JOB" -n "$NS"
echo "done"
