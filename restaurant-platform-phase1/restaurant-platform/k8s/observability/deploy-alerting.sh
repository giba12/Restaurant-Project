#!/usr/bin/env bash
# Rolls Alertmanager + alert-relay + Prometheus's new alerting rules into
# the already-installed `observability` release. Needs
# k8s/observability/build-alert-relay-image.sh run first (this script
# checks for the image and refuses to continue otherwise).
#
# Uses --reset-then-reuse-values, not --reuse-values: this chart's
# values.yaml just gained the alertmanager/alertRelay keys, and only
# --reset-then-reuse starts from the chart's current defaults for keys a
# past release never saw -- see the identical note in
# k8s/harden/harden-live-cluster.sh's minio upgrade.
#
# This deploys with the placeholder ntfy topic in values.yaml (functional,
# but guessable -- fine for confirming the pipeline works end-to-end).
# Run k8s/harden/harden-live-cluster.sh afterward to rotate in a real
# random topic before relying on this for anything real.
#
# Run from the repo root:
#   bash k8s/observability/deploy-alerting.sh --dry-run
#   bash k8s/observability/deploy-alerting.sh
set -euo pipefail

NS=kafka
DRY=0
[ "${1:-}" = "--dry-run" ] && DRY=1
run() { echo "+ $*"; [ "$DRY" = 1 ] || "$@"; }

[ -f k8s/observability/deploy-alerting.sh ] || { echo "run this from the repo root" >&2; exit 1; }

if [ "$DRY" = 0 ]; then
  kubectl get ns "$NS" >/dev/null
  sudo k3s ctr images list 2>/dev/null | grep -q 'local/alert-relay:1.0' \
    || { echo "alert-relay image not found in k3s -- run k8s/observability/build-alert-relay-image.sh first" >&2; exit 1; }
fi

echo "== 1. upgrade the observability release =="
run helm upgrade observability k8s/observability -n "$NS" --reset-then-reuse-values --wait --timeout 180s

echo "== 2. check =="
if [ "$DRY" = 0 ]; then
  kubectl rollout status deploy/alertmanager -n "$NS" --timeout=120s
  kubectl rollout status deploy/alert-relay -n "$NS" --timeout=120s
  kubectl rollout status deploy/prometheus -n "$NS" --timeout=120s
  echo
  echo "Alertmanager status (should show no config errors):"
  ALERTMANAGER_POD=$(kubectl get pods -n "$NS" -l app=alertmanager --no-headers -o custom-columns=":metadata.name")
  kubectl logs -n "$NS" "$ALERTMANAGER_POD" --tail=20
  echo
  echo "Prometheus's own view of the rules it loaded (should list all 5, State: ok):"
  PROM_POD=$(kubectl get pods -n "$NS" -l app=prometheus --no-headers -o custom-columns=":metadata.name")
  kubectl exec -n "$NS" "$PROM_POD" -- wget -qO- 'http://localhost:9090/api/v1/rules' | python3 -m json.tool | grep -E '"name"|"state"' || true
  echo
  echo "Prometheus's own view of Alertmanager discovery (should show 1 active target):"
  kubectl exec -n "$NS" "$PROM_POD" -- wget -qO- 'http://localhost:9090/api/v1/alertmanagers' | python3 -m json.tool
  echo
  echo "This confirms Prometheus -> Alertmanager is wired up. The alert-relay -> ntfy.sh hop"
  echo "itself is covered by services/alert-relay/test_alert_relay.py (mocked) plus"
  echo "alert-relay's own pod logs once a real alert fires -- subscribe to your ntfy topic"
  echo "(https://ntfy.sh/<topic>, or the ntfy app) to see it land."
fi
echo "done"
