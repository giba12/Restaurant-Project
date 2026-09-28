#!/usr/bin/env bash
# Builds services/alert-relay's image and imports it into k3s's containerd,
# same shape as k8s/mosquitto-tls/rebuild-edge-simulator-image.sh -- needs
# sudo for the containerd import (k3s ctr is root-only), so kept out of
# k8s/harden/harden-live-cluster.sh, which deliberately does not need sudo.
#
# Run from the repo root:
#   bash k8s/observability/build-alert-relay-image.sh --dry-run
#   bash k8s/observability/build-alert-relay-image.sh
set -euo pipefail

DRY=0
[ "${1:-}" = "--dry-run" ] && DRY=1
run() { echo "+ $*"; [ "$DRY" = 1 ] || "$@"; }

[ -f k8s/observability/build-alert-relay-image.sh ] || { echo "run this from the repo root" >&2; exit 1; }

if [ "$DRY" = 0 ]; then
  sudo -v
  docker version >/dev/null
fi

echo "== 1. build image =="
run docker build --network=host -t localhost/local/alert-relay:1.0 -f services/alert-relay/Dockerfile services/alert-relay

echo "== 2. import into k3s =="
echo "+ docker save localhost/local/alert-relay:1.0 | sudo k3s ctr images import -"
[ "$DRY" = 1 ] || docker save localhost/local/alert-relay:1.0 | sudo k3s ctr images import -

echo "done -- next: bash k8s/observability/deploy-alerting.sh"
