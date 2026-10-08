#!/usr/bin/env bash
# Build the game-bridge image and deploy it to k3s. Run from the repo root:
#
#   bash game/k8s/deploy-bridge.sh           # do it
#   bash game/k8s/deploy-bridge.sh --dry-run # print the steps only
#
# Not part of k8s/realign/realign-live-cluster.sh: that script brings the
# shared platform in line with the repo; this deploys the version-2-only
# bridge, which lives under game/k8s/ for the same reason (see
# game/README.md's boundary rule: version 1 never references version 2).
#
# The bridge bakes its schemas into the image at build time (unlike the
# Phase 5-7 services, which mount the phase5-schemas ConfigMap), so no
# ConfigMap install is needed here -- just build, import, upgrade.
#
# Needs: sudo (containerd is root-only), kubectl, helm, a container CLI.
# Under rootless Podman on WSL2:
#   export DOCKER_HOST=unix:///run/user/$(id -u)/podman/podman.sock
set -euo pipefail

NS=kafka
DRY=0
[ "${1:-}" = "--dry-run" ] && DRY=1

run() {
  echo "+ $*"
  [ "$DRY" = 1 ] || "$@"
}

[ -f game/k8s/bridge/Chart.yaml ] || { echo "run this from the repo root" >&2; exit 1; }

if [ "$DRY" = 0 ]; then
  sudo -v
  kubectl get ns "$NS" >/dev/null
  docker version >/dev/null
fi

echo "== 1. build the image"
run docker build --network=host -t local/game-bridge:1.0 -f game/bridge/Dockerfile .

echo "== 2. import into k3s"
echo "+ docker save local/game-bridge:1.0 | sudo k3s ctr images import -"
[ "$DRY" = 1 ] || docker save local/game-bridge:1.0 | sudo k3s ctr images import -

echo "== 3. helm upgrade"
run helm upgrade --install game-bridge game/k8s/bridge -n "$NS" --wait --timeout 5m

echo "== 4. check"
if [ "$DRY" = 0 ]; then
  kubectl rollout status deploy/game-bridge -n "$NS" --timeout=120s
  echo "Not exposed outside the cluster (ClusterIP, no authentication -- same as Compose's"
  echo "localhost-only binding). Reach it with:"
  echo "  kubectl port-forward svc/game-bridge -n $NS 8001:8001"
  echo "then point the Godot client at it: BRIDGE_URL=http://127.0.0.1:8001 godot4 --path game/client"
fi
echo "done"
