#!/usr/bin/env bash
# Rebuilds and redeploys just the edge-simulator image, for the CONNACK race
# fix in edge-simulators/common/runtime.py (see that file's own comment):
# client.connect() only opens the socket and sends the CONNECT packet, it
# does not wait for the broker's CONNACK -- plaintext's round trip was fast
# enough this never surfaced, TLS's extra handshake latency exposed it live
# as every publish failing with "client is not currently connected" in a
# tight, never-crashing, never-recovering loop. Mirrors
# k8s/kafka-tls/rebuild-kafka-python-images.sh's shape (needs sudo, same
# reason: containerd is root-only) but scoped to this one image since this
# fix has nothing to do with that one's kafka-python bump.
#
# Run from the repo root:
#   bash k8s/mosquitto-tls/rebuild-edge-simulator-image.sh --dry-run
#   bash k8s/mosquitto-tls/rebuild-edge-simulator-image.sh
set -euo pipefail

NS=kafka
DRY=0
[ "${1:-}" = "--dry-run" ] && DRY=1
run() { echo "+ $*"; [ "$DRY" = 1 ] || "$@"; }

[ -f k8s/mosquitto-tls/rebuild-edge-simulator-image.sh ] || { echo "run this from the repo root" >&2; exit 1; }

if [ "$DRY" = 0 ]; then
  sudo -v   # ask for the password once, up front, not halfway through
  kubectl get ns "$NS" >/dev/null
  docker version >/dev/null
fi

echo "== 1. build image =="
run docker build --network=host -t local/edge-simulator:1.0 -f edge-simulators/Dockerfile .

echo "== 2. import into k3s =="
echo "+ docker save local/edge-simulator:1.0 | sudo k3s ctr images import -"
[ "$DRY" = 1 ] || docker save local/edge-simulator:1.0 | sudo k3s ctr images import -

echo "== 3. restart all four simulator deployments =="
DEPLOYS="edge-sim-plate-waste edge-sim-pos-transaction edge-sim-service-timing edge-sim-staff-shift"
for d in $DEPLOYS; do run kubectl rollout restart "deploy/$d" -n "$NS"; done
for d in $DEPLOYS; do run kubectl rollout status "deploy/$d" -n "$NS" --timeout=120s; done

echo "== 4. check =="
if [ "$DRY" = 0 ]; then
  kubectl get pods -n "$NS" --no-headers | grep "^edge-sim-"
  echo
  echo "Watch a service-timing pod's logs for a clean 'published ... event_id=...' line"
  echo "with no 'client is not currently connected' errors:"
  echo "  kubectl logs -n $NS -l app=edge-sim-service-timing --tail=20 -f"
fi
echo "done"
