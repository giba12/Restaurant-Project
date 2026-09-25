#!/usr/bin/env bash
# Rebuilds and redeploys every image that got a kafka-python version bump
# (2.0.2 -> 3.0.11, see services/phase5_common.py's KAFKA_TLS_KWARGS comment
# for why: 2.0.2 could not complete a TLS handshake against this broker at
# all -- confirmed live, not guessed -- and 3.0.11 fixes it). Mirrors
# k8s/realign/realign-live-cluster.sh's shape for the same reason: rebuilding
# images and importing them into k3s needs sudo (containerd is root-only),
# which this shell cannot do on its own.
#
# Run from the repo root:
#   bash k8s/kafka-tls/rebuild-kafka-python-images.sh --dry-run
#   bash k8s/kafka-tls/rebuild-kafka-python-images.sh
#
# Why a restart is needed even though nothing in any chart's values changed:
# imagePullPolicy is IfNotPresent everywhere in this project, and re-importing
# a new image under a tag that's already present locally does not by itself
# restart pods already running on the old content -- only a new pod (a
# fresh rollout) picks up what the node's image store now has for that tag.
#
# anomaly-detector is already configured for SSL from the earlier failed
# cutover attempt (see k8s/kafka-tls/cutover-tls.sh's status output) -- this
# script's restart step alone fixes it, no need to run cutover-tls.sh for it
# again. Every other service is still on plaintext; run
# `bash k8s/kafka-tls/cutover-tls.sh <service>` for each after this finishes.
set -euo pipefail

NS=kafka
DRY=0
[ "${1:-}" = "--dry-run" ] && DRY=1
run() { echo "+ $*"; [ "$DRY" = 1 ] || "$@"; }

[ -f k8s/kafka-tls/rebuild-kafka-python-images.sh ] || { echo "run this from the repo root" >&2; exit 1; }

if [ "$DRY" = 0 ]; then
  sudo -v   # ask for the password once, up front, not halfway through
  kubectl get ns "$NS" >/dev/null
  docker version >/dev/null
fi

echo "== 1. build images =="
# The five phase5/6/7-style services build from the shared services/
# directory (phase5_common.py lives there); storage-consumer, edge-simulator
# and game-bridge each have their own build context, same as
# k8s/realign/realign-live-cluster.sh and game/k8s/deploy-bridge.sh already
# established for these exact images.
for svc in ticket-timing-aggregator anomaly-detector causal-engine digital-twin \
           scenario-injection-controller; do
  run docker build --network=host -t "local/$svc:1.0" -f "services/$svc/Dockerfile" services
done
run docker build --network=host -t local/finding-narrator:1.0 -f services/finding-narrator/Dockerfile services
run docker build --network=host -t local/storage-consumer:1.0 -f storage/consumer/Dockerfile .
run docker build --network=host -t local/edge-simulator:1.0 -f edge-simulators/Dockerfile .
run docker build --network=host -t local/game-bridge:1.0 -f game/bridge/Dockerfile .

echo "== 2. import images into k3s =="
for img in ticket-timing-aggregator anomaly-detector causal-engine digital-twin \
           scenario-injection-controller finding-narrator storage-consumer \
           edge-simulator game-bridge; do
  echo "+ docker save local/$img:1.0 | sudo k3s ctr images import -"
  [ "$DRY" = 1 ] || docker save "local/$img:1.0" | sudo k3s ctr images import -
done

echo "== 3. restart every affected deployment =="
DEPLOYS="ticket-timing-aggregator anomaly-detector causal-engine digital-twin \
         scenario-injection-controller llm-narrator storage-consumer game-bridge \
         edge-sim-plate-waste edge-sim-pos-transaction edge-sim-service-timing edge-sim-staff-shift"
for d in $DEPLOYS; do run kubectl rollout restart "deploy/$d" -n "$NS"; done
for d in $DEPLOYS; do run kubectl rollout status "deploy/$d" -n "$NS" --timeout=120s; done

echo "== 4. check =="
if [ "$DRY" = 0 ]; then
  echo "anomaly-detector should now be healthy on SSL (it was already configured for it):"
  kubectl get pods -n "$NS" -l app=anomaly-detector
  echo
  echo "Run 'bash k8s/kafka-tls/cutover-tls.sh status' to see every other service --"
  echo "they're all still on PLAINTEXT and ready to migrate one at a time."
fi
echo "done"
