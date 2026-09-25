#!/usr/bin/env bash
# Migrates Mosquitto's clients from the plaintext listener (port 1883) to a
# TLS one (port 8883), the same additive, migrate-then-verify shape as
# k8s/kafka-tls/cutover-tls.sh -- a second listener added alongside the
# existing plaintext one (not replacing it), so a consumer can move over on
# its own schedule with the plaintext listener staying up as an instant
# rollback the whole time.
#
# Usage (run from the repo root):
#   bash k8s/mosquitto-tls/cutover-mqtt-tls.sh listener              # one-time: generate the cert, turn on the tls listener
#   bash k8s/mosquitto-tls/cutover-mqtt-tls.sh status                # see which consumers are on which listener
#   bash k8s/mosquitto-tls/cutover-mqtt-tls.sh edge-simulators       # cut the simulators over to TLS
#   bash k8s/mosquitto-tls/cutover-mqtt-tls.sh rollback edge-simulators
# Add --dry-run after any subcommand to see the commands without running them.
#
# What this does NOT cover: Kafka Connect's four MQTT source connectors
# (k8s/kafka-connect-mqtt) stay on plaintext for now. Checked live via the
# Apache Camel project's own connector reference docs: the Kamelet-wrapped
# mqtt-source connector these use exposes exactly five config keys (topic,
# brokerUrl, clientId, username, password) -- no TLS/certificate option at
# all. The only real path there is injecting Mosquitto's cert into the JVM's
# own default trust store (an init container + keytool + KAFKA_OPTS
# pointing at a modified cacerts file) -- a meaningfully bigger, unverified
# change deliberately left out of this script rather than guessed at. That
# one hop (Mosquitto -> Kafka Connect) is internal cluster traffic between
# two pods in the same namespace, not exposed outside it.
set -euo pipefail

NS=kafka
run() { echo "+ $*"; [ "$DRY" = 1 ] || "$@"; }

[ -f k8s/mosquitto-tls/cutover-mqtt-tls.sh ] || { echo "run this from the repo root" >&2; exit 1; }

DRY=0
for a in "$@"; do [ "$a" = "--dry-run" ] && DRY=1; done

migrate() {
  local tls_on="$1"  # "true" or "false"
  local port="1883"
  [ "$tls_on" = "true" ] && port="8883"
  echo "== $([ "$tls_on" = true ] && echo "cutting over" || echo "rolling back") edge-simulators (mqtt.tlsEnabled=$tls_on, mqtt.port=$port) =="
  # --reset-then-reuse-values, not --reuse-values -- this release already
  # exists from before mqtt.tlsEnabled/mqtt.tlsSecretName existed in its
  # values.yaml; --reuse-values would only reuse what that OLD release
  # actually had (nil for a key that didn't exist yet), the exact bug
  # k8s/minio hit rotating its password. This starts from the chart's
  # current defaults and re-layers the previous release's actual overrides
  # on top, then --set wins over both.
  run helm upgrade edge-simulators k8s/edge-simulators -n "$NS" --reset-then-reuse-values \
    --set mqtt.tlsEnabled="$tls_on" --set mqtt.port="$port" --wait --timeout 120s
  for d in edge-sim-plate-waste edge-sim-pos-transaction edge-sim-service-timing edge-sim-staff-shift; do
    run kubectl rollout status "deploy/$d" -n "$NS" --timeout=90s
  done
  if [ "$DRY" = 0 ]; then
    echo "== waiting 15s to see if it stays up =="
    sleep 15
    kubectl get pods -n "$NS" --no-headers | grep "^edge-sim-"
    local first_pod
    first_pod=$(kubectl get pods -n "$NS" --no-headers -o custom-columns=":metadata.name" | grep "^edge-sim-service-timing-" | head -1)
    echo "--- recent logs: $first_pod ---"
    kubectl logs -n "$NS" "$first_pod" --tail=15 || true
  fi
}

status() {
  local val
  val=$(kubectl get deploy edge-sim-service-timing -n "$NS" -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="MQTT_TLS_ENABLED")].value}' 2>/dev/null || true)
  printf '%-16s %s\n' "edge-simulators" "$([ "$val" = "true" ] && echo TLS || echo "PLAINTEXT (${val:-pre-TLS chart version, needs cutover-mqtt-tls.sh edge-simulators to deploy})")"
}

cmd="${1:-}"
case "$cmd" in
  list)
    echo "edge-simulators"
    ;;
  status)
    status
    ;;
  listener)
    echo "== generating Mosquitto's self-signed TLS cert =="
    if [ "$DRY" = 1 ]; then
      echo "+ openssl req -x509 -newkey rsa:2048 ... -> kubectl create secret generic mosquitto-tls"
    else
      CERT_DIR=$(mktemp -d)
      openssl req -x509 -newkey rsa:2048 -nodes -days 3650 \
        -keyout "$CERT_DIR/tls.key" -out "$CERT_DIR/tls.crt" \
        -subj "/CN=mosquitto.$NS.svc.cluster.local" \
        -addext "subjectAltName=DNS:mosquitto.$NS.svc.cluster.local,DNS:mosquitto,DNS:localhost"
      kubectl create secret generic mosquitto-tls -n "$NS" \
        --from-file=tls.crt="$CERT_DIR/tls.crt" --from-file=tls.key="$CERT_DIR/tls.key" \
        --dry-run=client -o yaml | kubectl apply -f -
      rm -rf "$CERT_DIR"
    fi
    echo "== enabling the tls listener (port 8883) on Mosquitto =="
    run helm upgrade mosquitto k8s/mosquitto -n "$NS" --wait --timeout 120s
    run kubectl rollout status deploy/mosquitto -n "$NS" --timeout=90s
    if [ "$DRY" = 0 ]; then
      echo "== verifying =="
      kubectl get svc mosquitto -n "$NS" -o jsonpath='{.spec.ports[*].port}'; echo
      kubectl get secret mosquitto-tls -n "$NS" >/dev/null && echo "TLS cert secret present: mosquitto-tls"
    fi
    ;;
  rollback)
    svc="${2:-}"
    [ "$svc" = "edge-simulators" ] || { echo "usage: $0 rollback edge-simulators" >&2; exit 1; }
    migrate false
    ;;
  edge-simulators)
    migrate true
    ;;
  ""|-h|--help)
    grep '^#' "$0" | sed -n '2,20p' | sed 's/^# \{0,1\}//'
    ;;
  *)
    echo "unknown service: $cmd (see: $0 list)" >&2
    exit 1
    ;;
esac
