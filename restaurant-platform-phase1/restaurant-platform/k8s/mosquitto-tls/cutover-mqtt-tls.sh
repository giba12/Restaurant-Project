#!/usr/bin/env bash
# HISTORICAL: the broker's plaintext listener (1883) was removed on 2026-10-07, so this migration can no longer be run as it
# stands. It is kept as the record of how the clients were moved. Migrates Mosquitto's clients from the plaintext listener (port 1883) to a
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
#   bash k8s/mosquitto-tls/cutover-mqtt-tls.sh connect               # cut Kafka Connect's MQTT source connectors over
#   bash k8s/mosquitto-tls/cutover-mqtt-tls.sh rollback edge-simulators
#   bash k8s/mosquitto-tls/cutover-mqtt-tls.sh connect-rollback
# Add --dry-run after any subcommand to see the commands without running them.
#
# `connect` is its own case, not just another <service>: the four MQTT
# source connectors (k8s/kafka-connect-mqtt) run on the JVM via Apache
# Camel's Kamelet-wrapped mqtt-source connector, which exposes exactly five
# config keys (topic, brokerUrl, clientId, username, password) -- no TLS
# option at all, confirmed against Apache Camel's own connector reference
# docs. Underneath it's the standard Eclipse Paho MQTT v3 Java client, which
# uses the JVM's *default* trust store with no per-connector override --
# confirmed live via a throwaway test connector pointed at ssl://, which
# failed with a textbook PKIX "no trusted path" error. So this subcommand
# builds an actual Java truststore (a copy of the Connect image's own
# cacerts -- copy, not the original, which is root-owned and read-only --
# plus Mosquitto's cert added via keytool, run live inside the pod, which
# already has both keytool and Java) and wires the whole worker to it via
# Strimzi's jvmOptions.javaSystemProperties, proven with a raw SSLSocket
# handshake before any of this was written into the chart.
set -euo pipefail

NS=kafka
CONNECT_POD=connect-cluster-connect-0
CONNECTORS="plate-waste pos-transaction service-timing staff-shift"
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
  local kc_ts
  kc_ts=$(kubectl get kafkaconnect connect-cluster -n "$NS" -o jsonpath='{.spec.jvmOptions.javaSystemProperties[?(@.name=="javax.net.ssl.trustStore")].value}' 2>/dev/null || true)
  printf '%-16s %s\n' "connect" "$([ -n "$kc_ts" ] && echo TLS || echo PLAINTEXT)"
}

# Builds the JVM truststore Kafka Connect needs to trust Mosquitto's
# self-signed cert, entirely inside the live connect pod (it already has
# keytool and Java; nothing new to install), then pulls the result out and
# stores it as a Secret. Safe to re-run: always rebuilds from that pod's
# *current* cacerts rather than reusing a stale copy, so it stays correct if
# the base image's own CA bundle ever changes, and re-adding the same alias
# to a fresh copy each time is idempotent.
build_mqtt_truststore() {
  echo "== building the MQTT truststore (cacerts + Mosquitto's cert, via keytool inside $CONNECT_POD) =="
  if [ "$DRY" = 1 ]; then
    echo "+ kubectl exec $CONNECT_POD -- keytool -importcert ... -> kubectl create secret generic connect-truststore"
    return
  fi
  local work; work=$(mktemp -d)
  kubectl get secret mosquitto-tls -n "$NS" -o jsonpath='{.data.tls\.crt}' | base64 -d > "$work/mosquitto.crt"
  kubectl exec -n "$NS" -i "$CONNECT_POD" -- sh -c "cat > /tmp/mosquitto-tls-build.crt" < "$work/mosquitto.crt"
  kubectl exec -n "$NS" "$CONNECT_POD" -- sh -c "
    set -e
    cp /etc/pki/ca-trust/extracted/java/cacerts /tmp/mosquitto-tls-build-cacerts
    chmod u+w /tmp/mosquitto-tls-build-cacerts
    keytool -importcert -noprompt -alias mosquitto \
      -file /tmp/mosquitto-tls-build.crt -keystore /tmp/mosquitto-tls-build-cacerts -storepass changeit
  "
  kubectl exec -n "$NS" "$CONNECT_POD" -- sh -c "base64 /tmp/mosquitto-tls-build-cacerts" > "$work/cacerts.b64"
  base64 -d "$work/cacerts.b64" > "$work/cacerts"
  kubectl create secret generic connect-truststore -n "$NS" \
    --from-file=cacerts="$work/cacerts" --dry-run=client -o yaml | kubectl apply -f -
  kubectl exec -n "$NS" "$CONNECT_POD" -- rm -f /tmp/mosquitto-tls-build.crt /tmp/mosquitto-tls-build-cacerts
  rm -rf "$work"
}

migrate_connect() {
  local tls_on="$1"  # "true" or "false"
  echo "== $([ "$tls_on" = true ] && echo "cutting over" || echo "rolling back") Kafka Connect's MQTT source connectors (mqttTls.enabled=$tls_on) =="
  if [ "$tls_on" = "true" ]; then
    build_mqtt_truststore
  fi
  # --reset-then-reuse-values -- see the identical note on the
  # edge-simulators migrate() function above; this release already exists
  # from before mqttTls existed in its values.yaml.
  #
  # --timeout 360s, not 180s: confirmed live that a JVM-options/volume-mount
  # change here needs a full pod restart (image already local, but the JVM
  # itself plus Kafka Connect's internal topic reconciliation took longer
  # than 180s) -- a real, successful rollout, `helm upgrade --wait` just
  # gave up watching it too early. The rollout itself succeeded regardless
  # of Helm's own timeout (confirmed via observedGeneration matching and a
  # clean, 0-restart pod), so a `helm upgrade` timeout here does not mean
  # the change failed -- check `kubectl get kafkaconnect connect-cluster -n
  # kafka -o jsonpath='{.status.conditions}'` before assuming it did.
  run helm upgrade kafka-connect-mqtt k8s/kafka-connect-mqtt -n "$NS" --reset-then-reuse-values \
    --set mqttTls.enabled="$tls_on" --wait --timeout 360s
  run kubectl wait kafkaconnect/connect-cluster -n "$NS" --for=condition=Ready --timeout=180s
  echo "== applying the connector configs =="
  if [ "$tls_on" = "true" ]; then
    run kubectl apply -n "$NS" -f k8s/kafka-connect-mqtt/connectors/
  else
    # The checked-in files point at ssl://8883 (the steady state); rolling
    # back means applying the plaintext version without touching those
    # files, so a re-run of `connect` (no code changes needed) goes right
    # back to the tested, working TLS config.
    for f in k8s/kafka-connect-mqtt/connectors/*.yaml; do
      if [ "$DRY" = 1 ]; then
        echo "+ sed 's|ssl://...:8883|tcp://...:1883|' $f | kubectl apply -n $NS -f -"
      else
        sed 's|ssl://mosquitto.kafka.svc.cluster.local:8883|tcp://mosquitto.kafka.svc.cluster.local:1883|' "$f" \
          | kubectl apply -n "$NS" -f -
      fi
    done
  fi
  if [ "$DRY" = 0 ]; then
    echo "== waiting 15s, then checking each connector's task status =="
    sleep 15
    for c in $CONNECTORS; do
      echo "--- $c-source-connector ---"
      kubectl exec -n "$NS" "$CONNECT_POD" -- curl -s "localhost:8083/connectors/$c-source-connector/status"
      echo
    done
  fi
}

cmd="${1:-}"
case "$cmd" in
  list)
    echo "edge-simulators"
    echo "connect (its own subcommand, not a <service> name -- see --help)"
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
    [ "$svc" = "edge-simulators" ] || { echo "usage: $0 rollback edge-simulators (for connect, use: $0 connect-rollback)" >&2; exit 1; }
    migrate false
    ;;
  edge-simulators)
    migrate true
    ;;
  connect)
    migrate_connect true
    ;;
  connect-rollback)
    migrate_connect false
    ;;
  ""|-h|--help)
    grep '^#' "$0" | sed -n '2,16p' | sed 's/^# \{0,1\}//'
    ;;
  *)
    echo "unknown service: $cmd (see: $0 list)" >&2
    exit 1
    ;;
esac
