#!/usr/bin/env bash
# Migrates one Kafka client at a time from the plaintext "plain" listener
# (port 9092) to the TLS "tls" listener (port 9093) added in
# k8s/kafka-strimzi/templates/kafka-cluster.yaml, and from there to
# kafka-connect-mqtt's own broker connection. Deliberately NOT one big
# script that flips everything at once, the way k8s/harden/harden-live-cluster.sh
# is -- that script's steps all had to happen together (rotating a password
# and updating the Secret that uses it are one atomic unit of work); this is
# the opposite shape, nine independent services each switching at its own
# pace, so a problem with one is caught immediately and never touches the
# other eight, and the plaintext listener stays available as an instant
# rollback for any single service the whole time.
#
# Usage (run from the repo root):
#   bash k8s/kafka-tls/cutover-tls.sh listener              # one-time: turn on the tls listener
#   bash k8s/kafka-tls/cutover-tls.sh status                # see which services are on which listener
#   bash k8s/kafka-tls/cutover-tls.sh <service>              # cut one service over to TLS
#   bash k8s/kafka-tls/cutover-tls.sh rollback <service>     # put one service back on plaintext
#   bash k8s/kafka-tls/cutover-tls.sh connect                # cut kafka-connect-mqtt over (separate: a KafkaConnect CR, not a plain Deployment)
#   bash k8s/kafka-tls/cutover-tls.sh connect-rollback
#   bash k8s/kafka-tls/cutover-tls.sh list                   # print valid <service> names
# Add --dry-run after any subcommand to see the commands without running them.
#
# Order: run `listener` first (once). Everything else can run in any order,
# any number of times, spaced out however you like -- migrate one service,
# watch it for a while, then move to the next when you're satisfied.
set -euo pipefail

NS=kafka
KAFKA_CR=restaurant-platform-kafka
BOOTSTRAP_HOST=restaurant-platform-kafka-kafka-bootstrap.kafka.svc.cluster.local
PLAINTEXT="$BOOTSTRAP_HOST:9092"
TLS="$BOOTSTRAP_HOST:9093"

[ -f k8s/kafka-tls/cutover-tls.sh ] || { echo "run this from the repo root" >&2; exit 1; }

DRY=0
for a in "$@"; do [ "$a" = "--dry-run" ] && DRY=1; done
run() { echo "+ $*"; [ "$DRY" = 1 ] || "$@"; }

# service-key -> "chart-path release-name boot-set-path proto-set-path deployment-names..."
# deployment-names is a space-separated list because edge-simulators is one
# chart/release rendering four Deployments (one per simulated sensor type,
# per the Phase 1 "one container per sensor type" requirement) -- only
# edge-sim-service-timing's Kafka connection actually matters, but all four
# restart together since the chart applies KAFKA_* to all four uniformly
# (same as its existing SCENARIO_CONTROL_ENABLED precedent).
svc_def() {
  case "$1" in
    anomaly-detector)               echo "k8s/anomaly-detector anomaly-detector env.KAFKA_BOOTSTRAP_SERVERS env.KAFKA_SECURITY_PROTOCOL anomaly-detector" ;;
    causal-engine)                  echo "k8s/causal-engine causal-engine env.KAFKA_BOOTSTRAP_SERVERS env.KAFKA_SECURITY_PROTOCOL causal-engine" ;;
    digital-twin)                   echo "k8s/digital-twin digital-twin env.KAFKA_BOOTSTRAP_SERVERS env.KAFKA_SECURITY_PROTOCOL digital-twin" ;;
    llm-narrator)                   echo "k8s/llm-narrator llm-narrator env.KAFKA_BOOTSTRAP_SERVERS env.KAFKA_SECURITY_PROTOCOL llm-narrator" ;;
    scenario-injection-controller)  echo "k8s/scenario-injection-controller scenario-injection-controller env.KAFKA_BOOTSTRAP_SERVERS env.KAFKA_SECURITY_PROTOCOL scenario-injection-controller" ;;
    ticket-timing-aggregator)       echo "k8s/ticket-timing-aggregator ticket-timing-aggregator env.KAFKA_BOOTSTRAP_SERVERS env.KAFKA_SECURITY_PROTOCOL ticket-timing-aggregator" ;;
    storage-consumer)               echo "k8s/storage-consumer storage-consumer kafka.bootstrapServers kafka.securityProtocol storage-consumer" ;;
    edge-simulators)                echo "k8s/edge-simulators edge-simulators kafka.bootstrapServers kafka.securityProtocol edge-sim-plate-waste edge-sim-pos-transaction edge-sim-service-timing edge-sim-staff-shift" ;;
    game-bridge)                    echo "game/k8s/bridge game-bridge env.KAFKA_BOOTSTRAP_SERVERS env.KAFKA_SECURITY_PROTOCOL game-bridge" ;;
    *) return 1 ;;
  esac
}
ALL_SERVICES="anomaly-detector causal-engine digital-twin llm-narrator scenario-injection-controller ticket-timing-aggregator storage-consumer edge-simulators game-bridge"

# Prints each named Deployment's pods (restart count is the tell -- a
# service that can't complete a TLS handshake typically raises an uncaught
# exception out of its own KafkaConsumer/Producer constructor, which crashes
# the container and shows up here as a climbing RESTARTS count and
# eventually CrashLoopBackOff, not a hang `rollout status` would catch: none
# of these services define a readinessProbe, so Kubernetes considers a
# container "Ready" the instant it's Running, whether or not it actually
# connected to anything) and tails the first pod's log so you have the
# actual error message, not just a restart count, if something's wrong.
show_pods_and_logs() {
  local d
  for d in "$@"; do
    echo "--- pods for $d ---"
    kubectl get pods -n "$NS" --no-headers 2>/dev/null | awk -v d="$d" '$1 ~ ("^" d "-")'
  done
  local first_pod
  first_pod=$(kubectl get pods -n "$NS" --no-headers -o custom-columns=":metadata.name" 2>/dev/null | awk -v d="$1" '$1 ~ ("^" d "-")' | head -1)
  if [ -n "$first_pod" ]; then
    echo "--- recent logs: $first_pod ---"
    kubectl logs -n "$NS" "$first_pod" --tail=30 --timestamps || true
  fi
}

migrate() {
  local svc="$1" boot="$2"
  local def; def=$(svc_def "$svc") || { echo "unknown service: $svc (see: $0 list)" >&2; exit 1; }
  # shellcheck disable=SC2086
  set -- $def
  local chart="$1" release="$2" boot_path="$3" proto_path="$4"; shift 4
  local proto="SSL"; [ "$boot" = "$PLAINTEXT" ] && proto="PLAINTEXT"
  echo "== $([ "$proto" = SSL ] && echo "cutting over" || echo "rolling back") $svc ($boot_path=$boot, $proto_path=$proto) =="
  # --reset-then-reuse-values, not --reuse-values -- every one of these
  # charts already has a live release from before kafkaTlsSecretName /
  # KAFKA_SECURITY_PROTOCOL existed in its values.yaml. --reuse-values would
  # only reuse what that OLD release actually had (nil for a key that didn't
  # exist yet), the exact bug k8s/minio hit rotating its password. This
  # starts from the chart's current defaults and re-layers the previous
  # release's actual overrides on top, then --set wins over both.
  run helm upgrade "$release" "$chart" -n "$NS" --reset-then-reuse-values \
    --set "$boot_path=$boot" --set "$proto_path=$proto" --wait --timeout 120s
  for d in "$@"; do
    run kubectl rollout status "deploy/$d" -n "$NS" --timeout=90s
  done
  if [ "$DRY" = 0 ]; then
    echo "== waiting 15s to see if it stays up (a silent reconnect-loop failure crashes after the first attempt, not instantly) =="
    sleep 15
    show_pods_and_logs "$@"
  fi
}

status() {
  local svc def chart release boot_path proto_path first_deploy val
  for svc in $ALL_SERVICES; do
    def=$(svc_def "$svc")
    # shellcheck disable=SC2086
    set -- $def
    first_deploy="$5"
    if ! kubectl get deploy "$first_deploy" -n "$NS" >/dev/null 2>&1; then
      printf '%-32s %s\n' "$svc" "<deployment does not exist>"
      continue
    fi
    val=$(kubectl get deploy "$first_deploy" -n "$NS" -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="KAFKA_SECURITY_PROTOCOL")].value}' 2>/dev/null || true)
    # Empty (not missing) means the deployment predates this env var
    # existing at all -- still plaintext, just not yet helm-upgraded onto
    # the chart version that added the toggle.
    printf '%-32s %s\n' "$svc" "${val:-PLAINTEXT (pre-TLS chart version, needs cutover-tls.sh $svc to deploy)}"
  done
  echo
  local kc_tls
  kc_tls=$(kubectl get kafkaconnect connect-cluster -n "$NS" -o jsonpath='{.spec.tls}' 2>/dev/null || true)
  printf '%-32s %s\n' "kafka-connect-mqtt" "$([ -n "$kc_tls" ] && echo SSL || echo PLAINTEXT)"
}

cmd="${1:-}"
case "$cmd" in
  list)
    echo "$ALL_SERVICES" | tr ' ' '\n'
    ;;
  status)
    status
    ;;
  listener)
    # One-time prerequisite for everything else. Strimzi reconciles the new
    # listener onto the existing Kafka CR and performs its own rolling
    # restart of the brokers to add it -- brokers come back one at a time,
    # so the existing plaintext listener stays reachable throughout (this
    # step should not interrupt anything still on 9092).
    echo "== enabling the tls listener (port 9093) on the Kafka cluster =="
    run helm upgrade kafka-strimzi k8s/kafka-strimzi -n "$NS" --wait --timeout 300s
    run kubectl wait "kafka/$KAFKA_CR" -n "$NS" --for=condition=Ready --timeout=300s
    if [ "$DRY" = 0 ]; then
      echo "== verifying =="
      kubectl get svc "$KAFKA_CR-kafka-bootstrap" -n "$NS" -o jsonpath='{.spec.ports[*].port}'; echo
      kubectl get secret "$KAFKA_CR-cluster-ca-cert" -n "$NS" >/dev/null && echo "CA cert secret present: $KAFKA_CR-cluster-ca-cert"
    fi
    ;;
  rollback)
    svc="${2:-}"; [ -n "$svc" ] || { echo "usage: $0 rollback <service>" >&2; exit 1; }
    migrate "$svc" "$PLAINTEXT"
    ;;
  connect)
    echo "== cutting over kafka-connect-mqtt's own broker connection =="
    run helm upgrade kafka-connect-mqtt k8s/kafka-connect-mqtt -n "$NS" \
      --set bootstrapServers="$TLS" --set kafkaTls.enabled=true --wait --timeout 180s
    if [ "$DRY" = 0 ]; then
      run kubectl wait kafkaconnect/connect-cluster -n "$NS" --for=condition=Ready --timeout=180s
      echo "--- connect worker pod logs ---"
      kubectl logs -n "$NS" "$(kubectl get pods -n "$NS" --no-headers -o custom-columns=":metadata.name" | grep '^connect-cluster-connect' | head -1)" --tail=30 || true
    fi
    ;;
  connect-rollback)
    echo "== rolling back kafka-connect-mqtt to plaintext =="
    run helm upgrade kafka-connect-mqtt k8s/kafka-connect-mqtt -n "$NS" \
      --set bootstrapServers="$PLAINTEXT" --set kafkaTls.enabled=false --wait --timeout 180s
    ;;
  ""|-h|--help)
    grep '^#' "$0" | sed -n '2,21p' | sed 's/^# \{0,1\}//'
    ;;
  *)
    migrate "$cmd" "$TLS"
    ;;
esac
