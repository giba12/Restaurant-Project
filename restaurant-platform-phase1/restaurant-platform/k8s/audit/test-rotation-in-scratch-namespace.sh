#!/usr/bin/env bash
# Rehearses the whole master-key rotation (k8s/audit/rotate-master-key.sh) on real Kubernetes, in a throwaway namespace that holds its
# own copy of the broker, the real edge-simulators chart and the real plate-node image, with credentials and a certificate made here.
# Nothing is read from, or changed in, the real namespace: this is what lets the rotation be proved before it is run on the cluster.
#
#   bash k8s/audit/test-rotation-in-scratch-namespace.sh        # about 6 minutes; deletes its namespace however it ends
#
# Needs kubectl, helm, openssl, python3 and docker or podman (provision-mqtt-auth.sh builds the password file in the Mosquitto
# image), and the simulator image already imported into k3s (localhost/local/edge-simulator:1.0). Refuses to run in a real namespace.
set -Eeuo pipefail

NS="${NS:-rp-rotation-trial}"
case "$NS" in kafka|default|kube-*|restaurant-platform) echo "refusing to run in the namespace '$NS': this test deletes its namespace" >&2; exit 2 ;; esac
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$HERE/../.."
WAIT_SECONDS="${WAIT_SECONDS:-180}"

failures=0
pass() { echo "PASS  $*"; }
fail() { echo "FAIL  $*"; failures=$((failures + 1)); }

work="$(mktemp -d)"
chmod 700 "$work"
cleanup() {
  rm -rf "$work"
  if [ "${KEEP_NAMESPACE:-0}" = 1 ]; then echo "-- KEEP_NAMESPACE=1: leaving namespace $NS (delete it with: kubectl delete namespace $NS)"; return; fi
  echo "-- deleting namespace $NS"
  kubectl delete namespace "$NS" --wait=false >/dev/null 2>&1 || true
}
trap cleanup EXIT
umask 077

# The SHA-256 of one Secret key's value (never the value itself), for "did it change" checks.
fingerprint() { kubectl get secret "$1" -n "$NS" -o "jsonpath={.data.$2}" 2>/dev/null | base64 -d 2>/dev/null | sha256sum | cut -c1-16; }

echo "== setup: a scratch namespace with a broker, the plate node and every credential (made here)"
# a previous run's namespace may still be terminating
for _ in $(seq 1 60); do kubectl get namespace "$NS" >/dev/null 2>&1 || break; sleep 3; done
kubectl create namespace "$NS" >/dev/null
openssl req -x509 -newkey rsa:2048 -nodes -days 2 -keyout "$work/tls.key" -out "$work/tls.crt" -subj "/CN=mosquitto" \
  -addext "subjectAltName=DNS:mosquitto,DNS:mosquitto.$NS.svc.cluster.local,DNS:localhost" >/dev/null 2>&1
kubectl create secret generic mosquitto-tls -n "$NS" "--from-file=tls.crt=$work/tls.crt" "--from-file=tls.key=$work/tls.key" >/dev/null
# The simulators' chart mounts Kafka's CA; nothing here talks to Kafka, so any certificate will do.
kubectl create secret generic restaurant-platform-kafka-cluster-ca-cert -n "$NS" "--from-file=ca.crt=$work/tls.crt" >/dev/null
NS="$NS" bash "$ROOT/k8s/mosquitto/provision-mqtt-auth.sh" >/dev/null
helm install mosquitto "$ROOT/k8s/mosquitto" -n "$NS" --wait --timeout 3m >/dev/null
# The simulators' chart names its namespace in every template (value `namespace`, default kafka). Render it first and refuse to install
# if anything in it would still land in the real namespace.
SIM_SETS=(--set "namespace=$NS" --set "mqtt.host=mosquitto.$NS.svc.cluster.local" --set mqtt.port=8883 --set mqtt.tlsEnabled=true --set scenarioControlEnabled=false)
if helm template edge-simulators "$ROOT/k8s/edge-simulators" -n "$NS" "${SIM_SETS[@]}" | grep -q "namespace: kafka"; then
  echo "the simulators' chart would still create something in the namespace 'kafka'; refusing to install it" >&2
  exit 2
fi
helm install edge-simulators "$ROOT/k8s/edge-simulators" -n "$NS" --wait --timeout 4m "${SIM_SETS[@]}" >/dev/null
pass "the scratch broker and the four simulators are running, the plate node with its own control key"

master_before="$(fingerprint edge-control-master key)"
key_before="$(fingerprint edge-control-sim-plate-cam-01 key)"
logins_before="$(for u in sim-plate-cam-01 sim-pos-01 rp-mqtt-kafka-bridge edge-operator; do fingerprint "mqtt-$u" password; done | tr '\n' ' ')"
pod_before="$(kubectl get pod -n "$NS" -l app=edge-sim-plate-waste -o jsonpath='{.items[0].metadata.name}')"

# A recorder that logs every status the node publishes, with its time, so a surprising answer can be explained from what the node said.
kubectl apply -n "$NS" -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata: {name: status-recorder}
spec:
  restartPolicy: Never
  containers:
  - name: recorder
    image: eclipse-mosquitto:2
    command: ["/bin/ash", "-c"]
    args: ['mosquitto_sub --cafile /ca/tls.crt -h mosquitto -p 8883 -u "\$MQTT_USERNAME" -P "\$MQTT_PW" -t "edge/status/#" -F "%I %p"']
    env:
    - {name: MQTT_USERNAME, valueFrom: {secretKeyRef: {name: mqtt-edge-operator, key: username}}}
    - {name: MQTT_PW, valueFrom: {secretKeyRef: {name: mqtt-edge-operator, key: password}}}
    volumeMounts: [{name: ca, mountPath: /ca}]
  volumes:
  - name: ca
    secret: {secretName: mosquitto-tls, items: [{key: tls.crt, path: tls.crt}]}
EOF
kubectl wait --for=condition=Ready pod/status-recorder -n "$NS" --timeout=90s >/dev/null 2>&1 || true

echo "== the rotation"
rc=0
NS="$NS" ROTATE_CONFIRM=yes HELM_EXTRA_ARGS="--set namespace=$NS" bash "$ROOT/k8s/audit/rotate-master-key.sh" || rc=$?
if [ "$rc" = 0 ]; then pass "rotate-master-key.sh reported every check passed"; else fail "rotate-master-key.sh stopped (exit $rc)"; fi

echo "== what it left behind"
[ "$(fingerprint edge-control-master key)" != "$master_before" ] && pass "the master secret is a new one" || fail "the master secret did not change"
[ "$(fingerprint edge-control-sim-plate-cam-01 key)" != "$key_before" ] && pass "the node's key is a new one" || fail "the node's key did not change"
kubectl get secret edge-control-master-previous -n "$NS" >/dev/null 2>&1 && fail "the old master is still there" || pass "the old master is gone"
kubectl get secret edge-control-sim-plate-cam-01-previous -n "$NS" >/dev/null 2>&1 && fail "the node's old key is still there" || pass "the node's old key is gone"
[ "$(for u in sim-plate-cam-01 sim-pos-01 rp-mqtt-kafka-bridge edge-operator; do fingerprint "mqtt-$u" password; done | tr '\n' ' ')" = "$logins_before" ] \
  && pass "no broker login was touched" || fail "a broker login changed during a control-key rotation"
pod_after="$(kubectl get pod -n "$NS" -l app=edge-sim-plate-waste -o jsonpath='{.items[0].metadata.name}')"
[ "$pod_after" != "$pod_before" ] && pass "the node was restarted onto the new key ($pod_before -> $pod_after)" || fail "the node pod was never replaced"
previous_env="$(kubectl get deploy edge-sim-plate-waste -n "$NS" -o jsonpath='{.spec.template.spec.containers[0].env[*].name}' | tr ' ' '\n' | grep -c '^EDGE_CONTROL_KEY_PREVIOUS$' || true)"
[ "$previous_env" = 0 ] && pass "the node no longer holds a previous key" || fail "the node still holds a previous key"
others="$(kubectl get deploy -n "$NS" -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.status.readyReplicas}{"\n"}{end}' | grep -c ' 1$' || true)"
[ "$others" -ge 5 ] && pass "the broker and all four simulators are still running" || fail "something is not running after the rotation ($others of 5 ready)"

if [ "$failures" != 0 ]; then
  echo "-- every status the node published during the rotation (time, request, state, reason):"
  kubectl logs status-recorder -n "$NS" 2>&1 | python3 -c '
import json, sys
for line in sys.stdin:
    when, _, body = line.partition(" ")
    try:
        s = json.loads(body)
        print(when, s.get("request_id"), s.get("state"), "-", s.get("reason"))
    except ValueError:
        print(line.rstrip())' | cut -c1-200
fi

echo
if [ "$failures" = 0 ]; then echo "the master-key rotation works on k3s: both masters during the window, only the new one after, nothing else touched."; else echo "$failures check(s) failed."; exit 1; fi
