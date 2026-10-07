#!/usr/bin/env bash
# Two things the live cluster cannot be asked to prove without risking it, proved on a throwaway copy of the broker in its own
# namespace, with credentials and a certificate made here (nothing is read from the real Secrets, nothing real is touched):
#
#   A. THE CUTOVER ORDER on real Kubernetes. The Mosquitto chart is installed with auth off: an anonymous client publishes. Then the
#      chart is upgraded with auth on (the step k8s/realign ends with): an anonymous client is refused, each login works, and a
#      retained message published as a sensor reaches the bridge's login (the ACL lets the one and the other through).
#   B. THE LOSS OF THE BROKER'S VOLUME on k3s (local-path). The claim is deleted with its pod: the new pod cannot start until the
#      chart is applied again, and then it comes up EMPTY (the retained message is gone), accepts logins, and has a new, bound volume.
#
#   bash k8s/audit/test-broker-in-scratch-namespace.sh          # about 3 minutes; deletes its namespace when it ends, however it ends
#
# Needs kubectl, helm, openssl, python3 and docker or podman (provision-mqtt-auth.sh builds the password file in the Mosquitto image).
# It refuses to run in any namespace but a scratch one. Exit 0 only if every check passed.
set -Eeuo pipefail

NS="${NS:-rp-broker-trial}"
case "$NS" in kafka|default|kube-*|restaurant-platform) echo "refusing to run in the namespace '$NS': this test deletes its namespace" >&2; exit 2 ;; esac
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$HERE/../.."
IMAGE="${MOSQUITTO_IMAGE:-eclipse-mosquitto:2}"
WAIT_SECONDS="${WAIT_SECONDS:-120}"

failures=0
pass() { echo "PASS  $*"; }
fail() { echo "FAIL  $*"; failures=$((failures + 1)); }

work="$(mktemp -d)"
chmod 700 "$work"
cleanup() {
  rm -rf "$work"
  echo "-- deleting namespace $NS"
  kubectl delete namespace "$NS" --wait=false >/dev/null 2>&1 || true
}
trap cleanup EXIT
umask 077

# A one-shot client pod. $1 name, $2 broker user (or "" for anonymous), $3 a shell command that may use $MQTT_USER and $MQTT_PW and
# the CA at /ca/tls.crt. Prints the pod's output; the function's status is the container's exit code. Credentials reach the pod
# from its Secret through the environment, never through an argument.
client() {
  local name="$1" user="$2" command="$3" env=""
  if [ -n "$user" ]; then
    env="    env:
    - {name: MQTT_USER, valueFrom: {secretKeyRef: {name: mqtt-$user, key: username}}}
    - {name: MQTT_PW, valueFrom: {secretKeyRef: {name: mqtt-$user, key: password}}}"
  fi
  kubectl delete pod "$name" -n "$NS" --ignore-not-found --wait=true >/dev/null 2>&1 || true
  kubectl apply -n "$NS" -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata: {name: $name}
spec:
  restartPolicy: Never
  containers:
  - name: c
    image: $IMAGE
    command: ["/bin/ash", "-c"]
    args:
    - |
      $command
$env
    volumeMounts: [{name: ca, mountPath: /ca}]
  volumes:
  - name: ca
    secret: {secretName: mosquitto-tls, items: [{key: tls.crt, path: tls.crt}]}
EOF
  local deadline=$((SECONDS + WAIT_SECONDS)) phase=""
  while [ "$SECONDS" -lt "$deadline" ]; do
    phase="$(kubectl get pod "$name" -n "$NS" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
    case "$phase" in Succeeded|Failed) break ;; esac
    sleep 2
  done
  kubectl logs "$name" -n "$NS" 2>&1 || true
  local code
  code="$(kubectl get pod "$name" -n "$NS" -o jsonpath='{.status.containerStatuses[0].state.terminated.exitCode}' 2>/dev/null || true)"
  [ -n "$code" ] || code=99
  return "$code"
}

TLS='--cafile /ca/tls.crt -h mosquitto -p 8883'
AUTH='-u "$MQTT_USER" -P "$MQTT_PW"'
broker_ready() { kubectl rollout status deploy/mosquitto -n "$NS" --timeout="${WAIT_SECONDS}s" >/dev/null 2>&1; }

echo "== setup: a scratch namespace, a certificate, and every credential (made here)"
kubectl create namespace "$NS" >/dev/null
openssl req -x509 -newkey rsa:2048 -nodes -days 2 -keyout "$work/tls.key" -out "$work/tls.crt" -subj "/CN=mosquitto" \
  -addext "subjectAltName=DNS:mosquitto,DNS:mosquitto.$NS.svc.cluster.local,DNS:localhost" >/dev/null 2>&1
kubectl create secret generic mosquitto-tls -n "$NS" "--from-file=tls.crt=$work/tls.crt" "--from-file=tls.key=$work/tls.key" >/dev/null
NS="$NS" bash "$ROOT/k8s/mosquitto/provision-mqtt-auth.sh" >/dev/null
pass "the namespace, the certificate and the credentials exist"

echo "== A1. the chart with auth OFF (the first step of the cutover): an anonymous client is let in"
helm install mosquitto "$ROOT/k8s/mosquitto" -n "$NS" --set auth.enabled=false --wait --timeout 3m >/dev/null
out="$(client anon-open "" "mosquitto_pub $TLS -t trial/open -m hello -q 1 -d" || true)"
if echo "$out" | grep -q "PUBACK"; then pass "an anonymous publish was acknowledged while auth is off"; else fail "an anonymous publish was not acknowledged with auth off: $out"; fi

echo "== A2. the chart with auth ON (the last step of the cutover): anonymous refused, logins work, the ACL does its job"
helm upgrade mosquitto "$ROOT/k8s/mosquitto" -n "$NS" --set auth.enabled=true --wait --timeout 3m >/dev/null
broker_ready || fail "the broker did not roll out with auth on"
out="$(client anon-closed "" "mosquitto_pub $TLS -t trial/closed -m hello -q 1 -d" || true)"
if echo "$out" | grep -qi "not authori"; then pass "an anonymous client is refused"; else fail "an anonymous client was not refused: $out"; fi
if client sensor-login sim-pos-01 "mosquitto_pub $TLS $AUTH -t sensors/pos-transaction -m from-the-sensor -r -q 1 -d" >/dev/null; then pass "the sensor logs in and publishes its own topic (retained)"; else fail "the sensor could not log in and publish"; fi
out="$(client bridge-reads rp-mqtt-kafka-bridge "mosquitto_sub $TLS $AUTH -t sensors/pos-transaction -C 1 -W 10" || true)"
if echo "$out" | grep -q "from-the-sensor"; then pass "the bridge's login receives what the sensor published"; else fail "the bridge did not receive the sensor's message: $out"; fi
out="$(client sensor-forges sim-pos-01 "mosquitto_pub $TLS $AUTH -t sensors/plate-waste -m forged -r -q 1 -d" || true)"
echo "$out" | grep -q "PUBACK" || fail "the forging sensor's publish was not even acknowledged, so the next check would show nothing"
out="$(client bridge-reads-plate rp-mqtt-kafka-bridge "mosquitto_sub $TLS $AUTH -t sensors/plate-waste -C 1 -W 5; echo rc=\$?" || true)"
# rc=27 is mosquitto_sub's own time-out: it connected, subscribed and was sent nothing. (The same login received a message on the other topic above.)
if echo "$out" | grep -q "rc=27" && ! echo "$out" | grep -q "forged"; then pass "a sensor's publish to another sensor's topic reaches nobody"; else fail "a forged publish was delivered, or the reader never connected: $out"; fi

echo "== B. the loss of the broker's volume"
pvc_before="$(kubectl get pvc mosquitto-data -n "$NS" -o jsonpath='{.metadata.uid}')"
kubectl delete pvc mosquitto-data -n "$NS" --wait=false >/dev/null
kubectl delete pod -n "$NS" -l app=mosquitto --wait=true >/dev/null
sleep 5
pending="$(kubectl get pods -n "$NS" -l app=mosquitto --no-headers 2>/dev/null | grep -c Pending || true)"
if [ "$pending" -ge 1 ]; then pass "with its volume gone the new broker pod cannot start (Pending), it does not start empty by itself"; else fail "the broker pod started although its volume was deleted"; fi
helm upgrade mosquitto "$ROOT/k8s/mosquitto" -n "$NS" --set auth.enabled=true --wait --timeout 3m >/dev/null || true
broker_ready && pass "applying the chart again recreated the volume and the broker is running" || fail "the broker did not come back after the chart was applied again"
pvc_after="$(kubectl get pvc mosquitto-data -n "$NS" -o jsonpath='{.metadata.uid}' 2>/dev/null || true)"
if [ -n "$pvc_after" ] && [ "$pvc_after" != "$pvc_before" ]; then pass "the volume claim is a new one and is bound ($(kubectl get pvc mosquitto-data -n "$NS" -o jsonpath='{.status.phase}'))"; else fail "the volume claim was not recreated"; fi
out="$(client bridge-reads-again rp-mqtt-kafka-bridge "mosquitto_sub $TLS $AUTH -t sensors/pos-transaction -C 1 -W 5; echo rc=\$?" || true)"
# The same login, on the same topic, received the retained message before the loss: that is the control for this check.
if echo "$out" | grep -q "rc=27" && ! echo "$out" | grep -q "from-the-sensor"; then pass "the new broker is empty: the retained message is gone with the volume"; else fail "the retained message survived the loss of the volume, or the reader never connected: $out"; fi
if client sensor-again sim-pos-01 "mosquitto_pub $TLS $AUTH -t sensors/pos-transaction -m after-the-loss -q 1 -d" >/dev/null; then pass "logins still work on the new broker"; else fail "a login failed on the new broker"; fi

echo
if [ "$failures" = 0 ]; then echo "the cutover order works on k3s, and the loss of the broker's volume is recovered by applying the chart again (the broker's state is gone)."; else echo "$failures check(s) failed."; exit 1; fi
