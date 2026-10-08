#!/usr/bin/env bash
# Rotates the operator's MASTER control secret on a cluster, and with it the key of the node the operator commands, in the one order
# that never leaves the node unable to take a command, and shows at each step that it did what it should. A leaked master is only
# contained when the old key stops working; this ends there, and proves it.
#
#   bash k8s/audit/rotate-master-key.sh                    # asks you to type "rotate" first; NS=kafka, node sim-plate-cam-01
#   NS=other-namespace ROTATE_CONFIRM=yes bash k8s/audit/rotate-master-key.sh
#
# What it does (the steps k8s/mosquitto/provision-mqtt-auth.sh describes, put in order and checked):
#   0. preflight: the Secrets exist, no rotation is already in progress, the node is running, and the CURRENT master works
#   1. provision-mqtt-auth.sh rotate-master: a new master; each node's new key derived from it; the old master and the old key kept
#      in edge-control-master-previous / edge-control-<node>-previous
#   2. OPEN THE WINDOW: helm upgrade edge-simulators with controlKeyPreviousByNode.<node>, so the node holds its new key AND its old one
#   3. the node must answer a command signed under the NEW master and one signed under the OLD master (both "unchanged")
#   4. CLOSE THE WINDOW: helm upgrade again without the previous key, so the node restarts holding only the new one
#   5. the node must answer the NEW master ("unchanged") and REFUSE the OLD one ("rejected", bad signature)
#   6. provision-mqtt-auth.sh finish-rotation deletes every old key; the retained command is cleared
# Every check is a short-lived pod that gets the operator's login and the master through the environment from the Secrets, so no
# credential is printed or put on a command line. It stops at the first failed check and says where; until step 4 the node holds both
# keys, so stopping is safe (finish by hand with the steps above, or undo with: helm upgrade ... --set controlKeyPreviousByNode.<node>=
# and the old Secrets still in place). Exit 0 only if every check passed.
set -Eeuo pipefail

NS="${NS:-kafka}"
NODE="${NODE:-sim-plate-cam-01}"
DEPLOYMENT="${DEPLOYMENT:-edge-sim-plate-waste}"
RELEASE="${RELEASE:-edge-simulators}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$HERE/../.."
CHART="${CHART:-$ROOT/k8s/edge-simulators}"
PROVISION="${PROVISION_SCRIPT:-$ROOT/k8s/mosquitto/provision-mqtt-auth.sh}"
WAIT_SECONDS="${WAIT_SECONDS:-120}"
CHECK_SETTLE="${CHECK_SETTLE:-4}"
HELM_EXTRA=(${HELM_EXTRA_ARGS:-})  # e.g. extra --set flags a particular cluster needs

step=0
failures=0
pass() { echo "PASS  $*"; }
fail() { echo "FAIL  $*"; failures=$((failures + 1)); }
stop() {
  echo
  echo "STOPPED at step $step: $*"
  case "$step" in
    0) echo "nothing was changed." ;;
    1|2|3) echo "the node holds BOTH keys (or still only the old one if step 1 did not finish): nothing is broken; the old master is in edge-control-master-previous." ;;
    4|5) echo "the node was restarted holding only the new key; the old master is still in edge-control-master-previous until finish-rotation." ;;
    *) echo "the rotation is complete except for the clean-up above." ;;
  esac
  exit 1
}
trap 'echo; echo "interrupted at step $step"; exit 130' INT TERM

exists() { kubectl get secret "$1" -n "$NS" >/dev/null 2>&1; }

# One short-lived pod acting as the operator, holding the master from the Secret `$2` and running the shell script `$3` (placeholders
# __NODE__ and __SETTLE__ are filled in). $1 is a label for the pod's name. Prints the last lines of what the pod printed.
operator_pod() {
  local label="$1" master_secret="$2" script="$3" pod image
  pod="rotation-$label"
  script="${script//__NODE__/$NODE}"
  script="${script//__SETTLE__/$CHECK_SETTLE}"
  image="$(kubectl get "deploy/$DEPLOYMENT" -n "$NS" -o jsonpath='{.spec.template.spec.containers[0].image}')"
  kubectl delete pod "$pod" -n "$NS" --ignore-not-found --wait=true >/dev/null 2>&1 || true
  kubectl apply -n "$NS" -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata: {name: $pod}
spec:
  restartPolicy: Never
  containers:
  - name: operator
    image: $image
    imagePullPolicy: IfNotPresent
    workingDir: /app
    command: ["/bin/sh", "-c"]
    args:
    - |
$(printf '%s\n' "$script" | sed 's/^/      /')
    env:
    - {name: MQTT_HOST, value: mosquitto}
    - {name: MQTT_PORT, value: "8883"}
    - {name: MQTT_TLS_ENABLED, value: "true"}
    - {name: MQTT_TLS_CA_FILE, value: /ca/tls.crt}
    - {name: MQTT_USERNAME, valueFrom: {secretKeyRef: {name: mqtt-edge-operator, key: username}}}
    - {name: MQTT_PASSWORD, valueFrom: {secretKeyRef: {name: mqtt-edge-operator, key: password}}}
    - {name: EDGE_CONTROL_MASTER_KEY, valueFrom: {secretKeyRef: {name: $master_secret, key: key}}}
    - {name: EDGE_CONTROL_GENERATION, valueFrom: {secretKeyRef: {name: edge-control-$NODE-generation, key: generation, optional: true}}}
    volumeMounts: [{name: ca, mountPath: /ca}]
  volumes:
  - name: ca
    secret: {secretName: mosquitto-tls, items: [{key: tls.crt, path: tls.crt}]}
EOF
  local deadline=$((SECONDS + WAIT_SECONDS)) phase=""
  while [ "$SECONDS" -lt "$deadline" ]; do
    phase="$(kubectl get pod "$pod" -n "$NS" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
    case "$phase" in Succeeded|Failed) break ;; esac
    sleep 2
  done
  kubectl logs "$pod" -n "$NS" 2>&1 | tail -3
  kubectl delete pod "$pod" -n "$NS" --wait=false >/dev/null 2>&1 || true
}

# Commands the node, as the holder of the master in Secret `$2`, with the version it already runs; prints the node's own answer line.
CHECK_SCRIPT='set -e
version=$(python -m control.edge_control status --wait 5 | sed -n "s/^__NODE__: .*running \([0-9.]*\) (.*/\1/p" | head -1)
[ -n "$version" ] || { echo "no status from __NODE__"; exit 3; }
python -m control.edge_control rollout --version "$version" --nodes __NODE__ >/dev/null
sleep __SETTLE__
python -m control.edge_control status --wait 5 | grep "^__NODE__:"'
operator_check() { operator_pod "$1" "$2" "$CHECK_SCRIPT"; }
answer_state() { sed -n "s/^$NODE: \([a-z]*\) - .*/\1/p" | head -1; }

# Returns 0 if the node's answer to a command signed under the master in Secret `$2` has the state `$3` (and, for "rejected", names a bad signature).
expect() {
  local label="$1" master_secret="$2" wanted="$3" out state
  out="$(operator_check "$label" "$master_secret")"
  state="$(echo "$out" | answer_state)"
  if [ "$state" = "$wanted" ] && { [ "$wanted" != rejected ] || echo "$out" | grep -q "bad signature"; }; then
    pass "$label: the node answered '$state'$(echo "$out" | sed -n 's/^[^-]*- \([^;]*\);.*/: \1/p' | head -1)"
    return 0
  fi
  fail "$label: wanted '$wanted', got '${state:-no answer}' ($(echo "$out" | tail -1))"
  return 1
}

# Waits until the node has exactly ONE pod and none of them is terminating. `rollout status` is not enough: a rolling update starts the new
# pod without waiting for the old one to finish terminating, and both connect to the broker as the same client id, which takes the
# other's session over, so for as long as they overlap a command is answered by whichever is connected, the old one (holding the old
# key) included. A command sent during that overlap says nothing about either key.
wait_for_one_pod() {
  local deadline=$((SECONDS + WAIT_SECONDS)) pods
  while [ "$SECONDS" -lt "$deadline" ]; do
    pods="$(kubectl get pods -n "$NS" -l "app=$DEPLOYMENT" -o jsonpath='{range .items[*]}{.metadata.deletionTimestamp}{"|"}{.status.phase}{"\n"}{end}' 2>/dev/null || true)"
    [ "$(echo "$pods" | grep -c .)" = 1 ] && [ "$pods" = "|Running" ] && return 0
    sleep 2
  done
  return 1
}

helm_node_previous() {  # $1 = Secret name holding the previous key, or empty to clear
  helm upgrade "$RELEASE" "$CHART" -n "$NS" --reset-then-reuse-values --set "controlKeyPreviousByNode.$NODE=$1" "${HELM_EXTRA[@]}" --wait --timeout 5m >/dev/null
  kubectl rollout status "deploy/$DEPLOYMENT" -n "$NS" --timeout="${WAIT_SECONDS}s" >/dev/null
  wait_for_one_pod
  sleep "$CHECK_SETTLE"  # and the node's client has had a moment to connect and take its retained command
}

echo "== 0. preflight (namespace $NS, node $NODE)"
step=0
for secret in edge-control-master "edge-control-$NODE" mqtt-edge-operator mosquitto-tls; do
  exists "$secret" || stop "the Secret $secret does not exist (run k8s/mosquitto/provision-mqtt-auth.sh first)"
done
if exists edge-control-master-previous || exists "edge-control-$NODE-previous"; then
  stop "a rotation is already in progress (edge-control-master-previous or edge-control-$NODE-previous exists): finish it ('bash $PROVISION finish-rotation' after clearing controlKeyPreviousByNode) before starting another"
fi
kubectl rollout status "deploy/$DEPLOYMENT" -n "$NS" --timeout=30s >/dev/null 2>&1 || stop "deploy/$DEPLOYMENT is not running"
pass "the Secrets exist, no rotation is in progress, and the node is running"
expect "before" edge-control-master unchanged || stop "the CURRENT master does not work, so a rotation would start from a broken state"

if [ "${ROTATE_CONFIRM:-}" != "yes" ]; then
  echo
  echo "This will change the Secrets edge-control-master and edge-control-$NODE in namespace '$NS', upgrade the $RELEASE release twice"
  echo "(restarting $DEPLOYMENT each time), and delete the old keys at the end."
  [ -t 0 ] || { echo "no terminal to ask on: set ROTATE_CONFIRM=yes to run without asking" >&2; exit 2; }
  read -r -p 'Type "rotate" to go on: ' answer
  [ "$answer" = rotate ] || { echo "not confirmed; nothing was changed"; exit 2; }
fi

echo "== 1. a new master, and the node's new key derived from it"
step=1
NS="$NS" bash "$PROVISION" rotate-master >/dev/null || stop "rotate-master failed"
pass "the new master and key are in the Secrets; the old ones are kept as -previous"

echo "== 2. open the window: the node holds its new key and its old one"
step=2
helm_node_previous "edge-control-$NODE-previous" || stop "the chart upgrade that opens the window failed"
pass "$DEPLOYMENT restarted holding both keys"

echo "== 3. the window: both masters must work"
step=3
expect "window-new-master" edge-control-master unchanged || stop "the node does not accept the NEW master"
expect "window-old-master" edge-control-master-previous unchanged || stop "the node does not accept the OLD master during the window"

echo "== 4. close the window: the node restarts holding only the new key"
step=4
helm_node_previous "" || stop "the chart upgrade that closes the window failed"
pass "$DEPLOYMENT restarted holding only its new key"

echo "== 5. after the window: the new master works and the old one is refused"
step=5
expect "after-new-master" edge-control-master unchanged || stop "the node does not accept the new master after the window closed"
expect "after-old-master" edge-control-master-previous rejected || stop "the node STILL ACCEPTS THE OLD MASTER: do not delete the old keys; the node was not restarted without its previous key"

echo "== 6. delete the old keys, and clear the command left retained"
step=6
NS="$NS" bash "$PROVISION" finish-rotation >/dev/null || stop "finish-rotation failed"
operator_pod "clear" edge-control-master 'python -m control.edge_control clear --nodes __NODE__' >/dev/null || true
if exists edge-control-master-previous || exists "edge-control-$NODE-previous"; then fail "an old key is still in the cluster"; else pass "no old key is left in the cluster"; fi

echo
if [ "$failures" = 0 ]; then
  echo "the master secret was rotated: the node obeyed both masters during the window, refuses the old one now, and the old keys are gone."
else
  echo "$failures check(s) failed."
  exit 1
fi
