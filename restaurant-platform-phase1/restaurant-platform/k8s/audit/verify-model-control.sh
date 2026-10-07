#!/usr/bin/env bash
# Proves, on the live cluster, that the plate-waste node takes a signed model command and refuses a wrongly signed one.
# It changes no model: the signed command it sends is for the version the node is ALREADY running, so a correct node answers
# "unchanged", and the command signed with the wrong key must be answered "rejected".
#
#   bash k8s/audit/verify-model-control.sh              # needs kubectl, python3 with paho-mqtt, and the operator's access to the Secrets
#   PYTHON=/path/to/venv/bin/python bash k8s/audit/verify-model-control.sh    # if the default python3 has no paho-mqtt
#
# What it does (namespace $NS, node $NODE):
#   1. checks the node's pod holds a control key (prints only its length)
#   2. opens a port-forward to the broker's TLS port (killed by its exact PID on exit) and logs in as `edge-operator`
#   3. reads the node's status (a retained message the node publishes when it connects)
#   4. sends a command signed with a WRONG key: the node must reply "rejected"
#   5. sends the correctly signed command for the version it runs: the node must reply "unchanged"
#   6. clears the retained command, so a restarting node is told nothing
#
# The login, the master secret and the broker's certificate are read from the Secrets into this script's environment and handed to
# the operator tool that way; none is printed and none is placed on a command line.
set -Eeuo pipefail

NS="${NS:-kafka}"
NODE="${NODE:-sim-plate-cam-01}"
DEPLOYMENT="${DEPLOYMENT:-edge-sim-plate-waste}"
LOCAL_PORT="${LOCAL_PORT:-18883}"
PYTHON="${PYTHON:-python3}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TOOLS="${EDGE_TOOLS:-$HERE/../../edge-simulators}"

work="$(mktemp -d)"
forward_pid=""
cleanup() {
  [ -z "$forward_pid" ] || kill "$forward_pid" 2>/dev/null || true
  rm -rf "$work"
}
trap cleanup EXIT
chmod 700 "$work"
umask 077

failures=0
pass() { echo "PASS  $*"; }
fail() { echo "FAIL  $*"; failures=$((failures + 1)); }
secret() { kubectl get secret "$1" -n "$NS" -o "jsonpath={.data.$2}" | base64 -d; }
operator() { (cd "$TOOLS" && "$PYTHON" -m control.edge_control "$@"); }
state() { operator status --wait 4 | sed -n "s/^$NODE: \([a-z]*\) - .*/\1/p"; }
reason() { operator status --wait 4 | sed -n "s/^$NODE: [a-z]* - \(.*\); running .*/\1/p"; }

# The operator tool needs paho-mqtt. Say so before anything is opened, rather than failing with a traceback after the port-forward.
if ! "$PYTHON" -c 'import paho.mqtt.client' >/dev/null 2>&1; then
  echo "FAIL  '$PYTHON' cannot import paho.mqtt. Install it (pip install paho-mqtt==2.1.0) or point PYTHON at an environment that has it:" >&2
  echo "      PYTHON=/path/to/venv/bin/python bash k8s/audit/verify-model-control.sh" >&2
  exit 2
fi

echo "== 1. does the node hold a key?"
length="$(kubectl exec -n "$NS" "deploy/$DEPLOYMENT" -- sh -c 'echo ${#EDGE_CONTROL_KEY}' 2>/dev/null || echo 0)"
if [ "${length:-0}" -ge 32 ]; then pass "the node holds a control key (length $length, not shown)"; else fail "the node has no EDGE_CONTROL_KEY: set controlKeySecret in k8s/edge-simulators/values.yaml and upgrade the chart"; exit 1; fi

echo "== 2. broker access"
kubectl get secret mosquitto-tls -n "$NS" -o 'jsonpath={.data.tls\.crt}' | base64 -d > "$work/ca.crt"
kubectl port-forward -n "$NS" svc/mosquitto "$LOCAL_PORT:8883" >"$work/forward.log" 2>&1 &
forward_pid=$!
for _ in $(seq 1 20); do grep -q "Forwarding from" "$work/forward.log" 2>/dev/null && break; sleep 0.5; done
grep -q "Forwarding from" "$work/forward.log" || { echo "the port-forward did not start"; cat "$work/forward.log"; exit 1; }
export MQTT_HOST=localhost MQTT_PORT="$LOCAL_PORT" MQTT_TLS_ENABLED=true MQTT_TLS_CA_FILE="$work/ca.crt"
MQTT_USERNAME="$(secret mqtt-edge-operator username)"; export MQTT_USERNAME
MQTT_PASSWORD="$(secret mqtt-edge-operator password)"; export MQTT_PASSWORD
EDGE_CONTROL_MASTER_KEY="$(secret edge-control-master key)"; export EDGE_CONTROL_MASTER_KEY
RIGHT_MASTER="$EDGE_CONTROL_MASTER_KEY"
pass "logged in to the broker as the operator over TLS"

echo "== 3. the node's status"
before="$(operator status --wait 4)"
echo "$before" | sed "s/^/    /"
running="$(echo "$before" | sed -n "s/^$NODE: .*running \([0-9.]*\) (.*/\1/p" | head -1)"
if [ -n "$running" ]; then pass "the node reported a status; it runs model $running"; else fail "the node has reported no status"; exit 1; fi

echo "== 4. a command signed with the wrong key must be rejected"
EDGE_CONTROL_MASTER_KEY="not-the-master-secret-$$" operator rollout --version "$running" --nodes "$NODE" >/dev/null
got="$(state)"
if [ "$got" = rejected ]; then pass "rejected: $(reason)"; else fail "a wrongly signed command was answered '$got', not 'rejected'"; fi

echo "== 5. the correctly signed command for the version it already runs must leave it unchanged"
EDGE_CONTROL_MASTER_KEY="$RIGHT_MASTER" operator rollout --version "$running" --nodes "$NODE" >/dev/null
got="$(state)"
if [ "$got" = unchanged ]; then pass "unchanged: $(reason)"; else fail "a correctly signed command was answered '$got', not 'unchanged': $(reason)"; fi

echo "== 6. clear the retained command"
operator clear --nodes "$NODE" >/dev/null && pass "cleared"

echo
if [ "$failures" = 0 ]; then echo "model control works on the live node: it refuses a wrong signature and accepts the right one."; else echo "$failures check(s) failed."; exit 1; fi
