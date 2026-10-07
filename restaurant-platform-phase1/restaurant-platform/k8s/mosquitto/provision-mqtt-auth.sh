#!/usr/bin/env bash
# Creates the credentials the MQTT broker's login needs, as Kubernetes Secrets. Safe to re-run: a credential that already
# exists is kept, and the broker's password file is rebuilt from the Secrets, never from anything in this repository.
#
#   bash k8s/mosquitto/provision-mqtt-auth.sh                       # create what is missing
#   bash k8s/mosquitto/provision-mqtt-auth.sh --dry-run             # say what it would do, touch nothing
#   bash k8s/mosquitto/provision-mqtt-auth.sh rotate-node NODE      # a new key generation for one node (see below)
#
# What it makes (namespace $NS, default kafka); it never prints a password or a key:
#   mqtt-<user>               one per broker user: keys `username`, `password`. The simulators and the bridge read their
#                             own (k8s/edge-simulators and k8s/mqtt-kafka-bridge reference them); `mqtt-edge-operator` is
#                             the operator's login for control/edge_control.py.
#   mosquitto-auth            key `passwd`: Mosquitto's HASHED password file, built from the mqtt-<user> Secrets with
#                             mosquitto_passwd (run in the Mosquitto image, so docker or podman is needed here).
#   edge-control-master       key `key`: the operator's MASTER secret. Only the operator needs it; no node does.
#   edge-control-<node>       key `key`: that node's OWN control key, derived from the master (HMAC-SHA256 of
#                             "edge-control/v1/<node>/<generation>", the same derivation as `edge_control derive-key`), so a key
#                             taken from one node cannot command another. Naming it in
#                             k8s/edge-simulators/values.yaml (controlKeySecret) is what turns model control on for the node.
#
# ROTATING a node's key (rotate-node NODE): the node's current key moves to edge-control-<node>-previous and a new generation
# is written to edge-control-<node>. Then, in this order: (1) set controlKeyPreviousSecret on the node in values.yaml and
# upgrade the chart, so the node holds both keys; (2) command it with the new generation
# (`edge_control rollout ... --generation N`, or set EDGE_CONTROL_GENERATION); (3) remove controlKeyPreviousSecret and upgrade
# again, so the old key is no longer accepted. The generation lives in the Secret `edge-control-<node>-generation`.
#
# Order matters on a live cluster: clients must carry credentials BEFORE the broker starts requiring them. k8s/realign does it
# in that order (broker with auth off, secrets, clients, broker with auth on); do not `helm upgrade mosquitto` with the default
# values on a cluster whose clients have no credentials yet.
set -Eeuo pipefail

NS="${NS:-kafka}"
ENGINE="${CONTAINER_ENGINE:-docker}"
IMAGE="${MOSQUITTO_IMAGE:-docker.io/library/eclipse-mosquitto:2}"
USERS=(sim-plate-cam-01 sim-pos-01 sim-ticket-timer-01 sim-staffing-sensor-01 rp-mqtt-kafka-bridge edge-operator)
NODES=(sim-plate-cam-01)
DRY=0
ACTION=provision
NODE=""

for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY=1 ;;
    rotate-node) ACTION=rotate ;;
    -*) echo "unknown option: $arg" >&2; exit 2 ;;
    *) NODE="$arg" ;;
  esac
done
if [ "$ACTION" = rotate ] && [ -z "$NODE" ]; then echo "usage: $0 rotate-node NODE" >&2; exit 2; fi

say() { echo "$*"; }
exists() { kubectl get secret "$1" -n "$NS" >/dev/null 2>&1; }
field() { kubectl get secret "$1" -n "$NS" -o "jsonpath={.data.$2}" | base64 -d; }
random() { openssl rand -hex "$1"; }

# Secrets never go on a command line (anyone on this machine can read those): values are written to files in a private
# directory, handed to kubectl by file name, and the directory is removed on exit.
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
chmod 700 "$work"
umask 077

# The same derivation as control/edge_control.py derive_node_key (a test compares them). The master goes in through the
# environment of the child process, not its arguments.
derive() { MASTER="$1" python3 -c 'import hashlib, hmac, os, sys; print(hmac.new(os.environ["MASTER"].encode(), sys.argv[1].encode(), hashlib.sha256).hexdigest())' "edge-control/v1/$2/$3"; }

secret_args() {  # key value [key value ...] -> file arguments for kubectl, in the array `secret_file_args`
  secret_file_args=()
  while [ "$#" -gt 0 ]; do
    printf '%s' "$2" > "$work/$1"
    secret_file_args+=("--from-file=$1=$work/$1")
    shift 2
  done
}

make_secret() {  # name key value [key value ...]
  local name="$1"; shift
  if [ "$DRY" = 1 ]; then say "+ would create secret $name"; return; fi
  secret_args "$@"
  kubectl create secret generic "$name" -n "$NS" "${secret_file_args[@]}" >/dev/null
  say "created secret $name"
}

replace_secret() {  # name key value: create or replace
  local name="$1"; shift
  if [ "$DRY" = 1 ]; then say "+ would replace secret $name"; return; fi
  secret_args "$@"
  kubectl create secret generic "$name" -n "$NS" "${secret_file_args[@]}" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  say "wrote secret $name"
}

if [ "$ACTION" = rotate ]; then
  exists edge-control-master || { echo "no edge-control-master secret: run this script without arguments first" >&2; exit 1; }
  master="$(field edge-control-master key)"
  generation=1
  exists "edge-control-$NODE-generation" && generation="$(field "edge-control-$NODE-generation" generation)"
  next=$((generation + 1))
  if exists "edge-control-$NODE" && [ "$DRY" = 0 ]; then
    replace_secret "edge-control-$NODE-previous" key "$(field "edge-control-$NODE" key)"
  fi
  replace_secret "edge-control-$NODE" key "$(derive "$master" "$NODE" "$next")"
  replace_secret "edge-control-$NODE-generation" generation "$next"
  say "node $NODE is now at key generation $next; its previous key is in edge-control-$NODE-previous."
  say "next: set controlKeyPreviousSecret for $NODE, upgrade k8s/edge-simulators, then command it with --generation $next, then remove it (see the header)."
  exit 0
fi

say "== broker users"
for user in "${USERS[@]}"; do
  if exists "mqtt-$user"; then say "mqtt-$user exists"; else make_secret "mqtt-$user" username "$user" password "$(random 24)"; fi
done

say "== control keys"
if exists edge-control-master; then say "edge-control-master exists"; else make_secret edge-control-master key "$(random 32)"; fi
master=""
if [ "$DRY" = 0 ]; then master="$(field edge-control-master key)"; fi
for node in "${NODES[@]}"; do
  if exists "edge-control-$node"; then
    say "edge-control-$node exists"
  else
    make_secret "edge-control-$node" key "$(if [ "$DRY" = 1 ]; then echo dry-run; else derive "$master" "$node" 1; fi)"
    [ "$DRY" = 1 ] || replace_secret "edge-control-$node-generation" generation 1
  fi
done

say "== the broker's password file"
if [ "$DRY" = 1 ]; then
  say "+ would build mosquitto-auth from the mqtt-<user> secrets with mosquitto_passwd in $IMAGE"
else
  # "user password" lines on standard input, so no password appears in this machine's process list.
  for user in "${USERS[@]}"; do printf '%s %s\n' "$user" "$(field "mqtt-$user" password)"; done |
    "$ENGINE" run --rm -i -v "$work:/w" --entrypoint /bin/ash "$IMAGE" -c \
      ': > /w/passwd; while read -r user password; do mosquitto_passwd -b /w/passwd "$user" "$password" >/dev/null; done; chmod 0644 /w/passwd'
  kubectl create secret generic mosquitto-auth -n "$NS" "--from-file=passwd=$work/passwd" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  say "wrote secret mosquitto-auth"
fi

say "done. Nothing above printed a credential."
say "next: enable model control for a node by setting controlKeySecret: edge-control-<node> in k8s/edge-simulators/values.yaml; the broker cutover is k8s/realign."
