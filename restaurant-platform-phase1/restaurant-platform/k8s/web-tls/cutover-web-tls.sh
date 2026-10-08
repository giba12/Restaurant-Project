#!/usr/bin/env bash
# Turns on TLS for the dashboard and the game bridge -- the last plaintext
# piece of the project's whole TLS effort (see k8s/kafka-tls/ and
# k8s/mosquitto-tls/ for the rest). Deliberately NOT the same
# additive-listener-plus-migrate-one-consumer-at-a-time shape as those two:
# both services here have exactly one real consumer each (a developer's own
# browser tab, or their own Godot client), reached via `kubectl port-forward`,
# not nine independent production services with constant background
# traffic. A direct, verified switch is proportionate; the elaborate
# rollback machinery those two needed would be complexity this doesn't.
#
# dashboard-web (nginx) keeps port 80 alongside the new 443 anyway, since
# nginx can serve both from one server block for free. game-bridge (uvicorn)
# cannot -- a single uvicorn process serves either plain HTTP or TLS, not
# both -- so its switch is a straight cutover, with `rollback` to go back.
#
# Usage (run from the repo root):
#   bash k8s/web-tls/cutover-web-tls.sh status
#   bash k8s/web-tls/cutover-web-tls.sh dashboard
#   bash k8s/web-tls/cutover-web-tls.sh game-bridge
#   bash k8s/web-tls/cutover-web-tls.sh rollback dashboard
#   bash k8s/web-tls/cutover-web-tls.sh rollback game-bridge
# Add --dry-run after any subcommand to see the commands without running them.
#
# The Godot client trusts these certs properly (TLSOptions.client(cert)),
# not a blanket bypass -- proven live against a local test HTTPS server
# before writing a line of game/client/scripts/bridge_client.gd: Godot 4.5's
# HTTPRequest rejects a self-signed cert by default (same as a browser) and
# accepts it once pinned this way. See game/README.md for how to fetch each
# cert to a local path and point BRIDGE_TLS_CERT_PATH / DASHBOARD_TLS_CERT_PATH
# at it.
set -euo pipefail

NS=kafka
run() { echo "+ $*"; [ "$DRY" = 1 ] || "$@"; }

[ -f k8s/web-tls/cutover-web-tls.sh ] || { echo "run this from the repo root" >&2; exit 1; }

DRY=0
for a in "$@"; do [ "$a" = "--dry-run" ] && DRY=1; done

# Generates a self-signed cert for one service and stores it as a Secret.
# Safe to re-run: kubectl apply on the same Secret name just replaces it,
# and a fresh cert generated on every `dashboard`/`game-bridge` invocation
# (rather than reusing an old one) means there's never a stale cert lying
# around from an earlier, abandoned attempt.
gen_cert() {
  local name="$1" secret="$2"
  echo "== generating $name's self-signed TLS cert =="
  if [ "$DRY" = 1 ]; then
    echo "+ openssl req -x509 -newkey rsa:2048 ... -> kubectl create secret generic $secret"
    return
  fi
  local dir; dir=$(mktemp -d)
  openssl req -x509 -newkey rsa:2048 -nodes -days 3650 \
    -keyout "$dir/tls.key" -out "$dir/tls.crt" \
    -subj "/CN=$name.$NS.svc.cluster.local" \
    -addext "subjectAltName=DNS:$name.$NS.svc.cluster.local,DNS:$name,DNS:localhost"
  kubectl create secret generic "$secret" -n "$NS" \
    --from-file=tls.crt="$dir/tls.crt" --from-file=tls.key="$dir/tls.key" \
    --dry-run=client -o yaml | kubectl apply -f -
  rm -rf "$dir"
}

migrate_dashboard() {
  local tls_on="$1"
  echo "== $([ "$tls_on" = true ] && echo "cutting over" || echo "rolling back") dashboard-web (web.tls.enabled=$tls_on) =="
  [ "$tls_on" = "true" ] && gen_cert dashboard-web dashboard-web-tls
  # --reset-then-reuse-values -- this release predates web.tls existing in
  # its values.yaml; see the identical note in k8s/kafka-tls/cutover-tls.sh.
  run helm upgrade --install dashboard k8s/dashboard -n "$NS" --reset-then-reuse-values \
    --set web.tls.enabled="$tls_on" --wait --timeout 120s
  run kubectl rollout status deploy/dashboard-web -n "$NS" --timeout=90s
  if [ "$DRY" = 0 ] && [ "$tls_on" = "true" ]; then
    echo "== verifying: fetch over https from inside the pod =="
    local pod; pod=$(kubectl get pods -n "$NS" -l app=dashboard-web --no-headers -o custom-columns=":metadata.name" | head -1)
    # 127.0.0.1, not localhost: confirmed live that this image's /etc/hosts
    # resolves "localhost" to ::1 first, nginx has no IPv6 listener (only
    # 0.0.0.0 -- the stock image's IPv6-listener entrypoint script only
    # patches its own default config, which this chart's custom one isn't),
    # and wget doesn't fall back to the IPv4 address also in /etc/hosts --
    # a "Connection refused" from that combination looks exactly like a
    # broken TLS listener but isn't one; /proc/net/tcp showing 0.0.0.0:443
    # bound is what actually proves nginx is listening. A 401 here is
    # success, not failure: it proves the TLS handshake completed and
    # nginx's own Basic Auth correctly rejected a request sent with no
    # credentials -- grep for any HTTP response at all, not a 2xx.
    kubectl exec -n "$NS" "$pod" -- sh -c 'wget -qO- -S --no-check-certificate https://127.0.0.1/ -T 5' 2>&1 \
      | grep -q "HTTP/" && echo "https: reachable (TLS + auth_basic both responded)"
  fi
}

migrate_bridge() {
  local tls_on="$1"
  echo "== $([ "$tls_on" = true ] && echo "cutting over" || echo "rolling back") game-bridge (tls.enabled=$tls_on) =="
  [ "$tls_on" = "true" ] && gen_cert game-bridge game-bridge-tls
  run helm upgrade game-bridge game/k8s/bridge -n "$NS" --reset-then-reuse-values \
    --set tls.enabled="$tls_on" --wait --timeout 120s
  run kubectl rollout status deploy/game-bridge -n "$NS" --timeout=90s
  if [ "$DRY" = 0 ]; then
    echo "== waiting 10s, then checking the pod is actually up =="
    sleep 10
    kubectl get pods -n "$NS" -l app=game-bridge
    local pod; pod=$(kubectl get pods -n "$NS" -l app=game-bridge --no-headers -o custom-columns=":metadata.name" | head -1)
    kubectl logs -n "$NS" "$pod" --tail=10
  fi
}

status() {
  local dash_443
  dash_443=$(kubectl get deploy dashboard-web -n "$NS" -o jsonpath='{.spec.template.spec.containers[0].ports[?(@.containerPort==443)]}' 2>/dev/null || true)
  printf '%-16s %s\n' "dashboard" "$([ -n "$dash_443" ] && echo TLS || echo PLAINTEXT)"
  local bridge_ssl
  bridge_ssl=$(kubectl get deploy game-bridge -n "$NS" -o jsonpath='{.spec.template.spec.containers[0].command}' 2>/dev/null || true)
  printf '%-16s %s\n' "game-bridge" "$(echo "$bridge_ssl" | grep -q ssl-keyfile && echo TLS || echo PLAINTEXT)"
}

cmd="${1:-}"
case "$cmd" in
  list)
    echo "dashboard"
    echo "game-bridge"
    ;;
  status)
    status
    ;;
  dashboard)
    migrate_dashboard true
    ;;
  game-bridge)
    migrate_bridge true
    ;;
  rollback)
    svc="${2:-}"
    case "$svc" in
      dashboard) migrate_dashboard false ;;
      game-bridge) migrate_bridge false ;;
      *) echo "usage: $0 rollback dashboard|game-bridge" >&2; exit 1 ;;
    esac
    ;;
  ""|-h|--help)
    grep '^#' "$0" | sed -n '2,23p' | sed 's/^# \{0,1\}//'
    ;;
  *)
    echo "unknown service: $cmd (see: $0 list)" >&2
    exit 1
    ;;
esac
