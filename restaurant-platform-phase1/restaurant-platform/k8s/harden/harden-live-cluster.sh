#!/usr/bin/env bash
# Rotates every dev-only credential this project has ever printed in plain
# text (TimescaleDB, the narrator's restricted role, MinIO's root user),
# bootstraps pgBackRest backups of TimescaleDB into the existing MinIO
# deployment, turns on the dashboard/game-bridge API-key + HTTP Basic
# Auth added in services/dashboard-api/main.py, game/bridge/main.py and
# k8s/dashboard's nginx config, and rotates two more k8s/observability
# values: alert-relay's ntfy.sh topic name (not a credential exactly, but
# the same "don't leave the guessable placeholder in place" idea -- ntfy's
# free tier has no access control on a topic, so anyone who knows the name
# can read every alert sent to it) and Grafana's admin password (a real
# credential that was simply missed when everything else here was first
# rotated). Run from the repo root:
#
#   bash k8s/harden/harden-live-cluster.sh --dry-run   # see every step first
#   bash k8s/harden/harden-live-cluster.sh             # do it
#
# Why one script for three different concerns: all three need the same
# things this shell cannot do on its own (mutating a live k3s cluster,
# writing to a secret store), so bundling them means running this once
# instead of stitching several scripts together in the right order by hand.
# Each section below is independently understandable; nothing here is
# secretly entangled with the others except where a comment says so.
#
# Needs: sudo is NOT required (no image import here -- see
# k8s/realign/realign-live-cluster.sh and game/k8s/deploy-bridge.sh for the
# two scripts that do need it). This DOES need kubectl and helm write access
# to the live cluster, and openssl. Under rootless Podman on WSL2:
#   export DOCKER_HOST=unix:///run/user/$(id -u)/podman/podman.sock
#
# Idempotent where it can be: re-running rotates credentials again (a
# feature, not a bug -- rotation should be something you can do routinely,
# not a one-time event) and every helm upgrade / mc mb / stanza-create is
# already safe to repeat. It is NOT safe to run two copies at once.
set -euo pipefail

NS=kafka
DRY=0
[ "${1:-}" = "--dry-run" ] && DRY=1

run() {
  echo "+ $*"
  [ "$DRY" = 1 ] || "$@"
}

[ -f k8s/harden/harden-live-cluster.sh ] || { echo "run this from the repo root" >&2; exit 1; }

SECRETS_DIR=k8s/secrets
mkdir -p "$SECRETS_DIR"

if [ "$DRY" = 0 ]; then
  kubectl get ns "$NS" >/dev/null
  docker version >/dev/null
  command -v openssl >/dev/null
fi

rand() { openssl rand -base64 24 | tr -d '=+/\n'; }  # shell/URL-safe, ~32 chars

echo "== 1. generate credentials (nothing here is printed or committed) =="
if [ "$DRY" = 1 ]; then
  echo "+ generate: TimescaleDB restaurant_app + narrator_app passwords, MinIO root password,"
  echo "  a pgbackrest-scoped MinIO user+secret, dashboard-api/game-bridge API keys, an"
  echo "  nginx Basic Auth user+password, a random ntfy.sh alert topic name, and a Grafana"
  echo "  admin password"
else
  TS_PW=$(rand); NARRATOR_PW=$(rand); MINIO_ROOT_PW=$(rand)
  PGBACKREST_KEY=$(rand); PGBACKREST_SECRET=$(rand)
  DASHBOARD_API_KEY=$(rand); BRIDGE_API_KEY=$(rand)
  DASHBOARD_USER=admin; DASHBOARD_PASSWORD=$(rand)
  NTFY_TOPIC="restaurant-platform-alerts-$(rand)"
  GRAFANA_ADMIN_PW=$(rand)

  cat > "$SECRETS_DIR/timescaledb.values.yaml" <<EOF
credentials:
  password: "$TS_PW"
EOF
  cat > "$SECRETS_DIR/alert-relay.values.yaml" <<EOF
alertRelay:
  ntfyTopic: "$NTFY_TOPIC"
EOF
  cat > "$SECRETS_DIR/grafana.values.yaml" <<EOF
grafana:
  adminPassword: "$GRAFANA_ADMIN_PW"
EOF
  cat > "$SECRETS_DIR/narrator.values.yaml" <<EOF
narratorCredentials:
  password: "$NARRATOR_PW"
EOF
  cat > "$SECRETS_DIR/minio.values.yaml" <<EOF
credentials:
  rootPassword: "$MINIO_ROOT_PW"
EOF
fi

# ---------------------------------------------------------------------
# 2. TimescaleDB + narrator password rotation
# ---------------------------------------------------------------------
# Order matters: ALTER ROLE first (old password keeps working for anyone
# already connected; nothing breaks yet), THEN update the Secret via helm
# upgrade. Updating the Secret alone would do nothing on its own -- the
# postgres image only reads POSTGRES_PASSWORD during initdb, which already
# happened; an existing database ignores it on every later restart. That is
# also why this whole rotation needs the app deployments restarted below:
# their PGPASSWORD env var is a snapshot taken at container start, not
# re-read live.
echo "== 2. rotate TimescaleDB role passwords =="
if [ "$DRY" = 1 ]; then
  echo "+ kubectl exec timescaledb-0 -- psql -c \"ALTER ROLE restaurant_app WITH PASSWORD '...'\""
  echo "+ kubectl exec timescaledb-0 -- psql -c \"ALTER ROLE narrator_app WITH PASSWORD '...'\""
else
  kubectl exec -n "$NS" timescaledb-0 -- psql -U restaurant_app -d restaurant_platform \
    -c "ALTER ROLE restaurant_app WITH PASSWORD '$TS_PW';"
  kubectl exec -n "$NS" timescaledb-0 -- psql -U restaurant_app -d restaurant_platform \
    -c "ALTER ROLE narrator_app WITH PASSWORD '$NARRATOR_PW';"
fi

# ---------------------------------------------------------------------
# 3. MinIO TLS cert + root password rotation
# ---------------------------------------------------------------------
# A self-signed cert, not a real CA -- nothing outside this cluster ever
# talks to MinIO, so there's no one for a real CA to prove identity to.
# Required (not optional) because pgBackRest's S3 driver always speaks
# HTTPS with no plain-HTTP mode; every client of MinIO (mc, pgBackRest) is
# told to skip certificate validation instead. Re-running this regenerates
# the cert too -- harmless, since every consumer already skips validation
# rather than pinning it.
echo "== 3. generate MinIO's TLS cert and rotate its root password =="
if [ "$DRY" = 1 ]; then
  echo "+ openssl req -x509 -newkey rsa:2048 ... -> kubectl create secret generic minio-tls"
else
  CERT_DIR=$(mktemp -d)
  openssl req -x509 -newkey rsa:2048 -nodes -days 3650 \
    -keyout "$CERT_DIR/private.key" -out "$CERT_DIR/public.crt" \
    -subj "/CN=minio.$NS.svc.cluster.local" \
    -addext "subjectAltName=DNS:minio.$NS.svc.cluster.local,DNS:minio,DNS:localhost"
  kubectl create secret generic minio-tls -n "$NS" \
    --from-file=public.crt="$CERT_DIR/public.crt" --from-file=private.key="$CERT_DIR/private.key" \
    --dry-run=client -o yaml | kubectl apply -f -
  rm -rf "$CERT_DIR"
fi
# Simpler than Postgres: MinIO reads its root credentials from the
# environment on every start (nothing initdb-only about it), so updating
# the Secret and restarting the pod is the whole rotation. That restart now
# also happens for the cert-mount/args change above, on this same upgrade.
# --reset-then-reuse-values, not --reuse-values: the latter reuses only
# what the LAST release actually had computed, so a brand-new values.yaml
# key like tls.secretName (added after that release was deployed) would
# come back nil -- Helm has no way to know it should fall back to the
# chart's current default for a key it never saw before. --reset-then-reuse
# starts from the chart's current defaults, then re-layers whatever was
# explicitly set last time (e.g. an earlier rotation's -f override) on top.
run helm upgrade minio k8s/minio -n "$NS" -f "$SECRETS_DIR/minio.values.yaml" --reset-then-reuse-values --wait
run kubectl rollout status statefulset/minio -n "$NS" --timeout=120s

# ---------------------------------------------------------------------
# 4. The backup bucket and a bucket-scoped MinIO user for pgBackRest
# ---------------------------------------------------------------------
# Root credentials are used here ONLY to create a second, far less
# powerful user -- pgBackRest itself never sees the root password, the same
# least-privilege principle as the narrator's own restricted database role
# (storage/schema/003_phase6.sql). mc mb --ignore-existing and re-adding the
# same user/policy are both safe to repeat.
echo "== 4. create the backup bucket and a scoped MinIO user for it =="
POLICY_FILE=$(mktemp)
cat > "$POLICY_FILE" <<'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"],
      "Resource": ["arn:aws:s3:::timescaledb-backups", "arn:aws:s3:::timescaledb-backups/*"]
    }
  ]
}
EOF
if [ "$DRY" = 1 ]; then
  echo "+ mc alias set / mb timescaledb-backups / admin user add / admin policy create+attach"
else
  # --insecure skips certificate validation for MinIO's self-signed cert
  # (step 3) -- there's no CA here to validate against.
  kubectl exec -n "$NS" minio-0 -- sh -c "mc --insecure alias set local https://localhost:9000 minioadmin '$MINIO_ROOT_PW'"
  kubectl exec -n "$NS" minio-0 -- mc --insecure mb --ignore-existing local/timescaledb-backups
  # mc admin user add TARGET ACCESSKEY SECRETKEY -- re-running this rotates the
  # secret key for the same access key rather than failing, which is exactly
  # what a re-run of this whole script should do.
  kubectl exec -n "$NS" minio-0 -- mc --insecure admin user add local "$PGBACKREST_KEY" "$PGBACKREST_SECRET"
  # kubectl cp shells out to `tar` inside the target container to stream the
  # file -- the Chainguard MinIO image is minimal and doesn't ship one (only
  # sh and mc). Piping through stdin to `cat` sidesteps tar entirely.
  kubectl exec -n "$NS" -i minio-0 -- sh -c "cat > /tmp/pgbackrest-policy.json" < "$POLICY_FILE"
  kubectl exec -n "$NS" minio-0 -- mc --insecure admin policy create local pgbackrest-backup /tmp/pgbackrest-policy.json 2>/dev/null || true
  kubectl exec -n "$NS" minio-0 -- mc --insecure admin policy attach local pgbackrest-backup --user "$PGBACKREST_KEY"
fi
rm -f "$POLICY_FILE"

echo "== 5. create the pgbackrest-minio-credentials Secret =="
if [ "$DRY" = 1 ]; then
  echo "+ kubectl create secret generic pgbackrest-minio-credentials --from-literal=access-key=... --from-literal=secret-key=..."
else
  kubectl create secret generic pgbackrest-minio-credentials -n "$NS" \
    --from-literal=access-key="$PGBACKREST_KEY" --from-literal=secret-key="$PGBACKREST_SECRET" \
    --dry-run=client -o yaml | kubectl apply -f -
fi

# ---------------------------------------------------------------------
# 6. Install the backup chart, then upgrade timescaledb to pick up both
#    the new passwords (step 2) and the new pgbackrest ConfigMap/Secret
#    mount (k8s/timescaledb/templates/statefulset.yaml) in one restart.
#    Order matters -- the same reason k8s/phase5-schemas must be installed
#    before its consumers: a StatefulSet cannot mount a ConfigMap that
#    does not exist yet.
# ---------------------------------------------------------------------
echo "== 6. install k8s/timescaledb-backup, then upgrade timescaledb =="
run helm upgrade --install timescaledb-backup k8s/timescaledb-backup -n "$NS" --wait
# --reset-then-reuse-values, not --reuse-values -- see the identical note
# on the minio upgrade above; the same staleness risk applies to any chart
# whose values.yaml gains a key after a release already exists.
run helm upgrade timescaledb k8s/timescaledb -n "$NS" \
  -f "$SECRETS_DIR/timescaledb.values.yaml" -f "$SECRETS_DIR/narrator.values.yaml" \
  --reset-then-reuse-values --wait --timeout 5m
run kubectl rollout status statefulset/timescaledb -n "$NS" --timeout=180s

echo "== 7. restart every service that reads timescaledb-credentials or narrator-credentials =="
for d in anomaly-detector causal-engine digital-twin finding-reviewer storage-consumer \
         ticket-timing-aggregator llm-narrator dashboard-api; do
  run kubectl rollout restart "deploy/$d" -n "$NS"
done
for d in anomaly-detector causal-engine digital-twin finding-reviewer storage-consumer \
         ticket-timing-aggregator llm-narrator dashboard-api; do
  run kubectl rollout status "deploy/$d" -n "$NS" --timeout=180s
done

# ---------------------------------------------------------------------
# 8. Turn on WAL archiving. archive_mode is postmaster-context (Postgres
#    only reads it at startup, unlike archive_command which reloads live),
#    so this needs one more restart of just the database.
# ---------------------------------------------------------------------
echo "== 8. enable archive_mode + archive_command, then restart timescaledb once more =="
if [ "$DRY" = 1 ]; then
  echo "+ kubectl exec timescaledb-0 -- psql -c \"ALTER SYSTEM SET archive_mode='on'\""
  echo "+ kubectl exec timescaledb-0 -- psql -c \"ALTER SYSTEM SET archive_command='pgbackrest ...archive-push %p'\""
else
  kubectl exec -n "$NS" timescaledb-0 -- psql -U restaurant_app -d restaurant_platform -v ON_ERROR_STOP=1 \
    -c "ALTER SYSTEM SET archive_mode = 'on';" \
    -c "ALTER SYSTEM SET archive_command = 'pgbackrest --config=/etc/pgbackrest/pgbackrest.conf --stanza=restaurant-platform archive-push %p';"
fi
run kubectl rollout restart statefulset/timescaledb -n "$NS"
run kubectl rollout status statefulset/timescaledb -n "$NS" --timeout=180s

echo "== 9. stanza-create, check, and the first full backup =="
run kubectl exec -n "$NS" timescaledb-0 -- pgbackrest --config=/etc/pgbackrest/pgbackrest.conf --stanza=restaurant-platform stanza-create
run kubectl exec -n "$NS" timescaledb-0 -- pgbackrest --config=/etc/pgbackrest/pgbackrest.conf --stanza=restaurant-platform check
run kubectl exec -n "$NS" timescaledb-0 -- pgbackrest --config=/etc/pgbackrest/pgbackrest.conf --stanza=restaurant-platform --type=full backup

# ---------------------------------------------------------------------
# 10. Dashboard + game-bridge auth
# ---------------------------------------------------------------------
echo "== 10. create dashboard/game-bridge API keys and the dashboard's Basic Auth login =="
if [ "$DRY" = 1 ]; then
  echo "+ kubectl create secret generic dashboard-api-credentials --from-literal=api-key=..."
  echo "+ kubectl create secret generic game-bridge-credentials --from-literal=api-key=..."
  echo "+ htpasswd (openssl passwd -apr1) -> kubectl create secret generic dashboard-web-htpasswd"
else
  kubectl create secret generic dashboard-api-credentials -n "$NS" \
    --from-literal=api-key="$DASHBOARD_API_KEY" --dry-run=client -o yaml | kubectl apply -f -
  kubectl create secret generic game-bridge-credentials -n "$NS" \
    --from-literal=api-key="$BRIDGE_API_KEY" --dry-run=client -o yaml | kubectl apply -f -
  HTPASSWD_LINE="$DASHBOARD_USER:$(openssl passwd -apr1 "$DASHBOARD_PASSWORD")"
  kubectl create secret generic dashboard-web-htpasswd -n "$NS" \
    --from-literal=.htpasswd="$HTPASSWD_LINE" --dry-run=client -o yaml | kubectl apply -f -
fi

echo "== 11. upgrade the dashboard chart to pick up the new Secrets and nginx config =="
run helm upgrade --install dashboard k8s/dashboard -n "$NS" --wait
run kubectl rollout status deploy/dashboard-api -n "$NS" --timeout=120s
run kubectl rollout status deploy/dashboard-web -n "$NS" --timeout=120s

echo "== 12. check =="
if [ "$DRY" = 0 ]; then
  kubectl exec -n "$NS" timescaledb-0 -- pgbackrest --config=/etc/pgbackrest/pgbackrest.conf --stanza=restaurant-platform info
  echo
  echo "Backup info above should show one full backup. Next: bash k8s/timescaledb-backup/restore-drill.sh"
  echo "to prove it actually restores."
  echo
  echo "Dashboard login (save this -- it is not stored anywhere else, and this script does not print it again):"
  echo "  user:     $DASHBOARD_USER"
  echo "  password: $DASHBOARD_PASSWORD"
  echo
  echo "game-bridge's API key is in the game-bridge-credentials Secret. To run the Godot client"
  echo "against the k3s deployment (after game/k8s/deploy-bridge.sh):"
  echo "  export BRIDGE_API_KEY=\$(kubectl get secret game-bridge-credentials -n $NS -o jsonpath='{.data.api-key}' | base64 -d)"
fi

# ---------------------------------------------------------------------
# 13. Rotate alert-relay's ntfy.sh topic and Grafana's admin password
#     (both k8s/observability)
# ---------------------------------------------------------------------
# Assumes k8s/observability/deploy-alerting.sh has already been run once
# (Alertmanager/alert-relay/Prometheus rules installed) -- this only
# rotates the topic name on top of that. --reset-then-reuse-values for the
# same reason as every other upgrade in this script: values.yaml gaining a
# key after a release exists means only --reset-then-reuse starts from the
# chart's current defaults for it. Grafana's restart alone is the whole
# rotation, unlike TimescaleDB's ALTER-ROLE-then-restart dance above: its
# Deployment mounts no persistent volume for /var/lib/grafana (see
# k8s/observability/values.yaml's comment on adminPassword), so its
# internal sqlite user table is wiped every restart and
# GF_SECURITY_ADMIN_PASSWORD is re-read as a fresh install each time.
echo "== 13. rotate alert-relay's ntfy topic and Grafana's admin password =="
run helm upgrade observability k8s/observability -n "$NS" \
  -f "$SECRETS_DIR/alert-relay.values.yaml" -f "$SECRETS_DIR/grafana.values.yaml" \
  --reset-then-reuse-values --wait --timeout 120s
run kubectl rollout status deploy/alert-relay -n "$NS" --timeout=120s
run kubectl rollout restart deploy/grafana -n "$NS"
run kubectl rollout status deploy/grafana -n "$NS" --timeout=120s

if [ "$DRY" = 0 ]; then
  echo
  echo "Grafana login (save this -- it is not stored anywhere else, and this script does not print it again):"
  echo "  user:     admin"
  echo "  password: $GRAFANA_ADMIN_PW"
fi

if [ "$DRY" = 0 ]; then
  echo
  echo "Alert topic rotated. Subscribe to it to receive alerts (this script does not print it again):"
  echo "  https://ntfy.sh/$NTFY_TOPIC  (or the ntfy app/CLI, topic: $NTFY_TOPIC)"
fi
echo "done"
