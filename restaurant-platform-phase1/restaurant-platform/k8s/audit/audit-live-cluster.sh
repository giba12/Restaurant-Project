#!/usr/bin/env bash
# Checks for two classes of bug this project has found by hand, repeatedly,
# only by accident:
#
#   1. Drift -- a chart's values.yaml was fixed and committed, but nobody
#      ever re-ran `helm upgrade`, so the live cluster is still running the
#      old, broken config. This is exactly what left kafka-connect-mqtt's
#      cpu-starvation fix undeployed for two days (see the implementation-
#      status doc, items 12/17): the file said cpu:"4", the live CR still
#      said cpu:"1", and nobody noticed until a chaos test forced a restart.
#   2. Unbounded resources -- a container with no resources.limits at all,
#      the gap item 12 found and fixed for five containers project-wide.
#
#   3. (lighter touch) a credential still sitting at the insecure
#      placeholder k8s/harden/harden-live-cluster.sh exists to rotate --
#      the Grafana-password gap found and fixed 2026-09-28/29.
#
# Converts "a human has to remember to check" into "run this and it tells
# you" -- the same lesson every one of those bugs taught on its own.
#
# Read-only: never modifies the cluster or any chart. `helm upgrade
# --dry-run` renders what an upgrade *would* apply without applying it. No
# sudo needed (no image building/importing here).
#
# Run from the repo root:
#   bash k8s/audit/audit-live-cluster.sh
#
# Can't run in GitHub Actions CI -- it needs this specific live k3s
# cluster, which CI has no access to. This is the "prints a report" half
# of that idea, not the "fails CI" half; exits non-zero when it finds
# something, so it's still usable as a pass/fail gate in a script that
# does have cluster access (e.g. chained after a deploy).
set -euo pipefail

NS=kafka
[ -f k8s/audit/audit-live-cluster.sh ] || { echo "run this from the repo root" >&2; exit 1; }
kubectl get ns "$NS" >/dev/null

ISSUES=0
SCRATCH=$(mktemp -d)
trap 'rm -rf "$SCRATCH"' EXIT

# release:chart pairs for every chart this project hand-rolls and actually
# has running. Excludes strimzi-kafka-operator (not a chart of ours) and
# one-shot/install-time charts (phase5-schemas, timescaledb-backup) where
# "would an upgrade change anything" isn't a meaningful question.
CHARTS="
anomaly-detector:k8s/anomaly-detector
causal-engine:k8s/causal-engine
finding-reviewer:k8s/finding-reviewer
dashboard:k8s/dashboard
digital-twin:k8s/digital-twin
edge-simulators:k8s/edge-simulators
game-bridge:game/k8s/bridge
kafka-connect-mqtt:k8s/kafka-connect-mqtt
kafka-strimzi:k8s/kafka-strimzi
llm-narrator:k8s/llm-narrator
minio:k8s/minio
mosquitto:k8s/mosquitto
observability:k8s/observability
scenario-injection-controller:k8s/scenario-injection-controller
storage-consumer:k8s/storage-consumer
ticket-timing-aggregator:k8s/ticket-timing-aggregator
timescaledb:k8s/timescaledb
"

echo "== 1. drift: does the committed chart still match what's actually deployed? =="
for entry in $CHARTS; do
  release="${entry%%:*}"
  chart="${entry#*:}"
  if ! helm status "$release" -n "$NS" >/dev/null 2>&1; then
    echo "  --     $release (not installed, skipping)"
    continue
  fi
  helm get manifest "$release" -n "$NS" > "$SCRATCH/live.yaml" 2>/dev/null
  # `helm upgrade --dry-run` was tried first and rejected: its stdout also
  # carries Helm's own release-status header (NAME/STATUS/MANIFEST: ...)
  # ahead of the actual YAML, which both broke YAML parsing in step 2 and
  # made every single chart look like 100% drift against `helm get
  # manifest`'s clean output -- confirmed live, not assumed. `helm
  # template` only ever emits the manifest, so this instead pulls the live
  # release's own recorded values (equivalent to what --reuse-values would
  # reuse, including any -f secrets file applied in a past
  # harden-live-cluster.sh run) and renders today's chart templates against
  # those same values -- directly comparable to `helm get manifest`'s
  # output, and touches nothing.
  helm get values "$release" -n "$NS" -o yaml > "$SCRATCH/current-values.yaml" 2>/dev/null
  # --no-hooks: `helm get manifest` only ever shows the release's tracked,
  # non-hook resources, never anything annotated helm.sh/hook (like MinIO's
  # and TimescaleDB's post-install/post-upgrade init Jobs) -- confirmed
  # live, not assumed, after both false-positived as "drifted" on nothing
  # but their own hook Job showing up on one side and not the other.
  if ! helm template "$release" "$chart" -n "$NS" -f "$SCRATCH/current-values.yaml" --no-hooks \
       > "$SCRATCH/would-be.yaml" 2>"$SCRATCH/err.log"; then
    echo "  ?      $release: template render failed, skipping (see $SCRATCH/err.log while this script runs)"
    cat "$SCRATCH/err.log" >&2
    continue
  fi
  # Trailing-blank-line normalization: `helm get manifest` and `helm
  # template` format the end of their multi-document output slightly
  # differently (confirmed live -- mosquitto, the simplest chart here,
  # diffed as "drifted" on nothing but two trailing blank lines). Command
  # substitution strips all trailing newlines; printf puts exactly one back.
  normalize() { printf '%s\n' "$(cat "$1")"; }
  if diff -q <(normalize "$SCRATCH/live.yaml") <(normalize "$SCRATCH/would-be.yaml") >/dev/null 2>&1; then
    echo "  ok     $release"
  else
    echo "  DRIFT  $release -- what's deployed no longer matches what re-running helm upgrade would produce"
    ISSUES=$((ISSUES+1))
  fi
  # Feed this same render into the resource-limit check below instead of
  # re-rendering -- one helm upgrade --dry-run per chart, not two.
  cp "$SCRATCH/would-be.yaml" "$SCRATCH/render-$release.yaml"
done

echo
echo "== 2. unbounded resources: any container/component with no resources.limits? =="
if ! python3 - "$SCRATCH"/render-*.yaml <<'PYEOF'
import sys, yaml

def find_resources_blocks(obj, name_hint="?"):
    out = []
    if isinstance(obj, dict):
        name = obj.get("name", name_hint)
        if isinstance(obj.get("resources"), dict):
            out.append((name, obj["resources"]))
        for v in obj.values():
            out.extend(find_resources_blocks(v, name))
    elif isinstance(obj, list):
        for v in obj:
            out.extend(find_resources_blocks(v, name_hint))
    return out

found_any = False
for path in sys.argv[1:]:
    with open(path) as f:
        for doc in yaml.safe_load_all(f):
            if not doc or not isinstance(doc, dict):
                continue
            kind = doc.get("kind", "?")
            meta_name = doc.get("metadata", {}).get("name", "?")
            for comp_name, res in find_resources_blocks(doc.get("spec", {})):
                # A PersistentVolumeClaim's own `resources` field (storage
                # sizing, e.g. StatefulSet volumeClaimTemplates) matches the
                # same generic shape as a container's compute resources but
                # only ever has requests.storage, never limits.cpu/memory --
                # confirmed live, not assumed (MinIO/TimescaleDB's
                # StatefulSets both false-positived here on the first real
                # run). Skip anything that isn't plausibly compute: requests
                # or limits must mention cpu or memory somewhere.
                requests = res.get("requests") or {}
                limits = res.get("limits") or {}
                if not (set(requests) | set(limits)) & {"cpu", "memory"}:
                    continue
                missing = [k for k in ("cpu", "memory") if k not in limits]
                if missing:
                    found_any = True
                    print(f"  UNBOUNDED  {kind}/{meta_name} ({comp_name}): missing limits.{','.join(missing)}")
if not found_any:
    print("  ok     every container/component has resources.limits set")
sys.exit(1 if found_any else 0)
PYEOF
then
  ISSUES=$((ISSUES+1))
fi

echo
echo "== 3. credentials still at their insecure placeholder value? =="
# Only ever prints "placeholder"/"rotated"/"not found" -- never the actual
# decoded secret value, rotated or not.
check_secret_placeholder() {
  local secret=$1 key=$2 placeholder=$3
  local actual
  if ! actual=$(kubectl get secret "$secret" -n "$NS" -o jsonpath="{.data.$key}" 2>/dev/null | base64 -d 2>/dev/null); then
    echo "  ?      $secret/$key: secret not found"
    return
  fi
  if [ "$actual" = "$placeholder" ]; then
    echo "  PLACEHOLDER  $secret/$key is still its insecure default -- run k8s/harden/harden-live-cluster.sh"
    ISSUES=$((ISSUES+1))
  else
    echo "  ok     $secret/$key (rotated)"
  fi
}
check_secret_placeholder timescaledb-credentials password "REPLACE-AT-DEPLOY-TIME-see-k8s/harden/harden-live-cluster.sh"
check_secret_placeholder narrator-credentials password "REPLACE-AT-DEPLOY-TIME-see-k8s/harden/harden-live-cluster.sh"
check_secret_placeholder minio-credentials root-password "REPLACE-AT-DEPLOY-TIME-see-k8s/harden/harden-live-cluster.sh"
check_secret_placeholder grafana-credentials admin-password "changeme-local-dev-only"

ntfy_topic=$(kubectl get deploy alert-relay -n "$NS" \
  -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="NTFY_TOPIC")].value}' 2>/dev/null || echo "")
if [ -z "$ntfy_topic" ]; then
  echo "  ?      alert-relay/NTFY_TOPIC: deployment not found"
elif [ "$ntfy_topic" = "restaurant-platform-alerts-REPLACE-AT-DEPLOY-TIME" ]; then
  echo "  PLACEHOLDER  alert-relay's NTFY_TOPIC is still its guessable default -- run k8s/harden/harden-live-cluster.sh"
  ISSUES=$((ISSUES+1))
else
  echo "  ok     alert-relay/NTFY_TOPIC (rotated)"
fi

echo
if [ "$ISSUES" -gt 0 ]; then
  echo "$ISSUES issue(s) found."
  exit 1
else
  echo "No issues found."
fi
