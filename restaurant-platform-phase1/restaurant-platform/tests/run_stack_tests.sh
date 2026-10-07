#!/usr/bin/env bash
# Brings up the real Docker Compose stack as an isolated test project, runs
# the stack-level test layers against it, and tears everything down again.
#
#   bash tests/run_stack_tests.sh e2e          # data flows end to end        (~5 min after build)
#   bash tests/run_stack_tests.sh load         # throughput and stability     (~5 min)
#   bash tests/run_stack_tests.sh resilience   # crashes, outages, restarts   (~10 min)
#   bash tests/run_stack_tests.sh acceptance   # injected scenario -> finding (~10 min)
#   bash tests/run_stack_tests.sh all          # e2e + load + resilience
#   bash tests/run_stack_tests.sh full         # all + acceptance
#
# The stack runs as compose project "rp-test" with the dashboard on port 18080
# (override with TEST_COMPOSE_PROJECT / DASHBOARD_PORT), so it never touches a
# developer's own `docker compose up` stack, volumes or port 8080. Set
# KEEP_STACK=1 to leave it running afterwards for debugging.
set -uo pipefail

cd "$(dirname "$0")/.."
LAYER="${1:-all}"
shift || true
export DASHBOARD_PORT="${DASHBOARD_PORT:-18080}"
export TEST_COMPOSE_PROJECT="${TEST_COMPOSE_PROJECT:-rp-test}"
ARTIFACTS="tests/artifacts"
COMPOSE=(docker compose -p "$TEST_COMPOSE_PROJECT" -f docker-compose.yml -f tests/e2e/docker-compose.test.yml)

# One list of services, owned by tests/stack_fixture.py.
SERVICES="$(PYTHONPATH=tests python -c 'import stack_fixture; print(" ".join(stack_fixture.STACK_SERVICES))')"

cleanup() {
  if [ "${KEEP_STACK:-0}" = "1" ]; then
    echo "KEEP_STACK=1: leaving project '$TEST_COMPOSE_PROJECT' running (dashboard on port $DASHBOARD_PORT)"
    return
  fi
  "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

if ! "${COMPOSE[@]}" ps >/dev/null 2>&1; then
  echo "cannot reach the container engine. With rootless Podman (e.g. WSL after a restart) run:" >&2
  echo "  systemctl --user start podman.socket" >&2
  trap - EXIT; exit 2
fi

mkdir -p "$ARTIFACTS"
"${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true   # start from a clean slate

echo "== building and starting the stack ($LAYER) =="
# The operator's tool (a profile, so not part of the stack below) is run on demand by the end-to-end tests; build it first, or the
# first test to run it spends its whole time limit building. BEFORE the stack starts, not after: a build running while the services
# are joining Kafka competes for the CPU at exactly the moment they are most likely to time out and restart.
"${COMPOSE[@]}" build edge-operator || { echo "the edge-operator image failed to build" >&2; exit 2; }
# shellcheck disable=SC2086
"${COMPOSE[@]}" up -d --build $SERVICES || { echo "stack failed to start" >&2; exit 2; }

status=0
run() {
  echo; echo "== $* =="
  python -m pytest -p no:cacheprovider "$@" || status=1
}

case "$LAYER" in
  e2e)         run tests/e2e -v "$@" ;;
  acceptance)  run tests/e2e/test_scenario_acceptance.py -v -s --run-slow "$@" ;;
  load)        run tests/load -v -s "$@" ;;
  resilience)  run tests/resilience -v "$@" ;;
  all)         run tests/e2e -v "$@"; run tests/load -v -s "$@"; run tests/resilience -v "$@" ;;
  full)        run tests/e2e -v "$@"; run tests/e2e/test_scenario_acceptance.py -v -s --run-slow "$@"; run tests/load -v -s "$@"; run tests/resilience -v "$@" ;;
  *) echo "unknown layer '$LAYER' (e2e|acceptance|load|resilience|all|full)" >&2; exit 2 ;;
esac

if [ "$status" -ne 0 ]; then
  echo "== tests failed: saving container logs to $ARTIFACTS/stack.log =="
  "${COMPOSE[@]}" logs --no-color --tail 400 > "$ARTIFACTS/stack.log" 2>&1 || true
fi
exit "$status"
