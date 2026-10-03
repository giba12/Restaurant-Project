#!/usr/bin/env bash
# Runs the integration tests against a real, throwaway TimescaleDB.
#
# The database is initialised exactly the way docker-compose.yml initialises
# it: the five numbered migrations are mounted into /docker-entrypoint-initdb.d
# and run by the image's own entrypoint, with NARRATOR_PGPASSWORD set for
# 003's \getenv. So if a migration is broken, the database never comes up and
# this script fails before a single test runs -- the migrations are tested
# by being used, not by being parsed.
#
# Usage (from anywhere):   bash tests/integration/run_db_tests.sh [pytest args]
# Needs: docker (or podman's docker shim), python with psycopg2 + pytest.
set -euo pipefail

cd "$(dirname "$0")/../.."
NAME="${TEST_PG_CONTAINER:-rp-itest-db}"
PORT="${TEST_PG_PORT:-15432}"
IMAGE="docker.io/timescale/timescaledb-ha:pg16-ts2.16-all"   # same image/tag as docker-compose.yml
APP_PASSWORD="itest-app-password"
NARRATOR_PASSWORD="itest-narrator-password"

cleanup() { docker rm -f "$NAME" >/dev/null 2>&1 || true; }
trap cleanup EXIT
cleanup

docker run -d --name "$NAME" \
  -e POSTGRES_DB=restaurant_platform -e POSTGRES_USER=restaurant_app \
  -e POSTGRES_PASSWORD="$APP_PASSWORD" -e NARRATOR_PGPASSWORD="$NARRATOR_PASSWORD" \
  -p "127.0.0.1:${PORT}:5432" \
  -v "$PWD/storage/schema/001_hypertables.sql:/docker-entrypoint-initdb.d/001_hypertables.sql:ro" \
  -v "$PWD/storage/schema/002_phase5_hypertables.sql:/docker-entrypoint-initdb.d/002_phase5_hypertables.sql:ro" \
  -v "$PWD/storage/schema/003_phase6.sql:/docker-entrypoint-initdb.d/003_phase6.sql:ro" \
  -v "$PWD/storage/schema/004_player_source_kind.sql:/docker-entrypoint-initdb.d/004_player_source_kind.sql:ro" \
  -v "$PWD/storage/schema/005_ticket_origin.sql:/docker-entrypoint-initdb.d/005_ticket_origin.sql:ro" \
  "$IMAGE" >/dev/null

echo "waiting for TimescaleDB to finish running the migrations..."
for _ in $(seq 1 90); do
  # The last migration's artefact (column origin, from 005) means all five ran.
  if docker exec "$NAME" psql -U restaurant_app -d restaurant_platform -tAc \
      "SELECT 1 FROM information_schema.columns WHERE table_name='ticket_timing_summaries' AND column_name='origin'" 2>/dev/null | grep -q 1; then
    break
  fi
  sleep 2
done
# The image restarts Postgres once after init; wait for the real server.
for _ in $(seq 1 60); do
  if docker exec "$NAME" pg_isready -h 127.0.0.1 -U restaurant_app -d restaurant_platform >/dev/null 2>&1; then break; fi
  sleep 1
done
if ! docker ps --format '{{.Names}}' | grep -qx "$NAME"; then
  echo "database container exited -- a migration probably failed:" >&2
  docker logs "$NAME" 2>&1 | tail -30 >&2
  exit 1
fi

export TEST_PG_DSN="postgresql://restaurant_app:${APP_PASSWORD}@127.0.0.1:${PORT}/restaurant_platform"
export TEST_PG_NARRATOR_DSN="postgresql://narrator_app:${NARRATOR_PASSWORD}@127.0.0.1:${PORT}/restaurant_platform"
export TEST_PG_CONTAINER="$NAME"
export TEST_PG_NARRATOR_PASSWORD="$NARRATOR_PASSWORD"
export REQUIRE_DB=1   # a missing database must fail the run, never silently skip it

python -m pytest tests/integration -v -p no:cacheprovider "$@"
