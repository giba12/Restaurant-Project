#!/usr/bin/env bash
# Runs the statistical validation inside the project's own causal-engine image.
#
# DoWhy 0.11.1 supports Python <3.12 only, and this project pins every service
# image to python:3.11-slim. Running here, in that exact image, tests the same
# interpreter and the same pinned scipy/statsmodels/networkx production runs --
# not whatever Python a developer's machine happens to have.
#
# Usage (from anywhere):   bash tests/statistical/run_statistical_tests.sh [pytest args]
#   e.g. add --run-slow to include the refutation-gate measurement.
set -euo pipefail

cd "$(dirname "$0")/../.."
IMAGE="rp-causal-engine-test"

docker build -q -f services/causal-engine/Dockerfile -t "$IMAGE" services >/dev/null

# --entrypoint sh: the image's ENTRYPOINT is the service itself. pytest is a
# test-only dependency, so it is installed at run time rather than baked into
# the production image.
docker run --rm --entrypoint sh -v "$PWD":/work:ro -w /work -e PYTHONDONTWRITEBYTECODE=1 "$IMAGE" -c \
  'pip install -q --user pytest 2>&1 | tail -1; python -m pytest tests/statistical -v -p no:cacheprovider --rootdir=/work "$@"' sh "$@"
