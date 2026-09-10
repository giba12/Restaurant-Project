#!/usr/bin/env bash
# Run from repo root: bash storage/consumer/build.sh
#
# One command at a time is the established convention in this project
# (see study guide Section 1.3 on paste-corruption risk) -- this script
# exists so the multi-step build/copy/import sequence is written once,
# inspectable, and re-runnable, rather than pasted inline each session.
set -euo pipefail

REPO_ROOT="$(pwd)"
CONSUMER_DIR="${REPO_ROOT}/storage/consumer"

# The consumer's Dockerfile expects schemas/ inside its own build
# context. Rather than maintaining a second copy of the schema files by
# hand (violates the single-source-of-truth point of contract-first),
# this copies the real schemas/ directory in immediately before build
# and removes the copy immediately after -- schemas/ under
# storage/consumer/ should never be committed to the repo.
cp -r "${REPO_ROOT}/schemas" "${CONSUMER_DIR}/schemas"

docker build --network=host -t local/storage-consumer:1.0 "${CONSUMER_DIR}"

rm -rf "${CONSUMER_DIR}/schemas"

docker save local/storage-consumer:1.0 | sudo k3s ctr images import -

echo "verify with: sudo k3s ctr images ls | grep storage-consumer"
