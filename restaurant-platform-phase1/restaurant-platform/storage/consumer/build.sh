#!/usr/bin/env bash
# Run from repo root: bash storage/consumer/build.sh
#
# One command at a time is the established convention in this project
# (see study guide Section 1.3 on paste-corruption risk) -- this script
# exists so the multi-step build/import sequence is written once,
# inspectable, and re-runnable, rather than pasted inline each session.
set -euo pipefail

docker build --network=host -t local/storage-consumer:1.0 -f storage/consumer/Dockerfile .

docker save local/storage-consumer:1.0 | sudo k3s ctr images import -

echo "verify with: sudo k3s ctr images ls | grep storage-consumer"
