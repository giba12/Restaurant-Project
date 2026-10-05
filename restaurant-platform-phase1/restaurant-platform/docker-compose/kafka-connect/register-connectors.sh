#!/bin/sh
# One-shot init container: waits for the Connect REST API, then registers
# the four MQTT source connectors. Equivalent of the k8s deployment's
# KafkaConnector custom resources -- plain Connect REST calls here since
# there's no Strimzi operator in the portable path to manage them.
#
# PUT /connectors/<name>/config creates the connector if it is not there and
# updates it if it is. It used to be POST /connectors, which answers 409 for a
# connector that already exists (Kafka keeps connector configs across restarts),
# so a changed config file never reached a stack that had run before, and the
# script then printed "already exists or failed" and exited 0 whichever it was:
# a registration that genuinely failed (Connect answers 409 or 500 while it is
# rebalancing) looked like success. Now it retries, and fails if it cannot.
set -eu

CONNECT_URL="${CONNECT_URL:-http://kafka-connect:8083}"
CONNECTORS_DIR="${CONNECTORS_DIR:-/connectors}"
RETRY_DELAY="${RETRY_DELAY:-3}"
ATTEMPTS="${ATTEMPTS:-20}"

echo "waiting for Kafka Connect REST API at ${CONNECT_URL}..."
until curl -sf "${CONNECT_URL}/connectors" > /dev/null; do
  sleep "$RETRY_DELAY"
done

for f in "${CONNECTORS_DIR}"/*.json; do
  name=$(basename "$f" .json)
  # The file is {"name": ..., "config": {...}}; PUT takes just the config object.
  config=$(sed -n '/"config": *{/,/^  }/p' "$f" | sed '1s/.*/{/; $s/.*/}/')
  attempt=1
  until curl -sf -X PUT -H "Content-Type: application/json" --data "$config" \
        "${CONNECT_URL}/connectors/${name}/config" > /dev/null; do
    if [ "$attempt" -ge "$ATTEMPTS" ]; then
      echo "FAILED to register ${name} after ${attempt} attempts" >&2
      exit 1
    fi
    attempt=$((attempt + 1))
    sleep "$RETRY_DELAY"
  done
  echo "registered ${name}"
done

echo "done."
