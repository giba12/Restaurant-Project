#!/bin/sh
# One-shot init container: waits for the Connect REST API, then registers
# the four MQTT source connectors. Equivalent of the k8s deployment's
# KafkaConnector custom resources -- plain Connect REST calls here since
# there's no Strimzi operator in the portable path to manage them.
set -eu

CONNECT_URL="http://kafka-connect:8083"

echo "waiting for Kafka Connect REST API at ${CONNECT_URL}..."
until curl -sf "${CONNECT_URL}/connectors" > /dev/null; do
  sleep 3
done

for f in /connectors/*.json; do
  name=$(basename "$f" .json)
  echo "registering ${name}..."
  curl -sf -X POST -H "Content-Type: application/json" \
    --data @"$f" "${CONNECT_URL}/connectors" \
    || echo "  (already exists or failed -- check ${CONNECT_URL}/connectors/${name}/status)"
done

echo "done."
