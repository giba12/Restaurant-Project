#!/bin/sh
# One-shot init container: waits for Ollama's API, then pulls the
# portable path's tiny CPU-only model. Mirrors the MinIO bucket-init Job
# pattern already used in the k8s chart (wait, then do the one-time setup
# step, then exit).
#
# Ollama's /api/pull returns HTTP 200 with an {"error": ...} body on
# failure (e.g. a transient registry DNS lookup timeout, confirmed by
# actually hitting this) rather than a non-2xx status -- `curl -sf` alone
# does not catch that, so the response body must be checked explicitly.
# Retries a few times since this failure mode has been transient in
# testing, not permanent.
set -eu

OLLAMA_URL="http://ollama:11434"
MODEL="${OLLAMA_MODEL:-qwen2.5:0.5b-instruct}"

echo "waiting for Ollama at ${OLLAMA_URL}..."
until curl -sf "${OLLAMA_URL}/api/tags" > /dev/null; do
  sleep 3
done

attempt=1
max_attempts=5
while [ "$attempt" -le "$max_attempts" ]; do
  echo "pulling ${MODEL} (attempt ${attempt}/${max_attempts})..."
  response=$(curl -sf -X POST "${OLLAMA_URL}/api/pull" -d "{\"model\": \"${MODEL}\"}")
  if echo "$response" | grep -q '"error"'; then
    echo "  pull failed: ${response}"
    attempt=$((attempt + 1))
    sleep 5
    continue
  fi
  echo "done."
  exit 0
done

echo "giving up after ${max_attempts} attempts -- llm-narrator will fail until this model exists; retry manually with:"
echo "  docker compose exec ollama ollama pull ${MODEL}"
exit 1
