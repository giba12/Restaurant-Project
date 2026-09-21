# Phase 3 Resume Brief

> **Historical.** Written mid-Phase 3 to resume a session. Phase 3 was completed long ago (see the Section 4 table in `restaurant-platform-implementation-status.md`); the checklist below is kept for the record and is no longer actionable. Paths of the form `k8s/charts/...` are outdated: charts live directly under `k8s/`.

Paste this as the first message in a new chat to continue.

## Where things stand

Phase 3 (edge simulators) is built and mostly deployed, not yet confirmed done.

**Done:**
- Four simulator generators (`plate-waste`, `pos-transaction`, `service-timing`, `staff-shift`) built, validated against the real `jsonschema` library in a local venv — 0 violations across 4000+ generated events.
- One parameterized Docker image (`local/edge-simulator:1.0`), built and imported into k3s.
- Four Deployments live via Helm (`k8s/charts/edge-simulators/`), all `1/1 Running`, logs confirm active publishing with no schema violations.
- Four new `KafkaConnector` objects applied, consolidated into `k8s/charts/kafka-connect-mqtt/templates/kafka-connector.yaml` alongside the original Phase 2 connector.

**Not yet confirmed:**
- `plate-waste-source-connector`'s task failed right after all four connectors were applied simultaneously (`EOFException` — MQTT connection dropped mid-handshake to Mosquitto). Leading theory: connection-burst race condition, not a config defect, since an identically-configured `pos-transaction-source-connector` succeeded in the same batch. A manual task restart was the prescribed fix; whether it worked was never confirmed.
- `service-timing-source-connector` and `staff-shift-source-connector` were never individually checked with `kubectl describe kafkaconnector`.
- No Kafka topic has actually been consumed yet to confirm real messages are flowing end-to-end — this is the actual Phase 3 done condition, and it hasn't been attempted.

## To complete Phase 3

1. `kubectl get kafkaconnector -n kafka` — check current `READY` state of all five connectors.
2. For any not `READY: True`, run `kubectl describe kafkaconnector -n kafka <name>` to see task-level state and trace.
3. If a task is `FAILED`, restart it: `kubectl exec -n kafka <connect-pod> -it -- curl -s -X POST localhost:8083/connectors/<name>/tasks/0/restart` (find the Connect pod with `kubectl get pods -n kafka -l strimzi.io/kind=KafkaConnect` — it's a `StrimziPodSet`, not a `Deployment`, in this cluster).
4. Consume all four new Kafka topics to confirm readable JSON is flowing:
   ```
   kubectl exec -n kafka restaurant-platform-kafka-dev-pool-0 -it -- \
     /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
     --topic <topic-name> --from-beginning --max-messages 5
   ```
   Repeat for `plate-waste-events`, `pos-transaction-events`, `service-timing-events`, `staff-shift-events`.
5. Once all four show clean readable output, mark Phase 3 complete in `restaurant-platform-implementation-status.md` Section 4.

## Reference docs to bring into the new chat

- `restaurant-platform-implementation-status.md` (updated through Phase 3 — problem log items 1–25)
- `linux-k8s-docker-helm-study-guide.md` (updated through Phase 3)
- `restaurant-platform-project-notes.md` (original design brief, unchanged)
