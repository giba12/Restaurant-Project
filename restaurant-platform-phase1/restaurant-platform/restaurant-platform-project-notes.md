# Project context note — restaurant operations/CX analytics platform

Use this note to resume the project in a new chat. Paste it in full, or upload the file, as the first message.

## Purpose and constraints

- **Type**: personal resume/portfolio project, with secondary goal of being refined for potential sale if a company shows interest.
- **Cost constraint**: every component must be free/open-source, or hand-coded with assistance. No paid APIs or licensed vendor tools.
- **"Fairy dust" constraint**: current implementation stands in for production-grade components (real sensors, real POS integrations, real vendor analytics) that don't exist yet. Every stand-in must be built against a fixed contract/schema so it can be swapped for a real integration later without touching downstream code. This is a hard architectural requirement, not a nice-to-have — schemas are defined before any producer or consumer code is written.
- **Learning goal priority** (in order): meaningful AI integration → data integration/forming/analytics → IoT → AIoT → Kubernetes → digital twin. All six should be demonstrated at defensible depth rather than shallow coverage of all six.

## Prior critique that shaped the design

- The plate-waste-camera → recipe-change example from the original pitch is causally weak (uneaten food ≠ dissatisfaction; portion size, doggy-bag intent, and dietary restriction all confound it) and was **not** adopted as a literal feature. It was replaced with a general causal-inference engine that models confounders explicitly, rather than surfacing raw correlations.
- Computer-vision plate-waste detection is a solved problem already sold by Winnow, Metafoodx, and Clean Plate Innovations (all use CV + weight sensors, trained on large proprietary image sets). Building a competing CV model from scratch is out of scope; CV is a stretch/optional phase, not core.
- POS, KDS, and scheduling data (all structured, not sensor-derived) are higher-value, lower-risk data sources than cameras for the core use cases and are the priority in the simulated data model.
- Employee/customer camera surveillance carries real legal exposure (state biometric statutes, labor law notice requirements) — noted for realism in the simulated schemas/documentation, not something the simulation needs to solve, but worth a line in the project README as a "production considerations" section.

## Architecture (six layers, all inside a single local Kubernetes cluster)

1. **Simulated edge device pods** — one container per simulated sensor type (plate-waste camera, ticket timer, staffing sensor), publishing schema-conformant events at non-uniform (Poisson-process) rates.
2. **Message backbone** — Eclipse Mosquitto (MQTT, edge transport) bridged into Apache Kafka via the Strimzi Kubernetes operator (central event log).
3. **Storage** — PostgreSQL + TimescaleDB extension (time-series data), MinIO (S3-compatible object storage for any blob/image data). Note: confirm which TimescaleDB features are Apache-2.0 vs. separately licensed before depending on anything beyond community-edition features.
4. **Causal & anomaly engine** — Python microservice; anomaly detection first (control limits or isolation forest via scikit-learn), then causal inference with explicit confound modeling via Microsoft's `DoWhy` library and `statsmodels`. Paired with a scenario-injection controller that perturbs the simulation (e.g., drop staffing, spike order volume) so the engine has real, attributable signal to detect.
5. **Digital twin service** — separate stateful microservice maintaining current restaurant state (tables, staff-on-shift, kitchen station load) as a graph/state machine, updated from the event stream. Deliberately kept architecturally separate from the causal engine (twin = state simulator, causal engine = inference system) even though they sit adjacent in the pipeline.
6. **LLM narrator** — locally run open-weight model via Ollama. Strict separation enforced: the narrator only ever receives structured finding objects already computed by the causal engine, never raw data, and cannot originate a claim — it only phrases findings already established statistically. This forecloses hallucinated causal claims.
7. **Frontend** — React dashboard (business-facing: twin state + recommendations) and a separate Prometheus + Grafana dashboard (platform observability: pod health, Kafka consumer lag, event throughput).

Contract-first requirement: event schemas (`PlateWasteEvent`, `POSTransactionEvent`, `StaffShiftEvent`, `ServiceTimingEvent`) are defined as JSON Schema or AsyncAPI specs in a `schemas/` directory before any producer/consumer code, so simulated producers and future real-vendor adapters (e.g., a Winnow webhook, a Toast POS API) can both implement the same contract.

## Seven-phase build sequence

1. **Scaffold the cluster and repo** — install k3s or kind; repo structure `schemas/`, `edge-simulators/`, `platform-services/`, `k8s/` (Helm charts); commit event schemas before any other code.
2. **Stand up the message backbone** — Mosquitto + Strimzi Kafka + MQTT-Kafka bridge. Done when a hand-published MQTT test message appears on the corresponding Kafka topic.
3. **Build the edge simulators** — one container per sensor type, schema-conformant, non-uniform event rates. Done when Kafka receives a steady stream with no schema violations.
4. **Deploy storage and persist the stream** — TimescaleDB + MinIO via Helm, plus a Kafka-to-storage consumer. Done when historical data is queryable directly, independent of any dashboard.
5. **Build the causal and anomaly engine** — anomaly detector first, then DoWhy causal layer, plus the scenario-injection controller. Done when an injected scenario produces a correctly attributed finding, not a raw correlation.
6. **Build the digital twin and LLM narrator** — separate services, strict data-access boundary between them. Done when a finding is narrated with no invented detail beyond the finding object.
7. **Build the dashboard and observability** — React dashboard + separate Prometheus/Grafana deployment. Done when both run concurrently against the live simulated stream.

Stretch items (after phase 7, not interleaved earlier): KEDA-based autoscaling on Kafka consumer lag; a real CV model trained/run on public plate-waste datasets; Eclipse Ditto as a more formal digital-twin framework.

**Scope guardrail**: phases 1–5 alone constitute a complete, defensible IoT-to-data-platform project. Phases 6–7 are what convert it into an AI/digital-twin project specifically. If time is constrained, prioritize reaching phase 6 in minimal form over polishing phases 1–3 further.

## Current focus: Phase 1 — scaffold the cluster and repo

Tasks for this phase, to be carried into the next chat:

- Choose and install a local Kubernetes distribution: k3s or kind (decision not yet made — needs to happen at the start of the next chat).
- Create repo structure: `schemas/`, `edge-simulators/`, `platform-services/`, `k8s/`.
- Create an empty Helm chart skeleton for each future service (edge simulators, Mosquitto, Kafka/Strimzi, storage, causal engine, digital twin, LLM narrator, dashboard, observability) — chart bodies can be empty/placeholder at this stage.
- Draft the four core event schemas (`PlateWasteEvent`, `POSTransactionEvent`, `StaffShiftEvent`, `ServiceTimingEvent`) as JSON Schema or AsyncAPI files and commit them before writing any producer/consumer code.
- Done condition for phase 1: cluster is running locally, repo skeleton exists with committed schema files, and no application code has been written yet that depends on unfinalized schemas.
