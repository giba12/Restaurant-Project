# Software Requirements Specification

| | |
|---|---|
| Document | Software requirements specification (SRS) |
| Project | Restaurant Operations Digital Twin Platform ("restaurant-platform") |
| Version | 1.0 |
| Date | 2026-10-03 |
| Status | Retrospective baseline, current as of commit `898c7aa` |
| Prepared by | The project's AI assistant (Claude), for the project owner |
| Repository | `github.com/giba12/Restaurant-Project` |

## 0. How to read this document

This specification was written **after** most of the system existed. The project was originally driven by a design brief (`restaurant-platform-project-notes.md`) and a running technical log (`restaurant-platform-implementation-status.md`), not by a formal requirements document. This SRS gathers the requirements those documents state, makes them individually identifiable, and adds a small number of requirements that are *implied by a claim the project makes publicly* (marked "derived"), so that each can be traced to a verification (`03-requirements-traceability-matrix.md`).

It follows the structure of ISO/IEC/IEEE 29148 and IEEE 830 for readability. **No claim of compliance with either standard is made.**

Each requirement has an ID, a priority (MoSCoW: **M**ust, **S**hould, **C**ould), a source, and a verification method:

| Code | Verification method |
|---|---|
| **T** | Automated test in the repository |
| **D** | Demonstration or manual verification on a live system (recorded in the implementation log) |
| **I** | Inspection of code, configuration or documents |
| **A** | Analysis or measurement |

## 1. Introduction

### 1.1 Purpose

The system is a personal portfolio project: a simulated IoT-to-AI data platform for restaurant operations. Edge sensors (all simulated) feed a Kafka backbone into an anomaly-detection and causal-inference engine; its findings are narrated by a local language model behind a database-enforced read boundary. The point of the project is demonstrated depth in AI integration, data engineering, IoT, Kubernetes and digital-twin patterns, not restaurant operations itself.

### 1.2 Scope

In scope: the "version 1" platform (simulators, MQTT/Kafka backbone, storage, anomaly and causal engines, digital twin, narrator, dashboard, observability) and the "version 2" add-on (an interactive game, under `game/`). Two deployment paths: Kubernetes (k3s and Helm) and Docker Compose.

Out of scope (deliberately not built; see section 7): computer-vision plate-waste detection, KEDA autoscaling, Eclipse Ditto, Steam distribution.

### 1.3 Definitions

| Term | Meaning |
|---|---|
| Finding | A `CausalFinding`: a causal-effect estimate with its confounders and a refutation result |
| Refutation | A placebo-treatment test run by DoWhy against an estimate |
| Narrative-ready | The flag a finding must carry before the narrator may phrase it |
| Quarantine | Excluding interactive (player or crew) tickets from baselines and analyses |
| Crew | The game bridge's automated staff, covering roles nobody is playing |
| `source_kind` | The event field distinguishing simulated, vendor and player/crew producers |
| Hypertable | A TimescaleDB time-partitioned table |

### 1.4 References

`restaurant-platform-project-notes.md` (design brief); `restaurant-platform-implementation-status.md` (technical log, problem log); `CODEBASE-GUIDE.md`; `README.md`; `TESTING.md`; `game/README.md`.

## 2. Overall description

### 2.1 Product perspective

Seven layers: simulated edge devices, an MQTT-to-Kafka message backbone, TimescaleDB storage, an anomaly and causal engine, a digital twin, a guarded LLM narrator, and a React dashboard with a Prometheus and Grafana observability stack. A second, optional layer (version 2) lets a person play through restaurant scenarios, publishing the same event types.

### 2.2 User classes

| User | Needs |
|---|---|
| Reviewer (for example a recruiter) | Run the project with minimal effort; read credible evidence of quality |
| Project owner and developer | Change it safely; know quickly when something silently breaks |
| Player (version 2) | A playable shift, and feedback comparing their pace with the simulation |

### 2.3 Operating environment

Linux containers. Development was on WSL2 with rootless Podman and k3s. The portable path needs Docker (or Podman) with Compose, about 4 GB RAM, and no GPU.

### 2.4 Assumptions and dependencies

All data is simulated; there is no production deployment or real user. The system depends on third-party container images (Kafka, TimescaleDB, Mosquitto, Ollama, MinIO via Chainguard) whose distribution channels have changed during the project (see defects DEF-017, DEF-022 and DEF-023 in `06-defect-log.md`).

## 3. Project constraints

| ID | Requirement | Pri | Source | Method |
|---|---|---|---|---|
| CON-01 | Every component shall be free or open-source, or hand-built. No paid APIs, licensed vendor tools or paid registries. | M | Project notes, "Cost constraint" | I |
| CON-02 | The system shall be contract-first: event schemas are committed before producers or consumers, and every simulated source can be replaced by a real integration, via `source_kind`, without changing a downstream consumer. | M | Project notes, "Fairy dust constraint" | T, I |
| CON-03 | The six learning goals (AI integration, data engineering, IoT, AIoT, Kubernetes, digital twin) shall each be demonstrated at defensible depth, in that priority order. | S | Project notes | I |
| CON-04 | A reviewer with only Docker installed shall be able to run the system using a small number of documented commands, without replicating the development environment. | M | Project notes, "Portability requirement" (2026-09-17) | T, D |
| CON-05 | The Kubernetes (k3s and Helm) deployment shall remain the primary demonstration of Kubernetes depth. | S | Project notes | D |
| CON-06 | Version 2 shall be architecturally possible and strictly separated: version 1 shall never reference version 2. | M | Project notes, "Version 2" | T |
| CON-07 | Where a third-party chart or image proves unmaintained, infrastructure shall be hand-rolled from official images rather than depend on it. | S | Status log, key design decisions | I |
| CON-08 | Claims about the system shall be verified by real testing, and what has not been verified shall be stated. | M | Owner's standing instruction | I |

## 4. Functional requirements

### 4.1 Ingestion (ING)

| ID | Requirement | Pri | Source | Method |
|---|---|---|---|---|
| FR-ING-01 | Four simulated sources (plate-waste camera, POS terminal, ticket timer, staffing sensor) shall publish schema-conformant events to MQTT. | M | Project notes, phase 3 | T |
| FR-ING-02 | Simulators shall emit at non-uniform (Poisson-process) rates, not on a fixed tick. | S | Project notes, phase 3 | I |
| FR-ING-03 | The plate-waste simulator shall encode the relationship between to-go container use and waste (about a 75% reduction) and the other confounders in the generated values, not merely in the schema. | S | Status log, section 3.3 | I |
| FR-ING-04 | An MQTT-to-Kafka bridge shall carry all four sensor topics into Kafka. | M | Project notes, phase 2 | T |
| FR-ING-05 | A producer shall validate its own output against its schema, and a violation shall be fatal. | M | Status log, section 3.1 | T, I |
| FR-ING-06 | A scenario controller shall be able to inject a known disturbance (a staffing shortage) into the service-timing simulator for a defined window. | M | Project notes, phase 5 | T |
| FR-ING-07 | Staffing shall be a real driver in the simulated restaurant: the kitchen's backlog capacity shall be inversely proportional to the staff clocked in, and an injected staffing shortage shall act by lowering staffing (clocking staff out), not by a separate direct effect. | M | DEF-141, 2026-10-04 | T |

### 4.2 Storage (STO)

| ID | Requirement | Pri | Source | Method |
|---|---|---|---|---|
| FR-STO-01 | Every valid event of the four types shall be persisted in TimescaleDB with typed columns and a lossless `raw_payload` copy. | M | Project notes, phase 4 | T |
| FR-STO-02 | Every event shall be re-validated against its schema before storage; an unparseable or schema-violating event shall be skipped without blocking the topic. | M | Status log, section 3.5 | T |
| FR-STO-03 | Storage shall be idempotent: redelivery of an event shall not create a duplicate row. | M | Status log, section 3.5 | T |
| FR-STO-04 | A database failure shall be retried without losing the event, and the consumer shall recover without human help after the database restarts. | M | Status log, section 3.5; derived (see DEF-100, DEF-101) | T |
| FR-STO-05 | A POS transaction shall be stored as one parent row plus one row per line item, atomically. | M | Status log, 2026-09-21 | T |
| FR-STO-06 | The database's `source_kind` constraints shall agree exactly with the schemas' enums. | M | Derived (shared seam added 2026-09-20) | T |

### 4.3 Aggregation and anomaly detection (ANA)

| ID | Requirement | Pri | Source | Method |
|---|---|---|---|---|
| FR-ANA-01 | Stage events shall be aggregated into a per-ticket summary with the four stage durations and a total, published as the ticket progresses, with no unbounded memory. | M | Project notes, phase 5 | T |
| FR-ANA-02 | Each ticket shall carry an origin (simulated, vendor or interactive), and interactive tickets shall be excluded from baselines. | M | Status log, 2026-09-21 | T |
| FR-ANA-03 | Anomalies shall be detected per station by control limits and by an isolation forest, over bounded windows, with a low false-alarm rate and a high detection rate for a genuine shift. | M | Project notes, phase 5 | T |
| FR-ANA-04 | Anomaly events shall conform to their schema. | M | CON-02 | T |
| FR-ANA-05 | The control limits shall not be inflated by a few extreme outliers (for example tickets stalled for tens of minutes): extreme values shall be trimmed before the mean and spread are computed, leaving ordinary data unchanged. | S | DEF-056, 2026-10-04 | T |

### 4.4 Causal inference (CAU)

| ID | Requirement | Pri | Source | Method |
|---|---|---|---|---|
| FR-CAU-01 | The engine shall estimate causal effects with explicit confounder control, and shall recover a known planted effect. | M | Project notes, architecture layer 4 | T |
| FR-CAU-02 | A refutation result shall accompany every estimate, and only a finding whose refutation passed shall become narrative-ready. | M | Status log, phase 6 | T |
| FR-CAU-03 | A finding computed for an injected scenario shall carry the scenario's id, so it can be traced to its cause. This is the project's stated done-condition for phase 5. | M | Project notes, phase 5 | T |
| FR-CAU-04 | Analyses shall exclude human-driven (player and interactive) data. | M | Status log, 2026-09-21 | T |
| FR-CAU-05 | Too little data shall be refused rather than estimated. | S | Code behaviour | T |
| FR-CAU-06 | The refutation gate shall reject findings built from pure noise with high probability (derived: the README describes findings as "refutation-tested"). | S | Derived from README claim | T |
| FR-CAU-07 | The finding for an injected staffing-shortage scenario, computed over a window that includes the baseline, shall pass the refutation gate, have a negative effect (more staff, shorter pickup delay) and be promoted by the live reviewer. This is the phase-5 done-condition proper, beyond FR-CAU-03's traceability. | M | DEF-141, 2026-10-04 | T |

### 4.5 Digital twin (TWN)

| ID | Requirement | Pri | Source | Method |
|---|---|---|---|---|
| FR-TWN-01 | The twin shall maintain the current state of tables, staff and kitchen stations from the event streams. | M | Project notes, architecture layer 5 | T |
| FR-TWN-02 | A station's open-ticket count shall never be negative. | S | Derived | T |
| FR-TWN-03 | The twin's state shall be correct under at-least-once redelivery. | S | Derived | T |

### 4.6 Narrator (NAR)

| ID | Requirement | Pri | Source | Method |
|---|---|---|---|---|
| FR-NAR-01 | The narrator shall receive only a structured finding, never raw data, and this shall be enforced at the database layer: read-only on findings, no access to any other table, no ability to change the schema or its own privileges. | M | Project notes, architecture layer 6 | T |
| FR-NAR-02 | Every generated narration shall be verified before storage (numbers trace to the finding, direction matches the sign of the effect, no unsupported statistical claims, bounded length); a failing narration shall be regenerated, then replaced by a labelled deterministic template. | M | Status log, 2026-09-20 | T |
| FR-NAR-03 | Each stored narration shall record whether the model or the template produced it. | M | Status log, 2026-09-21 | T |
| FR-NAR-04 | If the model is unreachable the narrator shall fail loudly and retry, never storing unverified or empty output. | M | Derived | T |
| FR-NAR-05 | The narrator shall use a locally run open-weight model (Ollama). | M | Project notes | D |

### 4.7 Dashboard (DSH)

| ID | Requirement | Pri | Source | Method |
|---|---|---|---|---|
| FR-DSH-01 | The dashboard shall show current twin state, narrated findings with the evidence behind them, and anomaly counts. | M | Project notes, phase 7 | T |
| FR-DSH-02 | The dashboard API shall be read-only and authenticated by API key; the health route shall be open; an unconfigured key shall fail closed. | M | Status log, 2026-09-23 | T |
| FR-DSH-03 | The API shall bound and validate its inputs and resist SQL injection. | M | Derived | T |
| FR-DSH-04 | A browser shall never hold the API key; the web proxy shall inject it server-side. | M | Status log, 2026-09-23 | T |
| FR-DSH-05 | The API shall compare interactive tickets against the simulated restaurant. | S | Status log, 2026-09-22 | T |

### 4.8 Observability (OBS)

| ID | Requirement | Pri | Source | Method |
|---|---|---|---|---|
| FR-OBS-01 | The Python pipeline services shall expose pipeline-health metrics (what they process, skip and emit), beyond whether they are up. | S | Status log, 2026-10-01 | T |
| FR-OBS-02 | The Kubernetes path shall provide platform observability (pod health, consumer lag, throughput) and alerting on failure. | M | Project notes, phase 7 | D |
| FR-OBS-03 | Alertmanager notifications shall be reshaped and forwarded to ntfy, firing and resolved. | S | Status log, 2026-09-28 | T, D |

### 4.9 Game, version 2 (GAM)

| ID | Requirement | Pri | Source | Method |
|---|---|---|---|---|
| FR-GAM-01 | The bridge shall turn player actions into schema-validated events on the existing topics, owning the envelope and derived fields. | M | Project notes, "Version 2" | T |
| FR-GAM-02 | The bridge shall enforce ticket-stage order, shift sequencing, role gating and station scoping. | M | Status log, 2026-09-21/22 | T |
| FR-GAM-03 | A crew shall cover roles nobody is playing and stand back when a player can do the stage. | M | Status log, 2026-09-21 | T |
| FR-GAM-04 | Guests shall order, wait and pay, with table exclusivity and expiry of abandoned sessions. | M | Status log, 2026-09-22 | T |
| FR-GAM-05 | The Godot client shall work end to end against a live bridge. | S | Status log, 2026-09-20 | T |
| FR-GAM-06 | The bridge's authentication shall fail closed. | M | Status log, 2026-09-23 | T |

### 4.10 Edge intelligence (EDG)

Added 2026-10-03 at the owner's request: the plate-waste node estimates waste on the node itself, with a small model, and says how far to trust the estimate. The sensors are a simulation designed by the author, so none of these requirements says anything about real hardware.

| ID | Requirement | Pri | Source | Method |
|---|---|---|---|---|
| FR-EDG-01 | The plate-waste node shall estimate waste in grams on the node, from raw simulated sensor channels, using a model with int8-stored weights, and shall publish only the estimate and its own assessment of it; the estimate shall be markedly more accurate than the best single channel. | S | Owner request, 2026-10-03 | T |
| FR-EDG-02 | Every on-node estimate shall carry the model's id, version and a SHA-256 over everything that determines its behaviour; the node shall refuse to run a model that does not match its declared hash, and shall stop at startup rather than run on. | S | Owner request, 2026-10-03 | T |
| FR-EDG-03 | The on-node model shall fit stated budgets: an artifact of at most 16 KB, a p99 inference latency of at most 5 ms, and under 1 MB of allocations to load. | S | Owner request, 2026-10-03 | T |
| FR-EDG-04 | The node shall flag individual readings far from the model's training data and shall raise a drift alarm on a sustained shift in its sensor inputs; both thresholds shall be calibrated on held-out clean data and their false-alarm rate measured. | S | Owner request, 2026-10-03 | T |
| FR-EDG-05 | Estimates the node flagged out-of-distribution, or produced while its drift alarm was on, shall be excluded from causal analysis; events from sources with no on-node inference shall still be analysed. | S | Owner request, 2026-10-03 | T |
| FR-EDG-06 | The platform shall expose the edge nodes as a fleet through its API: for each node and model, the readings, how many the node distrusted, the drift rate, inference latency, and whether it is drifting now. | C | Owner request, 2026-10-03 | T |
| FR-EDG-07 | The model shall be reproducible: a seeded trainer, a model card of measured figures stored with the artifact and checked against fresh data, and a test that retraining reproduces the model's quality. | S | Owner request, 2026-10-03 | T |
| FR-EDG-08 | The plate-waste node shall take a new model from the cloud only after checking it, and shall be able to return to an older one: the command shall be authenticated; the artifact shall be checked for integrity, identity, size and speed budgets, and agreement with the cloud's own answers on probe readings; the candidate shall be compared on live readings with the model in service before it replaces it; an older stored version shall be installable without that comparison; and a rejected model shall leave the node's estimates unchanged. | S | Owner request, 2026-10-06 | T |
| FR-EDG-09 | The platform shall provide a cloud-to-edge control path: commands over MQTT held as a retained desired state, one per node, each signed with THAT node's own key (derived from an operator master secret, so a key taken from one node commands no other) and naming the node it is for, with the node's reply published as a status and a way to rotate a node's key without a gap; a node with no key shall take no commands; and a drift alarm shall be reported as advice to a person and shall not by itself start a rollout. | S | Owner request, 2026-10-06 | T |

## 5. Non-functional requirements

### 5.1 Security (SEC)

| ID | Requirement | Pri | Source | Method |
|---|---|---|---|---|
| NFR-SEC-01 | No secret shall be committed; real credentials shall live in a gitignored directory. | M | Status log, 2026-09-23 | T |
| NFR-SEC-02 | Containers shall run as a non-root user (the official nginx image excepted). | S | Derived | T |
| NFR-SEC-03 | The narrator shall connect as a least-privilege database role. | M | FR-NAR-01 | T |
| NFR-SEC-04 | On the Kubernetes path, Kafka, Mosquitto, Kafka Connect and the dashboard and bridge listeners shall use TLS. | S | Status log, items 4a-4d | D |
| NFR-SEC-05 | No pinned Python dependency shall have a known published vulnerability. | S | Derived | T |
| NFR-SEC-06 | Placeholder credentials shall be rotated at deploy time, and a live audit shall detect any left in place. | S | Status log, items 1, 11, 20 | D |
| NFR-SEC-07 | On the Compose path only the dashboard shall be published to the host. | S | Derived | T |
| NFR-SEC-08 | The MQTT broker shall refuse clients that do not log in and shall limit each user to the topics it needs (each sensor publishes only its own topic, only the bridge reads sensor topics, only the operator publishes control topics); a client whose publish the broker refuses shall report an error and not record the event as published. | S | Owner request, 2026-10-06 | T |

### 5.2 Reliability (REL)

| ID | Requirement | Pri | Source | Method |
|---|---|---|---|---|
| NFR-REL-01 | A crashed service shall be restarted by the platform without human action. | M | Status log, portability | T |
| NFR-REL-02 | The pipeline shall survive the loss and return of the database, Kafka and the MQTT broker, with every message stored exactly once. | M | Derived (chaos test, 2026-10-01) | T |
| NFR-REL-03 | Data shall persist across a stop and start of the stack. | M | Status log, 2026-09-17 | T |
| NFR-REL-04 | At-least-once delivery combined with idempotent storage shall give an exactly-once effect. **Every write the platform makes shall be idempotent**: the sensor events (keyed on `event_id`), the digital twin, the ticket summaries (an older summary never overwrites a newer one), and the rows derived from them, which take their ids from what they are about (an anomaly from its ticket, metric and method; a finding from its anomaly and treatment) so that a redelivered input finds the stored row, keeps it, and publishes it again instead of writing or estimating a second one. | M | Status log, section 3.5 | T |
| NFR-REL-05 | The MQTT-to-Kafka bridge shall be restarted automatically when it fails or is killed, shall report itself unhealthy while it is not connected to MQTT, and shall exit instead of acknowledging a message that Kafka has not confirmed (including when its oldest unconfirmed message has waited too long). | S | DEF-137, DEF-142, DEF-152, 2026-10-05 | T |
| NFR-REL-06 | **No sensor event shall be lost** to a restart or outage of the bridge, Kafka or Mosquitto (stop, crash or a long outage): the bridge keeps a persistent MQTT session and acknowledges a message only after Kafka has confirmed it, and Mosquitto persists the session and queue. Delivery is at-least-once; a duplicate is harmless because storage is idempotent on `event_id` (NFR-REL-04). | M | DEF-148, DEF-151, DEF-152, 2026-10-05 | T |

### 5.3 Performance (PER)

Targets were not stated by the original brief; these were set from measurement and are floors, not goals.

| ID | Requirement | Pri | Source | Method |
|---|---|---|---|---|
| NFR-PER-01 | The 95th-percentile delay from an event's timestamp to its storage shall be under 10 seconds. | S | Derived | T, A |
| NFR-PER-02 | The pipeline shall sustain at least 10 events per second end to end (about 14 times the real default traffic of ~0.7 events/s). | S | Derived | T, A |
| NFR-PER-03 | The dashboard API shall answer at p95 under 2 seconds for 20 concurrent users with no errors. | S | Derived | T, A |

### 5.4 Resource use (RES)

| ID | Requirement | Pri | Source | Method |
|---|---|---|---|---|
| NFR-RES-01 | Every Kubernetes container shall have CPU and memory limits. | M | Status log, items 12-13 | T, D |
| NFR-RES-02 | No pipeline service shall grow by more than 100 MB under a burst, nor restart. | S | Derived | T |
| NFR-RES-03 | In-memory state shall be bounded (windows, ticket state). | M | Derived | T |

### 5.5 Portability (POR)

| ID | Requirement | Pri | Source | Method |
|---|---|---|---|---|
| NFR-POR-01 | The stack shall run under Docker or Podman with Compose using one command. | M | CON-04 | T |
| NFR-POR-02 | Compose images shall be fully qualified and pinned. | M | Status log, portability | T |
| NFR-POR-03 | A GitHub Codespaces option shall let a reviewer run it with no local install. | C | Owner request, 2026-10-01 | D |
| NFR-POR-04 | The cold-start build time shall be documented honestly. | S | QUICKSTART | A |

### 5.6 Maintainability (MNT)

| ID | Requirement | Pri | Source | Method |
|---|---|---|---|---|
| NFR-MNT-01 | All Python images shall use Python 3.11 and every dependency shall be pinned exactly. | M | Status log, items 35, 47 | T |
| NFR-MNT-02 | The test catalogue and the codebase guide shall describe the code as it is. | S | Status log, items 22, 27 | T, I |
| NFR-MNT-03 | Files that must be copied by hand shall be kept in sync, and checked where possible. | S | Codebase guide 11.3 | T, I |
| NFR-MNT-04 | The code shall be free of syntax errors, undefined names and dead code. | S | Derived | T |

### 5.7 Operations (OPS)

| ID | Requirement | Pri | Source | Method |
|---|---|---|---|---|
| NFR-OPS-01 | The database shall be backed up, and a restore shall have been proven. | M | Status log, 2026-09-23 | D |
| NFR-OPS-02 | Migrations shall be idempotent, because every `helm upgrade` re-runs them. | M | Status log, section 3.5 | T |
| NFR-OPS-03 | Helm charts shall lint and render to well-formed objects. | M | Derived | T |
| NFR-OPS-04 | A read-only audit shall detect drift between committed charts and the live cluster. | S | Status log, item 20 | D |

### 5.8 Data (DAT)

| ID | Requirement | Pri | Source | Method |
|---|---|---|---|---|
| NFR-DAT-01 | Schemas shall be valid, uniquely identified, and reject unknown fields. | M | CON-02 | T |
| NFR-DAT-02 | The four raw event schemas shall share one envelope. | M | CON-02 | T |
| NFR-DAT-03 | Copies of schemas packaged into charts shall be byte-identical to the source of truth. | M | Status log, item 40 | T |

### 5.9 Licensing (LIC)

| ID | Requirement | Pri | Source | Method |
|---|---|---|---|---|
| NFR-LIC-01 | The project's own code shall carry an open licence, and third-party components that keep their own licences (MinIO server, AGPLv3; TimescaleDB TSL features, unused) shall be identified. | S | Status log, items 9, 8 | I |

## 6. External interfaces

| Interface | Description |
|---|---|
| MQTT | Four topics (`sensors/plate-waste`, `sensors/pos-transaction`, `sensors/service-timing`, `sensors/staff-shift`) from the simulators to Mosquitto |
| Kafka | Raw topics `plate-waste-events`, `pos-transaction-events`, `service-timing-events`, `staff-shift-events`; derived topics `ticket-timing-summaries`, `anomaly-events`, `causal-findings-events`, `narration-ready-events`; control topic `scenario-control-events` |
| Data contracts | JSON Schemas in `schemas/`, the single source of truth |
| HTTP | Dashboard API (`/api/*`, `X-API-Key`), game bridge (`/api/*`, `X-API-Key`), Kafka Connect REST |
| Database | TimescaleDB `restaurant_platform`, migrations `001`-`005`, roles `restaurant_app` and `narrator_app` |
| Model | Ollama HTTP API (`/api/generate`) |

## 7. Out of scope and not built

Computer-vision plate-waste detection (a solved, commercial problem; MinIO exists only to hold the storage contract and now pgBackRest backups); KEDA autoscaling on consumer lag; Eclipse Ditto; Steam distribution (a documented licensing and cost conflict, with itch.io recommended instead); real-vendor integrations; high-availability topologies (every service is a single replica); user-facing multi-tenancy.

## 8. Requirement history

| Date | Change |
|---|---|
| 2026-09 (start) | Cost constraint, contract-first rule, learning priority, six-layer then seven-phase architecture |
| 2026-09-17 | Portability requirement added (CON-04); version 2 added as a future possibility (CON-06) |
| 2026-09-20 | Version 2 separated under `game/`; the boundary became an enforced rule |
| 2026-09-21 | Interactive-session quarantine added (FR-ANA-02, FR-CAU-04) |
| 2026-09-23 | Authentication, secrets and backup added (FR-DSH-02, NFR-SEC-01, NFR-OPS-01) |
| 2026-09-24 to 28 | TLS added across Kafka, Mosquitto, Connect, dashboard and bridge (NFR-SEC-04) |
| 2026-10-01 | Pipeline-health metrics (FR-OBS-01); Codespaces option (NFR-POR-03) |
| 2026-10-02/03 | Reliability, performance and security requirements made explicit and measured (NFR-REL, NFR-PER, NFR-SEC-05) |
| 2026-10-03 | Edge intelligence added: the plate-waste node infers on the node, with integrity, budget, drift and fleet-view requirements (FR-EDG-01 to 07) |
| 2026-10-04 | Staffing made a real driver of the simulated kitchen (FR-ING-07); the done-condition finding required to pass refutation (FR-CAU-07); robust control limits (FR-ANA-05); self-healing connectors on Compose (NFR-REL-05) |
| 2026-10-05 | No sensor event may be lost to a bridge, Kafka or Mosquitto restart (NFR-REL-06); the Kafka Connect connectors and their supervisor were replaced by the MQTT-Kafka bridge, so NFR-REL-05 now describes the bridge's self-healing instead of the supervisor's (DEF-152) |
