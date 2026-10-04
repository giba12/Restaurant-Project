# Master Test Plan

| | |
|---|---|
| Document | Master test plan |
| Project | Restaurant Operations Digital Twin Platform |
| Version | 1.0 |
| Date | 2026-10-03 |
| Status | Retrospective plan, describing the strategy as actually executed |
| Prepared by | The project's AI assistant (Claude), for the project owner |

## 0. Note on provenance

The production-style test regime was designed and run on 2026-10-02 and 2026-10-03. Several older test suites predate it. This plan documents **what was done and why**, not an idealised process; where practice differed from what a larger team would do, the difference is stated (sections 3.3 and 13). It borrows its structure from IEEE 829 and ISO/IEC/IEEE 29119-3 but **does not claim compliance** with either.

## 1. Purpose and objectives

1. Establish, with evidence, whether the system meets the requirements in `01-requirements-specification.md`.
2. Find defects before a reviewer or a production incident does, with emphasis on the class that matters most for a data pipeline: **silent** failure (data lost, duplicated or corrupted without any error).
3. Test the system the way production attacks it: carelessly edited files, logic mistakes, drift between code and database, wrong statistics, a broken hop in a long pipeline, crashes and outages, bursts and long runs, and ageing dependencies.
4. Leave a repeatable regime that guards each defect found against returning.
5. Be honest about what has not been verified.

## 2. Scope

### 2.1 In scope

All first-party code and configuration: the simulators, storage consumer, aggregator, anomaly detector, causal engine, digital twin, narrator, dashboard API and web app, alert relay, game bridge and client; the Compose file, Dockerfiles, Helm charts and migrations; the JSON Schemas; the CI workflows.

### 2.2 Out of scope

| Item | Reason |
|---|---|
| Third-party software itself (Kafka, TimescaleDB, DoWhy, Ollama) | Trusted as shipped; tested only through our use of it |
| Real language-model output quality | Real models are non-deterministic; the narrator is tested against a fake model, and its guard against recorded real failures |
| The Kubernetes cluster as a test target | CI has no cluster; the k3s deployment is verified manually and by the live audit script |
| Browser rendering of the React interface | No frontend test tooling was built (a known gap) |
| Usability, accessibility, localisation | Not requirements |
| Security penetration testing | Beyond a database-permission attack, an injection check and a dependency audit |
| Data from a real restaurant | None exists; correctness of the causal findings on real data is untested by construction |

## 3. Test strategy

### 3.1 Test levels and the production analogy

Eight layers, each simulating one thing production does to a system. They are catalogued test by test in `TESTING.md`.

| # | Layer | Tests | What it simulates | Location |
|---|---|---|---|---|
| 1 | Static | 56 functions | Careless edits to config and files | `tests/static/` |
| 2 | Unit | 221 functions | Careless edits to logic | beside each service; `game/bridge/`; `schemas/` |
| 3 | Database integration | 74 | Drift between code and database | `tests/integration/` |
| 4 | Statistical | 14 | Subtly wrong maths | `tests/statistical/` |
| 5 | End-to-end and acceptance | 22 | One broken hop in a long pipeline | `tests/e2e/` |
| 6 | Resilience (chaos) | 8 | Crashes, outages, restarts | `tests/resilience/` |
| 7 | Load and stability | 4 | Bursts and long runs | `tests/load/` |
| 8 | Security and supply chain | 1 | Ageing dependencies | `tests/security/` |

Plus the Godot client's own end-to-end test (`game/client/tests/smoke_test.gd`, roughly 80 checks) and the live audit script (`k8s/audit/audit-live-cluster.sh`), which is not automated in CI. In total: **399 distinct test names** (one name is shared by two files) plus the GDScript suite.

### 3.2 Test design techniques used

| Technique | Where | Example |
|---|---|---|
| Equivalence and boundary analysis | Unit | Severity thresholds at exactly 0.3, 1.0 spans; limit values 0, 1, 200, 201 |
| State-transition testing | Unit, integration | Ticket stages; staff shift sequence; guest lifecycle |
| Contract testing | Static, unit, integration | Every producer's real output validated against its schema; database constraints equal to schema enums |
| Property testing | Unit, integration | The fallback template always passes its own guard; nothing unverified is ever returned |
| Ground-truth testing | Statistical | Plant a −90 g effect behind a confounder and require the engine to find it; show a naive comparison is wrong |
| False-positive control | Statistical | Zero true effect must give an estimate near zero |
| Fault injection | Integration, resilience | Server-side termination of a connection; `SIGKILL`; stopping the database, Kafka, MQTT |
| Conservation checking | Resilience | After every fault, messages in Kafka equal rows in the database, with no duplicate ids |
| Test doubles for external systems | Unit | A fake Ollama HTTP server; a fake Kafka consumer that models position |
| Mutation checking ("does it have teeth?") | All layers, ad hoc | Deliberately breaking the system (mistuned detector, loosened gate, reintroduced bugs, leaked permission) and confirming the right test fails, then restoring it |
| Regression capture | All | Each defect found gets a test that fails on the old behaviour where practical |
| Known-defect recording | Integration, statistical | `xfail(strict=True)`: a defect stays visible and, once fixed, forces the marker's removal (none remains) |
| Measurement | Load | Latency, throughput, memory growth and concurrency are printed on every run |

### 3.3 Where this differs from a team's process

- **Single developer and an AI assistant**, so there is no independent tester. The author of a test also wrote, or helped write, the code under test. The mutation checks mitigate this; they do not replace independence.
- Severity and priority were assigned by the same people who fixed the defects.
- There was no formal test-case review. The catalogue and its self-test (`test_testing_doc.py`) are the substitute.

## 4. Items under test and features

The features are the requirements of the SRS. Their grouping into test layers is shown in `03-requirements-traceability-matrix.md`.

## 5. Approach by layer

| Layer | Approach | Runs |
|---|---|---|
| 1 Static | Parse files; no containers. Rules are scars: each is a bug the project once shipped. Includes `helm lint` and render, secret scanning, `ruff`, and the version-boundary grep read from its own README | Every push, about 1 min |
| 2 Unit | Isolate one component; Kafka, Postgres and MQTT stubbed or faked; deterministic seeds | Every push, seconds per suite |
| 3 Integration | Real application code against a throwaway TimescaleDB initialised exactly as Compose does it, so migrations are tested by use | Every push, about 2 min |
| 4 Statistical | Run inside the project's own `python:3.11-slim` causal-engine image, because DoWhy 0.11.1 supports Python below 3.12 | Nightly, about 2 min (+3 slow) |
| 5 End-to-end | The real Compose stack as an isolated project (`rp-test`, port 18080), with only pacing changed | Nightly, about 10 min (+10 acceptance) |
| 6 Resilience | The same stack; one fault per test; a single conservation invariant checked after each | Nightly, about 40 min |
| 7 Load | A 3,000-event burst into Kafka plus 400 concurrent HTTP requests; numbers printed | Nightly, about 8 min |
| 8 Security | `pip-audit` of every exact pin against public advisories | Nightly (needs the internet) |

## 6. Entry, exit, suspension and resumption criteria

**Entry (to run the stack layers):** layers 1-3 pass; the container engine is reachable (the runner checks and says so); no developer stack competes for the machine.

**Exit (the regime passes):** every layer passes except tests recorded as strict expected-failures; every expected-failure maps to an open defect in `06-defect-log.md`; the measured results in `07-test-summary-report.md` meet the floors in the NFRs.

**Suspension:** a failure that makes later tests meaningless (for example the stack failing to start). The runner stops and saves container logs.

**Resumption:** after the cause is fixed; the full layer is re-run from a clean stack, not patched mid-run.

## 7. Test environment

Detailed in `09-test-environment-and-configuration-baseline.md`. In summary: a WSL2 laptop with 15 GB RAM, rootless Podman with a Compose v1 shim, a Python 3.12 host for layers 1-3 and 5-8, and Python 3.11 in the causal-engine image for layer 4. **CI on GitHub's runners was configured but has not yet run the new workflows.**

## 8. Test data

| Data | Source | Notes |
|---|---|---|
| Events | The real simulators' generators, plus hand-built events where a field must be pinned | `tests/integration/factory.py` |
| Statistical worlds | Seeded synthetic datasets with a planted effect and a confounder | Truth known by construction |
| Detector streams | Seeded normal and shifted samples | Deterministic |
| Load | Real, schema-valid plate-waste events published straight into Kafka | Marked with a unique `source_id` so rows can be counted |
| Credentials | Throwaway values created per run | No real secret is used or committed |

## 9. Tools

`pytest`; `ruff`; `helm`; `docker compose`; `psql` (in the database container); `pip-audit`; DoWhy and statsmodels (as the thing under test); `kafka-get-offsets.sh` and `kafka-consumer-groups.sh` (for the conservation check); Godot 4.5 headless; GitHub Actions; `gh` (read-only, to watch runs). All free and open-source.

## 10. Roles

| Role | Held by |
|---|---|
| Project owner, requirements, approvals, decisions on known defects | The owner |
| Test design, implementation, execution, defect analysis, documentation | The AI assistant, at the owner's direction |
| Independent review | None (see 3.3) |

## 11. Schedule (as it happened)

| Date | Activity |
|---|---|
| 2026-09-16 to 2026-09-25 | Component tests written alongside each phase; features verified live by hand and recorded in the status log |
| 2026-09-28 to 2026-09-30 | CI added; found never to have run; repaired (DEF-081 to DEF-083) |
| 2026-10-01 | Schema-compatibility test; first manual chaos test; live audit script |
| 2026-10-02 | The eight-layer regime designed and built; first runs; defects DEF-100 to DEF-107 found |
| 2026-10-03 | Resilience rerun after fixes; one complete clean run of every stack layer; documents prepared; the refutation gate repaired and re-verified (DEF-106, DEF-129) |

## 12. Deliverables

`TESTING.md` (the catalogue); this plan; `03-requirements-traceability-matrix.md`; `06-defect-log.md`; `07-test-summary-report.md`; the test code; the CI workflows; container logs of any failed stack run (`tests/artifacts/`, not committed).

## 13. Risks to testing

Maintained in `05-risk-register.md`. The principal ones: **no independence between author and tester; results from one machine only; the heavy layers have not run on GitHub; the stack layers take about an hour; a dependency changing under pinned versions.**

## 14. Defect handling

Each defect is reproduced by a test first where practical, fixed, then the test is shown to fail on the old behaviour and pass on the new. It is recorded in `06-defect-log.md` with severity, how it was found, its cause and its resolution. A defect the owner chooses not to fix is recorded as **Open** and kept visible as a strict expected-failure so the suite cannot silently forget it.

## 15. Pass and fail criteria for a single test

A test passes if every assertion holds. Measured thresholds are **floors**, set from evidence (for example 10 events/s against a measured 14.6 to 18), so that the suite detects a collapse, not a 10% change that would only make it flap. A test that fails because the *test* was wrong is a test defect (recorded in Part G of the defect log) and is corrected, never loosened to pass.

## 16. Approvals

| Role | Name | Date |
|---|---|---|
| Project owner | *(pending review)* | |
| Preparer | The project's AI assistant | 2026-10-03 |
