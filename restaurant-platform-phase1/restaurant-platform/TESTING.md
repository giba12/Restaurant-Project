# Testing

This document describes how the platform is tested, and — for every test in the repository — **what it is, what it does, why it was created, and why it matters.**

The wider quality documentation (requirements, test plan, traceability matrix, SQA plan, risk register, defect log, results, lessons, environment baseline and release readiness) is in [`docs/quality/`](docs/quality/README.md).

It is a catalogue meant to be read by a person, and it is also under test: `tests/static/test_testing_doc.py` fails if a test exists that is not described here, or if this document describes a test that no longer exists. It cannot quietly rot.

## The idea: test it the way production attacks it

A system is not tested by checking that it works. It is tested by checking that it keeps working while the world does to it what the world does to every production system. This regime is organised around exactly that list:

| What production does to a system | The layer that does it first | How it is checked |
|---|---|---|
| People change files carelessly | **1 · Static checks** | Compose, Dockerfiles, Helm charts, schemas, lint, secrets and architecture rules, read without running anything |
| People change logic carelessly | **2 · Unit tests** | Each component's rules, with edge cases and the bugs this project has already had kept as regressions |
| Code and database drift apart | **3 · Database integration** | Real application code against a real TimescaleDB built from the real migrations |
| The maths is subtly wrong | **4 · Statistical validation** | The causal engine versus datasets whose true answer is known by construction |
| One hop of a long pipeline quietly breaks | **5 · End-to-end & acceptance** | The real stack, from a simulated sensor to the API a browser calls |
| Things crash, restart and disappear | **6 · Resilience (chaos)** | Kill the consumer, the database, Kafka and MQTT; require self-healing with nothing lost or duplicated |
| Traffic arrives in bursts and runs for days | **7 · Load & stability** | A burst through the real stack, with throughput, backlog, memory and concurrency measured |
| Dependencies grow vulnerabilities | **8 · Security & supply chain** | Every pinned dependency against published advisories; the narrator's database permissions attacked directly |

## The layers at a glance

| # | Layer | Where | What it needs | Typical run time | When it runs in CI |
|---|---|---|---|---|---|
| 1 | Static | `tests/static/` | Python, `helm` | ~1 min | every push (`static` job) |
| 2 | Unit | beside each service, `game/bridge/`, `schemas/` | Python | seconds each | every push (one job per service) |
| 3 | Database integration | `tests/integration/` | Docker | ~2 min | every push (`integration` job) |
| 4 | Statistical | `tests/statistical/` | Docker | ~2 min (+3 for the slow measurement) | nightly |
| 5 | End-to-end / acceptance | `tests/e2e/` | Docker, ~4 GB RAM | ~10 min / ~10 min more | nightly |
| 6 | Resilience | `tests/resilience/` | Docker, ~4 GB RAM | ~15 min | nightly |
| 7 | Load & stability | `tests/load/` | Docker, ~4 GB RAM | ~5 min | nightly |
| 8 | Security | `tests/security/` | Python, internet | ~1 min | nightly |

Per-push CI is `.github/workflows/tests.yml`; the heavy layers run from `.github/workflows/production-readiness.yml` nightly and on demand. The Godot game client also has its own end-to-end test (`game/client/tests/smoke_test.gd`, roughly 80 checks against a live bridge), run by the `game-smoke-test` job.

## How to run it

From `restaurant-platform-phase1/restaurant-platform/`:

```bash
# Layers 1, 2, 8 — no containers needed
python -m pytest tests/static -v
python -m pytest tests/security -v                       # needs internet
(cd services/anomaly-detector && python -m pytest -v)    # and likewise for each service

# Layer 3 — starts a throwaway TimescaleDB, runs, removes it
bash tests/integration/run_db_tests.sh

# Layer 4 — runs inside the project's own Python 3.11 causal-engine image
bash tests/statistical/run_statistical_tests.sh            # add --run-slow for the noise-gate measurement

# Layers 5–7 — start the real stack as an isolated project, test it, tear it down
bash tests/run_stack_tests.sh e2e          # data flows end to end
bash tests/run_stack_tests.sh load
bash tests/run_stack_tests.sh resilience
bash tests/run_stack_tests.sh acceptance   # the injected-scenario test (~10 min)
bash tests/run_stack_tests.sh full         # everything above
```

The stack layers run as Compose project `rp-test` on dashboard port `18080`, so they never touch your own `docker compose up` stack or its volumes. `KEEP_STACK=1` leaves it running afterwards for debugging. On rootless Podman (for example WSL after a restart) run `systemctl --user start podman.socket` first; the script checks for this and says so.

## Design decisions worth knowing

- **Every layer is checked for teeth, not just for passing.** A test that cannot fail is decoration. For the layers where it mattered I deliberately broke the system — mistuning the detector, loosening the narrative gate, reintroducing the NULL-confounder bug, leaking a database permission — and confirmed the right tests went red before restoring it.
- **Known weaknesses are recorded as *strict* expected-failures, not hidden.** `xfail(strict=True)` keeps a real defect visible in every run, and the moment someone fixes it the test starts passing and CI demands the marker be removed. There is one left (the twin's redelivery double-count). The other, the refutation gate, was fixed on 2026-10-03 and its marker removed, exactly as this mechanism intends (see "What this regime found").
- **Failure is injected for real.** Resilience tests `SIGKILL` containers and stop the database; they do not mock a failure.
- **One invariant after every failure:** every message in Kafka is stored exactly once. One precise sentence is a better test than many vague ones.
- **Thresholds are floors, not targets.** Load assertions catch a collapse or a leak, not a 10% change, so the suite does not flap.
- **Isolation.** The stack layers use their own Compose project and port; the database layer uses its own container and port.

## Measured results

A complete run on 2026-10-03/04 on a developer laptop (WSL2, rootless Podman, 15 GB RAM, with the three Ollama services left out). These are *measurements, not benchmarks* — a CI runner will differ — recorded so each future run has something to be compared with.

| Layer | Result | Notes |
|---|---|---|
| 1 · Static | 190 passed, 3 skipped | the 3 skips are documented exceptions (two operator-managed charts, one nginx image) |
| 2 · Unit | 232 passed | edge node and trainer 46, detector 19, aggregator and origin 33, causal engine 16, narrator 35, dashboard API 21, alert relay 5, game bridge 50, schema compatibility 7 |
| 3 · Database integration | 108 passed, 1 expected-fail | the expected-fail is the twin's redelivery double-count |
| 4 · Statistical | 14 passed (including the slow gate tests; not re-run after the edge feature) | 0 of 30 noise findings pass the gate; 10 of 10 genuine effects pass |
| 5 · End-to-end | 64 passed in 12 min | |
| 5 · Acceptance | 3 passed in 13 min | below; **the finding it checks was refuted** |
| 6 · Resilience | 7 passed in 43 min (final run) | every fault left Kafka and the database in exact agreement; **but the layer was flaky in the second cycle: one clean pass in six runs, and the Kafka-restart test failed in five of six, on the old code too** |
| 7 · Load | 4 passed in 9 min | below |
| 8 · Security | 10 passed | one case per requirements file; no known vulnerabilities |

**Acceptance (an injected staffing shortage):** mean pickup delay at the loaded stations rose from 5.0 s to 22.1 s (**4.4×**); **103 anomalies** were flagged at the stations absorbing the load and **0** at the removed station. **The causal finding for the scenario was refuted by the repaired gate (effect p = 0.95): the shortage never changes the staff-shift events used as the treatment (DEF-141).**

**Load:** event-to-storage latency p95 **0.98 s**; a 3,000-event burst was fully stored, exactly once, at **45 events/s** end to end (a floor of 10 is asserted; the stack's real default traffic is ~0.7 events/s); no service grew by more than **1.3 MB** (except the causal engine's one-off +152 MB import of DoWhy) or restarted; the dashboard API held p95 **480 ms** at 20 concurrent users with no errors.

**Not yet seen anywhere but this laptop:** none of the GitHub workflows has run on GitHub. They were validated locally by running each command they contain, but their first real run may still surface a runner-specific difference.

## What building this regime found

Running the new layers against the existing codebase for the first time turned up real defects. Each is listed with what found it, and what was done.

**Fixed:**

| Finding | Found by | Resolution |
|---|---|---|
| **The storage consumer permanently stopped storing after any database restart.** It opened one connection at start-up and never replaced it; its error handler then crashed on the dead connection. The process stayed "running", so no restart policy ever fired and nothing alerted. | The resilience layer (`test_a_database_outage_is_survived_by_every_service_that_uses_it`), confirmed live: the database had been healthy for minutes while the consumer logged "giving up" on every message. | `write_with_retry` now replaces a dead connection and returns the live one. Regression: `test_the_consumer_reconnects_after_the_database_drops_its_connection`; also verified live: the database-outage chaos test now passes. |
| **The storage consumer lost events during any database outage longer than ~30 s.** After exhausting its retries it skipped the message, and the next success committed Kafka's position over it. The code's own comment claimed the opposite. | The resilience layer's conservation check: 517 messages in Kafka, 516 rows (off by exactly one). | The message loop is extracted into `handle_message`, which rewinds to a message it cannot store instead of skipping it. Regression: `test_a_later_message_can_never_commit_over_one_that_failed_to_store` (verified to fail on the old behaviour); also verified live: after the outage and the Kafka restart, every Kafka message is stored exactly once. |
| **The refutation gate passed most pure-noise findings (78% to 87%) and gave a different verdict from run to run.** DoWhy's placebo `new_effect` is the mean of 100 simulated runs, so it is ~10× quieter than a single estimate and the old rule `|placebo| < 0.25·|estimate|` was met by almost any noise; the placebo permutations were also unseeded. The estimates themselves were sound; the gate protected far less than "refutation-tested" implied. | The statistical layer (formerly a strict expected-failure) | **Fixed 2026-10-03.** A finding now passes only if the effect's own regression p-value is below 0.01 *and* DoWhy's seeded placebo refuter is consistent with zero. Measured: **0 of 60** noise datasets pass (a 0.05 threshold would pass 4), genuine effects down to −5 g on 1,500 rows pass **20 of 20**, and identical data gives an identical verdict. Guarded by four statistical tests. Also fixed along the way: the engine's INFO logs silently vanished after its first estimate, which hid each finding's verdict (DEF-132). |
| The Compose Kafka Connect image **ran as root**; the Kubernetes variant of the same image already dropped it. | `test_final_image_does_not_run_as_root` | Added `USER appuser`; the full stack's connectors still reach `RUNNING`. |
| **Three batch workloads had no resource limits** (the MinIO bucket-init Job, the TimescaleDB schema-init Job, the backup CronJob). The live audit script had missed them because it deliberately skips Helm hooks and excludes the backup chart. | `test_every_container_has_cpu_and_memory_limits` | Limits added to all three. |
| The narrator pinned **`requests==2.32.3`, which has two published vulnerabilities**. | `test_no_pinned_dependency_has_a_known_vulnerability` | Bumped to 2.33.0; verified the narrator's only call (`requests.post`) is unchanged and its tests pass. |
| **Six pieces of dead code**: unused imports and variables. | `test_python_has_no_syntax_errors_undefined_names_or_dead_code` | Removed. |

**Recorded but not fixed — a decision for the owner.** One strict expected-failure remains, visible in every run:

1. **The digital twin's per-station `open_ticket_count` double-counts on redelivery.** It is incremented rather than derived, so Kafka's at-least-once delivery can inflate it after a crash. Staff state does not have this problem (it is absolute).

**What the repaired gate does and does not promise.** It certifies that an effect is statistically distinguishable from noise given the confounders the engine was told about. It does **not** certify causation: an omitted confounder, or a treatment that is only a proxy (the staffing-level count used in the pickup-delay analysis is not a validated causal variable), is invisible to it. Live Kubernetes clusters keep the old gate until their images are rebuilt and imported (`k8s/realign/realign-live-cluster.sh`); findings already stored were judged by the old rule.



---

## The test catalogue

Every test in the repository, grouped by layer. For each: **what it is** (its kind) and **what it does** share a column, followed by **why I created it** and **why it matters**. Where a row says "(N cases)" or names a parameter, one test function runs against several inputs.

### Layer 1 — Static checks (`tests/static/`)

**What this layer is:** tests that read files and never start anything — no containers, no network, no database. They run in about a minute and run on every push.
**Why it exists:** the cheapest bugs to catch are the ones visible in a file. Most of the rules below are scars: each one is a bug this project actually shipped once, turned into a check so it cannot return unnoticed.

#### `test_compose_config.py` — the portable (Docker Compose) deployment

| Test | What it is and does | Why I created it | Why it matters |
|---|---|---|---|
| `test_every_service_has_exactly_one_of_image_or_build` | **Static check.** Every service must either pull an image or build one, never both, never neither. | A service with both silently ignores one; with neither it cannot start. | A mis-declared service fails at `up` time on someone else's machine, not on yours. |
| `test_every_image_is_fully_qualified` | **Static check.** Every `image:` must name its registry (`docker.io/...`). | Podman has no default registry, so a bare `nginx` failed to resolve — it was the first portability bug found. | A recruiter on Podman or a locked-down Docker would see the stack fail to even start. |
| `test_dockerfile_base_images_are_fully_qualified` | **Static check.** The same rule for every `FROM` line in every Dockerfile. | The same Podman failure, in the build rather than the pull. | Closes the other half of the same hole. |
| `test_external_images_are_pinned_to_a_tag` | **Static check.** No third-party image may float on `latest` (Ollama is the one documented exception). | An unpinned image can change under you overnight. | Reproducibility: the stack that passed yesterday is the stack that runs today. |
| `test_every_build_context_and_dockerfile_exists` | **Static check.** Each `build:` points at a real directory and Dockerfile. | Refactors move directories. | A wrong path is otherwise only discovered mid-build. |
| `test_every_dockerfile_copy_source_exists` | **Static check.** Every `COPY` in every compose-built Dockerfile names a file that exists. | The "missing schema file" bug (problem log item 40): a Dockerfile copied a file nobody had committed. | Catches an un-committed file before CI or a recruiter's build does. |
| `test_every_long_running_service_restarts_on_failure` | **Static check.** Every service has `restart: unless-stopped`, except the two run-once helpers which must be `"no"`. | With no policy, a transient crash left a service down forever (a real bug on the anomaly detector). | Kubernetes self-heals; Compose only does if told to. This keeps the two paths equivalent. |
| `test_depends_on_targets_exist_and_have_no_cycles` | **Static check.** Every dependency names a real service and the graph has no cycles. | A typo or a cycle makes startup hang or fail. | Startup order is how the stack avoids "Kafka not ready" crash loops. |
| `test_services_waiting_on_health_depend_on_services_that_define_one` | **Static check.** Waiting for `service_healthy` is only allowed on a service that has a healthcheck. | Waiting on a health state that can never occur blocks forever. | Prevents a silent startup deadlock. |
| `test_every_bind_mount_source_exists` | **Static check.** Every `./path:/container` mount has a real source. | Mounting a missing file makes Docker create an empty *directory* in its place, breaking the service confusingly. | Migration and config files are delivered this way. |
| `test_named_volumes_are_declared_and_used` | **Static check.** Declared volumes are all mounted, mounted ones all declared. | A volume used but undeclared, or declared but dead, is a persistence mistake. | The Kafka-data-loss bug was a volume that was declared but never actually mounted where Kafka wrote. |
| `test_connection_targets_resolve_to_compose_services` | **Static check.** Every host a service is told to connect to (Kafka, Postgres, MQTT, Ollama) is a real service name. | A typo'd hostname only shows up as an endless retry loop at runtime. | Turns a mystery into an instant failure naming the bad host. |
| `test_no_credential_default_other_than_the_documented_placeholder` | **Static check.** Password/key defaults in the compose file may only be `changeme-local-dev-only`. | Stops a real secret being committed as a "default". | Secrets in git are permanent. |
| `test_only_the_dashboard_is_published_to_the_host` | **Static check.** Only the dashboard maps a port to the host. | Publishing Kafka or Postgres would be convenient and wrong. | Minimises what a recruiter's machine exposes. |
| `test_docker_compose_itself_accepts_the_file` | **Static check, delegated to the real tool.** Runs `docker compose config -q`. | My checks cannot know everything Compose knows. | The tool's own opinion is the ultimate arbiter of validity. |

#### `test_dockerfiles.py` — container conventions

| Test | What it is and does | Why I created it | Why it matters |
|---|---|---|---|
| `test_dockerfiles_were_found` | **Sanity guard.** Fails if fewer than 12 Dockerfiles are discovered. | A check that finds nothing passes vacuously. | Proves the parametrised tests below are actually testing something. |
| `test_final_image_does_not_run_as_root` (one case per Dockerfile) | **Static check.** The last stage must set a non-root `USER`. Only the official nginx image is excepted. | Running as root is the default and the usual mistake. | A compromised service as root owns its container. **This test found a real gap on its first run:** the Compose Kafka Connect image never dropped root (now fixed). |
| `test_python_images_use_the_one_project_wide_python_version` (per Dockerfile) | **Static check.** Every Python image is `python:3.11-slim`. | DoWhy supports only 3.11 and kafka-python was verified only there. | Mixed interpreters are how "works on my machine" bugs get in. |
| `test_requirements_are_pinned_exactly` (per requirements file) | **Static check.** Every dependency is pinned with `==`. | Unpinned `scipy` and `networkx` each broke the causal engine at startup (problem log item 47). | An unpinned dependency is a time bomb on the next rebuild. |

#### `test_helm_charts.py` — the Kubernetes deployment

| Test | What it is and does | Why I created it | Why it matters |
|---|---|---|---|
| `test_charts_were_found` | **Sanity guard.** Fails if fewer than 15 charts are found. | Same vacuous-pass risk. | Keeps the per-chart tests honest. |
| `test_helm_lint_passes` (per chart) | **Static check.** `helm lint` on every chart. | The standard first gate for any chart. | Catches malformed templates early. |
| `test_chart_renders_well_formed_kubernetes_objects` (per chart) | **Static check.** Renders each chart and checks every object has `apiVersion`, `kind` and a name. | Lint can pass on a chart that renders nonsense. | The rendered output is what Kubernetes actually receives. |
| `test_every_container_has_cpu_and_memory_limits` (per chart) | **Static check on rendered manifests.** Every container, including init containers and Jobs, needs CPU and memory limits. | The live audit script checks the cluster; this catches it before deploy. | One unbounded pod can starve a shared node. **Found three real gaps on its first run:** the MinIO bucket-init Job, the TimescaleDB schema-init Job and the backup CronJob (now fixed). The live audit had missed them because it skips Helm hooks. |
| `test_no_container_image_is_unpinned` (per chart) | **Static check.** No rendered container uses `latest` or an untagged image. | Same reproducibility rule as the Compose path. | Deployments must be repeatable. |
| `test_no_plaintext_secret_with_a_real_looking_value` (per chart) | **Static check.** Any password/token/key in a rendered Secret must be a documented placeholder. | The deploy-time rotation scripts assume placeholders; a real value here would be committed. | Prevents credentials entering git through a Helm template. |

#### `test_schemas_consistency.py` — the data contract

| Test | What it is and does | Why I created it | Why it matters |
|---|---|---|---|
| `test_every_schema_is_itself_a_valid_json_schema` (per schema) | **Static check.** Each schema validates against the JSON Schema meta-schema. | An invalid schema fails at runtime inside a service. | The schemas are the contract every producer and consumer trusts. |
| `test_schema_ids_are_unique_and_match_file_names` | **Static check.** `$id` values are unique and end in the file's name. | Copy-pasting a schema and forgetting to change its `$id` is easy. | Duplicate ids make references ambiguous. |
| `test_raw_event_schemas_share_the_common_envelope` (per raw schema) | **Static check.** The four sensor schemas all require the same envelope fields and the right `event_type`. | Consumers rely on the envelope being identical. | A missing envelope field is a NULL column downstream. |
| `test_schemas_reject_unknown_fields` (per schema) | **Static check.** `additionalProperties: false` everywhere. | A typo'd field name should fail loudly at the producer. | Otherwise it silently becomes a NULL, which is exactly how the confounder bug hid. |
| `test_every_raw_schema_declares_source_kind_with_the_base_values` (per raw schema) | **Static check.** `source_kind` exists, defaults to `simulated`, and always allows `vendor_integration`. | The simulated-to-real swap promise depends on this one field. | It is the architectural seam the whole "swap in a real vendor" claim rests on. |
| `test_the_chart_copies_of_schemas_are_identical_to_the_source_of_truth` | **Static check.** The three schema copies packaged into a Kubernetes ConfigMap are byte-identical to `schemas/`. | A stale copy was a real bug (problem log item 40). | Two copies of a contract will drift unless something checks them. |
| `test_the_consumer_covers_exactly_the_raw_event_schemas` | **Static check.** The storage consumer's topic table maps to exactly the four raw schemas. | A fifth event type added to schemas but not the consumer would be silently ignored. | Completeness of the pipeline's front door. |

#### `test_repo_hygiene.py` — repository-wide rules

| Test | What it is and does | Why I created it | Why it matters |
|---|---|---|---|
| `test_python_has_no_syntax_errors_undefined_names_or_dead_code` | **Static analysis (ruff).** Syntax errors, undefined names, unused imports and variables. | Python only discovers an undefined name when that line runs. | Converts a possible production crash into a CI failure. Found and removed six pieces of dead code on its first run. |
| `test_no_secrets_are_committed` | **Static scan.** Looks for private keys, cloud and GitHub tokens, and credentials embedded in URLs, across every tracked file. | A leaked secret in git history is permanent. | The cheapest security control there is. |
| `test_the_real_credentials_directory_is_gitignored` | **Static check.** `k8s/secrets/` must be ignored by git. | That directory holds the real rotated credentials. | One accidental `git add .` would publish them. |
| `test_no_environment_file_with_real_values_is_tracked` | **Static check.** No tracked `.env` (only `.env.example`). | The classic leak. | Same reason. |
| `test_version_1_never_references_version_2` | **Architecture-boundary check.** Runs the documented grep proving the platform never depends on the game, reading its exclusion list from `game/README.md` itself. | The boundary had been broken three times by accident (each time a new script mentioned the game). | The "game is a removable add-on" promise only holds if it is enforced, and reading the list from the README means the doc and the test can never disagree. |
#### `test_testing_doc.py` — this document is itself tested

| Test | What it is and does | Why I created it | Why it matters |
|---|---|---|---|
| `test_every_test_function_is_documented_in_testing_md` | **Documentation test.** Scans every test file in the repository and fails if any test function is not described in this catalogue. | A test catalogue that silently falls out of date looks authoritative while being wrong. | Adding a test without explaining what it is and why it exists now fails CI. It caught three omissions while this document was being written. |
| `test_testing_md_does_not_describe_tests_that_no_longer_exist` | **Documentation test.** The reverse: fails if the catalogue names a test that has been deleted or renamed. | The other direction of the same rot. | The catalogue can be trusted in both directions. |

#### `test_stack_helpers.py` — the harness's own helpers *(new)*

The stack-level tests are only as good as the helpers they call, and one of them was wrong: the connector-health check read the connector's state, which Kafka Connect keeps at `RUNNING` while the task that moves the data has `FAILED`. These tests need no containers.

| Test | What it is and does | Why I created it | Why it matters |
|---|---|---|---|
| `test_a_connector_is_running_only_if_it_and_all_its_tasks_are` | **Unit test.** Connectors whose tasks all run are `RUNNING`. | The baseline for the cases below. | The healthy case still reads healthy. |
| `test_a_failed_task_under_a_running_connector_is_reported_failed` | **Regression test.** A `RUNNING` connector with a `FAILED` task is reported `FAILED`, including when only one of several tasks failed. | On 2026-10-03 a start-up DNS failure left all four tasks `FAILED` under four `RUNNING` connectors and nothing was ingested; the old check would have called that healthy. | The same failure was already in the problem log twice (items 8 and 35). |
| `test_a_connector_with_no_tasks_is_not_reported_running` | **Unit test.** A connector with no tasks yet is `NO_TASKS`. | A connector that has not started a task moves no data. | Readiness waits do not pass early. |
| `test_a_connector_that_is_not_itself_running_is_reported_as_such` | **Unit test.** A paused connector is reported `PAUSED` even if its task runs. | The summary must not hide the connector's own state. | Completeness. |

#### `test_quality_docs.py` — the quality documents are internally consistent

The documents in `docs/quality/` (requirements, traceability matrix, defect log, risk register and the rest) are registers whose value is that their numbers and cross-references can be trusted. Earlier documents in this project drifted from the truth, so these are checked mechanically.

| Test | What it is and does | Why I created it | Why it matters |
|---|---|---|---|
| `test_requirement_ids_are_unique_and_well_formed` | **Documentation test.** Every requirement id in the specification is unique and follows the naming scheme. | Everything else cites these ids. | An ambiguous id makes the whole traceability chain unreliable. |
| `test_every_requirement_is_traced_in_the_matrix_and_nothing_else_is_cited` | **Documentation test.** The matrix covers exactly the specification's requirements: none missing, none invented. | A requirement with no row looks verified by omission. | The matrix would otherwise quietly shrink or grow. |
| `test_every_test_cited_in_the_matrix_exists` | **Documentation test.** Every test the matrix names is a real test function. | Renamed or deleted tests leave dangling evidence. | A traceability claim pointing at nothing is worse than no claim. |
| `test_the_matrix_coverage_summary_matches_its_rows` | **Documentation test.** The coverage totals equal what the rows add up to. | A summary written by hand drifts from its rows. | Readers quote the summary, not the rows. |
| `test_defect_ids_are_unique_and_sequential` | **Documentation test.** Defect ids run `DEF-001`, `DEF-002`, ... with no gaps or repeats. | The original problem log lost its first 25 entries when it was revised (DEF-127). | Stable ids are what let other documents cite a defect safely. |
| `test_the_defect_log_summary_matches_its_entries` | **Documentation test.** The entry total and the severity and status tables equal the entries. | The same drift risk, in the document most likely to be quoted. | A headline number that is wrong discredits the whole log. |
| `test_every_defect_id_cited_anywhere_in_the_quality_documents_exists` | **Documentation test.** Every `DEF-nnn` mentioned in any quality document is a real entry. | I mis-cited defect ids while drafting these documents. | Dangling cross-references are the commonest form of documentation rot. |
| `test_risk_ids_are_unique_and_sequential` | **Documentation test.** Risk ids run `RSK-001`, `RSK-002`, ... with no gaps. | Same reasoning as the defect ids. | Stable ids for the risk register. |
| `test_every_risk_id_cited_in_the_quality_documents_exists` | **Documentation test.** Every `RSK-nnn` mentioned anywhere is a real risk. | Same reasoning. | Same. |
| `test_every_quality_document_has_a_version_and_a_date` | **Documentation test.** Each document opens with a control table holding a version and date. | An undated register cannot be judged for staleness. | Readers need to know when a document was last true. |
| `test_the_index_lists_every_document_and_every_link_resolves` | **Documentation test.** The index names every document in the folder and every relative link in the documents resolves. | A document missing from the index is effectively lost. | Broken links are how a document set rots. |

### Layer 2 — Unit tests (next to each service)

**What this layer is:** tests of one component's logic in isolation. Kafka, Postgres and MQTT are stubbed or faked, so there is nothing to start: they run in seconds and run on every push (one CI job per service).
**Why it exists:** logic bugs are cheapest to find where logic lives. Each file below tests one component's rules, with the emphasis on the rules whose violation would corrupt data *without raising an error*.

#### `services/anomaly-detector/test_detector.py` — does the detector stay quiet on normal data and speak up on abnormal data? *(new)*

Random data is seeded, so every run is identical. A detector wrong in either direction is useless: too many false alarms and every downstream finding is noise; too few detections and the platform's purpose silently fails. I checked these tests have teeth by deliberately mistuning the detector (limits too tight, then too loose) and confirming each mistuning turns a specific test red.

| Test | What it is and does | Why I created it | Why it matters |
|---|---|---|---|
| `test_no_alarm_until_the_baseline_has_enough_history` | **Unit test.** With fewer than the minimum history, even an absurd value must not alarm. | A detector with no baseline can only guess. | Prevents a flood of false alarms every time the service restarts. |
| `test_a_typical_value_is_not_flagged` | **Unit test.** A value at the mean passes. | The most basic sanity check. | If this fails nothing else is trustworthy. |
| `test_a_six_sigma_value_is_flagged_with_bounds_that_contain_the_baseline` | **Unit test, both directions.** A value 6σ high or low is flagged, and the reported bounds enclose the baseline. | Detection must work for slowdowns *and* unusual speed-ups. | The reported bounds are what a human reads to understand the alarm. |
| `test_false_alarm_rate_on_normal_data_stays_low` | **Statistical test.** 3,000 fresh normal points; fewer than 1.5% may alarm (theory says ~0.3%). | "Does it cry wolf?" is a measurable question. | Alarm fatigue is the usual way monitoring dies. |
| `test_detection_rate_on_a_real_shift_is_high` | **Statistical test.** 500 points from a 6σ-shifted process; over 95% must be caught. | The mirror image: "does it catch what it exists to catch?" | A staffing shortage looks exactly like this. |
| `test_the_window_is_bounded_so_memory_cannot_grow_forever` | **Unit test.** The rolling window never exceeds its configured size. | An unbounded list in a long-running service is a slow memory leak. | Services here run for days. |
| `test_old_history_ages_out_so_the_baseline_can_follow_a_permanent_change` | **Unit test.** After a permanent slowdown, the new level stops alarming. | A baseline that never forgets alarms forever after any lasting change. | Real restaurants change (a new menu, new staff). |
| `test_tickets_missing_a_metric_are_ignored_not_crashed_on` | **Robustness test.** In-flight tickets with missing metrics must not raise. | Real streams contain incomplete records. | One bad record must not kill the consumer. |
| `test_severity_from_deviation_scales_with_distance_beyond_the_limit` | **Boundary test.** Exact low/medium/high thresholds, including the edges. | Thresholds are where off-by-one mistakes live. | Severity is what a person triages by. |
| `test_severity_from_isolation_forest_score` | **Boundary test.** Score thresholds for the second detection method. | Same reasoning. | Same. |
| `test_isolation_forest_flags_a_joint_outlier_and_passes_a_typical_ticket` | **Behavioural test.** A ticket extreme on all four metrics scores as anomalous; a typical one does not. | The second method exists to catch combinations no single metric reveals. | Verifies the ML model does its one job. |
| `test_built_events_satisfy_the_published_contract` | **Contract test.** Both kinds of anomaly event validate against `AnomalyEvent.schema.json`. | Downstream consumers trust the schema. | A malformed event would crash the causal engine. |

#### `services/anomaly-detector/test_quarantine.py` — keeping human play out of the baseline

| Test | What it is and does | Why I created it | Why it matters |
|---|---|---|---|
| `test_interactive_is_quarantined_by_default` | **Unit test.** Tickets touched by a player are quarantined. | Human-driven tickets run at a different pace. | They would corrupt the simulated baseline. |
| `test_simulated_vendor_and_unlabelled_summaries_are_not` | **Unit test.** Simulated, vendor and unlabelled tickets are not quarantined. | Over-quarantining would starve the baseline. | Also covers older producers that send no origin. |
| `test_the_quarantined_set_is_configurable` | **Unit test.** The quarantined origins come from configuration. | Policy should not need a code change. | Operability. |
| `test_the_main_loop_never_adds_a_quarantined_ticket_to_a_window` | **Behavioural test.** Drives the real `main()` loop with a fake consumer: windows grow only for simulated tickets. | The unit above proves the predicate; this proves it is actually *used*. | The point of the feature is the outcome, not the function. |

#### `services/ticket-timing-aggregator/test_aggregator.py` — the arithmetic every finding rests on *(new)*

Durations are the raw material of every anomaly and causal finding. An off-by-one-stage mistake would corrupt everything silently. I confirmed the first test fails if the stage pairing is wrongly mutated.

| Test | What it is and does | Why I created it | Why it matters |
|---|---|---|---|
| `test_durations_are_computed_between_the_right_pairs_of_stages` | **Unit test.** Checks each of the four durations equals the exact gap between the right two stages. | Pairing the wrong stages gives plausible-looking wrong numbers. | There would be no error, only wrong science. |
| `test_an_in_flight_ticket_reports_only_the_durations_it_has_so_far` | **Unit test.** A ticket not yet picked up reports nulls for later stages. | Partial tickets are published as they progress. | Inventing a duration would poison the baseline. |
| `test_every_intermediate_summary_satisfies_the_published_contract` (5 cases) | **Contract test.** The summary after each of the five stages validates against its schema. | The aggregator treats an invalid summary as fatal. | A shape bug at any stage would crash-loop the pod. |
| `test_a_finished_ticket_is_dropped_from_memory` | **Unit test.** After delivery, no state is retained. | Otherwise memory grows by one record per ticket forever. | A leak in a service meant to run indefinitely. |
| `test_an_unfinished_ticket_stays_tracked_until_it_finishes` | **Unit test.** The opposite: open tickets are remembered. | Guards the previous test from being satisfied by forgetting everything. | Without it, nothing would ever complete. |
| `test_a_ticket_already_mid_sequence_at_restart_publishes_nothing_and_does_not_leak` | **Regression test.** A ticket whose first-seen event is not `order_fired` yields no summary, and is cleaned up. | Problem log item 43: this exact case crash-looped the pod on every restart. | A restart is routine; it must not become an outage. |
| `test_interleaved_tickets_never_contaminate_each_other` | **Unit test.** Two overlapping tickets keep separate, correct durations. | The kitchen handles many tickets at once. | Cross-contamination would be invisible and wrong. |
| `test_the_latest_station_wins_when_a_ticket_is_reassigned` | **Unit test.** A later station value replaces an earlier one. | Tickets can move. | Stations are the unit anomalies are grouped by. |
| `test_a_missing_station_on_a_later_event_does_not_erase_a_known_one` | **Unit test.** A null station on a later event keeps the known one. | Not every event carries a station. | Prevents a ticket losing its station mid-flight. |
| `test_a_duplicate_delivery_does_not_crash` | **Robustness test.** Redelivery of `delivered` after completion yields nothing and no exception. | Kafka can redeliver across a rebalance. | The documented behaviour is graceful, not a crash. |
| `test_duration_arithmetic_handles_formats_and_offsets` (4 cases) | **Unit test.** Timestamps with `Z`, millis, `+00:00` and non-UTC offsets all subtract correctly. | Timestamp formats vary by producer. | Time arithmetic is a classic source of silent errors. |
| `test_duration_arithmetic_returns_none_instead_of_raising` (3 cases) | **Robustness test.** Missing or garbage timestamps give `None`. | One malformed timestamp must not crash the stream. | Fail soft on bad input, loud on bad code. |
| `test_a_clock_that_steps_backwards_gives_an_unknown_duration_not_a_negative_one` | **Regression test (DEF-058).** A later timestamp earlier than the earlier one gives `None`; equal timestamps give 0 and a 1 ms difference gives 1. | Wall clocks step backwards. The simulator's own log showed six steps of 15 to 549 ms in one 11-minute load run. | A negative duration is impossible; the schema allows null; `None` is the honest value. |
| `test_a_backwards_step_between_two_stages_does_not_make_the_summary_invalid` | **Regression test (DEF-058).** A `cook_started` stamped 84 ms before its `order_fired` yields a summary that validates against the published schema with `time_to_cook_start_ms` null, and the ticket then completes normally. | Before the fix this produced -84, failed the schema's minimum of 0, and the aggregator (which treats its own schema failure as fatal) crashed, restarted and lost every ticket in flight, three times in one load run. | A single clock step can no longer crash-loop the pod. Verified to fail on the old code. |
| `test_a_backwards_step_in_one_ticket_does_not_affect_another` | **Isolation test.** One ticket's backwards step leaves another's durations untouched. | The state is per ticket; the fix must not leak. | Same rule as the interleaving test. |

#### `services/ticket-timing-aggregator/test_origin.py` — labelling where a ticket came from

| Test | What it is and does | Why I created it | Why it matters |
|---|---|---|---|
| `test_all_simulated_stays_simulated` | **Unit test.** Purely simulated tickets keep the `simulated` origin. | The default path. | The baseline is built from these. |
| `test_a_ticket_the_crew_fires_is_interactive_from_its_first_event` | **Unit test.** A crew-fired ticket is interactive from event one. | The crew acts for the player. | Otherwise its first stage would leak into the baseline. |
| `test_any_player_or_crew_event_makes_the_ticket_interactive` | **Unit test.** One player or crew event taints the ticket. | A ticket worked by both a simulator and a person is not clean. | Conservative quarantine. |
| `test_origin_is_sticky_once_interactive` | **Unit test.** Once interactive, always interactive. | Later simulated events must not "clean" it. | Prevents order-dependent labelling. |
| `test_vendor_integration_is_its_own_origin_not_interactive` | **Unit test.** Real-vendor data is its own category. | The simulated-to-real swap must not be quarantined. | Protects the platform's central promise. |
| `test_an_event_with_no_source_kind_counts_as_simulated` | **Unit test.** Missing `source_kind` defaults to simulated. | Backward compatibility with older producers. | Avoids rejecting legacy data. |
| `test_tickets_do_not_leak_origin_into_each_other` | **Unit test.** One ticket's origin never affects another's. | Shared state is the classic bug. | Isolation of tickets. |
| `test_summaries_validate_against_the_schema` | **Contract test.** Summaries carrying each origin validate. | The origin field is part of the contract. | Schema and code must agree. |

#### `services/causal-engine/test_causal_engine.py` — control flow and the narrative gate *(new)*

The statistical correctness of the estimates is tested separately (Layer 5); this file tests everything around them, chiefly the rule that **only a finding that survived its refutation test may be narrated**. I confirmed the gate test fails if the rule is loosened to let untested findings through.

| Test | What it is and does | Why I created it | Why it matters |
|---|---|---|---|
| `test_every_treatment_spec_is_complete_and_its_query_selects_what_it_names` (per metric) | **Static-ish unit test.** Each causal spec has all its fields and its SQL selects every variable it names. | A renamed column once produced all-NULL confounders with no error (problem log item 46). | Catches that class of drift without needing a database. |
| `test_human_driven_sessions_are_excluded_from_every_analysis_query` | **Unit test.** Each analysis query filters out player and interactive data. | Human play runs at a different pace. | Prevents contaminating the estimate. (The integration layer verifies the SQL *behaves* this way against real rows.) |
| `test_waste_estimates_the_edge_node_distrusted_are_excluded_from_analysis` | **Unit test.** The waste query contains both edge-node trust clauses (out-of-distribution and drift), each written so that events with no `edge_inference` are kept. | The node's own self-assessment decides which of its estimates the engine may analyse. | Stops either clause being dropped, or a NULL comparison quietly discarding every event from a source with no on-node inference. (The integration layer proves the SQL behaves this way.) |
| `test_built_findings_satisfy_the_published_contract` (6 cases) | **Contract test.** Findings validate for each treatment and each refutation outcome (true, false, null). | The engine treats a schema failure as fatal. | A nullable field mishandled would crash the engine. |
| `test_a_new_finding_is_never_narrative_ready` | **Unit test.** `narrative_ready` is always false when built. | Readiness is a separate, later decision. | It is the gate that keeps unreviewed findings away from the narrator. |
| `test_the_scenario_injection_id_is_carried_so_a_finding_can_be_checked_against_ground_truth` | **Unit test.** A scenario id passes through onto the finding. | It is how a finding is traced to an injected cause. | The acceptance test depends on it. |
| `test_finding_ids_are_unique` | **Unit test.** 50 findings, 50 distinct ids. | Duplicate ids would overwrite each other. | Identity integrity. |
| `test_an_anomaly_on_an_unmapped_metric_is_skipped_and_counted_not_crashed_on` | **Unit test with a metrics check.** Unknown metrics return nothing and increment the skip counter. | The joint isolation-forest anomalies have no causal mapping. | Must be a quiet skip, and visible on a dashboard. |
| `test_reviewer_passes_only_findings_that_survived_refutation` | **Behavioural test with fakes.** Four findings (passed, failed, untested, missing the field) go through the real reviewer loop; only the passed one is promoted and announced. | The gate is the platform's defence against narrating a spurious result. | The most safety-critical rule in the pipeline. |
| `test_reviewer_does_not_commit_an_offset_when_the_database_write_fails` | **Failure-path test.** If the update fails, the message is not acknowledged and the transaction is rolled back. | Otherwise a database blip would silently drop a finding. | At-least-once delivery depends on it. |

#### `services/finding-narrator/test_narration_guard.py` — checking what the LLM says

The narrator's guard verifies every generated sentence before it is stored. These tests include the real fabricated sentence a small model once produced (`1.81%`, a figure that appears nowhere in the finding), kept as a permanent regression.

| Test | What it is and does | Why I created it | Why it matters |
|---|---|---|---|
| `test_rejects_the_real_fabricated_percentage` | **Regression test.** The actual invented sentence is rejected. | It happened. | The whole guard exists because of it. |
| `test_accepts_faithful_text_with_rounded_or_truncated_numbers` | **Unit test.** Faithful rounding (7,627.92 → 7,628) is allowed. | A guard that rejects honest rounding is useless. | Balances strictness with usability. |
| `test_accepts_milliseconds_converted_to_seconds` | **Unit test.** ms-to-seconds conversion is allowed. | Natural phrasing. | Same. |
| `test_rejects_a_number_that_is_not_in_the_finding` | **Unit test.** An invented number is named in the error. | The core fabrication mode. | The main thing the guard prevents. |
| `test_rejects_a_wrong_magnitude` | **Unit test.** Wrong-sized numbers are rejected. | Subtler than inventing a number. | Catches misreported effect sizes. |
| `test_percent_sign_is_fine_only_for_percent_effects` | **Unit test.** `%` is allowed only when the effect is a percentage. | The real fabrication used `%`. | Targets the observed failure. |
| `test_confounder_count_and_identifier_digits_are_allowed` | **Unit test.** Counts and digits inside identifiers pass. | Avoids false rejections. | Over-rejection would push everything to the template. |
| `test_direction_must_match_the_sign` | **Unit test.** "Reduced" for a positive effect is rejected. | The model wrote exactly this. | A reversed direction is the worst kind of error. |
| `test_word_boundaries_avoid_false_positives` | **Unit test.** Word matching respects boundaries. | Substring matching misfires. | False positives erode trust in the guard. |
| `test_rejects_unsupported_statistical_claims` | **Unit test.** "Significant", confidence intervals and p-values are rejected. | The model invented these. | The finding contains none of that. |
| `test_refutation_wording_is_allowed_because_the_finding_has_it` | **Unit test.** Mentioning the refutation test is fine. | It is true. | Prevents over-blocking true statements. |
| `test_rejects_raw_identifiers_and_rambling` | **Unit test.** Column names and long outputs are rejected. | The model leaked raw names. | Output must read as prose. |
| `test_raw_identifier_and_length_problems_are_reported` | **Unit test.** Each problem is reported separately. | Debuggability. | Clear errors make tuning possible. |
| `test_plain_prose_with_a_decimal_number_is_not_mistaken_for_many_sentences` | **Regression test.** "7627.92" is not three sentences. | A naive splitter did this. | Avoids rejecting good text. |
| `test_empty_text_is_rejected` | **Unit test.** Empty output fails. | A model can return nothing. | Nothing must be stored as a narration. |
| `test_fallback_always_passes_its_own_guard` | **Property test.** The deterministic template passes the guard for any finding. | The fallback is the last line of defence. | If it could fail, there would be no safe output. |
| `test_fallback_wording` | **Unit test.** Template wording is as specified. | It is what users mostly see on CPU. | Readable, accurate fallback text. |
| `test_fallback_handles_a_zero_effect_and_missing_confounders` | **Edge-case test.** Zero effect and no confounders produce sensible text. | Edge inputs break templates. | Never crash while narrating. |
| `test_format_number` | **Unit test.** Number formatting rules. | Both guard and template depend on it. | Consistency. |

#### `services/finding-narrator/test_narrator_loop.py` — the narrator actually uses the guard *(new)*

The guard's own tests prove it can tell a faithful sentence from a fabricated one. These prove the narrator *acts on that verdict*, using a **fake Ollama**: a tiny local HTTP server that returns scripted "model output". Because the model is faked the tests are instant, free and deterministic — no model is downloaded and no GPU is involved. This is the standard technique for testing against an external dependency you do not control.

| Test | What it is and does | Why I created it | Why it matters |
|---|---|---|---|
| `test_a_faithful_sentence_is_accepted_first_time_and_labelled_with_the_model` | **Behavioural test with a test double.** A good reply is returned after exactly one request, labelled with the model's name. | The happy path, and the label is how a reader tells model prose from the template. | If the label were wrong, the dashboard would mislabel fallback text as model output. |
| `test_a_fabricated_sentence_is_rejected_and_the_retry_is_used` | **Behavioural test.** A fabricated first reply is discarded and the second, faithful one is used. | The retry loop is what makes a weak model usable at all. | Proves rejection leads to a retry, not to storing the bad text. |
| `test_a_reversed_direction_is_rejected_and_retried` | **Behavioural test.** "Reduced" for a positive effect is rejected. | The most damaging error a narrator can make. | End-to-end proof of the direction check, not just the function. |
| `test_after_the_last_attempt_the_template_is_stored_and_labelled_as_such` | **Behavioural test.** A model that never improves exhausts its attempts, and the verified template is used, labelled `template-fallback`. | On a CPU, with the small default model, this is the common outcome. | The guarantee that something honest is always produced. |
| `test_nothing_unverified_is_ever_returned` | **Property test.** Across fabricated, reversed, junk and empty replies, whatever comes back passes the guard. | The invariant stated as one sentence. | The single most important property of the narrator. |
| `test_an_unreachable_or_failing_model_raises_rather_than_storing_anything` | **Failure-path test.** A 500 from the model raises. | The consumer relies on the exception to leave the Kafka offset uncommitted. | A model outage must delay narration, not corrupt or lose it. |
| `test_the_prompt_contains_the_facts_and_withholds_the_method` | **Unit test.** The prompt carries the effect size and variables, but not the finding's method name. | The method is deliberately withheld because small models parrot jargon. | Confirms the prompt-engineering decision is actually in force. |

#### `services/dashboard-api/test_auth.py` and `test_comparison.py`

| Test | What it is and does | Why I created it | Why it matters |
|---|---|---|---|
| `test_a_request_with_no_key_is_401_before_reaching_the_database` | **Security unit test.** No key gives 401 without any database access. | Authentication must happen first. | Unauthenticated callers must not even touch data. |
| `test_a_request_with_the_wrong_key_is_401` | **Security unit test.** A wrong key is rejected. | Basic. | Basic. |
| `test_the_right_key_reaches_the_route` | **Unit test.** A right key gets through. | The inverse; otherwise 401-always would pass. | Prevents a lock-out regression. |
| `test_health_needs_no_key_at_all` | **Unit test.** `/api/health` is open. | Uptime checks need it. | A plain check must work. |
| `test_an_unset_api_key_fails_closed_not_open` | **Security unit test.** With no key configured, requests fail rather than pass. | A forgotten config must not mean "no security". | Fail closed, not open. |
| `test_percentile_interpolates_like_postgres_percentile_cont` | **Unit test.** The percentile function matches Postgres's. | The API and SQL must agree. | Numbers shown must be the numbers computed. |
| `test_percentile_of_nothing_is_none_and_ignores_nulls` | **Edge-case test.** Empty and null inputs. | Empty data is normal early on. | No crashes on an empty database. |
| `test_stats_reports_n_median_and_p90_in_whole_ms` | **Unit test.** Summary statistics shape. | The dashboard displays them. | The output format is a contract. |
| `test_same_clock_pools_and_splits_by_stage` | **Unit test.** Player-vs-crew comparison pools correctly. | The game's comparison feature. | Correct grouping. |
| `test_same_clock_ratio_is_none_without_both_sides` | **Edge-case test.** No ratio without both sides. | Avoids dividing by nothing. | No nonsense numbers. |
| `test_same_clock_ignores_other_kinds_and_null_elapsed` | **Unit test.** Other kinds and nulls are excluded. | Data hygiene. | Correct comparisons. |
| `test_vs_simulated_medians_and_the_share_of_simulated_that_was_slower` | **Unit test.** Hand-computed expected values. | The headline comparison. | Verified against arithmetic done by hand. |
| `test_vs_simulated_skips_other_origins_and_null_values` | **Unit test.** Filtering rules. | Same. | Same. |
| `test_empty_inputs_do_not_crash` | **Robustness test.** Everything empty. | First-run state. | A fresh install must not 500. |
| `test_endpoint_defaults_to_the_last_24_hours_and_all_players` | **Unit test.** Default parameters. | Documented behaviour. | Predictable API. |
| `test_endpoint_passes_source_id_since_and_hours_through` | **Unit test.** Parameters reach the query layer. | Wiring check. | Filters must actually filter. |
| `test_a_since_without_a_timezone_is_read_as_utc` | **Unit test.** Naive timestamps are UTC. | Ambiguity breeds bugs. | Time-zone mistakes are silent. |
| `test_bad_parameters_are_rejected` | **Security/robustness test.** Invalid inputs get 4xx. | Input validation. | Untrusted input must be bounded. |

#### `services/alert-relay/test_alert_relay.py` — the alert pipeline's last hop

| Test | What it is and does | Why I created it | Why it matters |
|---|---|---|---|
| `test_firing_alert_has_summary_and_priority` | **Unit test.** A firing alert becomes a notification with a priority. | The relay reshapes Alertmanager's format for ntfy. | An unreadable alert is a missed alert. |
| `test_resolved_alert_gets_lower_priority` | **Unit test.** Resolved alerts are lower priority. | A recovery should not wake someone. | Sensible paging. |
| `test_multiple_alerts_in_one_group_each_get_a_line` | **Unit test.** Grouped alerts all appear. | Alertmanager batches. | No alert silently lost in a batch. |
| `test_no_alerts_falls_back_to_status_and_alertname` | **Edge-case test.** Empty groups still produce something. | Malformed payloads happen. | Never swallow a notification. |
| `test_handler_forwards_body_to_send_to_ntfy` | **Unit test.** The HTTP handler calls the sender. | Wiring check. | The relay's one job. |

#### `game/bridge/test_bridge.py` — the version-2 game bridge (50 tests)

The bridge is the game's server: it owns the rules, and clients only ask. These tests drive its HTTP API with Kafka replaced by a recorder and the clock replaced by a controllable one, so the crew's timing is tested exactly. Grouped by what they protect.

| Test | What it is and does | Why I created it | Why it matters |
|---|---|---|---|
| `test_a_cook_cooks_and_the_crew_finishes_the_rest` | **Behavioural test.** A player cooks; the crew completes the remaining stages. | The core game loop. | If it fails the game cannot be played. |
| `test_envelope_is_owned_by_the_bridge` | **Unit test.** The bridge, not the client, builds event envelopes. | Clients must not forge data. | Data integrity at the trust boundary. |
| `test_stage_out_of_order_is_rejected_and_does_not_publish` | **Unit test.** Skipping a stage is refused and nothing is published. | Tickets are state machines. | Invalid data must never reach Kafka. |
| `test_unknown_ticket_is_404` | **Unit test.** Unknown ticket id gives 404. | Standard API behaviour. | Clear errors. |
| `test_order_fired_requires_known_table_and_station` | **Unit test.** Orders need real tables and stations. | The world is fixed. | Bad references corrupt analytics. |
| `test_duplicate_ticket_id_is_409` | **Unit test.** Reusing a ticket id conflicts. | Idempotency. | No double tickets. |
| `test_bad_player_id_and_stage_are_422` | **Validation test.** Malformed ids and stages are rejected. | Untrusted input. | Input bounds. |
| `test_failed_publish_does_not_advance_ticket` | **Failure-path test.** If Kafka fails, the ticket does not advance. | State and stream must agree. | No phantom progress. |
| `test_shift_sequence` | **Unit test.** Clock-in, break, clock-out order. | Shift state machine. | Valid staffing data. |
| `test_station_reassign_needs_a_known_station` | **Unit test.** Reassignment validates the station. | Same. | Same. |
| `test_schema_rejects_invalid_role` | **Contract test.** Invalid roles fail the schema. | The bridge validates before publishing. | The schema stays the single contract. |
| `test_role_is_fixed_for_the_shift` | **Unit test.** A role cannot change mid-shift. | Game rule. | Consistent data. |
| `test_world_endpoint_matches_schema_enums` | **Contract test.** The advertised world matches the schema's enums. | Client and schema must agree. | Prevents offering choices the schema rejects. |
| `test_station_expo_is_not_a_cooking_station` | **Unit test.** Expo is not a cooking station. | Game rule. | Correct role model. |
| `test_roles_only_perform_their_own_stages` | **Unit test.** A role may only perform its own stages. | Role gating. | The game's fairness. |
| `test_expo_and_server_are_not_scoped_to_a_station` | **Unit test.** Those roles roam. | Game rule. | Correct scoping. |
| `test_a_cook_only_works_their_own_station` | **Unit test.** Cooks stay at their station. | Same. | Same. |
| `test_players_must_be_clocked_in_and_off_break` | **Unit test.** Must be on shift and not on break to act. | State gating. | Valid data. |
| `test_crew_does_not_touch_a_stage_a_player_can_do` | **Unit test.** The crew stands back from stages a present player handles. | The crew only covers absent roles. | Player agency. |
| `test_crew_cooks_when_no_cook_is_at_that_station` | **Unit test.** The crew covers an empty station. | The kitchen must keep moving. | Playability. |
| `test_crew_events_are_tagged_crew` | **Unit test.** Crew events carry `source_kind: crew`. | Needed for quarantine. | Keeps the baseline clean. |
| `test_crew_covers_a_break_but_stands_back_once_the_player_returns` | **Behavioural test.** Coverage during a break, withdrawal after. | Smooth hand-over. | Correct crew behaviour. |
| `test_crew_finishes_tickets_after_the_player_clocks_out` | **Unit test.** Tickets still complete after the player leaves. | No stranded tickets. | State cleanliness. |
| `test_a_failed_crew_publish_backs_off_and_retries` | **Failure-path test.** A failed publish retries with backoff. | Kafka can blip. | No lost crew events. |
| `test_no_tickets_are_fired_with_nobody_on_shift` | **Unit test.** The dining room is idle when empty. | Don't pile up work. | Sensible simulation. |
| `test_tickets_spawn_at_the_cooks_station_on_a_timer_up_to_the_cap` | **Unit test.** Spawn pacing and the cap. | Capacity control. | Prevents runaway tickets. |
| `test_a_failed_spawn_does_not_retry_every_tick` | **Failure-path test.** A failed spawn backs off. | Avoids a retry storm. | Stability. |
| `test_a_backed_up_station_does_not_starve_a_cook_at_another` | **Unit test.** Fairness between stations. | One busy station must not block others. | Correct scheduling. |
| `test_ticket_board_says_who_each_ticket_waits_on` | **Unit test.** The board names the waiting role. | The client displays it. | Usable UI data. |
| `test_ordering_needs_no_clock_in_and_fires_a_ticket` | **Unit test.** Guests order without a shift. | Guests are not staff. | Guest mode works. |
| `test_order_total_sums_every_item_at_its_menu_price` | **Unit test.** Totals are computed server-side. | Money is never trusted from a client. | Correct bills. |
| `test_order_rejects_unknown_table_or_menu_item` | **Validation test.** Bad tables and items refused. | Input validation. | Data integrity. |
| `test_a_guest_cannot_order_twice_before_paying` | **Unit test.** One open order per guest. | Game rule. | State sanity. |
| `test_kitchen_full_is_503_and_does_not_open_a_ticket` | **Unit test.** A full kitchen refuses without side effects. | Backpressure. | No half-created tickets. |
| `test_guest_status_for_an_unknown_player_is_404` | **Unit test.** Unknown guest gives 404. | Standard. | Clear errors. |
| `test_paying_before_delivery_is_409` | **Unit test.** Cannot pay for undelivered food. | Game rule. | Correct flow. |
| `test_paying_after_delivery_publishes_a_pos_transaction_and_clears_the_guest` | **Behavioural test.** Payment emits a POS event and frees the guest. | The guest loop's end. | Produces the real POS data. |
| `test_an_invalid_payment_method_is_422` | **Validation test.** Bad payment method rejected. | Schema enum. | Contract adherence. |
| `test_a_guest_cannot_order_at_a_table_another_guest_is_using` | **Unit test.** No double-booking by guests. | Resource exclusivity. | Sane simulation. |
| `test_a_staff_order_cannot_be_fired_at_a_table_a_guest_is_using` | **Unit test.** Staff cannot use a guest's table. | Same. | Same. |
| `test_a_guest_cannot_order_at_a_table_a_staff_ticket_is_using` | **Unit test.** The reverse. | Same. | Same. |
| `test_the_table_is_free_again_once_the_ticket_closes` | **Unit test.** Tables are released. | No permanent occupancy. | Long-run viability. |
| `test_the_dining_room_never_double_books_a_table` | **Property test.** Across many spawns, no table is double-booked. | A stronger version of the above. | Invariant held under churn. |
| `test_leaving_frees_the_guest_but_not_a_table_the_kitchen_is_still_using` | **Unit test.** Leaving early does not free a table with a live ticket. | Subtle state interaction. | Consistency. |
| `test_leaving_twice_or_leaving_nobody_is_404` | **Unit test.** Idempotent errors. | API hygiene. | Predictable behaviour. |
| `test_an_abandoned_undelivered_order_is_evicted_and_frees_the_table_immediately` | **Unit test.** Abandoned orders are cleaned up. | Closed browser tabs happen. | Prevents leaked state. |
| `test_a_delivered_but_unpaid_order_is_evicted_after_max_guest_age` | **Unit test.** Unpaid orders expire. | Same. | Same. |
| `test_a_slow_but_still_progressing_order_is_not_evicted_early` | **Unit test.** Eviction does not punish slow progress. | The guard against over-eager cleanup. | Fairness. |
| `test_auth_is_enforced_on_a_real_request` | **Security test.** Auth applies to real requests. | Same as the dashboard's. | The API is writable, so this matters more. |
| `test_an_unset_api_key_fails_closed_not_open` | **Security test.** Missing key configuration fails closed. | Same. | Same. |

#### `schemas/test_producer_schema_compatibility.py` — producers match the contract

| Test | What it is and does | Why I created it | Why it matters |
|---|---|---|---|
| `test_plate_waste_event_matches_schema` | **Contract test.** A real event from the real simulator validates. | Schemas and producers are edited by hand separately. | Drift between them was a recurring bug. |
| `test_pos_transaction_event_matches_schema` | **Contract test.** Same, POS. | Same. | Same. |
| `test_service_timing_event_matches_schema` | **Contract test.** Same, service timing. | Same. | Same. |
| `test_staff_shift_event_matches_schema` | **Contract test.** Same, staffing. | Same. | Same. |
| `test_control_limit_anomaly_event_matches_schema` | **Contract test.** Detector output (method one). | Downstream consumers trust it. | A bad event crashes the causal engine. |
| `test_isolation_forest_anomaly_event_matches_schema` | **Contract test.** Detector output (method two). | The nullable `expected_range` once crashed live. | Regression for problem log item 45. |
| `test_causal_finding_matches_schema` | **Contract test.** Engine output. | Same. | Same. |
#### `edge-simulators/test_edge_ai.py` — the plate-waste node's on-device model *(new)*

The plate-waste node runs a small int8 model on the node itself (`edge-simulators/edge_ai/`), with a per-reading out-of-distribution guard and a rolling drift monitor. These tests check each claim that feature makes against something it could quietly get wrong. Thresholds are set from measurement, with the measured value beside each in the test file and in the model artifact's `card`; none was guessed. I checked the tests have teeth by breaking the node six ways (a model that echoes the scale channel, a drift monitor that never alarms, a node that leaks the true grams, the integrity check removed, a constant estimate, a lens-fouling knob that does nothing) and confirming a specific test went red for each.

| Test | What it is and does | Why I created it | Why it matters |
|---|---|---|---|
| `test_the_shipped_model_loads_and_reports_the_hash_it_was_published_with` | **Integrity test.** The artifact loads and its SHA-256 equals the one it declares. | Every stored estimate names the model by this hash. | Ties each estimate to the exact model that produced it. |
| `test_any_change_to_what_determines_behaviour_is_refused_at_load` (6 cases) | **Tamper test.** Changing a weight, a bias, the input statistics, either guard threshold or the output scale makes the loader refuse the model. | A corrupted or hand-edited model must stop the node, not emit plausible numbers under the old model's name. | Verified to fail when the check is removed. |
| `test_editing_only_the_descriptive_card_does_not_change_the_hash` | **Boundary test.** Documentation inside the artifact is outside the hash. | Otherwise every wording change would look like a new model. | The hash covers behaviour only, as claimed. |
| `test_the_artifact_fits_the_size_budget` | **Budget test.** The file is under 16 KB (measured 3.4 KB). | A constrained node has a size ceiling. | A model that grew unreasonably fails here. |
| `test_inference_fits_the_latency_budget` | **Budget test.** p99 over 2,000 readings is under 5 ms (measured about 0.13 ms mean). | Latency is a real edge constraint. | A slowed inference path fails here. |
| `test_loading_the_model_needs_little_memory` | **Budget test.** Loading allocates under 1 MB. | Memory is a real edge constraint. | Guards the model, not the box. |
| `test_the_model_is_far_more_accurate_than_the_scale_channel_alone` | **Value test.** Model error is at most half the scale channel's (measured 7.1 g against 20.8 g). | Without it the model could simply echo the scale. | Verified to fail on a model that echoes the scale. The reason to run a model on the node at all. |
| `test_the_published_accuracy_figures_are_true_on_fresh_data` | **Honesty test.** Accuracy on seeds the trainer never used is within 15% of the card. | A card nobody checks is marketing. | The recorded accuracy is the real accuracy. |
| `test_int8_weights_cost_almost_no_accuracy` | **Quantization test.** int8 error is within 5% of float32 error (7.11 g against 7.06 g). | Quantization is only worth it if it is nearly free. | The size saving costs nothing measurable. |
| `test_estimates_never_go_negative` | **Invariant test.** No estimate below zero. | The schema forbids it; a regression layer must never emit it. | A negative would be rejected as a schema violation. |
| `test_the_node_keeps_the_to_go_confounder_the_causal_engine_looks_for` | **Integration-of-purpose test.** Mean estimate for to-go plates is about a quarter of the others (0.20 to 0.32; measured 0.26). | The causal engine must still find a true effect in estimates that now come from a model. | Verified to fail on a constant estimate. A model that flattened the effect would leave the engine nothing to find. |
| `test_the_node_scores_well_against_the_truth_it_never_sees` | **Accuracy test.** Over 3,000 events the node's estimates are within 10 g RMSE of the simulator's true grams (measured 7.1 g). | The only place ground truth is available is the simulator. | The end-to-end accuracy of the whole node, not just the model. |
| `test_the_node_infers_its_estimates_it_does_not_copy_the_truth` | **Leak test.** Fewer than 5% of estimates equal the true grams exactly. | A node that quietly read the truth would score perfectly and mean nothing. | Verified to fail when the estimate is replaced by the truth. |
| `test_the_lens_fouling_knob_really_degrades_what_the_camera_sees` | **Premise test.** At fouling 0.8, camera area and light fall well below clean. | Every drift test relies on the fault being real. | A knob that did nothing would make the drift tests vacuous. Verified to fail when it does nothing. |
| `test_the_per_reading_guard_is_quiet_on_normal_readings` | **False-alarm test.** Under 0.5% of 10,000 normal readings are flagged (calibrated to 0.1%). | A guard that cries wolf is ignored. | The flag means something. |
| `test_the_per_reading_guard_flags_gross_outliers` (3 cases) | **Detection test.** An impossible weight, a stuck light sensor, and a camera that sees nothing while depth says a heap are each flagged. | The guard's reason to exist. | Gross sensor failure is caught on the spot. |
| `test_the_per_reading_guard_misses_a_slow_fault_which_is_why_the_drift_monitor_exists` | **Limit-pinning test.** With a fouled lens (fouling 0.4) error is more than 3× the clean figure while under 10% of readings are individually flagged (measured 0.8%). | I measured this and did not want anyone to believe the per-reading guard is enough. | An honest limit kept as a test, and the justification for the second guard. |
| `test_the_drift_monitor_reports_nothing_until_its_window_is_full` | **Mechanics test.** No score and no alarm until the window fills, then the right score. | A mean over a few readings is too noisy to act on. | Prevents false alarms after every restart. Verified to fail when the monitor never alarms. |
| `test_the_drift_monitor_is_a_true_rolling_mean_of_squared_distances` | **Arithmetic test.** The running total equals a recomputed mean of the last four squares at every step of 40 readings. | A running sum can silently drift if the add/subtract bookkeeping is off by one. | The score is what it claims to be. |
| `test_the_drift_monitor_stays_quiet_through_normal_operation` | **False-alarm test.** Under 1% of 20,000 clean readings are in alarm (calibrated and measured at about 0.1%). | An alarm that is always on is useless. | The monitor is quiet when nothing is wrong. |
| `test_the_drift_monitor_catches_a_fouling_lens` (2 cases) | **Detection test.** Over 30 onsets each, a lens fouled to 0.4 is caught within 150 readings and one fouled to 0.6 within 60, with no false alarm before onset. | The reason for the monitor. | Measured: every trial detected, median delay 26 and 12 readings. At the default rate that is about three minutes. |
| `test_a_restarted_node_starts_with_an_empty_drift_window` | **Documented-limit test.** A fresh node reports no drift score until its window fills. | Monitor state is in memory and lost on restart, like other simulator state. | Pins a documented limitation so it cannot silently change. |
| `test_a_node_with_a_fouled_lens_reports_drift_in_its_own_events` | **End-to-end-on-the-node test.** A node with `lens_fouling=0.6` publishes `drift_suspected=true` in its events; a healthy node does not. | The flag only matters if it reaches the event. | The signal the platform acts on. |
| `test_every_event_from_the_node_satisfies_the_schema` | **Contract test.** 300 node events validate against the published schema. | Producer and contract drift. | The additive `edge_inference` block is valid. |
| `test_the_event_names_the_exact_model_that_produced_the_estimate` | **Provenance test.** Model id, version and hash in the event equal the model's own, and `schema_version` is 1.1.0. | Accountability for every estimate. | Any stored estimate can be traced to its model. |
| `test_events_without_edge_inference_are_still_valid_for_older_producers` | **Backward-compatibility test.** A 1.0.0 event with no block still validates. | The change is additive and must stay so. | Existing producers and stored data keep working. |
| `test_the_schema_rejects_a_malformed_edge_inference` (7 cases) | **Contract-strictness test.** A bad hash, a non-semver version, a negative distance, a negative latency, a non-boolean flag, an unknown field and a missing id are each rejected. | A schema that accepts anything is not a contract. | Proves the new block is actually constrained. |
| `test_a_corrupt_model_stops_the_node_at_startup_instead_of_running_on` | **Failure-path test.** `main()` raises on a tampered model before the event loop starts. | The simulator's loop logs and continues past errors raised while generating events, so a check inside it would retry forever. | The node fails visibly where someone can see it. |

#### `edge-simulators/training/test_training.py` — the offline trainer still makes a model as good as the one that shipped *(new)*

Needs scikit-learn and is skipped without it. It runs the real trainer at reduced size (seconds, not the minute the full run takes). It does not compare bytes, because scikit-learn's optimiser can differ in the last bits between versions; the committed artifact and its hash are the source of truth, and these tests check the *quality* is reproducible.

| Test | What it is and does | Why I created it | Why it matters |
|---|---|---|---|
| `test_a_retrained_model_loads_and_is_internally_consistent` | **Integrity test.** A freshly trained artifact passes the loader's hash check and contains no NaN or infinity. | An early version of the trainer wrote `NaN` into the artifact (an unfilled drift window leaked into a percentile). | Invalid JSON in a deployed model would be caught here. |
| `test_a_retrained_model_is_as_accurate_as_the_one_that_shipped` | **Reproducibility test.** Retrained error is within 30% of the shipped card, at most half the scale channel's, and int8 within 5% of float32. | The model should be reproducible in quality. | Retraining cannot quietly produce a worse model. |
| `test_a_retrained_guard_is_calibrated_and_catches_a_fouling_lens` | **Reproducibility test.** Retrained guards flag under 0.5% of clean readings, spend under 1% of clean time in alarm, and catch at least 90% of fouling-0.4 onsets. | The guards are calibrated by the trainer, not hand-tuned. | A retrain cannot ship an uncalibrated monitor. |
| `test_the_committed_card_matches_what_the_committed_artifact_does` | **Honesty test.** The committed artifact's measured error on fresh data is within 15% of its own card. | The card lives inside the artifact and nothing else would check it. | Keeps the documentation inside the model truthful. |

### Layer 3 — Database integration tests (`tests/integration/`)

**What this layer is:** tests that run the real application code against a real, throwaway TimescaleDB, created by `run_db_tests.sh` with the real migrations mounted exactly the way Docker Compose mounts them. If a migration is broken the database never comes up and the layer fails before a single test runs, so the migrations are tested by being used.
**Why it exists:** the worst bugs this project had were all *seams between code and database* that no unit test could see — a field read under the wrong key stored NULL in every row; inserts to columns that did not exist silently failed. A fake cursor cannot tell you a column is NULL or does not exist. Only the real database can.
**How I know it has teeth:** I reintroduced two historical bugs on purpose (the NULL-confounder read, and a leaky narrator permission). Five tests went red in exactly the expected places, and I restored the files.

#### `test_storage_consumer_db.py` — what lands in the database

| Test | What it is and does | Why I created it | Why it matters |
|---|---|---|---|
| `test_plate_waste_confounders_are_stored_not_silently_null` | **Regression integration test.** Stores a plate-waste event with chosen confounder values and reads them back from the real columns. | Problem log item 46: the consumer read the confounders under the wrong key and stored NULL in all 342 rows, unnoticed until the causal engine tried to use them. | These three columns are the entire input to the causal analysis. |
| `test_the_edge_inference_block_is_stored_whole_and_queryable` | **Integration test.** A real node event is stored; `raw_payload -> 'edge_inference'` equals what the node sent, `schema_version` is `1.1.0`, and the model hash is readable by JSON path. | The edge feature was integrated with **no database migration**, on the strength of `raw_payload` already keeping the whole event. | That only holds if the block survives storage intact and can be queried by path, which the causal-engine filter and the dashboard's edge view both do. |
| `test_every_simulator_event_type_is_stored_with_no_unexpected_nulls` | **Integration test.** One real event from each of the four simulators is stored, and every column that should be populated is checked. | The same bug class, applied to all four tables. | Broad protection against "stored but empty". |
| `test_the_original_event_is_preserved_losslessly_in_raw_payload` (4 cases) | **Integration test.** The stored `raw_payload` equals the original event exactly. | `raw_payload` is the safety net: it is how the bad rows were backfilled. | A safety net must be proven to hold. |
| `test_redelivering_the_same_event_does_not_create_a_duplicate_row` (4 cases) | **Idempotency test.** Storing the same event three times yields one row. | Kafka delivers at least once. | Duplicates would silently skew every analysis. |
| `test_pos_transaction_stores_one_row_per_line_item_and_redelivery_adds_none` | **Integration test.** A transaction becomes one parent row plus the right number of ordered line items, and redelivery adds nothing. | The POS table is one-to-many. | Revenue data must be complete and exact. |
| `test_a_transaction_is_never_stored_without_its_line_items` | **Atomicity test.** A line item that fails inside the database rolls the parent back too. | A half-stored transaction is worse than none. | Verifies the single-transaction guarantee. |
| `test_a_transient_database_failure_is_retried_and_the_event_is_not_lost` | **Fault-injection test.** The insert fails twice, then succeeds; the event is stored. | The consumer promises retry-with-backoff. | A brief outage must not lose data. |
| `test_a_permanent_failure_is_raised_not_swallowed` | **Failure-path test.** After the retry budget the error is raised. | The caller depends on this to leave the Kafka offset uncommitted. | It is what makes redelivery, rather than loss, possible. |
| `test_the_consumer_reconnects_after_the_database_drops_its_connection` | **Regression integration test.** Opens the consumer's connection, stores an event, kills that connection from the *server* side (what a database restart does to a client), then stores another event. The consumer must replace the dead connection and succeed. | **Found by the resilience layer** (see "What building this regime found"): after a database outage the consumer logged "giving up ... offset not committed" for every message, indefinitely. It opened one connection at start-up and never replaced it, and its own error handler crashed on the dead connection. | A restart of the database, routine in production, silently and permanently stopped all storage, while the process stayed "running" so no restart policy ever fired. This reproduces it in two seconds, without a stack. |
| `test_validation_costs_microseconds_a_message_not_tens_of_milliseconds` | **Performance regression test.** 300 validations of a real event must take under one second. | `jsonschema.validate` re-checks the whole schema on every call (measured 30 to 60 ms), and the consumer called it per message; adding one nested object to the plate-waste schema cost about 17 ms a message and cut ingest throughput about 20% (14.6 to 18 events/s became 11 to 12). | Compiled once the 300 take about 20 ms, the old way 9 s or more, so the bound is wide on a loaded machine and still decisive. |
| `test_the_validator_is_built_once_per_schema_and_rebuilt_when_the_schema_object_changes` | **Cache-correctness test.** The same schema object reuses one validator; a different (stricter) schema object for the same topic gets a fresh one and its stricter rule is enforced. | A cache is only safe if it can never serve a stale validator. | Compared by identity, so a reused object id can never pick the wrong validator. |
| `test_validation_still_rejects_what_the_schema_rejects_and_names_the_problem` | **Behaviour-preservation test.** A negative weight and a malformed model hash are still rejected, with a message that names the problem. | The fix replaced the validation call; behaviour must be identical. | The skip-and-log path depends on the error carrying a readable message. |
| `test_a_stored_message_is_committed` | **Integration test with a fake Kafka consumer.** A valid message is stored and its offset committed. | The baseline for the four tests that follow, which use a fake consumer that models what real Kafka does: a *position*, where `commit()` records everything fetched so far. | If this fails, the loop's basic contract is broken. |
| `test_a_message_that_cannot_be_stored_is_rewound_to_not_skipped` | **Failure-path test.** With the database "down", the consumer must seek back to the failed message and must not commit. | Found by the resilience layer: the original code logged "offset not committed" and moved on. | Rewinding is what turns "an outage loses events" into "an outage delays events". |
| `test_a_later_message_can_never_commit_over_one_that_failed_to_store` | **Regression test for a data-loss bug.** Six messages; the database fails the first few attempts then recovers; every message must end up stored, once, in order. | **The bug:** `consumer.commit()` commits the consumer's *position*, not "this message". The old loop skipped a message it gave up on, and the next success committed straight over it, so an outage longer than the ~30 s retry budget silently lost events. The resilience test caught it as an off-by-one: 517 messages in Kafka, 516 rows. I confirmed this test fails (with the words "a message was lost") when the old behaviour is restored. | Silent, permanent data loss on a routine event, the worst class of bug a pipeline can have. |
| `test_unparseable_and_schema_violating_messages_are_deliberately_skipped_and_committed` | **Policy test.** A non-JSON message and a contract-violating one are committed past, not retried. | The opposite policy, on purpose: a message that can never be stored must not block every event behind it. | Pins the distinction between "cannot be stored yet" (retry) and "can never be stored" (skip), so the two cannot be confused again. |
| `test_database_accepts_every_source_kind_the_schema_allows_and_rejects_others` (4 cases) | **Contract test.** Every allowed `source_kind` stores; an invented one is refused by the database itself. | Constraints and schemas are maintained by hand in two places. | Defence in depth, and proof the two agree. |
| `test_the_database_source_kinds_match_the_json_schemas_exactly` | **Contract test.** The database constraint's values equal each schema's enum. | Drift here means valid events fail to store, or invalid ones get in. | Exactness, not just compatibility. |

#### `test_digital_twin_db.py` — the live picture of the restaurant

| Test | What it is and does | Why I created it | Why it matters |
|---|---|---|---|
| `test_firing_an_order_occupies_the_table_and_loads_the_station` | **Integration test.** `order_fired` marks the table occupied and counts a ticket at the station. | The twin's reason to exist. | Core behaviour. |
| `test_delivering_the_order_frees_the_table_and_unloads_the_station` | **Integration test.** `delivered` reverses it. | The other half. | A table that never frees is a wrong dashboard. |
| `test_intermediate_stages_do_not_change_table_or_station_state` | **Integration test.** Middle stages leave state alone. | Only the first and last matter to occupancy. | Prevents double counting. |
| `test_a_busy_station_counts_every_open_ticket` | **Integration test.** Four open tickets count four; one delivery makes three. | Workload per station is what the dashboard shows. | Accuracy under concurrency. |
| `test_the_open_ticket_count_can_never_go_negative` | **Invariant test.** Deliveries with no matching order leave the count at zero. | After a restart the twin can see an unmatched delivery. | A negative workload is nonsense. |
| `test_stations_are_tracked_independently` | **Integration test.** One station's events never move another's count. | Isolation. | Per-station accuracy. |
| `test_a_redelivered_order_fired_does_not_double_count` | **Documented known limitation (strict expected-fail).** Replaying `order_fired` inflates the count. | Found while writing the tests: the count is incremented rather than derived, so Kafka's at-least-once redelivery can overcount. | Recorded as a strict expected-fail so the weakness stays visible, and CI will demand the marker's removal the day it is fixed. |
| `test_a_staff_member_goes_through_a_whole_shift` | **Integration test.** Clock-in, break, break-end, clock-out, with the right status at each step. | The staff lifecycle. | The dashboard's staffing view. |
| `test_reassigning_a_station_changes_only_the_station` | **Integration test.** Reassignment leaves status and clock-in time alone. | An easy place to clobber state. | Preserves shift duration. |
| `test_staff_are_tracked_independently` | **Integration test.** One person's events never affect another's. | Isolation. | Same. |
| `test_replaying_a_staff_event_is_harmless` | **Idempotency test.** Replaying a clock-in leaves one row. | Contrast with the ticket counter. | Staff state is absolute, so it is safe to replay. |

#### `test_pipeline_writers_db.py` — derived data, and the SQL that feeds the causal engine

| Test | What it is and does | Why I created it | Why it matters |
|---|---|---|---|
| `test_a_ticket_summary_is_updated_in_place_as_the_ticket_progresses` | **Integration test.** One row per ticket, rewritten at each of five stages. | The aggregator upserts. | Not five rows per ticket. |
| `test_replaying_a_summary_is_harmless` | **Idempotency test.** Repeated upserts leave one row. | At-least-once delivery. | No duplicates. |
| `test_the_ticket_origin_survives_the_round_trip` | **Integration test.** `origin` is stored and read back. | A newer column added by migration 005. | Quarantine depends on it. |
| `test_anomaly_events_of_both_methods_are_stored` (2 cases) | **Integration test.** Both detector outputs insert into `anomaly_events`. | Column lists are hand-maintained. | A wrong column is a silent stuck consumer. |
| `test_a_causal_finding_is_stored_with_its_gate_and_refutation_flags` | **Integration test.** A finding is stored not-ready, with its refutation flag and scenario id intact. | The gate flag starts false. | The reviewer depends on it. |
| `test_the_waste_query_runs_against_the_real_schema_and_returns_the_confounders` | **Integration test.** The real analysis SQL runs and returns the seeded confounders. | The causal engine's first input. | If the SQL is wrong, every finding is wrong, silently. |
| `test_the_waste_query_excludes_human_player_sessions` | **Integration test of a safety rule.** A seeded `player` row is excluded. | Human play must not contaminate analysis. | The unit test checks the text; this checks the behaviour. |
| `test_the_waste_query_excludes_estimates_the_edge_node_flagged_as_untrustworthy` | **Integration test of a safety rule.** Four events: a normal edge estimate, one flagged out-of-distribution, one produced while the drift monitor was alarming, and one with no `edge_inference` at all. Only the normal one and the one with no block may reach the analysis. | Estimates the node itself does not trust must not become a causal finding, but sources that do no on-node inference must not be lost to a NULL comparison. | Verified to fail when a clause is removed. This is the point where the edge node's self-assessment changes what the platform concludes. |
| `test_the_waste_query_respects_the_time_window` | **Integration test.** A row a month earlier is excluded. | The window defines the analysed event. | Wrong windows give wrong answers. |
| `test_staffing_level_counts_only_staff_actually_on_shift_and_never_players` | **Integration test of derived logic.** Seeds two cooks (one clocked out) and a player; the staffing level must be exactly 1. | `staffing_level` is a derived treatment variable computed in SQL. | The most intricate query in the system, and the one most likely to be silently wrong. |
| `test_the_pickup_query_excludes_interactive_tickets` | **Integration test.** Interactive tickets are excluded. | Quarantine. | Same. |
| `test_the_pickup_query_excludes_tickets_with_no_pickup_delay` | **Integration test.** Incomplete tickets are excluded. | Null outcomes cannot be analysed. | Prevents crashes and bias. |

#### `test_dashboard_api_db.py` — the only thing a viewer touches

| Test | What it is and does | Why I created it | Why it matters |
|---|---|---|---|
| `test_health_is_open_so_a_plain_uptime_check_works` | **Integration test.** `/api/health` needs no key. | Uptime monitors cannot hold a secret. | Monitoring. |
| `test_every_data_route_rejects_a_missing_or_wrong_key` (7 routes) | **Security test.** Each data route returns 401 without the right key and 200 with it. | Unit tests mock the database; this proves it on the real app. | Authentication on every route, not just most. |
| `test_the_twin_routes_report_what_the_twin_wrote` | **Integration test.** Twin rows written by the twin appear correctly through the API. | The two services share only a database. | End-to-end between writer and reader. |
| `test_narrated_findings_are_joined_to_the_evidence_behind_them` | **Integration test.** A narration is returned joined to its finding's effect and refutation flag. | The dashboard's headline feed. | A narration shown without its evidence is just prose. |
| `test_the_anomaly_summary_groups_by_method_and_severity` | **Integration test.** Counts group correctly. | A SQL aggregate. | Right numbers on the dashboard. |
| `test_an_empty_database_returns_empty_lists_not_errors` | **Robustness test.** A fresh database gives empty lists everywhere. | The first-run state a recruiter sees. | A fresh install must not show errors. |
| `test_the_narrated_findings_limit_is_bounded` (5 cases) | **Validation test.** Limits 1 and 200 work; 0, 201 and -5 are rejected. | Unbounded limits are a denial-of-service lever. | Input bounds. |
| `test_the_comparison_endpoint_validates_its_inputs` | **Security/validation test.** Malformed ids and out-of-range hours are rejected. | The only endpoint taking free-text. | Input validation. |
| `test_sql_injection_through_a_query_parameter_does_nothing` | **Security test.** A classic injection string leaves the table intact. | The standard attack, checked against a real database. | Proof, not assumption. |
| `test_sustained_use_does_not_leak_database_connections` | **Resource-leak test.** 150 requests; open connections must not accumulate. | Every request opens a connection. | A leak here exhausts the database after a day of use. |
| `test_the_edge_route_reports_each_nodes_health_from_its_own_flags` | **Integration test.** Six edge events (one out-of-distribution, one drifting and most recent) become one node row with the right counts, rates, p50/p95 latency, model hash, and `drifting_now`. | `/api/edge/plate-waste` is the fleet view: ground truth does not exist in the field, so health is the node's own self-assessment. | The numbers a viewer would use to decide whether to trust a node. |
| `test_the_edge_route_ignores_events_with_no_on_node_inference_and_respects_the_window` | **Integration test.** A vendor-style event (no `edge_inference`) and a ten-hour-old edge event are both excluded from a 60-minute view. | The route groups on JSON paths that would be NULL for such events. | Verified to fail when the `edge_inference` filter is removed. |
| `test_the_edge_route_with_no_edge_data_is_an_empty_fleet_not_an_error` | **Robustness test.** A fresh database returns an empty node list. | The first-run state. | Same rule as the other routes. |
| `test_the_edge_route_rejects_a_nonsensical_window` (4 cases) | **Validation test.** `minutes` of 0, -5, 10081 and `abc` get a 422. | An unbounded look-back is a denial-of-service lever on a table scan. | Same defence as the other bounded parameters. |

#### `test_narrator_boundary_db.py` — the headline safety claim

The README states the LLM narrator is "structurally incapable of inventing a claim from data it can't see", because it connects as a database role that can read findings and nothing else. A claim like that is only worth anything if tested against the real database with that real role. These tests log in as `narrator_app` and try everything it must not be able to do.

| Test | What it is and does | Why I created it | Why it matters |
|---|---|---|---|
| `test_narrator_cannot_read_any_table_except_findings` (10 tables) | **Security test.** `SELECT` on every other table must raise "permission denied". | The boundary is the claim. | If this fails the project's central safety argument is false. |
| `test_narrator_can_read_causal_findings` | **Security test (positive).** It can read what it needs. | Otherwise "denied everywhere" would pass. | Proves the boundary is precise, not total. |
| `test_narrator_cannot_modify_findings` (4 statements) | **Security test.** `UPDATE`, `DELETE`, `INSERT`, `TRUNCATE` on findings are all denied. | It must not promote a finding past review or rewrite its evidence. | Protects the integrity of what is narrated. |
| `test_narrator_can_write_only_its_own_output_table` | **Security test (positive).** It can write `narrated_findings`. | It has one job. | Proves least privilege is also *sufficient*. |
| `test_narrator_cannot_change_the_schema_or_its_own_privileges` (5 statements) | **Security test.** `CREATE`, `DROP`, `ALTER`, `CREATE ROLE` and `GRANT` are all denied. | A compromised narrator must not widen its own access. | Privilege escalation. |
| `test_narrator_is_not_a_superuser` | **Security test.** Not superuser, cannot create roles or databases. | The strongest possible boundary check. | Anything else is moot if it is a superuser. |

#### `test_migrations_db.py` — the schema itself

| Test | What it is and does | Why I created it | Why it matters |
|---|---|---|---|
| `test_every_expected_table_exists` | **Integration test.** All 12 tables exist after the migrations. | A migration can run and still not create everything. | The first thing to break when a migration changes. |
| `test_the_time_series_tables_are_real_hypertables` | **Integration test.** Seven tables are TimescaleDB hypertables. | A plain table works but loses the time-series features. | The reason TimescaleDB was chosen. |
| `test_the_narrator_role_exists_and_can_log_in` | **Integration test.** The restricted role exists. | Created by a psql meta-command that is easy to break. | The boundary tests depend on it. |
| `test_re_running_a_migration_against_a_live_database_succeeds` (5 migrations) | **Idempotency test.** Each migration is re-applied to a populated database. | Every `helm upgrade` re-runs all five. | A non-idempotent migration breaks every upgrade. |

### Layer 4 — Statistical validation (`tests/statistical/`)

**What this layer is:** tests of the causal engine against synthetic datasets whose true answer is known by construction. It runs inside the project's own Python 3.11 image because DoWhy does not support newer Python.
**Why it exists:** every other test checks that code runs and data flows. This one checks the thing the platform exists for — that the numbers it reports are *right*. Almost nothing else in a data project tests this.

| Test | What it is and does | Why I created it | Why it matters |
|---|---|---|---|
| `test_the_engine_recovers_a_planted_plate_waste_effect` | **Ground-truth test.** Plants "a to-go container removes 90 g of waste", confounded by portion size, and asks the real engine to find it. | Passing code is not the same as correct statistics. | Recovered −90 g within 10 g. |
| `test_the_engine_recovers_a_planted_staffing_effect` | **Ground-truth test.** Plants a −4,000 ms-per-person effect confounded by station. | The second treatment the engine supports. | Recovered within 500 ms. |
| `test_the_planted_effect_is_actually_hidden_by_confounding` | **Meta-test.** Shows a naive comparison is off by more than 25 g, and the engine is at least twice as close. | Guards the two tests above against being vacuous. | If naive analysis also got it right, "the engine recovered it" would prove nothing about confounder control. |
| `test_estimates_are_stable_across_independent_samples` | **Statistical test.** Five independent datasets must all land near the truth. | One lucky seed proves little. | Reliability, not luck. |
| `test_a_strong_effect_passes_its_refutation_test_and_the_flag_is_a_real_bool` | **Regression test.** A real effect passes refutation, and the flag is a Python `bool`. | Problem log item 48: a `numpy.bool_` failed schema validation. | The last step between "estimated" and "stored". |
| `test_when_there_is_no_effect_the_engine_estimates_roughly_zero` | **False-positive control.** Truth is zero; the estimate must be near zero. | The mirror of recovering a real effect. | A detector that finds effects everywhere is worthless. |
| `test_the_refutation_gate_rejects_findings_that_are_pure_noise` | **Statistical test (slow); formerly a strict expected-failure.** 30 datasets with a true effect of exactly zero; fewer than 10% may pass the gate. **Now: 0 of 30 pass. Before the fix the same property failed: 78% to 87% passed in three measurements.** | I expected about 16% and found far worse. DoWhy's placebo `new_effect` is the mean of 100 simulated runs, so it is ~10× quieter than a single estimate, and the old rule was met by almost any noise. Fixed 2026-10-03 (DEF-106). | **The most important finding of this regime,** and the property the project's headline claim ("refutation-tested") depends on. Now guarded. |
| `test_the_refutation_gate_still_passes_genuine_effects` | **Statistical test (slow); power check.** 10 datasets with a real −20 g effect; at least 90% must pass. Measured 10 of 10. | A gate that rejected everything would also "reject noise". | Proves the repair made the gate discriminating, not merely strict. |
| `test_the_refutation_verdict_is_identical_for_identical_data` | **Determinism test.** The same data twice gives an identical verdict, identical p-values and an identical estimate. | The placebo permutations were unseeded, so a finding's verdict (and every measurement of the gate itself) varied from run to run (DEF-129). | A finding whose verdict can flip on re-running cannot be audited or reproduced. |
| `test_the_gate_decision_follows_the_two_published_conditions` | **Specification test.** On a noise dataset and a genuine one, the verdict equals (effect p < alpha) AND (placebo p ≥ alpha). | The p-values are now logged with every finding; this proves they are enough to audit any decision. | The rule is stated once and checked, so the log line and the behaviour cannot disagree. |
| `test_the_engines_own_info_logs_survive_importing_dowhy` | **Regression test in a fresh interpreter.** Imports the engine, then DoWhy, and requires the engine's own INFO logging to still be enabled. | Found while trying to display the repaired gate's p-values: `import dowhy` resets the root logger to WARNING, and the engine imports it lazily on its first estimate, so every later INFO line (including each finding's verdict and p-values) vanished (DEF-132). A fresh interpreter is used because inside pytest DoWhy is already imported first and would hide the bug; I confirmed the test fails without the fix. | An audit trail that silently stops after the first event is worse than none: the gate's p-values exist so a decision can be checked, and they must reach the log. |
| `test_too_few_rows_is_refused_rather_than_estimated` | **Robustness test.** 15 rows raise `ValueError`. | The stream consumer catches that and skips. | Never estimate from too little data. |
| `test_rows_with_missing_values_are_dropped_not_fatal` | **Robustness test.** 10% missing confounders still yields a good estimate. | Real data has gaps. | Graceful degradation. |
| `test_estimates_are_reproducible_for_identical_input` | **Determinism test.** The same data gives the same estimate. | A finding no one can reproduce cannot be audited. | Auditability. |
### Layer 5 — End-to-end tests (`tests/e2e/`)

**What this layer is:** tests against the real, running Docker Compose stack, started as an isolated project (`rp-test`, dashboard on port 18080) so it never touches a developer's own stack. The only difference from `docker compose up` is faster pacing (`tests/e2e/docker-compose.test.yml`): same images, same wiring, but events arrive fast enough to build a baseline in minutes instead of an hour.
**Why it exists:** everything below this layer tests a piece. This one starts where the data starts — a simulated sensor publishing over MQTT — and follows it all the way to the API a browser calls, checking at every hop. When a hop is broken, the first failing test names it.
**Scope note:** the three LLM services (Ollama, its model-pull helper, the narrator) are left out: pulling a model on every run would dominate the runtime for no pipeline coverage, since the narrator's logic is covered by the guard tests and the database-boundary tests.

#### `test_pipeline_e2e.py`

| Test | What it is and does | Why I created it | Why it matters |
|---|---|---|---|
| `test_every_long_running_service_is_up` (17 services) | **Smoke test.** Each service's container is running. | The cheapest question first. | If a service is down every later failure is a symptom. |
| `test_the_one_shot_connector_registration_completed_successfully` | **Smoke test.** The helper that registers the MQTT connectors exited with code 0. | It runs once and disappears, so failure is easy to miss. | Without it no sensor data reaches Kafka. |
| `test_all_four_mqtt_connectors_are_running` | **Health test via the Connect REST API.** Every connector reports `RUNNING`. | Problem log item 44: three of four connectors had been `FAILED` for days while the fourth worked, so the pipeline looked alive. | The most insidious failure this project has had: partial, silent, and invisible to "is it up". |
| `test_every_sensor_type_reaches_the_database` (4 tables) | **Data-flow test.** Waits for rows in each event table. | Proves the entire front half: simulator → MQTT → Connect → Kafka → consumer → database. | One test per sensor type shows exactly which path is dead. |
| `test_the_edge_nodes_model_and_inference_reach_the_database_intact` | **End-to-end test.** After five edge events have arrived, every stored `edge_inference` block names the model hash committed in the repository, carries schema 1.1.0, and reports inference latency inside the 5 ms budget. | The model runs inside the simulator image; this proves the image was built from the committed model and the block survives MQTT, Kafka Connect, Kafka and the consumer. | The one place a stale image (the DEF-013 class of bug) or a transport that drops the block would show. |
| `test_the_dashboard_api_reports_the_edge_node_as_a_fleet` | **End-to-end test.** `/api/edge/plate-waste` through the real nginx proxy returns the node with the committed model hash and a low distrust rate. | The fleet view is the only way a viewer sees the edge feature. | Proves the route works on live data, not only on test rows. |
| `test_ticket_timings_are_aggregated_into_complete_summaries` | **Data-flow test.** At least 10 complete ticket summaries appear. | The aggregator is the second stage. | Everything analytical depends on it. |
| `test_completed_ticket_summaries_are_internally_consistent` | **Data-integrity test.** No complete summary has a missing or negative duration, and the four durations add up to the total. | Individually valid rows can still be mutually inconsistent. | Catches corruption no schema can see. |
| `test_the_digital_twin_mirrors_the_restaurant` | **Data-flow and invariant test.** Twin tables populate, no negative counts, only valid staff statuses. | The twin is the "now" view. | Live state must be sane, not just present. |
| `test_no_event_was_stored_twice` (4 tables) | **Data-integrity test.** `count(*)` equals `count(DISTINCT event_id)`. | Duplicates silently skew analysis. | Verifies idempotency in the running system, not just in a unit test. |
| `test_no_python_service_has_logged_a_traceback_or_error` (7 services) | **Log-health test.** No traceback, `ERROR` line or schema violation in recent logs. | A service can be "up" and failing every message. | The check that finds quiet, ongoing failure. |
| `test_the_startup_topic_race_happens_once_at_start_and_never_persists` (7 services) | **Log-health test.** The one allowed error message (a consumer starting before its topic exists) may appear only a handful of times, at start-up. | The first end-to-end run flagged this message in five services. It is a benign startup race — topics are auto-created on first write, and consumers start first — so the error test now allows it, and this test stops the allowance from hiding a real problem. | Distinguishes a harmless one-off from a topic that never appears, which would be a stuck pipeline. |
| `test_no_python_service_has_crashed_and_restarted` (7 services) | **Stability test.** Restart count is zero. | A restart means a crash; the restart policy then hides it. | Zero is the production bar. |
| `test_the_dashboard_page_is_served` | **Smoke test.** The page loads and contains the React root. | The thing a visitor sees first. | A blank page is a failed project. |
| `test_the_api_answers_through_the_web_proxy_without_the_browser_knowing_the_key` (5 routes) | **Integration test.** Each data route returns a JSON list through nginx, with no key supplied by the caller. | nginx injects the API key server-side so a browser never holds it. | Verifies the security design works as designed. |
| `test_the_dashboard_shows_live_station_state` | **End-to-end test.** Live station data is visible through the dashboard path. | The whole chain, in one assertion. | The user-visible outcome. |
| `test_pipeline_health_metrics_are_exposed` (3 services) | **Observability test.** Each instrumented service serves its Prometheus counters. | The metrics were added to make silent failure visible. | An unmonitored metric is no metric. |
| `test_the_anomaly_detector_is_actually_consuming_summaries` | **Observability and flow test.** The processed-summaries counter rises above zero. | Proves the third stage is truly receiving data. | "Running" is not the same as "working". |

#### `test_scenario_acceptance.py` — the project's stated done condition (slow, ~8 minutes)

**What it is:** an *acceptance test*, the automated form of the project's definition of done: *"an injected scenario produces a correctly attributed finding."* A staffing shortage is injected at a known moment, removing `station-grill`, and the platform is judged against what it should have done. It took a manual 10-minute run on Kubernetes to verify this originally.

| Test | What it is and does | Why I created it | Why it matters |
|---|---|---|---|
| `test_the_shortage_measurably_slows_the_stations_that_absorb_it` | **Acceptance test.** Mean pickup delay at the loaded stations must exceed 1.5× its pre-shortage baseline. | The injected cause must have a measurable effect, or nothing downstream can mean anything. | Ground truth for the rest of the chain. |
| `test_anomalies_are_flagged_at_the_loaded_stations_and_not_at_the_removed_one` | **Acceptance test.** At least three anomalies at the loaded stations, and almost none at the removed one. | Detection must be *localised*, not merely present. | A detector that flags everything is as useless as one that flags nothing. |
| `test_a_finding_computed_for_the_scenario_carries_its_id` | **Acceptance test.** The causal engine, pointed at the scenario's window, stores a finding carrying the scenario's id, in milliseconds; then the *running* finding-reviewer must promote it to narrative-ready if, and only if, its refutation test passed. | Attribution: a finding must be traceable to its cause, and the review gate must act on it in the live system. (My first version wrongly assumed the finding would still be un-promoted when checked; the live reviewer had already, correctly, promoted it.) | The final link in "cause → effect → finding → gate", observed rather than assumed. |

#### `game/client/tests/smoke_test.gd` — the game client, end to end (version 2)

**What it is:** a headless end-to-end test of the Godot game client against a *live* game bridge and the real platform behind it, run with `godot4 --headless --path game/client -s res://tests/smoke_test.gd` and in CI by the `game-smoke-test` job. It is a GDScript file of roughly 80 checks rather than Python test functions, so it is described here by section. It takes up to a minute because it waits for the bridge's automated crew to cook and deliver real tickets.
**Why it exists:** the bridge's rules are unit-tested, but the client is the thing a player touches, and the wiring between client, bridge, Kafka, database and dashboard can only be verified by playing it. It is the single true integration test of version 2.

| Section | What it is and does | Why it exists | Why it matters |
|---|---|---|---|
| Shift report formatting | **Unit-style check with canned data.** The end-of-shift summary text renders correctly with no network. | Pure formatting logic needs no live system. | A garbled report is the first thing a player sees at the end of a shift. |
| Client | **Integration checks.** The HTTP client talks to the bridge; the 409, 422 and 404 rejections are surfaced correctly; a full shift is worked and a ticket advanced. | The client must interpret the bridge's errors, not just its successes. | A client that mishandles a rejection hangs or lies to the player. |
| Guest: ordering, eating, paying | **End-to-end checks.** A guest orders, waits for the crew to cook and deliver, then pays. | The guest loop is a whole second game mode. | It generates the real point-of-sale events the platform analyses. |
| Table exclusivity and leaving | **Integration checks.** Two parties cannot take one table; leaving frees it. | Shared resources are where concurrency bugs hide. | Prevents double-booked tables in the shared dining room. |
| Main scene, driven through its own handlers | **UI-logic checks.** The real main scene is driven through the same handlers a click would call. | Tests the UI's logic without needing a display. | Catches wiring bugs between buttons and the client. |
| Main scene, guest ordering | **UI-logic checks.** Ordering and payment through the scene. | Same, for guest mode. | Same. |
| Main scene, leaving before paying | **UI-logic check.** Closing out early reaches the bridge and frees the table. | A real player abandons a meal. | Cleans up state instead of leaking an occupied table. |

### Layer 6 — Resilience tests (`tests/resilience/`)

**What this layer is:** *chaos tests*. Each one breaks exactly one thing in the running stack, waits for recovery, and then checks the same invariant — **every message ever written to Kafka is stored exactly once.** The check stops the producers, waits for every consumer to drain, then compares each topic's end offset with the table's row count and checks for duplicate ids.
**Why it exists:** in production nothing stays up. A process is OOM-killed, the database restarts for maintenance, the broker is rescheduled. "It works when nothing goes wrong" proves little. This is the repeatable version of the one-off broker-kill chaos test originally done by hand, and the first thing it caught was that the original had never been repeatable.

| Test | What it is and does | Why I created it | Why it matters |
|---|---|---|---|
| `test_baseline_every_message_is_stored_exactly_once_with_nothing_going_wrong` | **Control experiment.** Runs the conservation check with no fault injected. | Validates the invariant itself. | If the check can fail on a healthy system, every failure below would be uninterpretable. |
| `test_a_killed_storage_consumer_is_restarted_by_the_platform_and_loses_nothing` | **Chaos test (crash).** `SIGKILL` the consumer's process from the host, as the kernel's out-of-memory killer would; let a backlog build; then check the platform restarts it by itself and nothing is lost or duplicated. | The restart policy was added after a service stayed dead forever. My first version used `docker kill`, which failed here: **Podman treats that API call as a deliberate stop and does not restart the container** (measured: a host-side `SIGKILL` was restarted, `docker kill` was not). The test platform was fine; the crash simulation was unfaithful, so it now kills the process directly. | Proves self-healing and at-least-once recovery together, using a failure that actually resembles production. |
| `test_a_killed_aggregator_resumes_producing_ticket_summaries` | **Chaos test (crash with state).** Kill the aggregator, which holds in-memory state. | Its in-flight tickets are lost by design (documented). | Verifies the loss is *bounded*: new tickets must flow again. |
| `test_a_database_outage_is_survived_by_every_service_that_uses_it` | **Chaos test (dependency outage).** Stop the database for 25 seconds, restart it, and require that the storage consumer, aggregator, twin and dashboard API each recover with no human help. | Every database client was written separately, and each handles a lost connection its own way. | Production databases restart. |
| `test_a_kafka_restart_is_survived` | **Chaos test (broker restart).** Restart Kafka; connectors must return to `RUNNING` and data must resume. | The original manual chaos test, made repeatable. | The backbone of the platform. |
| `test_an_mqtt_broker_restart_is_survived` | **Chaos test (edge outage).** Restart Mosquitto; simulators and connectors must reconnect. | The sensor-facing edge; the shape of problem log item 44. | If this fails, data silently stops. |
| `test_stopping_and_restarting_the_whole_stack_preserves_data` | **Persistence test.** Bring everything down and up again (volumes kept); no rows or Kafka messages may disappear. | The Kafka-data-loss bug: every restart once wiped all topics. | The most basic promise of persistent storage. |

### Layer 7 — Load and stability tests (`tests/load/`)

**What this layer is:** realistic bursts pushed through the real stack, measuring what happens. Numbers are printed on every run (`-s`) so each run leaves a record; the assertions are deliberately loose floors, which catch a collapse or a leak without making the suite flaky over a 10% speed change.
**Why it exists:** a pipeline that works at a trickle can still fail at volume. A consumer slower than the arrival rate builds a backlog forever; a leak of a few MB per thousand events takes a service down in a day.

| Test | What it is and does | Why I created it | Why it matters |
|---|---|---|---|
| `test_data_reaches_the_database_within_seconds_of_being_produced` | **Latency test.** The 95th-percentile gap between an event's timestamp and its storage must be under 10 s. | Data freshness is what a user feels. | A pipeline that is correct but an hour late is a failed pipeline. |
| `test_a_burst_of_events_is_absorbed_and_fully_stored` | **Load test.** Publishes 3,000 real, schema-valid events directly into Kafka; all must be stored, exactly once, at no less than 10 events/s (a floor set from measurement, not a target; measured 45 events/s on 2026-10-03 after the consumer stopped re-checking the schema on every message, 11 to 12 before that fix, and 14.6 to 18 with the smaller schema), with no service crashing and no service growing more than 100 MB (the causal engine is allowed a further 160 MB for the one-off lazy import of DoWhy, a measured +153 MB step that lands in the window only if the first anomaly happens to arrive during it). | A burst is the realistic stress: a venue opening, a connector catching up. | Tests throughput, loss, duplication, crashes and memory leaks in one run. |
| `test_the_backlog_drains_completely_after_a_burst` | **Recovery test.** The storage consumer's lag returns to zero. | A system that absorbs a burst but never catches up is broken. | The difference between "busy" and "falling behind forever". |
| `test_the_dashboard_api_serves_many_concurrent_users_without_errors` | **Concurrency test.** 400 requests across 20 threads; all must return 200 with p95 under 2 s. | The dashboard is the project's front door. | A slow or erroring dashboard makes everything behind it look broken. |

### Layer 8 — Security and supply chain (`tests/security/`)

**What this layer is:** a check of every pinned Python dependency against public vulnerability advisories (via `pip-audit`). It needs the internet, so it runs nightly rather than on every push — a new advisory is not caused by a commit.
**Why it exists:** pinning exact versions makes builds reproducible but also freezes in whatever flaws those versions had on the day they were pinned.

| Test | What it is and does | Why I created it | Why it matters |
|---|---|---|---|
| `test_no_pinned_dependency_has_a_known_vulnerability` (one case per requirements file) | **Supply-chain test.** Audits every `==` pin and fails on any published advisory not explicitly accepted with a written reason. | Dependencies are the largest part of any codebase I did not write. | **Found a real issue on its first run:** `requests==2.32.3` in the narrator had two published vulnerabilities (now bumped to 2.33.0). The advisory allow-list is deliberately empty. |

---

## Keeping this document honest

- `tests/static/test_testing_doc.py` fails if any test function is missing from this catalogue, and fails if the catalogue names a test that does not exist.
- Adding a test therefore means writing its row: *what it is, what it does, why it exists, why it matters.* If a test cannot be explained in those four parts, it probably should not exist.
- Strict expected-failures (`xfail(strict=True)`) are the only way a known defect is allowed to live in the suite, and each one carries its measurement and cause in its `reason`.
