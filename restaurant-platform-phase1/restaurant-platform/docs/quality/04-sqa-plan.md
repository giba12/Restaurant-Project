# Software Quality Assurance Plan

| | |
|---|---|
| Document | Software quality assurance (SQA) plan |
| Project | Restaurant Operations Digital Twin Platform |
| Version | 1.0 |
| Date | 2026-10-03 |
| Status | Retrospective: records the quality practices actually in force, then assesses them honestly |
| Prepared by | The project's AI assistant (Claude), for the project owner |

## 0. Note on provenance and scale

This is a one-owner portfolio project built with an AI assistant. It never had a formal SQA process; it had a **set of habits that hardened after specific incidents**. This plan writes those habits down as a process, states which gates are automated and which depend on a person, and assesses the result against what a larger project would expect. The structure follows IEEE 730 for familiarity. **No claim of compliance with IEEE 730, ISO 9001 or any capability-maturity model is made.**

## 1. Purpose and scope

To assure that the system's code, configuration, data contracts and documents meet the requirements in `01-requirements-specification.md`, and that the evidence for each claim the project makes is real and current. It covers all first-party artefacts in `github.com/giba12/Restaurant-Project`.

## 2. Management

### 2.1 Organisation

| Role | Held by | Responsibility |
|---|---|---|
| Owner | The project owner | Sets requirements and constraints; decides what to fix and what to leave open; says when a change is committed; pushes |
| Builder and verifier | The AI assistant | Implements, tests, analyses defects, writes documents |
| Independent reviewer | **None** | See 11 |

### 2.2 Standing rules (decided by the owner during the project)

1. **Nothing is committed or pushed unless the owner says so.** The assistant prepares a change, reports what it contains, and waits.
2. **Verify by real testing, not assumption, and say what is unverified.** Every claim in the documentation is expected to trace to a run. Statements such as "not yet confirmed by an actual run" are used deliberately.
3. **Free and open-source only** (CON-01).
4. **Version 1 never references version 2** (CON-06).
5. **Hand-roll infrastructure from official images** when a third-party chart or image proves unmaintained (CON-07).

## 3. Documentation

| Document | Purpose | Kept current by |
|---|---|---|
| `01-requirements-specification.md` | What the system must do | Owner and assistant; checked against the RTM by a test |
| `02-test-plan.md` | How it is verified | Assistant |
| `03-requirements-traceability-matrix.md` | Requirement to test to result | Hand-maintained; machine-checked |
| `04-sqa-plan.md` | This document | Assistant |
| `05-risk-register.md` | What could go wrong | Assistant, reviewed with the owner |
| `06-defect-log.md` | Every failure and difficulty | Hand-maintained; machine-checked |
| `07-test-summary-report.md` | Results of a test cycle | Regenerated per cycle |
| `08-lessons-learned.md` | What the project teaches | Assistant |
| `09-test-environment-and-configuration-baseline.md` | Where results were obtained | Updated when tooling changes |
| `10-release-readiness-and-known-issues.md` | Whether to recommend it, and with what caveats | Per cycle |
| `TESTING.md` | The catalogue of every test | **Self-testing** (`test_testing_doc.py`) |
| `CODEBASE-GUIDE.md` | Every file explained | Hand-edited; not machine-checked |
| `restaurant-platform-implementation-status.md` | Chronological technical log and problem log | Hand-edited |
| `restaurant-platform-project-notes.md` | The original design brief | Frozen except for dated additions |
| `linux-k8s-docker-helm-study-guide.md` | Lessons indexed by failure mode | Hand-edited |

## 4. Standards, practices, conventions and metrics

### 4.1 Coding and configuration conventions (enforced where noted)

| Convention | Enforced by |
|---|---|
| Python 3.11 in every image; every dependency pinned with `==` | `test_python_images_use_the_one_project_wide_python_version`, `test_requirements_are_pinned_exactly` |
| Non-root containers | `test_final_image_does_not_run_as_root` |
| Fully qualified, pinned images | three static tests |
| CPU and memory limits on every Kubernetes container | `test_every_container_has_cpu_and_memory_limits`, plus the live audit script |
| Schemas are the single source of truth, strict, with one envelope | four static tests |
| Idempotent migrations | `test_re_running_a_migration_against_a_live_database_succeeds` |
| No dead code, undefined names or syntax errors | `ruff` via `test_python_has_no_syntax_errors_undefined_names_or_dead_code` |
| No committed secrets | `test_no_secrets_are_committed` and four related tests |
| Comments explain *why*, not what; the reasoning behind a decision is written beside it | Review only |
| A fix is accompanied by its explanation in the status log | Habit only |

### 4.2 Quality gates

| Gate | When | Automated | Status |
|---|---|---|---|
| Static checks, unit tests, database integration, schema compatibility, game smoke test | Every push and pull request (`tests.yml`) | Yes | Workflow repaired 2026-09-30 and run on GitHub then; the `static` and `integration` jobs added 2026-10-02 have **not yet run on GitHub** |
| End-to-end, acceptance, resilience, load, statistical, dependency audit | Nightly and on demand (`production-readiness.yml`) | Yes | Written; **never run on GitHub**; every command in it was run locally |
| Live-cluster drift, limits and placeholder-credential audit | Manually, after any deploy | No (needs the cluster) | Clean on 2026-10-01 |
| Owner review before commit | Every commit | No | In force |
| Branch protection, required reviews, pre-commit hooks | - | - | **Not in place** (single `main` branch) |

### 4.3 Metrics (from `06-defect-log.md`, as of 2026-10-05)

| Metric | Value |
|---|---|
| Entries in the defect log | 171 (155 defects, 16 informational) |
| By severity | S1 Critical 8; S2 High 31; S3 Medium 53; S4 Low 57; Info 16 |
| By status | Fixed and guarded by a test 56; fixed 79; mitigated 8; clarified 17; open 5 |
| Distinct tests | 670, plus the GDScript smoke test (about 80 checks) |
| Requirements | 100: 75 verified by automated test, 6 by inspection, 9 live or manual only, 9 partial, 1 not verified, 0 not met |
| Latest complete test cycle | 2026-10-04: every layer re-run passed on the final code, after repeated failed attempts at the heavy layers (flaky in this environment); the injected scenario's finding is refuted (DEF-141) |
| Line or branch code coverage | **Not measured** |
| Mean time to detect a silent failure | Not tracked; observed range: minutes (found by test) to 12 days (the silent MQTT bridge failure, DEF-035) |

**What the defect metrics say.** Of the 35 serious defects (S1 and S2), **none was found by a unit test**: 22 by live operation or deployment, 5 by static review before deployment, 3 by attempting the done condition, 3 by chaos testing, 1 by CI and 1 by the statistical test. Several lived undetected for days. The automated regime of 2026-10-02/03 then found two silent-data-loss defects that all earlier verification had missed. The lesson is recorded in `08-lessons-learned.md`.

## 5. Reviews and audits

| Activity | Practice | Assessment |
|---|---|---|
| Static review before first deployment | Done for Phase 5; found five defects before anything ran (DEF-029 to DEF-033) | Effective; not repeated for later phases |
| Self-review ("rate this project as a PM", 2026-09-29) | Produced a prioritised backlog: CI, schema compatibility, chaos test, instrumentation, audit script | Effective as a prompt for work |
| Live audit | `k8s/audit/audit-live-cluster.sh`: drift, limits, placeholder credentials | Effective; found a real unrotated credential |
| Mutation checks on tests | Deliberately breaking the system to confirm a test fails | Applied ad hoc to the detector, aggregator, reviewer gate, consumer and a database permission; **not systematic** and not automated |
| Peer or independent code review | None | The largest process gap |
| Documentation audits | Cleanup pass 2026-10-01; this document set | Found stale statements each time |

## 6. Testing

See `02-test-plan.md`.

## 7. Problem reporting and corrective action

1. A failure is observed (by a test, a live run, CI or the owner).
2. It is **reproduced** and, where practical, captured as a failing test before being fixed.
3. The root cause is found by elimination, with each ruled-out layer written down (an explicit habit: for example the 41-hour exporter failure, DEF-092, records everything ruled out).
4. It is fixed, then verified by re-running the real thing, not only the new test.
5. The test is shown to **fail on the old behaviour**.
6. It is logged in the status log and now in `06-defect-log.md`, with severity, method of detection, cause, resolution and status.
7. A defect deliberately not fixed is marked **Open** and kept visible as a strict expected-failure (none is in that state now; both were fixed and their markers removed).

Corrective actions that changed the process, in the order they happened:

| Incident | Process change |
|---|---|
| CI had never run (DEF-081) | The assistant was given read-only `gh` access to watch real CI runs rather than trust a workflow file |
| A fix committed but never redeployed caused a 10-hour outage (DEF-090) | The live audit script (drift check) |
| A routine redeploy exposed a silently broken connection (DEF-094) | Same audit, plus a rule to treat "redeploy exposes an old problem" as a finding |
| Silent data loss in the consumer (DEF-100, DEF-101) | A conservation invariant (messages in Kafka equal rows) checked after every injected fault |
| A self-inflicted failure on the owner's machine (DEF-109) | The stack tests use an isolated project and port and always tear down |
| A test that flagged itself only after being committed (DEF-114) | Rule: run the static layer again after committing new files, since `git ls-files` only sees tracked files |
| A refutation gate that passed most noise and gave unreproducible verdicts (DEF-106, DEF-129) | Measure the thing before repairing it: candidate gates were evaluated on 60 noise datasets and four genuine effect sizes, and the threshold was chosen from that evidence, not from intuition |

## 8. Tools, techniques and methodologies

Test techniques are listed in `02-test-plan.md` section 3.2. Tooling is listed in section 9 of the same document. Everything is free and open-source.

## 9. Configuration management

| Item | Practice |
|---|---|
| Version control | Git, single `main` branch, 60+ commits from 2026-08-24; commit messages explain *why*; commits are made only on the owner's instruction and pushed by the owner |
| Baselines | None formally; the commit hash is the baseline (this document set: `898c7aa`) |
| Releases and tags | None; the project has never been versioned or released |
| Reproducibility | Every Python dependency pinned; images pinned by tag or digest (Ollama is the one documented exception); Python 3.11 everywhere |
| Controlled data items | `schemas/` (single source of truth), `storage/schema/*.sql` (migrations, idempotent) |
| Hand-synchronised copies | Listed in the codebase guide 11.3; only the schema copies in the charts are machine-checked. A stale SQL copy once caused a real outage of correctness (DEF-053) |
| Secrets | Real values in a gitignored directory, never committed; placeholders elsewhere; checked by tests |
| Environment baseline | `09-test-environment-and-configuration-baseline.md` |

## 10. Supplier and third-party control

Third parties are the largest source of surprise in this project: **eight** of the defect log's entries are classed Dependency or Supply chain (a dependency, image or chart changing, disappearing or being incompatible: for example DEF-017, DEF-022, DEF-023, DEF-026, DEF-027), and more involve third-party behaviour. Controls:

1. Pin every dependency exactly; pin images by tag or digest.
2. Prefer hand-rolled manifests around an official image to wrapping a third-party chart (CON-07).
3. Check that a dependency is still maintained before depending on it further (`helm search repo ... --versions`), rather than adjusting a version constraint.
4. Audit pins against public advisories nightly (`tests/security`).
5. Record licence obligations (MinIO server is AGPLv3 and runs as a separate container).

Not in place: image vulnerability scanning (for example Trivy) and a software bill of materials.

## 11. Independence and the limits of self-verification

**There is no independent verification.** The same two parties wrote the code, wrote the tests and judged the results. This weakens every "verified" in the traceability matrix: a shared misunderstanding of a requirement would pass every test. Partial mitigations: tests assert measured facts (rows in a real database, bytes in a real broker) rather than the author's expectations; mutation checks confirm tests can fail; known weaknesses are published rather than hidden (DEF-106, DEF-107). The correct remedy is external review, which this plan recommends in `10-release-readiness-and-known-issues.md`.

## 12. Records

Retained in the repository: this document set, `TESTING.md`, the status log, CI run history on GitHub, and `tests/artifacts/` (container logs from a failed stack run, deliberately not committed). Not retained: terminal transcripts of the assistant's sessions, beyond what was written into those documents.

## 13. Training

None formal. The study guide (`linux-k8s-docker-helm-study-guide.md`) is the project's own learning record, indexed by failure mode.

## 14. Risk management

See `05-risk-register.md`.

## 15. Assessment against a larger project's expectations

| Expectation | State |
|---|---|
| Requirements baselined and traceable | Now yes, retrospectively; they were not at the start |
| Independent test | No |
| Peer review of code | No |
| Automated gates on every change | Yes (once the GitHub runs are confirmed) |
| Coverage measured | **No** (requirement coverage only) |
| Static analysis | Lint (errors and dead code) only; no security analyser |
| Dependency and image scanning | Dependencies yes; images no |
| Release process and versioning | No |
| Defects tracked with root cause | Yes, in unusual depth |
| Known issues disclosed | Yes |
| Disaster recovery proven | Backup restored once (2026-09-24) |
