# Quality documentation

| | |
|---|---|
| Project | Restaurant Operations Digital Twin Platform |
| Version | 1.0 |
| Date | 2026-10-03 |
| Baseline | Repository commit `898c7aa` |

## What this is, and what it is not

A complete set of quality documents for the project: requirements, test plan, traceability, SQA plan, risks, defects, results, lessons, environment and release readiness.

**It was prepared retrospectively.** The project began from a design brief and a running technical log, not a formal requirements document, and it has a single owner and an AI assistant, not a quality department. These documents were written on 2026-10-03 by the assistant, at the owner's request, from the project's own records (the technical log and its problem log, the codebase guide, the study guide, 60 commits of git history, and the 2026-10-02/03 test-regime work). Where they describe a process, they describe the process *as practised*, with its gaps. They borrow the structure of IEEE 829 / ISO 29119-3 (test documentation), IEEE 730 (SQA), ISO 29148 / IEEE 830 (requirements) and ISO 31000-style risk registers for familiarity. **No claim of compliance with any standard is made.**

The three registers (traceability matrix, defect log, risk register) were first produced by script from structured data. The Markdown here is now the source of truth and is maintained by hand; `tests/static/test_quality_docs.py` checks it stays consistent.

## The documents

| # | Document | What it answers | Read it if you are |
|---|---|---|---|
| 01 | [Requirements specification](01-requirements-specification.md) | What must the system do? (96 identified requirements) | Anyone judging whether the project meets its goals |
| 02 | [Master test plan](02-test-plan.md) | How is it verified, and with what limits? | A tester or reviewer |
| 03 | [Requirements traceability matrix](03-requirements-traceability-matrix.md) | Which test proves each requirement, and is it met? | An auditor |
| 04 | [SQA plan](04-sqa-plan.md) | What quality practices were in force, and how good are they honestly? | A manager |
| 05 | [Risk register](05-risk-register.md) | What could go wrong, what already did, what is open? | A manager or maintainer |
| 06 | [Defect and difficulty log](06-defect-log.md) | Every failure, bug and difficulty (147 entries) | Anyone curious how it really went |
| 07 | [Test summary report](07-test-summary-report.md) | What do the latest results say? | Everyone: start here |
| 08 | [Lessons learned](08-lessons-learned.md) | What does the project teach? | An engineer, an interviewer |
| 09 | [Environment and configuration baseline](09-test-environment-and-configuration-baseline.md) | Where were the results obtained? | Anyone reproducing them |
| 10 | [Release readiness and known issues](10-release-readiness-and-known-issues.md) | Is it ready, and with what caveats? | The owner, a reviewer |

Related documents elsewhere in the repository: [`TESTING.md`](../../TESTING.md) (the catalogue of every test: what it is, what it does, why it exists, why it matters), [`CODEBASE-GUIDE.md`](../../CODEBASE-GUIDE.md) (every file explained), [`restaurant-platform-implementation-status.md`](../../restaurant-platform-implementation-status.md) (the technical log and the original problem log), and [`restaurant-platform-project-notes.md`](../../restaurant-platform-project-notes.md) (the original design brief).

## Suggested reading order

- **In five minutes:** 07, then 10.
- **To judge the engineering:** 07, 08, then the S1 and S2 rows of 06.
- **To judge the process:** 04, 05, 02.
- **To reproduce a result:** 09, then `TESTING.md`.

## Identifiers

| Prefix | Meaning | Defined in |
|---|---|---|
| `CON-`, `FR-`, `NFR-` | Constraint, functional and non-functional requirement | 01 |
| `DEF-nnn` | Defect or difficulty | 06 |
| `RSK-nnn` | Risk | 05 |
| `test_...` (in backticks) | A test function, described in `TESTING.md` | the test code |

## Glossary

| Term | Meaning |
|---|---|
| Acceptance test | A test of the project's stated done-condition: an injected scenario produces a correctly attributed finding |
| Chaos test | A test that breaks something real (kills a process, stops the database) and checks recovery |
| Conservation check | After a fault, the messages in Kafka must equal the rows in the database, with no duplicate ids |
| Contract | A JSON Schema every producer and consumer agrees on |
| Finding | A causal-effect estimate with its confounders and a refutation result |
| Narrative gate | The rule that only a finding whose refutation passed may be narrated |
| Quarantine | Keeping human-driven (game) tickets out of baselines and analyses |
| Silent failure | Data lost, duplicated or corrupted with no error raised |
| Strict expected-failure | A test of a known, unfixed defect, marked so that it stays visible and demands removal of the marker once fixed |
| S1-S4 | Defect severities: critical, high, medium, low |
| `source_kind` | The event field distinguishing simulated, vendor and player/crew producers |
