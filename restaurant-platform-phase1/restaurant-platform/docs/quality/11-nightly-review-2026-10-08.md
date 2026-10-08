# Nightly Review, 2026-10-08

| | |
|---|---|
| Document | Review of a full nightly run, with the issues found and what was done about them |
| Project | Restaurant Operations Digital Twin Platform |
| Version | 1.0 |
| Date | 2026-10-08 |
| Run reviewed | `production-readiness` run 37813546847, commit `6aa38b4`, all layers (`stack_layer=full`) |

## 1. The run

It passed every layer: security 12, statistical 14, end-to-end 69 (4 skipped), acceptance 4, load 8, resilience 13. Durations were in line with the previous full run (end-to-end 3 min 29 s against 3 min 40 s; resilience 16 min 0 s against 15 min 11 s and 17 min 20 s), so no threshold is drifting towards flapping. The four skipped end-to-end tests are the slow acceptance tests, which run in their own layer (acceptance 4 passed) and are skipped in the end-to-end layer by design.

A pass is not the same as no issues, so the run's log, the repository and the live cluster were read for anything the tests do not fail on.

## 2. Issues found

| # | Finding | Evidence | Severity | What was done | Status |
|---|---|---|---|---|---|
| 1 | **Every workflow job ran on `ubuntu-latest`, which GitHub moves to Ubuntu 26 on 2026-10-19.** The next runner image could change Docker, Python or the toolchain under the nightly with no change of ours. | The annotation on every job of every run; `runs-on: ubuntu-latest` in all three workflows. | Low (a dated, known risk) | Pinned to `ubuntu-24.04` in `tests.yml`, `production-readiness.yml` and `publish-images.yml` (DEF-175). Moving to Ubuntu 26 is now a deliberate change, made when its first run has been read. | Fixed; confirmed by the runs in section 4 |
| 2 | **The actions were on versions that target Node 20, which GitHub is retiring.** | The `Node.js 20 is deprecated ... actions/checkout@v4, actions/setup-python@v5` annotation. | Low | `checkout@v5`, `setup-python@v6`, `upload-artifact@v6` and (found on the first run after the change, in the dashboard job) `setup-node@v5` (each read from the action's own `action.yml`: `node24`) in the two workflows that can be run. The docker actions and `checkout` in `publish-images.yml` were left alone: that workflow cannot be tried without publishing images, which is not done unasked. | Fixed (DEF-175), same confirmation |
| 3 | **One third-party warning was 5,757 of the statistical layer's 5,975 warnings, and hides anything new.** DoWhy 0.11.1 indexes a pandas Series by position, which pandas 2.1 deprecates and pandas 3 removes: bumping pandas would break the causal engine at its first estimate. | `FutureWarning: Series.__getitem__ treating keys as positions` at `dowhy/causal_estimators/regression_estimator.py:179`, 5,757 times. | Low today, high on a careless upgrade | The runner hides that one warning (`-W`, checked to hide it and nothing else); a static guard fails if pandas is moved off 2.x while DoWhy 0.11 is pinned (it was shown to fail on `pandas==3.0.0`) (DEF-176). | Fixed and tested |
| 4 | **A status document still said the broker had an anonymous listener on 1883 with no persistence.** | `restaurant-platform-implementation-status.md`, the infrastructure list. | Low (documentation) | Rewritten: TLS only on 8883, login and per-user rules, persistent. | Fixed |
| 5 | **DEF-171 (root-owned `__pycache__` blocking image builds) was still listed open** after the owner removed the directories and builds worked. | Defect log open items. | Info | Closed. | Fixed |

## 3. Checked and found fine

- **Live cluster:** nothing firing in Prometheus; no warning events; every recent backup job complete (the only failed backup job is from 2026-09-27, three attempts blocked by a pgBackRest lock, and every diff and full backup since has completed); no check pods left behind by the rotation; the plate node a single pod with no previous key; the ledger at 838 published and 0 missing across the rotation.
- **Restart counters of 5 to 14 on many pods:** every one dates from the laptop's boot on 2026-10-08 at about 14:06Z, when services restart until Kafka is ready (restart policy doing its job); none is from the day's work. Not changed.
- **Lint (`ruff`), the version-boundary rule, Helm lint and rendering, the executable bit on all 19 scripts:** clean.
- **Skips in the static layer:** two are documented exceptions (the official nginx image, limits held inside an operator resource); the third was `ruff` not being installed locally, now installed and passing.
- **Other warnings in the log:** the `kafka-python` 3.0.11 `importlib.resources` deprecation (109 times, upstream, harmless on the pinned Python) and a Node `punycode` notice from the actions themselves. Not ours to fix.

## 4. After the fixes

`tests` was green on `4d98b6c` and on `f60afb0` (the second added `setup-node@v5`, the one Node 20 warning the first pinned run still showed), with no annotation left. The full nightly was then run again on `f60afb0` (run 37819415191, the changed workflow itself): it passed every layer on `ubuntu-24.04` (confirmed in the job log): security 12, statistical 14, end-to-end 69 (4 skipped), acceptance 4, load 8, resilience 13; no deprecation annotation; the statistical layer's warnings fell from 5,975 to 218; durations in line (end-to-end 3 min 23 s, resilience 14 min 2 s).

## 5. What this review does not cover

A node loss on the cluster (the one node is the production machine); real plate-sensor data (none exists: the model's channels are simulated); the `publish-images` workflow (never run without asking). The pinned runner and the bumped actions were checked by the runs that followed the push, recorded in `07-test-summary-report.md`.
