# Release Readiness and Known Issues

| | |
|---|---|
| Document | Release readiness assessment and known-issues register |
| Project | Restaurant Operations Digital Twin Platform |
| Version | 1.0 |
| Date | 2026-10-03 |
| Baseline | Commit `898c7aa` |
| Status | Assessment for a decision by the project owner |

## 1. Recommendation

**Ready to show, with its caveats disclosed. Not ready to call "released" or to describe as production-grade.** A caveat from the previous days is resolved: the injected staffing shortage's finding was refuted under the repaired gate (DEF-141) and now passes it, because the simulation contains the staffing effect the analysis looks for; that effect is a design of the simulation.

The system does what it claims on the machine it was built on, the evidence is unusually thorough for a project of this size, and its known weaknesses are written down rather than hidden. It has **not** been run on any machine other than the author's, its new CI workflows have not run on GitHub, and live Kubernetes clusters keep the old, weaker refutation gate until their images are rebuilt and imported (the gate itself was repaired in the repository on 2026-10-03).

For a recruiter or reviewer the project is credible *because* of that honesty. The conditions in section 4 would turn "ready to show" into "ready to rely on".

## 2. Readiness criteria

| Criterion | Status | Evidence |
|---|---|---|
| All "must" requirements met | **Met** | 96 requirements: 73 verified by test, 6 by inspection, 8 live or manual only, 8 partial, 1 not verified, 0 not met (`03-requirements-traceability-matrix.md`) |
| No open critical (S1) defects | **Met** | All six S1 defects are fixed; five are guarded by tests (`06-defect-log.md`) |
| No open high (S2) defects | **Met** | DEF-141 (the injected shortage's finding refuted under the repaired gate) was fixed on 2026-10-04 and verified live in two acceptance runs; DEF-106 (the gate itself) was repaired on 2026-10-03. No defect above informational is open |
| Full test cycle passes | **Met on the final code, after several failed attempts** | 2026-10-04: static, unit, integration (no expected-failures), end-to-end, acceptance (4, finding passes refutation) and resilience (7) passed; load (35 events/s), statistical (14) and security (11) were re-run and passed (`07-test-summary-report.md`) |
| Test cycle repeated | **Done, and it showed instability that was then explained** | The heavy layers were repeated in the second and third cycles; the Kafka-restart test failed in five of six runs (DEF-142) until its cause, Kafka Connect's default five-minute rebalance delay, was found in the logs and set to zero; the full resilience layer then passed 8 of 8, and eight further Kafka restarts resumed in 64 to 101 s. The live k3s cluster has the old setting until upgraded |
| CI green on GitHub | **Met for `tests`; mostly for the nightly workflow** | 2026-10-04: all 13 jobs of `tests` passed on the first run of the new ones (static 192, integration 116, edge-ai 63, connector-supervisor 20, nine unit jobs, the Godot smoke test). The first run of the nightly workflow passed statistical (14), security (11), acceptance (4), load (4) and resilience (8) and failed two e2e tests, both genuine and fixed (DEF-146, DEF-147); the fixes are verified locally and not yet re-run on GitHub. The image-publish workflow has not been run |
| Runs on a clean machine | **Verified on a GitHub runner; not on a recruiter's own machine** | A clean, cold-cache Ubuntu runner built and started the whole Compose stack in 3 min 22 s and ran every layer |
| Runs on Docker Engine | **Verified** | GitHub's Docker Engine with Compose v2: acceptance, load and resilience passed; the two e2e failures were a Compose-v2 harness bug and a twin robustness gap (DEF-146, DEF-147), not product incompatibilities |
| Security basics | **Met for the demo scope** | No committed secrets, authentication on both APIs, least-privilege narrator, no known vulnerable dependency, TLS on the Kubernetes path. Plaintext and anonymous access on the Compose path are a documented local-only trade-off |
| Backup restored | **Met once** | 2026-09-24 |
| Documentation current | **Mostly** | The catalogue and registers are machine-checked; the codebase guide and status log are hand-edited and have been wrong before |
| Independent review | **Not done** | See `04-sqa-plan.md` section 11 |

## 3. Phase exit criteria (from the original brief)

| Phase | The brief's "done when" | Evidence | Verdict |
|---|---|---|---|
| 2 Message backbone | A hand-published MQTT message appears on the Kafka topic | Connectors `RUNNING`; data flows (`test_all_four_mqtt_connectors_are_running`, `test_every_sensor_type_reaches_the_database`) | Met |
| 3 Simulators | Kafka receives a steady stream with no schema violations | Producer compatibility tests; zero schema violations in logs (`test_no_python_service_has_logged_a_traceback_or_error`) | Met |
| 4 Storage | Historical data queryable directly, independent of any dashboard | Row counts equal Kafka offsets, exactly (resilience conservation check) | Met |
| 5 Causal engine | An injected scenario produces a correctly attributed finding, not a raw correlation | `test_a_finding_computed_for_the_scenario_carries_its_id`: two live runs, staffing 9 to 2, 2.9x to 3.6x slowdown, about 100 anomalies at the loaded stations and none or one at the removed one, and a finding that passes the repaired gate with a negative effect (p = 4.8e-6 and 5.2e-4) and is promoted | **Met.** It was *not* met between 2026-10-03 and 2026-10-04: under the repaired gate the finding was refuted because the simulated world contained no staffing effect (DEF-141), and the earlier "met" verdict came from the original gate that passed most noise (DEF-106). The effect is one the simulation was designed to contain, so this shows the pipeline recovers a real effect, not that a real kitchen has it. The gate certifies significance, not causation (DEF-131) |
| 6 Twin and narrator | A finding is narrated with no invented detail beyond the finding | Guard tests; fake-model loop tests; database-boundary tests; live narrations | Met for faithfulness to the finding; the checks are mechanical, and on CPU most narrations are the template |
| 7 Dashboard and observability | Both run concurrently against the live simulated stream | Dashboard: automated. Prometheus and Grafana: verified live on k3s only; the Compose path has none | Met on Kubernetes, partly on Compose |

## 4. Conditions to turn "ready to show" into "ready to rely on"

In order of value:

1. **Deploy the repaired refutation gate.** It is fixed and verified in the repository; the live Kubernetes cluster still runs the old rule until its images are rebuilt and imported (`k8s/realign/realign-live-cluster.sh`, which needs `sudo`). Findings already stored were judged by the old rule.
2. **Push, then watch the first GitHub runs** (`gh run list`, `gh run watch`) of `tests.yml`'s new jobs and `production-readiness.yml`; fix whatever the runners surface.
3. **Run the first-run path on a clean machine and on Docker Engine,** measure the cold-build time, and try the Codespaces devcontainer.
4. **Seek one independent review** of the requirements and the tests.
5. **Repeat the full stack pass** (nightly, several times) to learn whether its thresholds flap.
6. **Measure code coverage** (`pytest-cov`) and close the weakly verified requirements listed at the end of the traceability matrix.

## 5. Known issues

### 5.1 Open defects (from the defect log)

| ID | Issue | Severity | Workaround |
|---|---|---|---|
| DEF-143 | One full stop and start did not resume in time; not reproduced | Info | See the log |
| DEF-131 | The repaired refutation gate certifies statistical significance, not causation: an omitted confounder or a proxy treatment is invisible to it | Info | Treat findings as "unlikely to be noise", not "proven causal" |
| DEF-015, DEF-091, DEF-093, DEF-123 | Unconfirmed hypotheses and observations | Info | See the log |

### 5.2 Design limitations (accepted trade-offs, not bugs)

- Every service is a single replica; the aggregator, simulators and game bridge keep state in memory, so a restart drops tickets in flight.
- The Compose path has no Prometheus or Grafana and no MinIO, backup or TLS; Mosquitto allows anonymous clients. It is a local demo.
- Scenario injection affects only the service-timing simulator; scenarios aimed at the other three publish but do nothing.
- On a CPU, the 0.5B model rarely passes verification, so most narrations are the labelled deterministic template (`template-fallback`).
- Prometheus stores metrics in an `emptyDir`; history resets when its pod restarts.
- Interactive (game) tickets produce no anomalies or findings of their own, by design.
- The system has only ever seen simulated data; whether its findings are meaningful on a real restaurant is untested by construction.

### 5.3 What a reviewer should expect

| Expectation | Reality |
|---|---|
| "One command" | `docker compose up --build`, from `restaurant-platform-phase1/restaurant-platform` |
| Disk | About 15 GB of images (the TimescaleDB and Ollama images are about 11 GB of it) |
| Memory | About 4 GB free; roughly 1.7 GB in use once running |
| First start | The first build and model pull take several minutes (not measured cold); findings take a while to appear unless one is triggered by hand (`QUICKSTART.md`) |
| Narration | Mostly template text on CPU, labelled as such |
| Podman users | After a WSL restart start the API socket; do not run another stack at the same time |

## 6. Sign-off

| Role | Name | Decision | Date |
|---|---|---|---|
| Project owner | *(pending)* | | |
| Independent reviewer | *(none yet)* | | |
