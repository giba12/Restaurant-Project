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
| All "must" requirements met | **Met** | 100 requirements: 75 verified by test, 6 by inspection, 9 live or manual only, 9 partial, 1 not verified, 0 not met (`03-requirements-traceability-matrix.md`) |
| No open critical (S1) defects | **Met** | All six S1 defects are fixed; five are guarded by tests (`06-defect-log.md`) |
| No open high (S2) defects | **Met** | DEF-141 (the injected shortage's finding refuted under the repaired gate) was fixed on 2026-10-04 and verified live in two acceptance runs; DEF-106 (the gate itself) was repaired on 2026-10-03. No defect above informational is open |
| Full test cycle passes | **Met on the final code, after several failed attempts** | 2026-10-04: static, unit, integration (no expected-failures), end-to-end, acceptance (4, finding passes refutation) and resilience (7) passed; load (35 events/s), statistical (14) and security (11) were re-run and passed (`07-test-summary-report.md`) |
| Test cycle repeated | **Done, and it showed instability that was then explained** | The heavy layers were repeated in the second and third cycles; the Kafka-restart test failed in five of six runs (DEF-142) until its cause, Kafka Connect's default five-minute rebalance delay, was found in the logs and set to zero; the full resilience layer then passed 8 of 8, and eight further Kafka restarts resumed in 64 to 101 s. The live k3s cluster has the old setting until upgraded |
| CI green on GitHub | **Met for `tests`; mostly for the nightly workflow** | 2026-10-04: all 13 jobs of `tests` passed on the first run of the new ones (static 192, integration 116, edge-ai 63, connector-supervisor 20, nine unit jobs, the Godot smoke test). The first run of the nightly workflow passed statistical (14), security (11), acceptance (4), load (4) and resilience (8) and failed two e2e tests, both genuine and fixed (DEF-146, DEF-147); the fixes were re-run on GitHub the same evening (the nightly workflow with only the end-to-end layer: e2e 65 passed, 4 skipped; the statistical and security jobs passed again); a later full nightly run (2026-10-05, run `37355406811`) passed every layer, resilience 9 of 9 included, after the first nightly of that day failed two new ledger checks (DEF-151). The image-publish workflow has not been run |
| Runs on a clean machine | **Verified on a GitHub runner; not on a recruiter's own machine** | A clean, cold-cache Ubuntu runner built and started the whole Compose stack in 3 min 22 s and ran every layer |
| Runs on Docker Engine | **Verified** | GitHub's Docker Engine with Compose v2: acceptance, load and resilience passed; the two e2e failures were a Compose-v2 harness bug and a twin robustness gap (DEF-146, DEF-147), not product incompatibilities |
| Security basics | **Met for the demo scope** | No committed secrets, authentication on both APIs, least-privilege narrator, no known vulnerable dependency, TLS on the Kubernetes path. Plain HTTP on the Compose path is a documented local-only trade-off; the MQTT broker requires a login with a per-user ACL and (on Compose, since 2026-10-07) speaks TLS only, in the repository and, since 2026-10-07, on the live cluster (RSK-036) |
| Backup restored | **Met once** | 2026-09-24 |
| Documentation current | **Mostly** | The catalogue and registers are machine-checked; the codebase guide and status log are hand-edited and have been wrong before |
| Independent review | **Not done** | See `04-sqa-plan.md` section 11 |

## 3. Phase exit criteria (from the original brief)

| Phase | The brief's "done when" | Evidence | Verdict |
|---|---|---|---|
| 2 Message backbone | A hand-published MQTT message appears on the Kafka topic | The MQTT-Kafka bridge is connected and forwards all four topics; data flows (`test_the_bridge_is_connected_to_mqtt_and_subscribed`, `test_the_bridge_forwards_all_four_sensor_topics`, `test_every_sensor_type_reaches_the_database`); and no event is lost to a restart (the resilience layer's ledger checks) | Met |
| 3 Simulators | Kafka receives a steady stream with no schema violations | Producer compatibility tests; zero schema violations in logs (`test_no_python_service_has_logged_a_traceback_or_error`) | Met |
| 4 Storage | Historical data queryable directly, independent of any dashboard | Row counts equal Kafka offsets, exactly (resilience conservation check) | Met |
| 5 Causal engine | An injected scenario produces a correctly attributed finding, not a raw correlation | `test_a_finding_computed_for_the_scenario_carries_its_id`: two live runs, staffing 9 to 2, 2.9x to 3.6x slowdown, about 100 anomalies at the loaded stations and none or one at the removed one, and a finding that passes the repaired gate with a negative effect (p = 4.8e-6 and 5.2e-4) and is promoted | **Met.** It was *not* met between 2026-10-03 and 2026-10-04: under the repaired gate the finding was refuted because the simulated world contained no staffing effect (DEF-141), and the earlier "met" verdict came from the original gate that passed most noise (DEF-106). The effect is one the simulation was designed to contain, so this shows the pipeline recovers a real effect, not that a real kitchen has it. The gate certifies significance, not causation (DEF-131) |
| 6 Twin and narrator | A finding is narrated with no invented detail beyond the finding | Guard tests; fake-model loop tests; database-boundary tests; live narrations | Met for faithfulness to the finding; the checks are mechanical, and on CPU most narrations are the template |
| 7 Dashboard and observability | Both run concurrently against the live simulated stream | Dashboard: automated. Prometheus and Grafana: verified live on k3s only; the Compose path has none | Met on Kubernetes, partly on Compose |

## 4. Conditions to turn "ready to show" into "ready to rely on"

In order of value:

0. **Done 2026-10-06: the ledger test on the cluster.** Run by hand with `k8s/audit/k3s_scenarios.py`: six disturbances, 569 events published, **0 missing**. Bridge rolled (52 published), bridge force-deleted with no grace period (43), bridge scaled to zero for 90 s (139), Mosquitto rolled (42), Mosquitto force-deleted (40), Kafka broker pod deleted (253; the pipeline took 5 min 38 s to resume while Kafka's group coordinator reloaded). An unplanned full-stack restart (the machine was off about 11 hours) lost 0 of 979. The first look after the Kafka restart in an earlier trial showed 125 to 151 events 'missing' that were only in transit; with the settle time and re-look the script now uses, they had arrived. Tried on Compose on 2026-10-07 (not on the cluster): the loss of Mosquitto's volume (2,240 events published from just before it, 0 missing, once) and of the database's volume (rebuilt from Kafka to Kafka's offsets). On k3s the loss of the broker's volume was tried on a throwaway copy of the chart in its own namespace (the pod waits until the chart is applied again, then the broker is empty and accepts logins). Not covered on the cluster: a node loss, or a PVC loss on the live broker or database (single node, local-path volumes, real data), and a Mosquitto kill landing inside the 5 s save window (a hard kill, seen 0 of 2,632 on Compose, was not repeated five times there).
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
- The Compose path has no Prometheus or Grafana and no MinIO, backup or TLS (Mosquitto requires a login there, since 2026-10-06). It is a local demo.
- The plate-waste node's drift detection is measured on one simulated fault (a fouling lens, a pure bias) and nothing else. Two window monitors, spread and shift, now catch fouling of 0.15 to 0.2 in about 99% of onsets within a few minutes of readings (model 1.1.0, 2026-10-06), but at 0.1 a quarter of onsets still take over 150 readings, the shift monitor flags any sustained input change and cannot say what changed, the combined alarm is in alarm about 0.27% of clean time, and the live cluster still runs the previous simulator image until it is rebuilt and imported (RSK-031, RSK-032, DEF-155).
- A node's model can now be changed from the cloud (signed retained commands, a versioned store, a canary, a shadow comparison, a rollback; 2026-10-06), run on the Compose stack and in unit tests, and switched on for the plate node on the cluster on 2026-10-07 (its pod holds its own key; the live check, `k8s/audit/verify-model-control.sh`, passed there once: a wrong signature was rejected and the right one answered `unchanged`). The gate measures agreement with the model in service, not correctness (there is no ground truth in the field); each node has its own key under one operator master secret, with a scripted, rehearsed rotation of the master (`rotate-master-key.sh`: rehearsed on k3s in a throwaway namespace, not yet run on the live cluster); and a drift alarm only produces advice, never a rollout, on purpose (RSK-033, RSK-036, DEF-156).
- The plate node's drift monitoring was checked against real drift from a public chemical-sensor dataset (method only: no public data has the plate model's channels). Large drift was caught; the 0.1% false-alarm calibration and the mild-drift detection measured on the simulation did not transfer, and a mild gain loss (0.7) is caught only about a fifth of the time; a flatline monitor (2026-10-06, model 1.2.0) now catches a stuck sensor or a larger gain loss (DEF-157). The node needs about 40 MiB for its runtime (the model is 5 KB), measured in a container under the chart's CPU and memory limits, not on any other processor (RSK-031, RSK-032).
- **The MQTT broker requires a login everywhere, and a wrong access rule is silent.** Compose, the Helm charts and, since 2026-10-07, the live cluster refuse anonymous clients and limit each login to its own topics, and every edge node has its own control key (RSK-036, NFR-SEC-08). The cluster cutover was run by the owner with `k8s/realign/realign-live-cluster.sh` and checked by hand afterwards (not by a repeatable test). Mosquitto enforces its rules silently (a refused subscription is granted and delivers nothing), so a wrong ACL would show as silence rather than an error: the tests catch it, and a `SensorTopicSilent` alert (ten silent minutes on a topic while the bridge is connected) now covers a sensor that stops arriving, tested with `promtool` and deployed on the cluster on 2026-10-07 (loaded in Prometheus, nothing firing). The cluster's broker has had no plaintext listener since 2026-10-07: only TLS on 8883, which every client already used.
- The React dashboard has 15 component tests against canned API answers, run in CI, and no test in a real browser (FR-DSH-01).
- On-node inference exists only on the plate-waste node, by decision (2026-10-06): the other three sensors emit events that already are the information, so there is no raw signal for a model to work on, and the cloud already analyses them (the reasoning is in `edge-simulators/MODEL_CARD.md`).
- Scenario injection acts on the service-timing and staff-shift simulators (the staffing shortage needs both); scenarios aimed at the plate-waste and POS simulators publish but do nothing.
- Sensor events are not lost to a restart or outage of the bridge, Kafka or Mosquitto (the MQTT-Kafka bridge keeps a persistent session and acknowledges only after Kafka confirms; Mosquitto persists the queue). Left: a hard kill of Mosquitto itself can in principle lose what it had acknowledged but not yet saved (saved every 5 s; five kills lost 0 of 2,632 events), the queue is capped at 500,000 messages per client, and a crash between Kafka's confirmation and the acknowledgement delivers a message twice (harmless to storage). On the live k3s cluster the same guarantee held once, by hand (2026-10-06: six disturbances, 569 events, 0 missing; RSK-035); it is not repeated automatically.
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
