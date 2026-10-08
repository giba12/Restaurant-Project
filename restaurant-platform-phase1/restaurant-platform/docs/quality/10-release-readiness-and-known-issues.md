# Release Readiness and Known Issues

| | |
|---|---|
| Document | Release readiness assessment and known-issues register |
| Project | Restaurant Operations Digital Twin Platform |
| Version | 1.0 |
| Date | 2026-10-08 (refreshed; first written 2026-10-03) |
| Baseline | Commit `6f5c34e` |
| Status | Assessment for a decision by the project owner |

## 1. Recommendation

**Ready to show, with its caveats disclosed. Not ready to call "released" or to describe as production-grade.**

The evidence is much stronger than when this was first written (2026-10-03). Both GitHub workflows now run and pass: `tests` on every push, and the full nightly (end-to-end, acceptance, resilience, load, statistical, security) on GitHub's clean Docker Engine, whose latest six full runs all passed. The repaired refutation gate is live on the cluster (deployed by the realign of 2026-10-07). The MQTT broker requires a login and speaks TLS only, on the cluster and on Compose; every edge node has its own control key and the master was rotated on the live cluster (2026-10-08); sensor events are not lost to a restart or outage of the bridge, Kafka or Mosquitto, nor to the loss of Mosquitto's disk (the resilience layer's ledger); and every write to the database is idempotent in the repository (DEF-173).

What is still true, and what a reader should weigh:
- **The live cluster does not yet run the idempotent-writes fix.** The service images (anomaly detector, causal engine, narrator, aggregator, storage consumer) were last built in the realign of 2026-10-06/07; only the simulator and bridge images were rebuilt and imported since. Deploying means rebuilding and importing them (needs `sudo`) and rolling the services (`k8s/realign/realign-live-cluster.sh` does it). Until then the cluster can still store a second anomaly or finding for a redelivered input.
- **It has been run only on the author's laptop and on GitHub's runners,** never on a recruiter's own machine, under the reader's own Compose plugin, or in a Codespace.
- **There has been no independent review:** the author of the code also wrote the tests and judged the results.
- **It has only ever seen simulated data.** Whether its findings mean anything on a real restaurant, and whether the edge model's calibration holds on real sensors, is untested by construction (RSK-031).
- **Not tried:** a node loss on the cluster (the one node is the production machine); a browser-level test of the dashboard; a repeated restore of the database backup (RSK-016).

For a recruiter or reviewer the project is credible *because* of that honesty. The conditions in section 4 would turn "ready to show" into "ready to rely on".

## 2. Readiness criteria

| Criterion | Status | Evidence |
|---|---|---|
| All "must" requirements met | **Met** | 100 requirements: 75 verified by test, 6 by inspection, 9 live or manual only, 9 partial, 1 not verified, 0 not met (`03-requirements-traceability-matrix.md`) |
| No open critical (S1) defects | **Met** | All eight S1 defects are fixed; seven are guarded by a test (`06-defect-log.md`) |
| No open high (S2) defects | **Met** | All 31 S2 defects are fixed, 14 of them guarded by a test. Only five entries are open, all informational (DEF-015, DEF-091, DEF-093, DEF-123, DEF-131) |
| Full test cycle passes | **Met** | GitHub, 2026-10-08, commit `6f5c34e`: `tests` green on every job (static 347 passed and 2 skipped, database integration 180 including the real-broker tests, unit tests 417 across ten suites, dashboard 15), and the full nightly passed every layer (security 12, statistical 14, end-to-end 69 with 4 skipped by design, acceptance 4, load 8, resilience 13) (`07-test-summary-report.md`) |
| Test cycle repeated | **Done; every failure was real** | The nightly has run on GitHub on every day since 2026-10-04. Of its latest 20 runs 14 passed and 6 failed (2026-10-05 to 10-08); each failure was a real defect or a fault in one of my tests and was fixed (the ingest path losing events, a footprint probe, end-to-end faults, the rebuild test; DEF-148, DEF-151, DEF-159, DEF-160, DEF-172), and the latest six full runs passed in a row. Layer times are stable (end-to-end 3 min 21 s to 3 min 40 s, resilience 14 to 17 min) |
| CI green on GitHub | **Met** | `tests` is green on the latest pushes with no deprecation annotation (runner pinned to `ubuntu-24.04`, actions on Node 24; DEF-175). Five pushes on 2026-10-07/08 were red (not consecutive), for assumptions my machine hides (rootless Podman, a Python that has `paho`), the executable bit (`core.fileMode` is off here) and a version-boundary rule that only sees tracked files; each was fixed in the next push (DEF-170 and the status-log entries 39 to 42) |
| Runs on a clean machine | **Verified on a GitHub runner; not on a recruiter's own machine** | A clean, cold-cache Ubuntu runner built and started the whole Compose stack in 3 min 22 s (2026-10-04) and runs every layer every night |
| Runs on Docker Engine | **Verified** | GitHub's Docker Engine with Compose v2 runs every stack layer nightly; the failures it surfaced were test-harness and robustness faults, not product incompatibilities (DEF-146, DEF-147, DEF-170) |
| Security basics | **Met for the demo scope** | No committed secrets, authentication on both APIs, least-privilege narrator, no known vulnerable dependency (the security layer, nightly), TLS on the Kubernetes path. The MQTT broker requires a login with a per-user rule set and speaks TLS only on both paths (the cluster's plaintext listener was closed 2026-10-07); each edge node has its own control key, and the master was rotated on the live cluster on 2026-10-08 (RSK-036). Plain HTTP on the Compose path is a documented local-only trade-off |
| Backup restored | **Met once** | 2026-09-24. Scheduled full and differential backups run and the recent ones complete (one differential failed on 2026-09-27 on a pgBackRest lock); the restore drill has not been repeated (RSK-016) |
| Live cluster runs the repository's code | **Not entirely** | The repaired refutation gate (2026-10-03) is live; the simulator and bridge images were re-imported on 2026-10-07/08; the service images predate the idempotent-writes fix (section 1) |
| Documentation current | **Yes, as of 2026-10-08** | The catalogue and registers are machine-checked. On 2026-10-08 every quality document was re-read against the cluster, the CI runs and the code and refreshed (`11-nightly-review-2026-10-08.md` records what the review of the nightly found). The codebase guide and status log are hand-edited and have been wrong before |
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

**Done since this was first written (2026-10-03):**
- The ledger test on the cluster (2026-10-06, `k8s/audit/k3s_scenarios.py`): six disturbances, 569 events published, 0 missing; an unplanned full restart lost 0 of 979. On Compose (2026-10-07/08): the loss of Mosquitto's volume (2,240 published, 0 missing, once) and of the database's volume (rebuilt from Kafka to Kafka's offsets).
- The repaired refutation gate deployed to the cluster (the realign of 2026-10-07).
- The first GitHub runs watched and what they surfaced fixed; the full nightly repeated (section 2).
- Broker authentication and TLS, per-node control keys, the master-key rotation on the live cluster, the arrival alarm seen firing on the cluster, and every database write made idempotent in the repository.

**Remaining, in order of value:**
1. **Deploy the idempotent-writes fix to the cluster:** rebuild and import the service images (`sudo`) and roll the services (`k8s/realign/realign-live-cluster.sh`), then run `k8s/audit/k3s_ledger.py` over the roll.
2. **Run the first-run path on a machine that is not the author's or a GitHub runner,** under the reader's own Compose plugin, and try the Codespaces devcontainer (RSK-022).
3. **Seek one independent review** of the requirements and the tests (RSK-026).
4. **Schedule the restore drill and alert when it has not succeeded recently** (RSK-016; it changes the live cluster).
5. **Measure code coverage** (`pytest-cov`) and close the weakly verified requirements listed at the end of the traceability matrix.
6. **Add a browser-level test of the dashboard** (FR-DSH-01; Playwright against the Compose stack).
7. **Test the simulators' timing distribution,** which the causal analysis relies on and nothing tests (RSK-008).
8. **Try a node loss on a disposable cluster.** Not possible on this one: the single node is the production machine.
9. **Real sensor data** for the edge model's calibration (RSK-031). None exists: the model's channels are simulated.

## 5. Known issues

### 5.1 Open defects (from the defect log; five, all informational)

| ID | Issue | Severity | Workaround |
|---|---|---|---|
| DEF-131 | The repaired refutation gate certifies statistical significance, not causation: an omitted confounder or a proxy treatment is invisible to it | Info | Treat findings as "unlikely to be noise", not "proven causal" |
| DEF-015, DEF-091, DEF-093, DEF-123 | Unconfirmed hypotheses and observations | Info | See the log |

### 5.2 Design limitations (accepted trade-offs, not bugs)

- Every service is a single replica; the aggregator, simulators and game bridge keep state in memory, so a restart drops tickets in flight.
- The Compose path has no Prometheus or Grafana and no MinIO or backup, and plain HTTP for the dashboard (the MQTT broker requires a login and speaks TLS only there, since 2026-10-07). It is a local demo.
- The plate-waste node's drift detection is measured on one simulated fault (a fouling lens, a pure bias) and nothing else. Two window monitors, spread and shift, now catch fouling of 0.15 to 0.2 in about 99% of onsets within a few minutes of readings (model 1.1.0, 2026-10-06), but at 0.1 a quarter of onsets still take over 150 readings, the shift monitor flags any sustained input change and cannot say what changed, and the combined alarm is in alarm about 0.27% of clean time (RSK-031, RSK-032, DEF-155). The live cluster runs the current simulator image (imported 2026-10-08).
- A node's model can now be changed from the cloud (signed retained commands, a versioned store, a canary, a shadow comparison, a rollback; 2026-10-06), run on the Compose stack and in unit tests, and switched on for the plate node on the cluster on 2026-10-07 (its pod holds its own key; the live check, `k8s/audit/verify-model-control.sh`, passed there once: a wrong signature was rejected and the right one answered `unchanged`). The gate measures agreement with the model in service, not correctness (there is no ground truth in the field); each node has its own key under one operator master secret, with a scripted, rehearsed rotation of the master (`rotate-master-key.sh`: rehearsed on k3s in a throwaway namespace, then run once on the live cluster on 2026-10-08); and a drift alarm only produces advice, never a rollout, on purpose (RSK-033, RSK-036, DEF-156).
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
| First start | The first build and model pull take several minutes (a cold GitHub runner built and started the stack in 3 min 22 s on 2026-10-04, without the model pull); findings take a while to appear unless one is triggered by hand (`QUICKSTART.md`) |
| Narration | Mostly template text on CPU, labelled as such |
| Podman users | After a WSL restart start the API socket; do not run another stack at the same time |

## 6. Sign-off

| Role | Name | Decision | Date |
|---|---|---|---|
| Project owner | *(pending)* | | |
| Independent reviewer | *(none yet)* | | |
