# Test Summary Report

| | |
|---|---|
| Document | Test summary report |
| Project | Restaurant Operations Digital Twin Platform |
| Version | 1.0 |
| Date | 2026-10-03, updated 2026-10-04 |
| Baseline | Commit `4960c62` plus the edge-inference feature (the commit that follows it) |
| Cycle | The 2026-10-02/03 production-style regime, one complete clean pass on 2026-10-03 (first cycle); a second cycle on 2026-10-03/04 after the edge-inference feature, which was not clean on its first attempt; a third on 2026-10-04 that fixed the open findings and re-ran what they touched (sections 1, 2 and 6) |
| Environment | See `09-test-environment-and-configuration-baseline.md` (one laptop, rootless Podman) |

## 1. Verdict

**First cycle (2026-10-03).** Every layer of the regime passed, apart from one test that deliberately fails because it records a known, unfixed defect. Building the regime found and fixed **two silent-data-loss defects** and surfaced **one significant weakness the project had been understating, the refutation gate, which was then repaired the same day** (section 3.4).

**Second cycle (2026-10-03/04), after the edge-inference feature.** It was not a clean first pass: the heavy layers failed several times, for reasons recorded as DEF-133 to DEF-143. Three real defects were fixed on the way (the aggregator crashing on a backwards clock step, a per-message schema re-check that cost ingest throughput, a harness check that could not see failed connector tasks), and it surfaced three findings that mattered more than the passes: the injected staffing shortage's causal finding was refuted under the repaired gate (DEF-141, S2), a start-up DNS failure could leave every connector task failed with nothing to restart them (DEF-137), and Kafka Connect took about six minutes to resume after a Kafka restart (DEF-142).

**Third cycle (2026-10-04), fixing those findings and re-running what they touch.** All three are addressed, and so are the other open items that could be fixed:
- **DEF-141, fixed and verified live.** The simulated world now contains the relationship the analysis depends on (staffing drives the kitchen's capacity; the shortage clocks staff out). In two independent acceptance runs the finding for the injected shortage **passes the repaired refutation gate** (effect -2,399 ms per additional staff member, p = 4.8e-6; and -2,098 ms, p = 5.2e-4) and is promoted by the live reviewer. The effect is one the simulation was designed to contain: this shows the pipeline recovers a real effect, not that a real kitchen has this one.
- **DEF-137, fixed and verified live.** A connector supervisor restarts failed tasks; the failure was reproduced on purpose and the supervisor healed it, and the same test **fails with the supervisor stopped**.
- **DEF-142, root cause found and fixed.** Captured logs show a wait of exactly 300 s: Kafka Connect's default `scheduled.rebalance.max.delay.ms`. Set to zero (Compose and Kubernetes); eight consecutive Kafka restarts then resumed in 64 to 101 s (before: 376 to 378 s), and the full resilience layer passed 8 of 8.
- **DEF-143, probable cause removed.** Not reproduced in 13 stress cycles; the most likely cause is the same 300 s delay recurring around the full restart. An inference, not a demonstration.
- **DEF-107** (the twin double-counting a redelivered order) and **DEF-056** (stalled tickets blinding the anomaly detector) are fixed and tested; the integration layer now has no expected failures. **DEF-128** (no coverage measure) is closed by a measurement (section 3.6).

On the final code, every layer was re-run and passed: static 191 (3 skipped), unit 275, database integration 116, end-to-end 65, acceptance 4, resilience 7 (plus the new supervisor test, run separately), load 4, statistical 14, security 11.

| | |
|---|---|
| Requirements | 100 identified: 75 verified by automated test, 6 by inspection, 9 live or manual only, 9 partial, 1 not verified, **0 not met** |
| Open defects | 5, all informational (DEF-015, DEF-091, DEF-093, DEF-123, DEF-131) |
| Recommendation | **Ready to show, with caveats; not "released"** (`10-release-readiness-and-known-issues.md`) |

**Fourth step (2026-10-04): the first GitHub runs.** The commits were pushed once, with permission, and the new workflows ran on GitHub's runners (Docker Engine, Compose v2, a clean cold-cache machine). `tests`: all 13 jobs green on the first run of the new ones. The nightly workflow, triggered by hand: statistical 14 of 14 (64 s), security 11 of 11, and in the stack job acceptance 4 of 4 (9 min), load 4 of 4 (55 s), resilience 8 of 8 (11 min), e2e 63 passed and **2 failed**, both genuine: a harness helper that relied on Compose v1 listing exited containers (DEF-146) and a twin that created a staff row with no status when its history was incomplete (DEF-147). Both are fixed and verified locally (integration 118, e2e 65); both fixes were then **re-run on GitHub the same evening (the nightly workflow with only the end-to-end layer: e2e 65 passed, 4 skipped; the statistical and security jobs passed again)**, after a second push the owner asked for. The whole stack built and started cold in 3 min 22 s on the clean runner.

**Fifth step (2026-10-05): a new check found what the old ones could not, and the ingest path was replaced.** The resilience layer gained a *ledger*: the simulators' own log of every event they published, compared with what reached the database, which "every message in Kafka is stored exactly once" cannot see. The first run found **37% of events lost across one 45 s Connect outage** (DEF-148), and, once a gate in the simulators had shrunk that, GitHub's nightly run found 26 of 734 lost across a Kafka restart and 2 of 624 across a Mosquitto restart (DEF-151). The owner asked for a real fix; **Kafka Connect and its connectors were replaced by an MQTT-Kafka bridge** with a persistent MQTT session that acknowledges a message only after Kafka has confirmed it, with Mosquitto persisting the session and queue (DEF-152). Results on the Compose stack, rebuilt from nothing: end-to-end 66 passed (4 skipped); acceptance 4 of 4 (the finding passes refutation again); load 4 of 4 (event-to-stored latency p95 0.06 s, down from 0.95 s); **resilience 11 tests with no loss allowed in any of them** (Kafka restart, Mosquitto restart, Mosquitto `SIGKILL`ed, bridge stopped, bridge killed, Kafka stopped 100 s, full restart): ten passed as a layer, and one timed out once on Docker's health state for Kafka (not on loss) and then passed alone, a wait that is now wider; static 201 passed; security 12 passed. Bridge unit tests 28 and wiring tests 8, each checked by breaking the code they guard. The first real run of the bridge also found a settings mistake no unit test could (DEF-153). **Then GitHub's Docker Engine ran it for the first time** (nightly 37396768496): e2e 66, acceptance 4, load 4, statistical and security passed, but resilience passed only 6 of 11, because the harness could not really crash a root-owned container there and one dead Mosquitto cascaded into five failures (DEF-154); fixed, and the re-run on GitHub (nightly 37402707159, commit 273cf47) **passed every layer**: e2e 66, acceptance 4, load 4, resilience 11 of 11 with no loss allowed, statistical and security. **Kubernetes:** the first realign run failed at the Mosquitto upgrade (a strategy-type change that Helm's apply rejected on the live object, now avoided); the second succeeded. The bridge has run on k3s since 2026-10-05, healthy with 0 Kafka errors, and Kafka Connect is gone. **Then the ledger test on the live cluster (2026-10-06, `k8s/audit/k3s_scenarios.py`, by hand):** bridge rolled, bridge force-deleted, bridge stopped 90 s, Mosquitto rolled, Mosquitto force-deleted, Kafka broker pod deleted: 569 events published, **0 missing**; an unplanned full restart of the stack (machine off about 11 hours) lost 0 of 979. **Not done:** a node or volume loss, and running these scenarios on a schedule.

**Sixth step (2026-10-06): the drift monitor's known weakness was measured, and the proposed fix was found not to work.** After the cluster ledger test (six disturbances, 569 events, none missing), the owner asked for the AI work to continue on the plate-waste node only. The risk register had said a CUSUM statistic would narrow the monitor's gap on mild sensor faults; measured, CUSUM on the distance and on three transforms of it was no better than the rolling mean (best 57.5% against 59.5% at fouling 0.2). A test on the *mean* deviation of the readings, which is what a fouling lens moves, worked: fouling 0.2 caught in 99.5% of onsets with a median of 23 readings (before: 59.5%, 143), 0.15 in 99.5% (before: 22.5%), 0.1 in 94.5% (before: 8.5%), at a combined clean false-alarm time of 0.27% against 0.13%. It was added without retraining (the weights are identical, model 1.1.0, event schema 1.2.0) and carries 16 new tests, checked by six deliberate breakages, one of which first survived and produced a further test (DEF-155). Edge suite 84 passed, schemas 7, causal engine 16, dashboard API 21. On the Compose stack rebuilt from the repository, **end-to-end passed 66 (4 skipped)** with the stored events naming the new model hash and carrying `shift_score`. **Not run:** the integration, load, resilience and acceptance layers, a GitHub run, and the live cluster.

**Seventh step (2026-10-06): the rest of the owner's AI list, in the order that made each step usable by the next.** The React dashboard, which had no tests of its own, gained 15 component tests and a CI job (eight deliberate breakages each caught). A cloud-to-edge control path and a model update path were built for the plate-waste node: signed retained commands, a versioned store, a canary, a shadow comparison against the model in service on live readings, and a rollback to any stored version (FR-EDG-08, FR-EDG-09). Their first real use found a defect (DEF-156): the 1.1.0 loader could not load the 1.0.0 artifact, so a rollback would have failed on every node. Verification: 49 tests of the node, store, commands and CLI, 6 static wiring tests, and a live-stack test in which a running node was rolled back to 1.0.0, refused a corrupted artifact and a model 30 g heavier that passed every check but the shadow comparison, and was rolled forward with no restart; sixteen deliberate breakages of the safeguards and four of the wiring were each caught the first time. 56 new distinct tests (501 in all). **Not run:** the cluster (control is off there until a Secret is created), a GitHub run, the constrained-device test and the real-data validation.

**Eighth step (2026-10-06): the last two items on the list, with the results as they came.** *Constrained device.* With no real hardware, the real node image was run under the limits its chart already imposes (100m CPU, 96Mi): 41-43 MiB resident (31 MiB of it the Python libraries, the model 5 KB), a floor between 24 and 28 MiB, inference p99 0.88 ms at the node's real pace, and a back-to-back p99 near 90 ms because a CPU quota pauses a container. Four load-layer tests keep it. It says nothing about a microcontroller or a slow ARM core. *Real data.* No public dataset has the plate model's channels, so the model could not be validated. The drift-monitoring method was checked on a public chemical-sensor drift dataset: the shift monitor caught large drift in every batch (the spread monitor missed two), **but the 0.1% false-alarm calibration did not transfer (0.66% and 4.5%) and mild drift was not reliably caught**, so the simulated figures for those do not generalise. The harness also exposed a gap neither monitor covers (DEF-157, open). Five offline tests of the harness; the real check needs the network and is not in CI.

**Ninth step (2026-10-06): GitHub, and the gap the real-data harness left open.** GitHub's `tests` workflow failed twice in the `integration` job on one test that hardcoded a schema version the node had moved past (the other 117 integration tests and every other job passed); it was fixed, run locally (118 passed) and passed on GitHub in all 14 jobs, including the new `dashboard-web` job and the edge tests. The open defect from the real-data harness (DEF-157: neither monitor sees a signal that goes quiet) was first confirmed on the plate node itself (a stuck light sensor: spread, shift and per-reading guard all 0%) and then closed with a third monitor, the flatline monitor (model 1.2.0, schema 1.3.0): stuck sensors caught 100%, a gain loss to 0.5 99.8%, **a gain loss to 0.7 only 21%**, all three monitors in alarm about 0.24% of clean time. 17 new tests, six deliberate breakages each caught; 526 distinct tests in all. **GitHub's nightly run on that commit** (Docker Engine) passed end-to-end 67 (4 skipped), acceptance 4, resilience 11 of 11, statistical and security, and 7 of 8 load tests; the eighth was my own footprint test, whose fixed burst was too small to be throttled on a faster machine (DEF-159), now fixed. DEF-158 and DEF-160 record the integration-test and latency-assertion faults. **The re-run on the fix commit (nightly 37522200991) passed every layer on GitHub:** statistical 14, end-to-end 67 (4 skipped), acceptance 4, load 8 of 8, resilience 11 of 11, security 12, and the `tests` workflow in all 14 jobs. The footprint figures on GitHub: 41 MiB resident, inference p99 0.21 ms under 100m CPU (about four times quicker than on the laptop), cgroup peak 25 MiB.

**Tenth step (2026-10-06): per-node control keys and broker authentication, built against the real broker.** The MQTT broker now refuses anonymous clients and limits each login to its own topics, and every edge node has its own control key. 41 tests run the real Mosquitto image (refused logins; each simulator publishing only its own topic; the bridge reading four topics and publishing nothing; only the operator sending a command; no session hijack by claiming a client id; the operator's tool against the real broker), 22 static tests pin the cross-file facts and the cutover order, 13 test the provisioning script against recording stand-ins for `kubectl`; broken on purpose seven, eight and five ways, each caught (one anonymous-access mutant only once a second lock was removed too). **Measured on the way, and the design rests on it:** Mosquitto grants a subscription its ACL refuses and delivers nothing, and acknowledges a refused publish as a success in MQTT 3.1.1, so reads and publishes are tested by delivery and the simulators and the operator tool moved to MQTT 5 (DEF-161). It also found the broker will refuse root-owned password and ACL files in a future version (DEF-162), and four faults of mine that only the real stack showed (DEF-163 to DEF-165, including a name clash that crashed every publish of the operator's tool). **On the authenticated stack locally:** end-to-end 68 passed (4 skipped), resilience 11 of 11 with no loss allowed (the Mosquitto restart and the hard kill included; 1 h 21 min on an overloaded laptop), footprint 4 of 4. 612 distinct tests. **On GitHub** the nightly (37555637314) passed every layer on the authenticated broker: statistical 14, end-to-end 68 (4 skipped), acceptance 4, load 8 of 8, resilience 11 of 11 with no loss allowed (13 min 40 s on a clean runner, against 1 h 21 min on the overloaded laptop), security 12; the `integration` job ran the real-broker tests first time, and the one `tests` failure (the new scripts recorded as non-executable under `core.fileMode=false`) was fixed. **Not done:** the cutover on the k3s cluster (its broker is anonymous until `k8s/realign/realign-live-cluster.sh` is run), broker TLS on the Compose path, a master-secret rotation procedure.

**Eleventh step (2026-10-07): the cutover on the live cluster.** The owner ran the staged cutover (`k8s/realign/realign-live-cluster.sh`) and it was checked read-only afterwards: an anonymous client is refused, the bridge is connected and forwarding all four topics with no Kafka errors, all four simulators logged in, the database clients are back and rows land within seconds, no alert is firing, and the no-loss ledger over the window since the new simulator pods started found 151 published and 0 missing. A few events were not published during the roughly 30 seconds the broker pod was replaced (logged as errors, not counted as published). This was a single manual check, not a repeatable test.

**Twelfth step (2026-10-07): model control on the cluster and the remaining gaps.** Model control was switched on for the plate node on the cluster (the chart upgrade differed from the live manifest by one environment variable; the ledger since found 4,133 published and 0 missing), but its live check (`k8s/audit/verify-model-control.sh`) was **not run**: reading the operator's credentials from the cluster to drive the production broker was refused, so the script was tested against stand-ins (7 tests, including a node that accepts a wrong signature and a node that ignores a right one, both of which must fail it) for the owner to run. The arrival alarm was tested with `promtool test rules` against the rule the chart renders (7 tests; removing each of its three guard clauses turned exactly the matching test red) and the master-secret rotation with stand-ins (7 tests, five deliberate breakages caught) and through the whole key window at the node. The Compose broker became TLS-only: 47 tests on the real Mosquitto image (six new: no plaintext listener, no downgrade, an untrusted certificate refused, TLS 1.2 or newer, the key's ownership and mode, the certificate kept across restarts), the static layer, the end-to-end layer on the live stack (66 of 68; the two failures, a ticket-summary consistency test and a start-up log check, were traced to a step of WSL2's wall clock of 1.2 to 1.5 s and to a Kafka time-out under load, DEF-168, with the ledger showing 5,370 published and 0 missing), and the resilience layer (13 of 13 in 1 h 27 min on a loaded machine, including Mosquitto's restart and `SIGKILL`). Two new resilience tests lose a disk: Mosquitto's (2,240 published from just before the loss, 0 missing, measured once) and the database's (rebuilt from Kafka to exactly Kafka's offsets). The edge simulators' unit suite (183) and the bridge's (33) passed. What this does not show: the live model-control check, the alert on the cluster, a live rotation, a node loss or a volume loss on k3s.

**Thirteenth step (2026-10-07): the simulator's timestamps and the cluster's plaintext listener.** DEF-168 was fixed in the service-timing simulator and pinned by a test that steps the wall clock back 1.5 s and forward 4 s while tickets are open (it fails on the old per-event stamping). The cluster broker's plaintext listener was closed: the chart's config is now run whole, over TLS, in the real Mosquitto image, with port 1883 published to see whether anything answers. That test failed first (a CONNACK came back from 1883), which showed the chart's settings, being before its first `listener` line, created an implicit default listener; the TLS listener was moved to the top and the test passed, and failed again when a 1883 listener was added back. The change was then applied to the cluster after a server-side dry run, and checked: 1883 refuses, anonymous clients are refused on 8883, only 8883 is exposed, every simulator publishes with no authentication errors, the bridge is connected, and the ledger across the broker's restart found 22 published and 0 missing (a short window; the resilience tests are what show a broker restart loses nothing). Afterwards: static 276 passed, the real-broker suite 47, the edge simulators 185, the bridge 33; and the end-to-end layer on a fresh stack, 66 of 68. The ticket-summary test that failed before DEF-168 passed (one run); the two failures were the storage consumer's start-up checks, one Kafka "Connection timed out" while the stack was starting on a loaded machine (the service restarted once), the same start-up weakness under load seen earlier with the aggregator and not connected to MQTT. \1

**Fourteenth step (2026-10-07): the live checks on the cluster.** The owner deployed the rebuilt images and the alert and ran `k8s/audit/verify-model-control.sh` (after it was taught to check for `paho-mqtt` first): a command signed with a wrong key was answered `rejected: bad signature`, the correctly signed command for the version the node runs was answered `unchanged: already running this model`, and the retained command was cleared. A read-only check of the pipeline afterwards found the bridge connected (0 Kafka errors, 0 unconfirmed), all four simulators publishing with no authentication errors, `SensorTopicSilent` loaded in Prometheus with nothing firing, and the ledger since the simulators restarted at 113 published and 0 missing (it cannot see the 19:05Z restart of Mosquitto, a clean SIGTERM). \1

**Fifteenth step (2026-10-07, later): GitHub, a rotation on a real node, and a trial of the broker on throwaway Kubernetes.** Reading GitHub's runs, which I had not been watching, showed the `tests` workflow red on the last three pushes: the `integration` job from the first commit of the day (the certificate tests read a private key that rootful Docker writes as root; rootless Podman, here, maps root to the user, which hid it) and the `static` job on the last (the stand-in tests of `verify-model-control.sh` broke when the script learned to check for `paho-mqtt`, which CI's Python lacks; reproduced locally in a virtual environment with no `paho`). Both were fixed and re-run locally (DEF-170); they are **not yet confirmed green on GitHub**. The master rotation was then played on the running Compose stack: a real plate-node container recreated holding its key under a new master and the old one, commanded by the real operator tool as each master (both answered `unchanged`), recreated again without the old key (the old master answered `rejected: bad signature`, the new one still accepted): 2 passed in 12 min 31 s with the live-node test. Two scripts for the owner to run on the live cluster were written and tested against stand-ins: `k8s/audit/test-sensor-alert.sh` (pauses one simulator about 13 minutes and checks the alert reaches Prometheus, Alertmanager and the relay and clears; **not run**: pausing a production workload was refused, so the arrival alarm has still never been seen to fire on the cluster) and, run by me because it touches only its own throwaway namespace, `k8s/audit/test-broker-in-scratch-namespace.sh`, which passed (11 checks) on k3s: with auth off an anonymous client publishes; with auth on it is refused, each login works, a sensor's forged publish to another sensor's topic reaches nobody, and a retained message reaches the bridge's login; then with the broker's volume claim deleted the new pod stays Pending until the chart is applied again, comes up with a new bound claim and **empty** (the retained message is gone), and accepts logins. Its two negative checks first passed vacuously (a client pod that printed nothing would have satisfied them), so they now require a positive signal (the forged publish acknowledged; the reader connecting and timing out cleanly). The namespace and its volume were verified gone and the `kafka` namespace untouched.

**The limits that matter most:** one machine and one container engine; the two e2e fixes from the first GitHub run were re-run there, and the full nightly workflow passed every layer on 2026-10-05 after two new ledger checks first failed (DEF-151); the heavy layers are flaky in this environment (the resilience layer needed six runs in the second cycle to produce one clean pass), so their stability is unmeasured; the staffing effect and the edge sensors are designs of the simulation, so passing shows the pipeline works, not that the real world behaves so; there is no independent reviewer.

## 2. Results by layer

| # | Layer | Result | Time | Last run | Notes |
|---|---|---|---|---|---|
| 1 | Static | **192 passed, 3 skipped** | about 45 s | 2026-10-04 | The 3 skips are documented exceptions (two operator-managed charts; the nginx image); lint ran |
| 2 | Unit | **275 passed** | seconds per suite | 2026-10-04 | edge node, world coupling and trainer 63, detector 25, aggregator and origin 33, causal engine 16, narrator 35, dashboard API 21, alert relay 5, game bridge 50, schema compatibility 7, connector supervisor 20 |
| 3 | Database integration | **118 passed, no expected-fail** | 107 s | 2026-10-04 | The twin's redelivery double-count (DEF-107) is fixed, so the one expected-fail is gone |
| 4 | Statistical | **14 passed** | 5 min 36 s | 2026-10-04 | Re-run on the final code: unchanged |
| 5 | End-to-end | **65 passed, 4 skipped** | 8 min 27 s | 2026-10-04 | The 4 skips are the slow acceptance tests; includes the supervisor service and two edge-inference tests |
| 5 | Acceptance | **4 passed** (two runs) | 12 min 34 s | 2026-10-04 | The finding for the injected shortage now **passes refutation** (section 3.1, DEF-141); a first run failed one new check because it measured staffing too late (DEF-145) |
| 6 | Resilience | **8 passed** (full run on the fixed Connect configuration, 48 min 44 s) | 2026-10-04 | Includes the supervisor test, which fails with the supervisor stopped. Earlier in the cycle the layer needed six runs for one clean pass (DEF-137, DEF-142, DEF-143) |
| 7 | Load | **4 passed** | 10 min | 2026-10-04 | Burst 35 events/s (45 in an earlier run), p95 latency 0.95 s, dashboard API p95 602 ms; the causal engine's one-off import +112 MB, every other service at most +1.5 MB |
| 8 | Security | **11 passed** | 27 s | 2026-10-04 | One case per requirements file, now including the edge trainer's pins; no known vulnerabilities |
| - | Godot client smoke test | Not re-run | - | 2026-09-30 (CI) | About 80 checks; passed on GitHub on 2026-09-30 |

**Totals:** 670 distinct test names across the Python suites, plus the GDScript smoke test.

## 3. Measured results

### 3.1 The system under an injected fault (acceptance)

A staffing shortage was injected at `station-grill` for four minutes after a baseline.

| Measure | First cycle (2026-10-03) | After the fix, run 1 (2026-10-04) | After the fix, run 2 (2026-10-04) |
|---|---|---|---|
| Staff clocked in | not measured | 9 at the start, 5 sampled after the controller returned (too late; the roster was refilling) | 9 at the start, **2** in the middle |
| Mean pickup delay at the loaded stations | 5,141 ms to 16,134 ms: **3.1x** | 6,378 ms to 22,920 ms: **3.6x** | 6,497 ms to 19,097 ms: **2.9x** |
| Anomalies at the loaded stations / at the removed station | **142 / 1** | **96 / 4** | **104 / 1** |
| The finding for the scenario | Passed refutation, under the **original** gate that passed most noise | Passes refutation: effect **-2,399 ms** per additional staff member, p = 4.8e-6, placebo p = 0.94; promoted | Passes refutation: effect **-2,098 ms**, p = 5.2e-4, placebo p = 0.96; promoted |

Between the first cycle and these runs the repaired gate **refuted** this finding (effect +169 ms, p = 0.95, DEF-141), because the simulated world contained no staffing effect: the shortage slowed the kitchen directly and never changed the staff-shift events the analysis uses. The world now encodes the relationship, with staffing driving the kitchen's capacity and the shortage clocking staff out, and the analysis window includes the baseline so there is contrast. The sign is right (more staff, shorter delay) and the finding is promoted by the running reviewer. The two p-values differ by two orders of magnitude, both far below the 0.01 threshold; the margin has been measured twice. The effect is a designed property of the simulation, so this shows the pipeline recovers an effect that is truly there, not that a real kitchen has one.

### 3.2 Reliability (resilience)

The invariant checked after every fault: **every message written to Kafka is stored exactly once.** Producers were stopped, every consumer allowed to drain, then each topic's end offset compared with its table's row count and duplicate ids counted.

| Fault injected | Outcome |
|---|---|
| None (control) | Held |
| `SIGKILL` of the storage consumer under traffic | Restarted by the platform; invariant held |
| `SIGKILL` of the aggregator | Restarted; new summaries resumed |
| Database stopped for 25 s and restarted | Storage, aggregation, twin and dashboard all recovered unaided; invariant held |
| Kafka restarted | First cycle: connectors returned to `RUNNING`; invariant held. Second cycle: the pipeline took about six minutes to resume and the test failed in five of six runs, identically on the code from before the edge feature (DEF-142). Third cycle: passed (twice), with the supervisor taking no action either time, so the stall did not recur |
| MQTT broker restarted | Simulators and connectors reconnected |
| Whole stack stopped and started | No rows or Kafka messages lost |
| MQTT broker stopped while Kafka Connect restarts (new, DEF-137) | All four connector tasks fail as they start; the supervisor restarts them and they stay running once the broker is back; the same test fails with the supervisor stopped |

### 3.3 Performance (load)

| Measure | Result | Floor asserted |
|---|---|---|
| Event time to stored, p95 | **0.98 s** | under 10 s |
| A 3,000-event burst | All stored, exactly once; **45 events/s** end to end in the second cycle (about 18 events/s in the first; 11 to 12 events/s while every message paid a 30 to 60 ms schema re-check, DEF-139) | at least 10 events/s |
| Memory growth under the burst | At most **+1.3 MB** for every service except the causal engine; no service restarted | under 100 MB |
| Dashboard API, 400 requests at 20 concurrent users | p95 **480 ms**, no errors | under 2 s |

The causal engine grew by +152 MB in the second cycle, a one-off step from its lazy import of DoWhy that lands in the window only if its first estimate happens to fall inside it; the test now allows for it (DEF-138). The ingest ceiling comes from one database transaction and one Kafka offset commit per message on a single thread; at the real default traffic (about 0.7 events/s) there is more than 60x headroom.

### 3.3a The edge node

Measured on the plate-waste node's on-device model (`edge-simulators/MODEL_CARD.md`): 7.1 g RMSE against 20.8 g for the scale channel alone; int8 weights cost 0.7% against float32; a 3.4 KB artifact; about 0.13 ms per inference; 39.6 MiB peak in a container limited to 96 MiB after 50,000 events. The per-reading out-of-distribution guard flags 0.84% of readings at lens fouling 0.4 (estimates then 4.7 times worse), so a rolling drift monitor was added: 0.11% of clean time in alarm, fouling of 0.3 and above caught in every one of 200 trials, **but fouling of 0.2 missed in about 40% of onsets within 300 readings** (DEF-133, RSK-032). All of this is measured against the project's own simulated sensors (RSK-031).

### 3.4 Statistical validity

| Measure | Result |
|---|---|
| Plate-waste effect (planted: -90 g, confounded by portion size) | Recovered within 10 g; a naive comparison was off by more than 25 g |
| Staffing effect (planted: -4,000 ms per person, confounded by station) | Recovered within 500 ms |
| No true effect | Estimated near zero |
| **Refutation gate on pure noise, before the repair** | **Passed 78% to 87%** in three measurements (47 of 60 with a seeded permutation placebo; 26 of 30 and 23 of 30 with the engine's original unseeded placebo). DEF-106 |
| **Refutation gate on pure noise, after the repair** | **0 of 60 pass** at the chosen threshold (0.01); a 0.05 threshold would pass 4 of 60 |
| **Refutation gate on genuine effects, after the repair** | **20 of 20** pass down to a -5 g effect on 1,500 rows; 10 of 10 at -20 g in the test suite |
| Reproducibility | Identical data now gives an identical verdict and identical p-values (it varied from run to run before; DEF-129) |

### 3.5 Security

The narrator's database role was attacked directly: it could not read any of the ten other tables, modify or delete findings, create or alter objects, grant itself access or act as a superuser. A classic injection string left the schema intact. No pinned dependency has a known advisory (one did; fixed).

### 3.6 Code coverage (first measurement, 2026-10-04)

Line coverage with `pytest-cov`, per module, **not gated in CI**:

| Source | Unit suites | Notes |
|---|---|---|
| Edge model, sensors, staffing | 100% | |
| Timing, staff-shift and plate-waste simulators | 91%, 83%, 83% | The POS simulator is 0% here (covered only by the schema-compatibility test) |
| Simulator runtime | 36% | MQTT connection code that needs a broker |
| Anomaly detector | 86% | |
| Connector supervisor | 87% | |
| Alert relay | 74% | |
| Aggregator | 67% | |
| Dashboard API | 57% (`comparison.py` 100%) | The routes are exercised by the integration layer |
| Narrator | 53% | |
| Causal engine | 44% | The estimation path needs DoWhy and runs in the statistical layer |

The database integration layer alone covers 61% of the 651 statements in the consumer, twin, causal engine, detector, aggregator and dashboard API. A figure per module is not a figure for the project: the React interface and the shell scripts are unmeasured, and the statistical layer's coverage of the causal engine was not included.

## 4. Requirement coverage

From `03-requirements-traceability-matrix.md`:

| Result | Requirements |
|---|---|
| Verified (automated) | 75 |
| Verified by inspection | 6 |
| Verified (live or manual only) | 9 |
| Partially verified | 9 |
| Not verified | 1 |
| **Not met** | 0 |
| **Total** | **100** |

**Not met:** none. FR-TWN-03 (twin counts correct under redelivery) was the last, and was fixed on 2026-10-04 (DEF-107); FR-CAU-06 (the gate rejects noise) was fixed on 2026-10-03. **Not verified:** NFR-POR-03 (the Codespaces devcontainer). Four requirements were added on 2026-10-04 and verified at once: FR-ING-07 (staffing is a real driver), FR-ANA-05 (robust control limits), FR-CAU-07 (the scenario's finding passes refutation) and NFR-REL-05 (self-healing connectors). The traceability matrix lists the weakly verified requirements and what would close each.

## 5. Defects

From `06-defect-log.md` (143 entries: 128 defects, 15 informational):

| Severity | Count |
|---|---|
| S1 Critical | 6 |
| S2 High | 30 |
| S3 Medium | 45 |
| S4 Low | 47 |
| Info | 15 |

### Found by this cycle

| ID | Defect | Severity | Resolution |
|---|---|---|---|
| DEF-100 | Storage consumer never reconnected after a database restart | S1 | Fixed, regression-tested, verified live |
| DEF-101 | Storage consumer lost events during an outage over about 30 s | S1 | Fixed, regression-tested, verified live |
| DEF-102 | Compose Kafka Connect image ran as root | S3 | Fixed |
| DEF-103 | Three batch workloads had no resource limits | S3 | Fixed |
| DEF-104 | `requests` 2.32.3 had two published advisories | S3 | Fixed |
| DEF-105 | Six unused imports or variables | S4 | Fixed |
| DEF-106 | The refutation gate passed 78% to 87% of noise | S2 | **Fixed 2026-10-03**, guarded by four statistical tests |
| DEF-129 | The placebo permutations were unseeded, so verdicts were not reproducible | S3 | Fixed, regression-tested |
| DEF-107 | Twin open-ticket count double-counts on redelivery | S3 | **Fixed 2026-10-04**, tested |

### Found in the second cycle (edge-inference feature and re-run), and what became of it

| ID | Defect | Severity | Status now |
|---|---|---|---|
| DEF-058 | The aggregator crashed on a negative duration (a backwards clock step) | S3 | Fixed, tested |
| DEF-133 | The per-reading out-of-distribution guard missed a slow sensor fault | S3 | Fixed (rolling drift monitor); subtle faults (fouling 0.2) still missed 40% of the time, narrowed by DEF-155 (shift monitor: 99.5% caught) |
| DEF-134 | The trainer wrote `NaN` into the model artifact | S4 | Fixed, tested |
| DEF-136 | The connector-health check could not see a failed task | S3 | Fixed, tested |
| DEF-138 | The load test counted a one-off library import as a memory leak | S4 | Fixed |
| DEF-139 | Per-message schema validation capped throughput | S3 | Fixed, tested (11 to 12 became 45 events/s) |
| DEF-141 | The injected shortage's finding was refuted under the repaired gate | **S2** | **Fixed, tested, verified live twice** |
| DEF-137 | A start-up DNS failure left every connector task failed | S3 | **Fixed, tested, reproduced and verified live** |
| DEF-142 | Kafka Connect took about six minutes to resume after a Kafka restart | S3 | **Fixed, tested**: the cause was Connect's default five-minute rebalance delay (exactly 300 s in the logs) |
| DEF-143 | One full stop and start did not resume in time | Info | Mitigated: probable cause (the same delay) removed; not reproduced in 13 stress cycles, so not proven |
| DEF-144 | I deleted three entries from the codebase guide | S4 | Fixed |
| DEF-145 | My new staffing check measured too late | S4 | Fixed |

Also fixed in the third cycle: **DEF-107** (the twin's double-count on redelivery) and **DEF-056** (stalled tickets blinding the anomaly detector); **DEF-128** (no coverage measurement) is closed by section 3.6.

### Open now (5)

DEF-015, DEF-091, DEF-093, DEF-123 and DEF-131, all informational. **No defect above informational is open.**

## 6. How the cycle went

The regime was not clean on its first runs, and the failures are recorded because they show what the tests are worth.

| When | Run | Outcome | What it was |
|---|---|---|---|
| 2026-10-02 | Static, first | 165 passed, 7 failed | 4 real defects (root user, three unbounded workloads); 3 were my own over-strict secret test |
| 2026-10-02 | Integration, first | 90 passed, 1 expected-fail | Clean first time; later mutation checks showed it had teeth |
| 2026-10-02 | Statistical, first | 9 passed; the slow measurement failed | Found DEF-106; recorded as an expected failure |
| 2026-10-03 | Statistical, re-measured | The same measurement gave 77% instead of 87% | The placebo permutations were unseeded (DEF-129) |
| 2026-10-02 | End-to-end, first | 50 passed, 5 failed | My log-health test was too strict; corrected three times (DEF-115) |
| 2026-10-02/03 | Acceptance, first | 2 of 3 passed | My assertion assumed the finding would still be un-promoted |
| 2026-10-02/03 | Load, first | 3 of 4 passed | My throughput floor was a guess (20; measured 14.6) |
| 2026-10-02/03 | Resilience, first (with `docker kill`) | Stopped | Measured Podman's semantics, not the platform's (DEF-118) |
| 2026-10-03 | Resilience, second | 3 of 7 passed | **DEF-100**: the consumer never reconnected |
| 2026-10-03 | Resilience, third | 5 of 7 passed | **DEF-101**: one message lost (517 in Kafka, 516 stored) |
| 2026-10-03 | Full pass | **Everything passed** | After both consumer fixes |
| 2026-10-03 (later) | Gate repair | Measured candidate gates on 60 noise datasets and 4 genuine effect sizes; implemented the best; statistical layer re-run | 14 of 14 passed; the acceptance test was then re-run on the live stack (3 of 3) |
| 2026-10-03 | End-to-end, after the edge feature | 64 passed, 3 skipped | Clean first time; the two new edge tests passed on the live stack |
| 2026-10-03 | Load, first of the second cycle | 3 of 4 passed | The aggregator crashed three times on a backwards clock step (DEF-058); throughput 11 events/s |
| 2026-10-03 | Resilience, first of the second cycle | Aborted by me | All four connector tasks had failed at start-up (DEF-137, `UnknownHostException`); the run could not mean anything |
| 2026-10-03/04 | Load, second | 3 of 4 passed | Memory bound counted a one-off import (DEF-138); throughput 12 events/s because of per-message schema re-checks (DEF-139) |
| 2026-10-04 | Load, third (final code) | **4 of 4 passed** | 45 events/s after the fixes |
| 2026-10-04 | Resilience, second | 5 of 7 passed | Kafka restart (DEF-142) and the full restart (DEF-143) failed |
| 2026-10-04 | Acceptance (final code) | 3 of 3 passed | The finding it checks was refuted (DEF-141) |
| 2026-10-04 | Comparison on the code from before the edge feature | Kafka restart failed there too; full restart passed | Established that DEF-142 is not a regression |
| 2026-10-04 | Resilience, aborted start | Stack not ready | A second start-up DNS failure; proven cause (DEF-137) |
| 2026-10-04 | Resilience, final | **7 of 7 passed** | The first clean pass in the second cycle |
| 2026-10-04 | Acceptance, after the staffing coupling (run 1) | 3 of 4 passed | The finding **passed refutation** (DEF-141 fixed); my new staffing check measured too late (DEF-145) |
| 2026-10-04 | End-to-end, with the supervisor | 65 passed, 4 skipped | Clean |
| 2026-10-04 | Acceptance (run 2) | **4 of 4 passed** | Finding passes again (p = 5.2e-4) |
| 2026-10-04 | Resilience, full, with the supervisor | **7 of 7 passed** | The Kafka-restart test passed with the supervisor taking no action |
| 2026-10-04 | Resilience, the new supervisor test | **Passed**; **failed** with the supervisor stopped | DEF-137 reproduced and its fix verified |

## 7. Effectiveness of the tests themselves (mutation checks)

Tests were checked for teeth by breaking the system on purpose and confirming the right ones failed, then restoring it.

| Deliberate breakage | Tests that failed |
|---|---|
| Detector limits mistuned too tight | The false-alarm-rate test |
| Detector limits mistuned too loose | Three detection tests |
| Aggregator stage pairing changed | The duration-arithmetic test |
| Review gate loosened to pass untested findings | The reviewer-gate test |
| The NULL-confounder bug reintroduced, plus a leaked narrator permission | Five integration tests |
| The old skip-and-continue behaviour restored in the consumer | Two handler tests, one reporting "a message was lost" |

The second cycle added: a model that echoes the scale channel, a drift monitor that never alarms, a node that leaks the true grams, the integrity check removed, a constant estimate and a lens-fouling knob that does nothing (each turned a specific `test_edge_ai.py` test red); the causal-engine trust clause and the dashboard route's `edge_inference` filter removed (each turned a database test red); and per-call `jsonschema.validate` restored in the consumer (the performance test went red).

These checks were ad hoc, not automated or exhaustive.

## 8. Deviations from the test plan

| Planned | What happened |
|---|---|
| The new CI workflows run on GitHub | Not yet run; every command was run locally |
| Godot smoke test in the nightly pass | Not included; last run 2026-09-30 |
| Run on a clean machine and on Docker Engine | Not done |
| Several repetitions to establish flakiness | Repeated in the second cycle, which showed the heavy layers to be flaky here; still no stable measure |
| Independent review | None |
| The Codespaces devcontainer tried | Not tried (allowance exhausted) |

## 9. Anomalies and cautions

- **Podman specifics.** Restart behaviour, socket state and load sensitivity are Podman's; they may differ on Docker.
- **Warm cache.** All stack runs reused cached image layers, so cold-start time is unmeasured.
- **Retrospective ratings.** Severity and likelihood scores were assigned after the fact by the assistant that helped build the system.
- **The heavy layers are not stable here.** The resilience layer needed six runs in the second cycle to produce one clean pass, and its Kafka-restart test failed in five of six runs on both the old and the new code; it then passed in the next two. A single green run is weak evidence in this environment, and the supervisor's handling of that stall is not shown on a live stall.
- **Designed effects.** The plate-waste to-go effect, the staffing effect and the edge sensors are all properties the simulation was built to contain. Passing tests show the pipeline recovers what is there; they do not show a real restaurant behaves so.
- **A figure that moved.** The refutation-gate measurement changed from 87% to 77% between runs because its permutations were unseeded; the repaired gate is seeded and reproducible.
- **Passing is not proof of meaning.** The narrator's checks are mechanical; the statistical layer proves the estimator on synthetic data, not on a real restaurant.

## 10. Conclusion

The platform meets its stated functional and non-functional requirements on the machine and engine it was built on, and the evidence is repeatable with one command per layer. The two most serious defects the project had (a consumer that silently stopped, and one that silently lost events) are fixed and now guarded. What remains are choices and verifications only the owner or an outsider can supply, listed in `10-release-readiness-and-known-issues.md`.

## 11. Sign-off

| Role | Name | Date |
|---|---|---|
| Prepared by | The project's AI assistant | 2026-10-03, updated 2026-10-04 |
| Reviewed by | *(pending)* | |
| Accepted by | *(pending)* | |
