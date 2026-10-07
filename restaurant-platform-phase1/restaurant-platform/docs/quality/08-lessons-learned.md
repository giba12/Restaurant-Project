# Lessons Learned

| | |
|---|---|
| Document | Lessons learned |
| Project | Restaurant Operations Digital Twin Platform |
| Version | 1.0 |
| Date | 2026-10-03 |
| Status | Drawn from the 147 entries of `06-defect-log.md`; each lesson cites the entries that taught it |

## 1. The short version

1. **The worst bugs were silent.** Data was lost, skipped or corrupted without a single error, and the system looked healthy throughout.
2. **"Running" is not "working."** Nearly every long-lived failure was a process that was up, a pod that was `Running`, a connector that said `RUNNING`.
3. **Tests found few of the serious bugs until they started breaking things on purpose.** Live operation and chaos testing found most of the S1 and S2 defects; happy-path tests found almost none.
4. **A fix is not done until it is deployed and re-verified live.**
5. **Third parties are the biggest source of surprise,** and pinning exactly is only half the answer.
6. **A test that cannot fail is decoration.** The suite is trustworthy only because it was repeatedly broken on purpose.
7. **Say what you have not verified.** The credibility of this project rests more on its open defects being published than on its passing tests.

## 2. What the data says about how defects were found

From the defect log (151 entries):

| Found by | Entries | Notes |
|---|---|---|
| Live operation, deployment or a manual run | 74 | The overwhelming majority of all entries |
| Review, static or manual | 21 | Five serious defects found before Phase 5 was ever deployed; four documentation discrepancies found while preparing these documents; two observations from investigating the refutation gate; one stale threshold in the test catalogue |
| The test regime's own runs and observations | 28 | Mostly defects *in the tests* (Part G), found by running them; eight found while building and verifying the edge feature (Part J), two of them in the author's own new work and one a blind spot in the harness |
| The automated test regime | 7 | Including the statistical finding about the refutation gate (DEF-106) |
| Chaos testing | 5 | Including the 10-hour silent outage (DEF-090, manual) and both silent-data-loss defects (DEF-100, DEF-101, automated) |
| The rest | 12 | CI, the audit script, user reports, the done-condition attempt |

**Of the 35 serious (S1 and S2) defects, none was found by a unit test.** 22 were found by live operation or deployment, 5 by static review before deployment, 3 by attempting the done condition with a real injected scenario, 3 by chaos testing, 1 by CI and 1 by the statistical test.

Two readings. First, **it is cheaper to find a defect by review than by deployment**, and the Phase 5 static review is the proof (five defects, each of which would have crashed a pod). Second, **no amount of reading found the two consumer defects**; only injecting a fault and checking a conservation invariant did. Both methods are needed.

## 3. Lessons, each with its evidence

### 3.1 Check conservation, not just health

The strongest test in the project is one sentence: *after any fault, every message in Kafka is stored exactly once.* It found a defect that every earlier check had missed, as an off-by-one (517 messages, 516 rows). Health checks, log checks and row-count checks all passed. **When a pipeline moves data, test that what went in equals what came out.** (DEF-101; also DEF-035, where `service-timing-events` held ~35 messages after 12 days.)

### 3.2 "Up" is not "doing its job"

- A connector reported `RUNNING` while its task had `FAILED` (DEF-008) and later while three of four connectors were broken (DEF-035).
- The storage consumer stayed "running" for as long as you liked after the database restarted, storing nothing (DEF-100).
- A pod showed `1/1 Running` while connected to a listener that no longer existed (DEF-094).

Monitor the *function* (rows arriving, lag falling, findings flowing), not the process. The project's pipeline-health metrics and the end-to-end tests exist for this reason.

### 3.3 Skipping is not retrying

`consumer.commit()` commits the consumer's *position*, not "this message". A loop that logged "offset not committed" and moved on committed straight over the message it had given up on (DEF-101). The original code's own comment asserted the opposite. **A comment describing a safety property is not evidence of it;** the property needed a test with a fake consumer that models position.

### 3.4 Fix, deploy, verify

A correct fix sat in the repository for days while the live cluster kept the bug and the pipeline stopped for 10 hours (DEF-090). The drift audit exists because of it. Similarly a migration to TLS reached nine services and missed a tenth (DEF-094). **Treat "a routine redeploy exposed an old problem" as a finding, and check the live system against the repository after every change.**

### 3.5 Run the failure path

Reconnection and rewinding are not exercised by a happy-path test, and in the storage consumer both were wrong (DEF-100, DEF-101). The resilience layer's first full run failed four of seven tests (database outage, Kafka restart, MQTT restart, full stop and start); after the reconnect fix the second run failed two (database outage and Kafka restart), each with a one-message mismatch (517 messages in Kafka, 516 rows), which was the rewind defect; the third run passed. What is and is not established about those failures:

- **Run 2 is verified:** the assertion messages show a one-message loss, the fix was written for it, and a regression test written for it fails on the old behaviour.
- **Run 1 is attributed, not individually verified.** I did not diagnose the four failures one by one. The reconnect fix removed them, and the consumer stayed unable to store after the database test, which the three later tests share, so a cascade from DEF-100 is the likely explanation. The fix and the later passes are the evidence; the per-test causes were not separately confirmed.
- **Not every failure along the way was a product defect.** The first version of the crash test used `docker kill`, which Podman does not treat as a crash, so it measured Podman and not the platform (DEF-118). That was found by a small experiment during an earlier, partial run, and corrected before the three runs above.
- **Some recovery behaviour was right all along.** The Kafka restart, MQTT restart and full stop/start tests passed in run 2 and run 3, so the claim is not that every restart path was broken. The aggregator's loss of tickets caught mid-flight by a crash is a documented limitation that the test tolerates, not a defect it found.

**Run the failure path, and read each failure as evidence about the product only after ruling out the test.**

### 3.6 Third parties change under you

Three distribution failures in a single phase (a chart repository archived, a vendor's free images withdrawn, a catalogue restructured), plus an old client library that was incompatible with a new Python, a new broker and TLS. What helped: hand-rolling around an official image; pinning by digest where no free version tag exists; checking whether a dependency is *maintained* before adjusting a version constraint; and a nightly audit of pins against published advisories (which then found a real one). What did not exist: scanning of the images themselves.

### 3.7 Two copies will drift

A stale copy of one SQL file caused weeks of confusing column errors (DEF-053, DEF-051, DEF-052). The three schemas copied into a chart are now compared byte for byte. The SQL copies, connector definitions and Mosquitto config are still unchecked and remain a risk (RSK-018). **If a file must exist twice, make a test that proves the copies match, or generate one from the other.**

### 3.8 A test that cannot fail is decoration

Every layer was checked for teeth by breaking the system on purpose: a detector tuned too tight and too loose, a review gate loosened to pass untested findings, the NULL-confounder bug reintroduced, a database permission leaked, the old skip-and-continue behaviour restored. Each turned specific tests red. The statistical layer also contains a test whose only job is to prove the other statistical tests are not vacuous (a naive analysis must be shown to be wrong). **Prove the test can fail before trusting that it passes.**

### 3.9 Record known defects so they cannot be forgotten, then fix them with evidence

Two defects were deliberately left unfixed at first because fixing them changes behaviour the owner should decide on. They were made strict expected-failures: visible on every run, and the moment someone fixes one the suite demands the marker's removal. This worked as designed. When the owner asked for the refutation gate to be repaired, the repair came with its own proof: the expected-failure became a passing assertion and the marker was deleted. The same happened to the other, the twin's redelivery double-count, on 2026-10-04: the marker went, a property test took its place, and the integration layer has no expected-failures left. This is more honest than a comment and more durable than a ticket.

### 3.9a Measure before you repair

The refutation gate was fixed only after an experiment: four candidate rules were evaluated on 60 pure-noise datasets and four genuine effect sizes, inside the production image. The result chose the rule and the threshold (0 of 60 noise datasets pass at 0.01; 0.05 would pass 4; genuine effects down to -5 g pass), and it also showed something a guess would have missed: DoWhy's own placebo p-value never rejected anything, noise or genuine, so it could not have been the discriminator (DEF-130). Two earlier "measurements" of the broken gate had also disagreed (87%, then 77%) because the permutations were unseeded (DEF-129), a warning to seed anything random before trusting a figure drawn from it. Trying to *show* the repaired gate's verdict on the live stack then exposed an unrelated defect: importing DoWhy silently switched off the engine's own INFO logging (DEF-132). Two cheap experiments ruled out output being lost in transport before the cause was sought in the code. Whatever you build to make a decision auditable has to be checked end to end, down to whether its log line reaches a human.

### 3.9b Measure what a guard misses, not just what it falsely flags

The edge node's per-reading out-of-distribution guard was calibrated the standard way: flag about 0.1% of clean readings. That is a statement about false alarms. Injecting the fault it exists to catch (a fouling lens) showed the other side: at fouling 0.4, where the estimates were already 4.7 times worse than clean, it flagged 0.84% of readings. Every reading stays individually plausible while the population drifts, so a per-reading threshold cannot see it (DEF-133). The remedy was a second guard over a window, and the measurement of *that* guard found its own limit: fouling of 0.2 is missed in about 40% of onsets. **A detector has two error rates; calibrate on the clean side, but publish the detection rate against the fault you built it for, and keep the weakness as a test so nobody forgets it.**

### 3.9j A proposed remedy is a hypothesis, and the limit may be in what you measure, not how you measure it

The risk register said that a CUSUM statistic "would narrow" the drift monitor's weakness on mild sensor faults. It read as an obvious improvement and nobody had tried it. Tried at the same false-alarm rate, on four versions of the same quantity, it did no better than the rolling mean it was meant to beat (DEF-155). The cause was the quantity: the squared distance from the training data moves very little under a fouling lens, and no accumulator can recover a signal the statistic has thrown away. Testing the *mean* deviation, the thing the fault actually moves, caught the same fault in 99.5% of trials instead of 59.5%. **Write a remedy down as a hypothesis with the measurement that would confirm it; when it fails, ask first whether the statistic carries the signal, then how it is accumulated. And keep the monitor that does the other job: the new one is much weaker on added noise than the old, so both stay.**

### 3.9k Build the way back before you need it, and put the old version in the store first

The model update path was designed around rolling forward. The first time the *previous* model was put into the store as a rollback target, the new loader could not read it (DEF-156): adding the shift monitor had made the loader require keys the earlier artifact never had. No test had ever loaded an older artifact, and nothing would have until the day a rollback was needed, which is the worst day to find out. **Seed the store with the version you would roll back to before you write the rollout, and make "the previous version still loads and runs" a test. Compatibility with the past is a feature of the loader, not a property of the old file.**

### 3.9l When a system enforces silently, measure what it does before you design around it, and test by delivery

I designed the bridge's handling of a broker's access rules around an assumption: that a refused subscription comes back as a failure code, so a wrong rule would show as "not connected". I wrote the handler, and a comment saying so, before running the broker. The first real test showed that Mosquitto *grants* the subscription and then delivers nothing, and that a refused publish is acknowledged as a success in MQTT 3.1.1: the simulators would have logged events as published that the broker had thrown away, which the loss ledger would then have trusted (DEF-161). Two fixes followed: the simulators and the operator tool use MQTT 5, where a refusal is reported, and every access test judges by what is *delivered* (a forbidden reader never receives what an allowed publisher sent; an allowed reader never receives what a forbidden publisher sent; the legitimate publish in the same conditions does arrive). **Probe the real component before writing the code that depends on how it fails; for anything that refuses quietly, a refusal can only be shown by an absence, so pair every absence with a control that proves the test could have seen the message.**

### 3.9m Code that only runs inside the stack needs a fast run of its own

Two bugs of mine were invisible to every fast test and found only by running the stack: the operator tool's `Link` used one attribute name for two things, so every publish crashed (its unit tests used a fake link), and the footprint probe called a function that had gained a parameter (it only runs inside the node's image under CPU and memory limits). Each took a heavy run of tens of minutes to surface, on a machine whose load made the run slower and noisier. **A tool or script that is normally run only in a container or against a broker deserves one quick test that runs the real code, against the real component where it is cheap (a broker in a container takes two seconds) and in-process where it is not; the fake is for the cases a real run cannot reach, not a substitute for ever running the real one.**

### 3.9c The thing you analyse has to exist in the data, and a test that accepts either answer proves nothing

The causal engine had a `staffing_level` to pickup-delay analysis from the start, and the Phase 5 done-condition was recorded as met. But the simulators never encoded that staffing affects anything: the injected "shortage" slowed the kitchen through a direct multiplier that left the staffing signal untouched. The original refutation gate passed noise, so it "found" an effect that was not there; the repaired gate refused it (p = 0.95), and the acceptance test, which had been relaxed to accept either verdict, kept passing over a refuted finding (DEF-141). Three habits would have caught it earlier: **put every relationship an analysis is meant to find into the data on purpose, and test that it is there** (the to-go effect on waste always had this; staffing did not); **make an acceptance test assert the outcome it names, not "a finding exists"**; and **treat a quality gate you have just fixed as a new instrument that may reveal old results were never real.** The fix was in the world, not in the engine or the gate: staffing now drives the kitchen's capacity, the shortage acts on staffing, and the finding passes with the right sign, in two independent runs. It remains a designed effect: passing shows the pipeline recovers what is there.

### 3.9d Make the failure reproducible before trusting the fix for it

The connector supervisor (DEF-137) was only believed after the failure was reproduced deliberately (stop the broker so its name stops resolving, restart Kafka Connect) and the same test was shown to **fail with the supervisor stopped**. The sister failure (DEF-142, a six-minute stall after a Kafka restart) could not be reproduced on demand, so its fix is recorded as a mitigation verified by unit tests only; a test that passed once, with the supervisor taking no action, is not evidence for it. **A fix is as verified as the failure is reproducible.**

### 3.9h A check that compares two stages cannot see loss before the first

"Every message in Kafka is stored exactly once" passed after every fault, and was true, while 37% of what the sensors published during one Connect outage (and the first 30 s of every start) never reached Kafka at all (DEF-148). The invariant compared Kafka with the database; the loss was upstream of Kafka. It surfaced only because a pasted container log showed the simulators publishing for 30 s before the bridge had subscribed. The fix was easy to get nearly right and hard to get right: the textbook answer (a persistent MQTT session) deadlocked the connector (DEF-150); the first gate trusted `RUNNING`, which Connect reports 3 to 7 s before the task has subscribed, and lost every event it sent in that gap; the second polled once a second and lost the events published in the half second between Connect's REST API stopping and the poll noticing. **Each version looked right until a ledger kept at the source (the simulators' own log of what they published) was compared with the database, repeatedly, and the misses were matched to the timestamps in Connect's log.** Measure from the origin of the data, and measure more than once: one clean run of the first version would have shipped it. **And measure every scenario you claim, not just the one you built for:** I applied the ledger check, with no allowance, to the Kafka restart and the MQTT restart tests without having run them, because they "should" hold. GitHub's nightly run said otherwise (DEF-151): 26 of 734 events lost across a Kafka restart, because Connect's status lies while Kafka is down, and a few across a Mosquitto restart, because the connectors reconnect after the simulators. Both were then reproduced with logs and fixed the same way, by letting the simulator check the fact that matters directly instead of trusting a status.

### 3.9i When the guarantee keeps needing heuristics, move it to something that owns it

The event-loss defect was attacked twice from outside the component that caused it. First a gate in the simulators watched Connect's REST status (DEF-148); a real Kafka restart then showed the status lies while Kafka is down, so the gate learned to watch Kafka and its own MQTT connection too (DEF-151). Each step was measured and each worked, and each left a residual (1 to 3 events a disturbance) and a set of hold times that were recovery times times two, not guarantees. The cause was never the simulators: **the Camel connectors subscribe with clean sessions and acknowledge on arrival, so the broker never holds anything for them, and nothing configurable changes that** (a persistent session deadlocked them, DEF-150). The real fix was about 250 lines that own the guarantee: a persistent session, acknowledge only after Kafka confirms. It needed no hold times, removed three components and two workarounds, and made the property structural, so the same ledger tests went from "allow 4" to "allow none" and passed (DEF-152). **Two rounds of measured heuristics around a component that cannot be made to behave were a signal to replace the component, and it would have been cheaper to see that after the first.** It also found, on its first real run, the one bug unit tests with a faked producer could not (DEF-153): run the real thing early.

### 3.9g A first run on a new platform finds the platform's assumptions

The nightly workflow had never run anywhere but one laptop. Its first run on GitHub (Docker Engine, Compose v2, a clean cold-cache machine) passed acceptance 4 of 4, load 4 of 4, resilience 8 of 8, statistical 14 of 14 and security 11 of 11, and failed two e2e tests, both genuine: a helper that relied on Compose v1 listing exited containers (DEF-146) and a twin that created a staff row with no status when its history was incomplete (DEF-147). Neither showed in 300 local runs of the same test, because both depend on the environment's speed or version. **Passing tests in one environment is evidence about that environment; run the same tests somewhere different before claiming portability, and expect the first run to find something.**

### 3.9f Capture the logs of the failure, and read the timestamps

DEF-142 was explained at first as a stale broker address, because 193 connection attempts to the old address were in the log. That was a symptom. The cause appeared only when a stress harness reproduced the stall with the logs saved: the worker rejoined its group, and then nothing happened for **exactly 300 seconds** (19:19:50 to 19:24:50, and 18:59:34 to 19:04:34 in a second capture). A round number in the gap between two log lines is a timeout, and Kafka Connect's `scheduled.rebalance.max.delay.ms` defaults to 300000. The fix was one line in two files. DEF-143, a one-off failure whose logs had been overwritten by the next test, could not be reproduced at all (13 stress cycles); it is recorded as probably the same delay, not proven. **Save the evidence of a failure before the next run overwrites it, build a harness that repeats the operation and times it, and treat a suspiciously round duration as the answer.**

### 3.9e Replacing a range deletes what is in it

While writing one entry in the codebase guide I replaced everything between two headings, and silently deleted three other entries (DEF-144). The edit was committed. Nothing checked that the guide covers every file. **When an edit replaces a span, read what was in the span; and where a document claims to be complete, a test should say so.**

### 3.10 Measure thresholds, do not guess them

The load floor was guessed at 20 events/s; the measurement was 14.6, then 18. Every threshold is now a *floor set from evidence*, with the measurement and the cause written beside it, so the suite catches a collapse and not noise.

### 3.11 The environment has semantics too

- Podman does not apply a restart policy after `docker kill`; the first resilience run measured Podman, not the platform (DEF-118).
- `localhost` resolved to IPv6 and nginx listened on IPv4 only, so a correct deployment looked broken (DEF-075).
- A JVM given a one-CPU limit serialised its startup and was killed by its own liveness probe (DEF-078), and the first remedy tried, doubling memory, was a red herring (DEF-079).
- A stale network state fixed only by deleting the pod caused 41 hours of silent failure (DEF-092).

**Eliminate layer by layer and write down what was ruled out;** the 41-hour exporter investigation is the model, including its honesty that the final cause was inferred, not proven.

### 3.12 Self-inflicted problems are real problems

The assistant left a test stack running and the owner's own build then failed under the resulting load (DEF-109). A `pkill` pattern killed the assistant's own shell (DEF-120). Tests encoded wrong assumptions (DEF-116, DEF-117). A test flagged itself the moment its file was committed (DEF-114). None of these is exotic, and all were found because results were read instead of assumed. **Run the new test again after committing, isolate test infrastructure from the owner's own, and never loosen a failing assertion to make it pass without finding out why.**

### 3.13 Documentation drifts, and history must not be deleted

The chart layout was documented wrongly for a whole phase (DEF-018); the k3s status and a disk figure were stale (DEF-125, DEF-126); and entries 1-25 of the original problem log were lost in a revision (DEF-127), so the very record meant to prevent repeating mistakes is incomplete for the project's first phase. **Make the log append-only, give every entry a stable id, and machine-check what can be.** The test catalogue, the traceability matrix and the registers in this folder are checked by a test for that reason.

### 3.14 Independence is the missing control

Everything here was verified by the people who built it. The mutation checks and published weaknesses blunt that without removing it. The honest recommendation is external review (RSK-026).

## 4. Difficulties, grouped

| Area | What made it hard | Entries |
|---|---|---|
| Environment | WSL2 and rootless Podman: image-name prefixes, no restart on `docker kill`, a socket that disappears, no `sudo` in the assistant's shell, a loaded machine dropping connections | DEF-006, DEF-025, DEF-070, DEF-109, DEF-118, DEF-121 |
| Supply chain | Vanishing charts and images, abandoned client libraries | DEF-017, DEF-022, DEF-023, DEF-026, DEF-027, DEF-071 |
| Distributed-systems semantics | Commit semantics, at-least-once delivery, partial failure, connection lifetime, start-up races | DEF-035, DEF-072, DEF-100, DEF-101, DEF-107, DEF-108 |
| JVM and Kubernetes behaviour | Resource limits changing startup, liveness probes, Helm hooks and `--reuse-values`, ordering of env substitution | DEF-028, DEF-067, DEF-069, DEF-078, DEF-079 |
| Statistics and machine learning | Confounding, a refutation test that does not refute, a detector blinded by outliers, a language model that invents | DEF-042, DEF-054, DEF-056, DEF-106 |
| Process and tooling | CI that never ran, workflows that cannot be discovered, test infrastructure written wrongly | DEF-081, DEF-084, DEF-114 to DEF-120 |

## 5. What worked well

- **Writing down the root cause and what was ruled out,** in the status log, every time.
- **Testing one invariant after every fault** instead of many vague checks.
- **Hand-rolling infrastructure** around official images when wrappers failed.
- **Strict expected-failures** for deliberately open defects (both were later fixed and their markers removed).
- **Making the documentation itself testable.**
- **Honest verification language** ("not yet confirmed by an actual run"): it prevented several overclaims and is why the CI-never-ran discovery (DEF-081) was possible at all.

## 6. What to do differently next time

| Change | Because |
|---|---|
| Write requirements and a risk register first | They were reconstructed here, and the early phases are poorly recorded |
| Confirm CI ran on the first day (`gh run list`) | A workflow sat unrun for days (DEF-081) |
| Add the conservation check and a fault-injection test with the first consumer | The two worst defects were present from the start |
| Alert on connector and task state, not just pod state | A connector can be `RUNNING` with a failed task |
| Keep the problem log append-only with stable ids | Entries 1-25 were lost |
| Measure code coverage | Only requirement coverage is known |
| Pin Ollama and scan images | The one unpinned image and the unscanned layers remain |
| Get an independent review early | Self-verification has a ceiling |
| Keep the heavy tests in a nightly job from the start | A one-hour run is easy to avoid |
