# Lessons Learned

| | |
|---|---|
| Document | Lessons learned |
| Project | Restaurant Operations Digital Twin Platform |
| Version | 1.0 |
| Date | 2026-10-03 |
| Status | Drawn from the 132 entries of `06-defect-log.md`; each lesson cites the entries that taught it |

## 1. The short version

1. **The worst bugs were silent.** Data was lost, skipped or corrupted without a single error, and the system looked healthy throughout.
2. **"Running" is not "working."** Nearly every long-lived failure was a process that was up, a pod that was `Running`, a connector that said `RUNNING`.
3. **Tests found few of the serious bugs until they started breaking things on purpose.** Live operation and chaos testing found most of the S1 and S2 defects; happy-path tests found almost none.
4. **A fix is not done until it is deployed and re-verified live.**
5. **Third parties are the biggest source of surprise,** and pinning exactly is only half the answer.
6. **A test that cannot fail is decoration.** The suite is trustworthy only because it was repeatedly broken on purpose.
7. **Say what you have not verified.** The credibility of this project rests more on its open defects being published than on its passing tests.

## 2. What the data says about how defects were found

From the defect log (132 entries):

| Found by | Entries | Notes |
|---|---|---|
| Live operation, deployment or a manual run | 74 | The overwhelming majority of all entries |
| Review, static or manual | 19 | Five serious defects found before Phase 5 was ever deployed; four documentation discrepancies found while preparing these documents; two observations from investigating the refutation gate |
| The test regime's own runs and observations | 17 | Mostly defects *in the tests* (Part G), found by running them |
| The automated test regime | 7 | Including the statistical finding about the refutation gate (DEF-106) |
| Chaos testing | 5 | Including the 10-hour silent outage (DEF-090, manual) and both silent-data-loss defects (DEF-100, DEF-101, automated) |
| The rest | 10 | CI, the audit script, user reports, the done-condition attempt |

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

Reconnection, rewinding, restart behaviour, the recovery of in-memory state: none of these is exercised by a happy-path test. Each was wrong. The resilience layer's first full run failed four of seven tests; the second failed two; the third passed. **Each of those failures was a real product defect, not a test defect,** which is the layer paying for itself.

### 3.6 Third parties change under you

Three distribution failures in a single phase (a chart repository archived, a vendor's free images withdrawn, a catalogue restructured), plus an old client library that was incompatible with a new Python, a new broker and TLS. What helped: hand-rolling around an official image; pinning by digest where no free version tag exists; checking whether a dependency is *maintained* before adjusting a version constraint; and a nightly audit of pins against published advisories (which then found a real one). What did not exist: scanning of the images themselves.

### 3.7 Two copies will drift

A stale copy of one SQL file caused weeks of confusing column errors (DEF-053, DEF-051, DEF-052). The three schemas copied into a chart are now compared byte for byte. The SQL copies, connector definitions and Mosquitto config are still unchecked and remain a risk (RSK-018). **If a file must exist twice, make a test that proves the copies match, or generate one from the other.**

### 3.8 A test that cannot fail is decoration

Every layer was checked for teeth by breaking the system on purpose: a detector tuned too tight and too loose, a review gate loosened to pass untested findings, the NULL-confounder bug reintroduced, a database permission leaked, the old skip-and-continue behaviour restored. Each turned specific tests red. The statistical layer also contains a test whose only job is to prove the other statistical tests are not vacuous (a naive analysis must be shown to be wrong). **Prove the test can fail before trusting that it passes.**

### 3.9 Record known defects so they cannot be forgotten, then fix them with evidence

Two defects were deliberately left unfixed at first because fixing them changes behaviour the owner should decide on. They were made strict expected-failures: visible on every run, and the moment someone fixes one the suite demands the marker's removal. This worked as designed. When the owner asked for the refutation gate to be repaired, the repair came with its own proof: the expected-failure became a passing assertion and the marker was deleted. One such defect remains (the twin's redelivery double-count). This is more honest than a comment and more durable than a ticket.

### 3.9a Measure before you repair

The refutation gate was fixed only after an experiment: four candidate rules were evaluated on 60 pure-noise datasets and four genuine effect sizes, inside the production image. The result chose the rule and the threshold (0 of 60 noise datasets pass at 0.01; 0.05 would pass 4; genuine effects down to -5 g pass), and it also showed something a guess would have missed: DoWhy's own placebo p-value never rejected anything, noise or genuine, so it could not have been the discriminator (DEF-130). Two earlier "measurements" of the broken gate had also disagreed (87%, then 77%) because the permutations were unseeded (DEF-129), a warning to seed anything random before trusting a figure drawn from it. Trying to *show* the repaired gate's verdict on the live stack then exposed an unrelated defect: importing DoWhy silently switched off the engine's own INFO logging (DEF-132). Two cheap experiments ruled out output being lost in transport before the cause was sought in the code. Whatever you build to make a decision auditable has to be checked end to end, down to whether its log line reaches a human.

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
- **Strict expected-failures** for deliberately open defects (one was later fixed and its marker removed).
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
