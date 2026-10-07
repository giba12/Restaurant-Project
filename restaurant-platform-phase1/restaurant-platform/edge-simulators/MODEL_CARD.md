# Model card: plate-waste edge regressor

| | |
|---|---|
| Model | `plate-waste-edge-regressor` version `1.2.0` (the weights are those of 1.0.0; 1.1.0 added the shift monitor and 1.2.0 the flatline monitor, both 2026-10-06) |
| SHA-256 | `ccbb7a351773` (full value in the artifact; 1.1.0 was `279d8a64fd02`, 1.0.0 `155f41cd2a84`) |
| Runs in | `edge-simulators/simulators/plate_waste.py`, on the (simulated) node, through `edge_ai/model.py` |
| Trained by | `edge-simulators/training/train_plate_waste_model.py` (offline; scikit-learn is not on the node) |
| Artifact | `edge_ai/plate_waste_edge_model.json`, 5799 bytes |

All figures below are the ones recorded in the artifact's `card` and re-checked on fresh data by `test_edge_ai.py` and `training/test_training.py`.

## What it is for

Estimating the waste left on a bussed plate, in grams, from four raw sensor channels, on the node, so that only the estimate (and the node's own assessment of how far to trust it) leaves the node.

## What it is, and what it is not

- A 4-16-8-1 network. Weights are stored as int8 with one scale per layer and dequantised once at load; arithmetic is float32. This is **weight-only quantization**, not integer inference.
- Inference needs numpy and nothing else.
- **It is trained on, and evaluated against, data from the project's own sensor simulation** (`edge_ai/sensor.py`). The sensors are a designed stand-in: a load cell that is sometimes wrong (cutlery or a napkin adds 15 to 60 g on one plate in four), a camera area channel that saturates, a noisy depth channel, and an ambient-light reading that sets how much to trust the camera. The accuracy below shows that the pipeline works and that fusing channels beats the best single one. **It says nothing about how a real camera and scale would perform.**
- It is not embedded firmware. It runs in a container on a laptop; the size and latency budgets are ceilings enforced by tests, not characteristics of any real device.

## Accuracy (20,000 held-out readings the trainer never saw)

| | RMSE |
|---|---|
| This model (int8 weights) | **7.11 g** |
| The same weights in float32 | 7.06 g |
| The load-cell channel alone | 20.83 g |

Quantizing to int8 costs 0.7% of accuracy; using all four channels instead of the scale alone cuts the error by 66%.

## Budgets

| Budget | Ceiling | Measured |
|---|---|---|
| Artifact size | 16 KB | 5.7 KB (1.2.0; 3.4 KB for 1.0.0) |
| Inference latency, p99 | 5 ms | about 0.13 ms mean per reading on the development laptop |
| Memory to load | 1 MB of allocations | well under that |

These describe the model, measured on the development laptop. The whole node, measured in its own image under the limits the Kubernetes chart gives it (100m CPU, 96Mi; `footprint_probe.py`, run by `tests/load/test_edge_footprint.py`):

| What | Measured |
|---|---|
| Resident memory of the node process | 41-43 MiB, of which 31 MiB is importing Python's libraries (numpy, jsonschema, paho); the model is 5 KB. Memory did not grow over 500 events |
| Memory the container actually needs | the cgroup peaked at about 26 MiB; the probe ran in 28 MiB and was killed (out of memory) at 24 MiB, so the chart's 96 MiB is about 3.5 times the floor |
| Inference at the node's real pace (one reading every few seconds) | p50 0.38 ms, p99 0.88 ms against the 5 ms budget |
| The same call back to back | p99 about 88 ms: a CPU quota pauses the container for the rest of each 100 ms period (the probe runs until it has used 0.3 s of CPU, so a fast machine cannot finish inside one period: 0.34 s of work took 3.3 s, 34 throttled periods). The node never does this on its own |
| Taking a model update | the checks take about 0.4 s of CPU time (so one 90 ms pause in the event loop), the shadow comparison adds under 1 MiB, and the update was promoted inside the limits |

**What this does and does not show.** The same code under a CPU and memory ceiling: what the node needs, and how it behaves when the CPU is rationed. It is not a different processor, clock speed or instruction set, so it says nothing about a microcontroller or a slow ARM core. The finding that matters is that the budgets above describe the *model*: what a device needs is the *runtime*, about 40 MiB, which is why a node on something far smaller than a Raspberry Pi would need the inference path rewritten without Python and numpy, not just the model shrunk.

## Knowing when not to be trusted

Nothing on the node can tell that an estimate is wrong, because there is no ground truth in the field. What it can tell is that its **inputs have stopped looking like anything it was trained on.** There are four guards, calibrated on held-out clean data.

1. **Per reading** (`out_of_distribution`): Mahalanobis distance from the training data above a threshold set at the 99.9th percentile of clean readings (0.10% of clean readings are flagged). It catches gross failures: an impossible weight, a stuck light sensor, a camera that sees nothing while the depth says a heap.
2. **Spread over a window** (`drift_score`): the rolling mean of the squared distance over the last 50 readings (about six minutes at the simulator's default rate), alarming above 5.658. It reacts to how far readings are from the training data, not to which way.
3. **Shift over a window** (`shift_score`, added in 1.1.0): a Hotelling T-squared of the *mean* deviation over the last 30 readings (about four minutes), alarming above 18.559. On clean data it follows a chi-squared law with four degrees of freedom (one per channel), so the calibrated threshold sits close to that law's 99.9th percentile (18.5). A sustained bias in any direction adds to it with every reading.
4. **Flatline over a window** (`flatline_score`, added in 1.2.0): the smallest per-channel standard deviation over the last 30 readings, in units of the training spread, alarming when it falls **below** 0.575 (the lower 0.1% tail of clean windows). On clean data it sits around 0.9; a channel that has stopped varying, or a gain that has fallen so every channel shrinks toward normal, drags it toward 0.

`drift_suspected` is true when **any** of the three window guards alarms. On a fresh clean stream of about 300,000 readings the spread monitor alarmed 0.06% of the time, the shift monitor 0.10%, the flatline monitor 0.08% and any of them 0.24% (on a different stream the first two together were 0.27%, so read these as 0.2 to 0.3%): the combined guard costs about two to three times the false alarms of one.

### The per-reading guard is not enough on its own

A fouling lens (`lens_fouling`, 0 clean to 1 badly fouled) leaves every individual reading plausible while the error grows many times over. Over 200 onsets each (clean until the window fills, then the lens fouls; readings are counted from the onset, an alarm before it is a false alarm and not a detection):

| Lens fouling | RMSE | Readings flagged individually | Spread monitor: caught, median readings | Shift monitor: caught, median readings |
|---|---|---|---|---|
| 0.1 | 11.21 g | 0.20% | 8.5%, 197 | **94.5%, 70** |
| 0.15 | 14.83 g | 0.20% | 22.5%, 197 | **99.5%, 30** |
| 0.2 | 19.1 g | 0.12% | 59.5%, 143 | **99.5%, 23** |
| 0.3 | 26.62 g | 0.48% | 100%, 48.5 | 99.5%, 16 |
| 0.4 | 33.66 g | 0.84% | 100%, 26 | 99.5%, 12 |
| 0.6 | 47.93 g | 5.00% | 100%, 11.5 | 99.5%, 8 |
| 0.8 | 62.18 g | 15.88% | 100%, 7 | 99.5%, 6 |

The 99.5% for the shift monitor from 0.15 upward is one trial of 200 that raised a false alarm *before* the fault began (the same clean stretch in every row), not a miss.

So: individually, the guard misses almost all of a slow fault. The spread monitor alone caught fouling of 0.3 and above in every trial but **missed about 40% of onsets at 0.2 and about three quarters at 0.15**, where the error is already 2.7 and 2.1 times the clean figure. The shift monitor closes most of that gap: a fault that is a small consistent bias is exactly what a test on the window's mean is built for, and it detects 0.2 in about three minutes of readings where the spread monitor took 18.

**What the two monitors are for.** The spread monitor reacts to added noise far more strongly than the shift monitor does (noise with half the sensors' own variance and no bias: spread alarmed on 100% of readings, shift on 24%), and the shift monitor reacts to a small bias the spread monitor mostly misses. They are complementary, which is why the node alarms on either.

**A mistake worth keeping in the record.** The risk register (RSK-032) had recorded that "a more sensitive statistic, for example CUSUM, would narrow" the gap. It was tried first, calibrated to the same 0.1% clean alarm time, on the squared distance and on three tamer transforms of it (the distance, the squared distance capped at 9 and at 16, its logarithm): none beat the rolling mean (best 57.5% at fouling 0.2, against 59.5%). The limit was the quantity being monitored, not the way it was accumulated: at fouling 0.2 the squared distance moves only from 4.0 to 4.6 against a spread of 3.3. Monitoring the mean deviation, which the fault actually moves, was what worked. (The CUSUM experiment is not in the repository; its numbers are from a scratch script and are recorded here as a measurement, not a test.)

**What the first two monitors cannot see, and the flatline monitor.** The spread and shift monitors alarm on readings moving away from the training data (how far, and which way on average). A sensor that goes dead at a normal value, or a gain that falls so the readings shrink toward normal, moves neither: found while testing the real-data harness (DEF-157), then confirmed on the plate node. Time in alarm, 5,000 readings per fault (`edge_ai/sensor.py` `inject_fault`):

| Fault | Per-reading guard | Spread | Shift | Flatline |
|---|---|---|---|---|
| Light sensor stuck at its normal value | 0.0% | 0.0% | 0.0% | **100%** |
| Camera-area channel stuck | 4.1% | 100% | 11.8% | 100% |
| Scale channel stuck | 4.0% | 99.5% | 11.6% | 100% |
| Every channel's departure from normal halved (gain 0.5) | 0.0% | 0.0% | 0.0% | **99.8%** |
| Departures cut by 30% (gain 0.7) | 0.0% | 0.0% | 0.0% | **21%** |

A stuck camera or scale channel was already caught by the spread monitor, because it breaks the correlation between channels; the light sensor is the channel that is independent of the others, so it was invisible. **The limit:** a mild gain loss (0.7) is mostly not caught, and the other monitors never see it; a fault that gentle goes unnoticed. The 0.575 threshold and these rates are measured on the simulated sensors (RSK-031), and the real-data check above did not test this monitor.

**What the shift monitor does not do.** It is not a fault diagnosis. A change in the real mix of plates (a menu change that makes plates heavier) or a change in the room's light would raise it just as a fouling lens does: it says *the inputs have shifted*, and what has shifted needs a person. At fouling 0.1, where the error is already 1.6 times the clean figure, about a quarter of onsets still take more than 150 readings (about 19 minutes) to alarm (7 of 30 in the test's trials), and about 5% (the one early false alarm included) never alarm within 300. And, as everywhere in this card, the fault is the author's own simulation of a fouled lens, a pure bias with a known direction, which is the case this statistic is best at.

## Checked against drift the project did not generate

Everything above is measured on the project's own simulation of a fouled lens (RSK-031). `validation/real_drift_check.py` runs the shipped `DriftMonitor` and `ShiftMonitor` classes, with the same calibration recipe, on the UCI *Gas Sensor Array Drift Dataset at Different Concentrations* (13,910 measurements from 16 chemical sensors over 36 months, in ten batches, with real drift; CC BY 4.0; the script downloads it and checks its SHA-256). It needs the network, so it is not in CI; its harness is tested offline on synthetic data (`test_real_drift_harness.py`).

**What this is not.** A validation of the plate-waste model: the data has none of its channels and no leftover-food weights, and no public dataset that I know of does. It checks the drift-detection *method* on real drift. The sensors are chemical; the features are the 16 steady-state responses projected onto 4 principal components (the node has 4 channels), chosen once and not tuned; readings are drawn in random order (within a batch they are grouped by gas, which a window would mistake for drift) and each batch is resampled to the first batch's gas mix (gases 1 to 5). The ground truth for "this drift matters" is the error of a classifier trained on batch 1. Eight random splits of batch 1, fixed seeds.

**Clean data.** Held-out batch-1 readings, calibrated for 0.1%: the spread monitor was in alarm **4.5% of the time** (sd 7.8 points across splits) and the shift monitor **0.66%** (sd 1.1). The calibration did not transfer. The calibration set here is about 110 readings of a mixture of gases, a harsher setting than the node's 300,000 simulated readings, but real clean data of that size does not exist in this set, so the 0.1% figure is a property of the simulation and should not be quoted for a real sensor.

**Large real drift.** From batch 2 on, a classifier trained on batch 1 is wrong about half the time (4.6% on clean data). The shift monitor was in alarm **100% of the time in every later batch** (98.7% in batch 10). The spread monitor was uneven: 100% in batches 3 to 5, 80% in batch 2, 39 to 51% in batches 6, 7 and 10, and **0% in batches 8 and 9**, where the classifier was 49 and 55% wrong.

| Batch | Spread monitor | Shift monitor | Classifier error |
|---|---|---|---|
| 2 | 80.3% | 100% | 52.8% |
| 3 | 100% | 100% | 49.9% |
| 4 | 100% | 100% | 50.5% |
| 5 | 100% | 100% | 49.3% |
| 6 | 39.2% | 100% | 51.4% |
| 7 | 41.8% | 100% | 50.5% |
| 8 | 0.0% | 100% | 49.3% |
| 9 | 0.0% | 100% | 55.1% |
| 10 | 51.1% | 98.7% | 42.3% |

**Mild drift: not supported.** To see how mild a drift is caught, a fraction of batch-2 readings was mixed into clean batch-1 data:

| Drifted readings | Spread monitor | Shift monitor | Classifier error |
|---|---|---|---|
| 0% | 3.6% | 0.5% | 4.8% |
| 5% | 5.1% | 1.1% | 7.2% |
| 10% | 7.2% | 1.9% | 9.6% |
| 20% | 14.1% | 7.8% | 14.4% |
| 30% | 22.5% | 21.2% | 19.4% |
| 50% | 42.5% | 65.4% | 28.6% |
| 100% | 78.8% | 100% | 52.6% |

With a classifier error 2 to 3 times the clean figure (10 to 20% mixed in), neither monitor is clearly above its own false-alarm level, and the spread monitor is as sensitive as the shift monitor or more so (and has the far higher false-alarm baseline). The claim that the shift monitor catches mild drift in 99.5% of onsets is for a pure bias in the readings' average, the shape of the simulated lens fault; a mixture of two populations is a different fault, and it does not carry over. This metric (time in alarm over a steady stream) is also not the onset-detection metric of the simulated table.

**What the real data supports.** Both monitors detect large drift; the shift monitor was the reliable one (it never missed, where the spread monitor missed two batches entirely). It does not support the 0.1% false-alarm calibration, or mild-drift detection, outside the simulation.

## How the platform uses the node's self-assessment

- Estimates flagged `out_of_distribution`, and estimates produced while `drift_suspected` is true, are **excluded from causal analysis** (`services/causal-engine`), as human-driven sessions already are. Events from sources with no on-node inference are kept.
- `GET /api/edge/plate-waste` (dashboard-api) shows each node, the model it ran, how often it distrusted itself, its inference latency, and whether it is drifting now.
- Every stored estimate carries the model hash. The node refuses to start if its artifact does not match its declared hash.

## Known limitations

- State (the spread and shift windows) is in memory and starts empty after a restart: no score and no alarm for the first 30 readings (shift) and 50 (spread).
- The sensor model is the author's design; the relationships the model learns are ones the author put there.
- The update path (below) gates a new model on agreement with the model in service, not on correctness: there is no ground truth in the field. It cannot tell a model that is wrong the same way, or one that differs by less than the tolerance, from a good one; judge a model on a held-out labelled set before it goes into the store.
- After a restart a node runs the model baked into its image until the retained command arrives (seconds after it reconnects).
- One operator master secret per deployment, with a rotation procedure (`rotate-master` / `finish-rotation`, and `rotate-node` for one node) that has been tested with stand-ins and at the node but not run live; the Compose broker is TLS-only (2026-10-07) and the live cluster's broker has required a login since 2026-10-07 (it still has a plaintext listener on 1883 beside TLS on 8883). Model control has been on for the plate node on the cluster since 2026-10-07 (its pod holds its own key); `k8s/audit/verify-model-control.sh` is the live check of it (a wrong signature must be rejected, a right one accepted), written and tested against stand-ins but **not yet run on the cluster**. The update path has been run on the Compose stack and in tests, and no model has been rolled out on k3s.
- Drift is injected by a static `EDGE_LENS_FOULING` setting; it is not yet a scenario the scenario-injection controller can start and stop.
- The per-reading, spread and shift thresholds are calibrated on the same simulated process they are tested on, and so is the claim that the shift monitor catches mild fouling (RSK-031).

## Scope decision: on-node inference stays on the plate-waste node only (2026-10-06)

The owner decided not to add models to the POS, ticket-timer or staffing sensors. The case was weighed both ways first.

**For adding them.** (1) A fleet of one node does not exercise fleet management (versioning, canary, the fleet view of FR-EDG-06). (2) The ticket timer could flag a stalled or at-risk ticket locally, which works through a network outage and addresses a known weakness (stalled tickets once blinded the anomaly detector, DEF-056; the aggregator drops in-flight tickets by design). (3) POS void and discount scoring is a recognised edge use and keeps detail local. (4) Latency, offline operation and privacy are generic edge benefits. (5) Breadth.

**Against.** (1) **There is no raw signal to infer from.** Plate waste fuses four noisy continuous channels into a quantity; a POS transaction, a ticket stage change and a clock-in are already the information, so a model there would only re-score numbers the author's own generator produced. (2) **It would repeat the circularity already recorded for the one model** (RSK-031: judged only against the simulator that made its data), three times over, with no new evidence. (3) **It would muddy the product's purpose.** The platform's value is auditable root-cause analysis; an edge model that predicts delay from staffing would contaminate the very staffing-to-delay estimate the causal engine makes, and the plate-waste node already needed an out-of-distribution and drift filter (FR-EDG-05) for that reason. (4) **The cloud does it better and cheaper**: the anomaly detector sees every ticket across stations with simple robust control limits, while a per-station model sees only its own. (5) **No pressure to move compute to the edge**: about 0.7 events a second in total, so no bandwidth or latency problem to relieve. (6) **Cost**: the single node took a trainer, hash and budgets, two guards, a fleet view and dozens of tests, and re-verifying it surfaced eleven log entries (DEF-133 to DEF-143); every additional model multiplies that, and there is still no model update path (RSK-033). (7) A reviewer may read models added for their own sake as AI for show, which would cost the project's main strength, that its claims are measured and its limits stated.

**Decision and what would change it.** Edge inference only where there is a raw signal and a physical or bandwidth reason. It would be worth revisiting with real hardware or real data, a genuinely raw source (a camera counting covers or queue length, say), an offline-first kitchen display, or a privacy requirement that keeps data on the device. If a cheap version of the stalled-ticket idea is wanted, it should be a deterministic threshold in the ticket-timer node, labelled as edge logic and not as AI.

## Updating a model, and rolling it back

A node takes a new model from the cloud over MQTT, checks it, compares it with the model in service on live readings, and only then swaps (`edge_ai/updater.py`; cloud side `control/edge_control.py`). The versions are in `model_store/plate-waste-edge-regressor/` (1.0.0, the first model, byte for byte as first shipped; 1.1.0, with the shift monitor; 1.2.0, with the flatline monitor too, the one baked into the image). The tool is a separate service, `edge-operator` (a Compose `tools` profile, not started with the stack): it logs in to the broker as the `edge-operator` user, the only login allowed to publish control topics, and holds the master secret from which each node's key is derived. No node container has either:

```bash
docker compose run --rm edge-operator list
docker compose run --rm edge-operator rollout  --version 1.2.0 --nodes sim-plate-cam-01   # a canary
docker compose run --rm edge-operator status
docker compose run --rm edge-operator rollout  --version 1.2.0 --nodes all                 # then every node that has reported a status
docker compose run --rm edge-operator rollback --to 1.0.0 --nodes all                      # back, no comparison
docker compose run --rm edge-operator clear    --nodes all                                 # drop the desired state
docker compose run --rm edge-operator derive-key --node sim-plate-cam-01 --generation 2    # a node's own key
```

**What the node checks, in order.** The command names this node and is signed with this node's own key (a node with no key takes no commands at all); it is newer than the last command acted on; the artifact is this model (same id and input channels), within the 16 KB budget, passes the loader's hash check, and carries the hash the command declares; the node computes the same grams as the cloud on sixteen probe readings (a known-answer test, which catches an artifact read differently on the node, not a bad model); and inference meets the latency budget. Then **a shadow comparison**: for 50 live readings (the default) the candidate runs beside the model in service on the same inputs, the published estimates still come from the model in service, and the candidate is promoted only if its estimates agree (mean absolute difference under 10 g by default) and it raised no errors. A rejected model leaves the node's estimates unchanged and the node reports why.

**Why the shadow comparison and not the out-of-distribution rate.** The rate depends on the inputs, not the weights: a fouled lens would look like a bad model, and a bad model fed normal inputs would look fine.

**Rollback** is a forced rollout of an older stored version: it skips only the shadow comparison (a rollback must not be judged against the faulty model it replaces); the signature, hash, probe and budget checks still apply. A version from before the shift monitor runs with the spread monitor alone.

**Desired state.** A rollout is a retained message, so a node that restarts is told again. A rollback replaces what a restarting node will be told; `clear` removes it, and a node that restarts after that runs the model baked into its image. There is no fleet-wide topic: every command is signed for one node with that node's key and published on that node's topic, and `--nodes all` means every node that has reported a status (a node announces itself, retained, whenever it connects), so a canary can go first.

**What the cloud does with a drift alarm: nothing, except advise.** `curl ... /api/edge/plate-waste | python -m control.edge_control advise` says what to look at. A drift alarm cannot tell a dirty lens from a changed population, and a model retrained on a fouled lens's data would learn the fault, so no rollout is triggered by it: a person decides.

**Keys, and rotating one.** Each node has its own key: HMAC-SHA256 of its id and a generation number under a master secret only the operator holds (`derive-key` prints it). A key taken from one node commands no other node, and a command also names its target, so a command signed for one node and sent to another is refused. To rotate a node's key: derive the next generation, give the node both keys (`EDGE_CONTROL_KEY` the new one, `EDGE_CONTROL_KEY_PREVIOUS` the old one, which the node accepts as well), command it with `--generation N+1`, then take the previous key away. On Kubernetes `k8s/mosquitto/provision-mqtt-auth.sh rotate-node NODE` does the Secret side. The master secret has no rotation procedure yet.

**The broker login.** The broker refuses anonymous clients and limits each user to its own topics (`docker-compose/mosquitto/acl`, copied into the chart): each simulator publishes only its own sensor topic, only the bridge reads sensor topics, only the operator publishes control topics, a node reads only its own control topic. Mosquitto enforces this *silently* (a refused subscription is granted and delivers nothing; a refused publish is a success in MQTT 3.1.1), so the simulators and the operator tool use MQTT 5 and a refused publish is an error. Passwords are `MQTT_PASSWORD_<USER>` environment variables with a local-development placeholder, like the database passwords.

**Turning it on.** Compose needs nothing: the node's default key is the one derived for it from the local-development master secret, and the broker login uses the placeholders (set `EDGE_CONTROL_MASTER_KEY`, `EDGE_CONTROL_KEY` and the `MQTT_PASSWORD_*` variables for anything shared). On Kubernetes, `bash k8s/mosquitto/provision-mqtt-auth.sh` makes every credential as a Secret (nothing is printed), the broker login is cut over by `k8s/realign/realign-live-cluster.sh` in an order that never locks a client out, and model control stays off until you set `controlKeySecret: edge-control-sim-plate-cam-01` for the plate-waste node in `k8s/edge-simulators/values.yaml` and upgrade the chart. The cluster was cut over by that realign on 2026-10-07 and checked.

## Retraining

```bash
pip install -r edge-simulators/training/requirements.txt
python edge-simulators/training/train_plate_waste_model.py
```

To add the shift monitor to an artifact **without retraining** (this is how 1.1.0 was made from 1.0.0: the weights are byte for byte the same, only the monitor's calibration, the version, the hash and the card change, and the script refuses an artifact that does not match its own hash):

```bash
python edge-simulators/training/train_plate_waste_model.py --add-shift-monitor
```

The flatline monitor was added the same way (`--add-flatline-monitor`, 1.2.0 from 1.1.0: weights, input statistics and the other monitors' thresholds unchanged, holdout error unchanged at 7.1 g). To see a sensor fault on a running node, set `EDGE_SENSOR_FAULT` (one of `stuck-light`, `stuck-camera-area`, `stuck-scale`, `gain-loss-0.7`, `gain-loss-0.5`) as `EDGE_LENS_FOULING` is set.

A model is published by adding its artifact to `model_store/<model_id>/<version>.json` (the file name is the version; the store refuses a file whose hash or name does not match) and, if it should be the default for new nodes, copying it over `edge_ai/plate_waste_edge_model.json`: a test fails if the newest stored version and the image's default differ.

Every random choice is seeded. Different scikit-learn versions can differ in the last bits, so the committed artifact and its hash are the source of truth and the tests check that retraining reproduces the quality, not the bytes. A retrained model gets a new hash; bump `MODEL_VERSION` in the trainer when you publish one.
