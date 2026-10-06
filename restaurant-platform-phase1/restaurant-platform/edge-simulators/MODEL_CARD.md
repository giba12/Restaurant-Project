# Model card: plate-waste edge regressor

| | |
|---|---|
| Model | `plate-waste-edge-regressor` version `1.1.0` (1.0.0 had the same weights and one drift monitor; 1.1.0 added the shift monitor, 2026-10-06) |
| SHA-256 | `279d8a64fd02` (full value in the artifact; 1.0.0 was `155f41cd2a84`) |
| Runs in | `edge-simulators/simulators/plate_waste.py`, on the (simulated) node, through `edge_ai/model.py` |
| Trained by | `edge-simulators/training/train_plate_waste_model.py` (offline; scikit-learn is not on the node) |
| Artifact | `edge_ai/plate_waste_edge_model.json`, 4992 bytes |

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
| Artifact size | 16 KB | 3.4 KB |
| Inference latency, p99 | 5 ms | about 0.13 ms mean per reading on the development laptop |
| Memory to load | 1 MB of allocations | well under that |

## Knowing when not to be trusted

Nothing on the node can tell that an estimate is wrong, because there is no ground truth in the field. What it can tell is that its **inputs have stopped looking like anything it was trained on.** There are three guards, calibrated on held-out clean data.

1. **Per reading** (`out_of_distribution`): Mahalanobis distance from the training data above a threshold set at the 99.9th percentile of clean readings (0.10% of clean readings are flagged). It catches gross failures: an impossible weight, a stuck light sensor, a camera that sees nothing while the depth says a heap.
2. **Spread over a window** (`drift_score`): the rolling mean of the squared distance over the last 50 readings (about six minutes at the simulator's default rate), alarming above 5.658. It reacts to how far readings are from the training data, not to which way.
3. **Shift over a window** (`shift_score`, added in 1.1.0): a Hotelling T-squared of the *mean* deviation over the last 30 readings (about four minutes), alarming above 18.559. On clean data it follows a chi-squared law with four degrees of freedom (one per channel), so the calibrated threshold sits close to that law's 99.9th percentile (18.5). A sustained bias in any direction adds to it with every reading.

`drift_suspected` is true when **either** window guard alarms. On a fresh clean stream of about 300,000 readings the spread monitor alarmed 0.13% of the time, the shift monitor 0.15% and either 0.27%, so the combined guard costs about twice the false alarms of one.

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

**What the shift monitor does not do.** It is not a fault diagnosis. A change in the real mix of plates (a menu change that makes plates heavier) or a change in the room's light would raise it just as a fouling lens does: it says *the inputs have shifted*, and what has shifted needs a person. At fouling 0.1, where the error is already 1.6 times the clean figure, about a quarter of onsets still take more than 150 readings (about 19 minutes) to alarm (7 of 30 in the test's trials), and about 5% (the one early false alarm included) never alarm within 300. And, as everywhere in this card, the fault is the author's own simulation of a fouled lens, a pure bias with a known direction, which is the case this statistic is best at.

## How the platform uses the node's self-assessment

- Estimates flagged `out_of_distribution`, and estimates produced while `drift_suspected` is true, are **excluded from causal analysis** (`services/causal-engine`), as human-driven sessions already are. Events from sources with no on-node inference are kept.
- `GET /api/edge/plate-waste` (dashboard-api) shows each node, the model it ran, how often it distrusted itself, its inference latency, and whether it is drifting now.
- Every stored estimate carries the model hash. The node refuses to start if its artifact does not match its declared hash.

## Known limitations

- State (the spread and shift windows) is in memory and starts empty after a restart: no score and no alarm for the first 30 readings (shift) and 50 (spread).
- The sensor model is the author's design; the relationships the model learns are ones the author put there.
- No model-update path exists yet: changing the model means rebuilding the simulator image. A versioned model store with canary and rollback is the natural next slice.
- Drift is injected by a static `EDGE_LENS_FOULING` setting; it is not yet a scenario the scenario-injection controller can start and stop.
- The per-reading, spread and shift thresholds are calibrated on the same simulated process they are tested on, and so is the claim that the shift monitor catches mild fouling (RSK-031).

## Scope decision: on-node inference stays on the plate-waste node only (2026-10-06)

The owner decided not to add models to the POS, ticket-timer or staffing sensors. The case was weighed both ways first.

**For adding them.** (1) A fleet of one node does not exercise fleet management (versioning, canary, the fleet view of FR-EDG-06). (2) The ticket timer could flag a stalled or at-risk ticket locally, which works through a network outage and addresses a known weakness (stalled tickets once blinded the anomaly detector, DEF-056; the aggregator drops in-flight tickets by design). (3) POS void and discount scoring is a recognised edge use and keeps detail local. (4) Latency, offline operation and privacy are generic edge benefits. (5) Breadth.

**Against.** (1) **There is no raw signal to infer from.** Plate waste fuses four noisy continuous channels into a quantity; a POS transaction, a ticket stage change and a clock-in are already the information, so a model there would only re-score numbers the author's own generator produced. (2) **It would repeat the circularity already recorded for the one model** (RSK-031: judged only against the simulator that made its data), three times over, with no new evidence. (3) **It would muddy the product's purpose.** The platform's value is auditable root-cause analysis; an edge model that predicts delay from staffing would contaminate the very staffing-to-delay estimate the causal engine makes, and the plate-waste node already needed an out-of-distribution and drift filter (FR-EDG-05) for that reason. (4) **The cloud does it better and cheaper**: the anomaly detector sees every ticket across stations with simple robust control limits, while a per-station model sees only its own. (5) **No pressure to move compute to the edge**: about 0.7 events a second in total, so no bandwidth or latency problem to relieve. (6) **Cost**: the single node took a trainer, hash and budgets, two guards, a fleet view and dozens of tests, and re-verifying it surfaced eleven log entries (DEF-133 to DEF-143); every additional model multiplies that, and there is still no model update path (RSK-033). (7) A reviewer may read models added for their own sake as AI for show, which would cost the project's main strength, that its claims are measured and its limits stated.

**Decision and what would change it.** Edge inference only where there is a raw signal and a physical or bandwidth reason. It would be worth revisiting with real hardware or real data, a genuinely raw source (a camera counting covers or queue length, say), an offline-first kitchen display, or a privacy requirement that keeps data on the device. If a cheap version of the stalled-ticket idea is wanted, it should be a deterministic threshold in the ticket-timer node, labelled as edge logic and not as AI.

## Retraining

```bash
pip install -r edge-simulators/training/requirements.txt
python edge-simulators/training/train_plate_waste_model.py
```

To add the shift monitor to an artifact **without retraining** (this is how 1.1.0 was made from 1.0.0: the weights are byte for byte the same, only the monitor's calibration, the version, the hash and the card change, and the script refuses an artifact that does not match its own hash):

```bash
python edge-simulators/training/train_plate_waste_model.py --add-shift-monitor
```

Every random choice is seeded. Different scikit-learn versions can differ in the last bits, so the committed artifact and its hash are the source of truth and the tests check that retraining reproduces the quality, not the bytes. A retrained model gets a new hash; bump `MODEL_VERSION` in the trainer when you publish one.
