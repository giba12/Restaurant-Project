"""
Tests for the plate-waste node's on-device model (edge_ai/) and the node
that uses it (simulators/plate_waste.py).

These check the claims the feature makes, each against something it could
quietly get wrong: that the model is tamper-evident, that it fits the node's
budgets, that it is genuinely better than the sensor it replaces, that its
accuracy figures are true, that it keeps the confounder structure the causal
engine depends on, that its guards fire when they should and stay quiet when
they should not, and that the events it emits satisfy the contract.

Thresholds are set from measurement, with the measured value beside each (the
same figures are recorded in the model artifact's "card"); none was guessed.

No Kafka, MQTT or database: paho is stubbed, as in the other simulator tests.

    pip install numpy jsonschema pytest
    cd edge-simulators && python -m pytest test_edge_ai.py -v
"""
import copy
import json
import os
import random
import sys
import time
import tracemalloc
import types

import numpy as np
import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
os.environ.setdefault("SCHEMA_DIR", os.path.join(os.path.dirname(HERE), "schemas"))

_paho = types.ModuleType("paho")
_paho_mqtt = types.ModuleType("paho.mqtt")
_paho_mqtt_client = types.ModuleType("paho.mqtt.client")
_paho_mqtt_client.Client = None
_paho_mqtt.client = _paho_mqtt_client
_paho.mqtt = _paho_mqtt
sys.modules.setdefault("paho", _paho)
sys.modules.setdefault("paho.mqtt", _paho_mqtt)
sys.modules.setdefault("paho.mqtt.client", _paho_mqtt_client)

import jsonschema  # noqa: E402

from edge_ai import model as edge_model  # noqa: E402
from edge_ai import sensor  # noqa: E402
from simulators import plate_waste  # noqa: E402

SCHEMA = json.load(open(os.path.join(os.environ["SCHEMA_DIR"], "PlateWasteEvent.schema.json")))
ARTIFACT = json.load(open(edge_model.DEFAULT_MODEL_PATH))


@pytest.fixture(scope="module")
def model():
    return edge_model.EdgeModel.from_file()


def draw(n, seed, lens_fouling=0.0):
    rng = random.Random(seed)
    features, truth = [], []
    for _ in range(n):
        g = sensor.true_waste_grams(rng, to_go=rng.random() < 0.15)
        features.append(sensor.feature_vector(sensor.read_sensors(g, rng, lens_fouling)))
        truth.append(g)
    return np.asarray(features), np.asarray(truth)


def rmse(estimate, truth):
    return float(np.sqrt(np.mean((np.asarray(estimate) - np.asarray(truth)) ** 2)))


# ------------------------------------------------------------------ integrity and budgets

def test_the_shipped_model_loads_and_reports_the_hash_it_was_published_with(model):
    assert model.sha256 == ARTIFACT["weights_sha256"] == edge_model.behaviour_hash(ARTIFACT)


@pytest.mark.parametrize("tamper", [
    lambda a: a["layers"][0]["weights_int8"][0].__setitem__(0, a["layers"][0]["weights_int8"][0][0] + 1),
    lambda a: a["layers"][1]["bias"].__setitem__(0, a["layers"][1]["bias"][0] + 0.01),
    lambda a: a["input_mean"].__setitem__(0, a["input_mean"][0] + 1.0),
    lambda a: a["ood"].__setitem__("threshold", a["ood"]["threshold"] * 2),
    lambda a: a["ood"].__setitem__("drift_threshold", a["ood"]["drift_threshold"] * 2),
    lambda a: a.__setitem__("output_scale_g", a["output_scale_g"] * 1.01),
], ids=["a-weight", "a-bias", "input-statistics", "ood-threshold", "drift-threshold", "output-scale"])
def test_any_change_to_what_determines_behaviour_is_refused_at_load(tamper):
    # The point of the hash: a corrupted or hand-edited model must stop the
    # node, not emit plausible-looking numbers under the old model's name.
    artifact = copy.deepcopy(ARTIFACT)
    tamper(artifact)
    with pytest.raises(edge_model.ModelIntegrityError):
        edge_model.EdgeModel(artifact)


def test_editing_only_the_descriptive_card_does_not_change_the_hash():
    artifact = copy.deepcopy(ARTIFACT)
    artifact["card"]["note"] = "documentation only"
    assert edge_model.EdgeModel(artifact).sha256 == ARTIFACT["weights_sha256"]


def test_the_artifact_fits_the_size_budget():
    # Measured 3.4 KB against a 16 KB ceiling.
    assert os.path.getsize(edge_model.DEFAULT_MODEL_PATH) <= edge_model.MAX_ARTIFACT_BYTES


def test_inference_fits_the_latency_budget(model):
    # Measured about 0.13 ms per reading against a 5 ms ceiling at p99, which
    # leaves room for a loaded or much slower device.
    features, _ = draw(2000, seed=11)
    times = []
    for row in features:
        start = time.perf_counter()
        model.infer(row)
        times.append((time.perf_counter() - start) * 1000.0)
    assert np.percentile(times, 99) <= edge_model.MAX_INFERENCE_P99_MS


def test_loading_the_model_needs_little_memory():
    # Measured well under 0.5 MB of allocations for the whole load; the
    # container's own limit is far larger, so this guards the model, not the box.
    tracemalloc.start()
    edge_model.EdgeModel.from_file()
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    assert peak < 1024 * 1024


# ------------------------------------------------------------------ the model is worth having

def test_the_model_is_far_more_accurate_than_the_scale_channel_alone(model):
    # Measured 7.1 g RMSE against 20.8 g for the scale alone (about 66% lower).
    # Requiring at least a 50% reduction fails a model that merely echoes the
    # scale, which is what a broken or untrained one would do.
    features, truth = draw(5000, seed=21)
    estimate, _ = model.predict_batch(features)
    assert rmse(estimate, truth) <= 0.5 * rmse(np.maximum(features[:, 0], 0), truth)


def test_the_published_accuracy_figures_are_true_on_fresh_data(model):
    card = model.card
    features, truth = draw(8000, seed=31)  # seeds the trainer never used
    estimate, _ = model.predict_batch(features)
    assert 0.85 * card["holdout_rmse_g"] <= rmse(estimate, truth) <= 1.15 * card["holdout_rmse_g"]


def test_int8_weights_cost_almost_no_accuracy():
    card = ARTIFACT["card"]
    int8, float32 = card["holdout_rmse_g"], card["holdout_rmse_g_float32_weights"]
    assert int8 <= float32 * 1.05, f"int8 {int8} g vs float32 {float32} g"


def test_estimates_never_go_negative(model):
    features, _ = draw(3000, seed=41)
    estimate, _ = model.predict_batch(features)
    assert estimate.min() >= 0.0


# ------------------------------------------------------------------ the node and the causal engine's confounder

def test_the_node_keeps_the_to_go_confounder_the_causal_engine_looks_for():
    # The simulator still draws true waste so that a to-go box cuts what is
    # left to a quarter. The engine's job is to find that effect in estimates
    # that now come from a model; a model that flattened it would leave the
    # engine nothing true to find. Measured ratio: about 0.26.
    node = plate_waste.PlateWasteNode(edge_model.EdgeModel.from_file(), rng=random.Random(51))
    to_go, other = [], []
    for _ in range(6000):
        event = node.next_event()
        (to_go if event["confounder_flags"]["to_go_container_used"] else other).append(event["estimated_waste_grams"])
    ratio = np.mean(to_go) / np.mean(other)
    assert 0.20 <= ratio <= 0.32, ratio


def test_the_node_scores_well_against_the_truth_it_never_sees():
    node = plate_waste.PlateWasteNode(edge_model.EdgeModel.from_file(), rng=random.Random(61))
    errors = []
    for _ in range(3000):
        event = node.next_event()
        errors.append(event["estimated_waste_grams"] - node.last_true_grams)
    assert float(np.sqrt(np.mean(np.square(errors)))) < 10.0  # measured 7.1 g


def test_the_lens_fouling_knob_really_degrades_what_the_camera_sees():
    # The premise of the drift tests below: the fault they inject changes the inputs.
    clean, _ = draw(3000, seed=71, lens_fouling=0.0)
    fouled, _ = draw(3000, seed=71, lens_fouling=0.8)
    assert fouled[:, 1].mean() < 0.6 * clean[:, 1].mean()  # area
    assert fouled[:, 3].mean() < 0.7 * clean[:, 3].mean()  # light


# ------------------------------------------------------------------ the out-of-distribution guard

def test_the_per_reading_guard_is_quiet_on_normal_readings(model):
    features, _ = draw(10000, seed=81)
    _, distance = model.predict_batch(features)
    assert np.mean(distance > model.ood_threshold) < 0.005  # calibrated to about 0.1%


@pytest.mark.parametrize("bad", [
    [-500.0, 0.5, 5.0, 450.0],   # an impossible weight
    [100.0, 0.5, 5.0, 5000.0],   # a light sensor stuck high
    [100.0, 0.0, 40.0, 450.0],   # a camera that sees nothing while the depth says a heap
], ids=["impossible-weight", "light-sensor-stuck", "camera-and-depth-disagree"])
def test_the_per_reading_guard_flags_gross_outliers(model, bad):
    assert model.infer(bad).out_of_distribution is True


def test_the_per_reading_guard_misses_a_slow_fault_which_is_why_the_drift_monitor_exists(model):
    # An honest limit, measured and pinned so nobody believes otherwise: with a
    # fouled lens the estimates are several times worse, yet well under a tenth
    # of readings are individually flagged (0.8% at fouling 0.4, 5% at 0.6).
    features, truth = draw(5000, seed=91, lens_fouling=0.4)
    estimate, distance = model.predict_batch(features)
    assert rmse(estimate, truth) > 3 * model.card["holdout_rmse_g"]
    assert np.mean(distance > model.ood_threshold) < 0.10


# ------------------------------------------------------------------ the drift monitor

def test_the_drift_monitor_reports_nothing_until_its_window_is_full():
    monitor = edge_model.DriftMonitor(window=5, threshold=1.0)
    for _ in range(4):
        assert monitor.update(10.0) == (None, False)
    score, suspected = monitor.update(10.0)
    assert score == 100.0 and suspected is True


def test_the_drift_monitor_is_a_true_rolling_mean_of_squared_distances():
    monitor = edge_model.DriftMonitor(window=4, threshold=1e9)
    rng = random.Random(101)
    seen = []
    for _ in range(40):
        d = round(rng.uniform(0, 6), 3)
        seen.append(d * d)
        score, _ = monitor.update(d)
        if len(seen) >= 4:
            assert score == pytest.approx(round(sum(seen[-4:]) / 4, 3), abs=1e-3)


def test_the_drift_monitor_stays_quiet_through_normal_operation(model):
    # Calibrated to alarm about 0.1% of the time on clean data (measured 0.11%).
    # Allowing 1% is a loose bound that still fails a monitor set far too tight.
    features, _ = draw(20000, seed=111)
    _, distance = model.predict_batch(features)
    monitor = model.new_drift_monitor()
    alarms = readings = 0
    for d in np.round(distance, 3):
        score, suspected = monitor.update(float(d))
        if score is not None:
            readings += 1
            alarms += suspected
    assert alarms / readings < 0.01


@pytest.mark.parametrize("fouling, within", [(0.4, 150), (0.6, 60)])
def test_the_drift_monitor_catches_a_fouling_lens(model, fouling, within):
    # Measured: every trial detected, median delay 26 readings at 0.4 and 12 at
    # 0.6. Each trial runs clean until the window is full, then the lens fouls.
    # At the simulator's default 8 readings a minute, 26 readings is ~3 minutes.
    missed = 0
    for trial in range(30):
        clean, _ = draw(model.drift_window, seed=1000 + trial)
        fouled, _ = draw(within, seed=2000 + trial, lens_fouling=fouling)
        _, distance = model.predict_batch(np.vstack([clean, fouled]))
        monitor = model.new_drift_monitor()
        alarmed_after_onset = False
        for i, d in enumerate(np.round(distance, 3)):
            _, suspected = monitor.update(float(d))
            if suspected and i < model.drift_window:
                pytest.fail("false alarm before the fault began")
            alarmed_after_onset |= bool(suspected and i >= model.drift_window)
        missed += not alarmed_after_onset
    assert missed == 0, f"{missed} of 30 fouling onsets at {fouling} went unnoticed within {within} readings"


def test_a_restarted_node_starts_with_an_empty_drift_window():
    # State is in memory and is documented as lost on restart; this pins it.
    node = plate_waste.PlateWasteNode(edge_model.EdgeModel.from_file(), rng=random.Random(121))
    for _ in range(60):
        node.next_event()
    assert node.next_event()["edge_inference"]["drift_score"] is not None
    reborn = plate_waste.PlateWasteNode(edge_model.EdgeModel.from_file(), rng=random.Random(121))
    assert reborn.next_event()["edge_inference"]["drift_score"] is None


def test_a_node_with_a_fouled_lens_reports_drift_in_its_own_events():
    node = plate_waste.PlateWasteNode(edge_model.EdgeModel.from_file(), lens_fouling=0.6, rng=random.Random(131))
    events = [node.next_event() for _ in range(120)]
    assert any(e["edge_inference"]["drift_suspected"] for e in events[60:])
    healthy = plate_waste.PlateWasteNode(edge_model.EdgeModel.from_file(), rng=random.Random(131))
    assert not any(healthy.next_event()["edge_inference"]["drift_suspected"] for _ in range(120))


# ------------------------------------------------------------------ the contract

def test_every_event_from_the_node_satisfies_the_schema():
    node = plate_waste.PlateWasteNode(edge_model.EdgeModel.from_file(), rng=random.Random(141))
    for _ in range(300):
        jsonschema.validate(node.next_event(), SCHEMA)


def test_the_event_names_the_exact_model_that_produced_the_estimate(model):
    event = plate_waste.PlateWasteNode(model, rng=random.Random(151)).next_event()
    assert event["schema_version"] == "1.1.0"
    inference = event["edge_inference"]
    assert (inference["model_id"], inference["model_version"], inference["model_sha256"]) == (
        model.model_id, model.model_version, model.sha256)


def test_the_node_infers_its_estimates_it_does_not_copy_the_truth():
    # A node that leaked the simulator's true grams would report estimates
    # equal to them. A real inference is close but almost never exact: with a
    # 7 g RMSE and one-decimal rounding, exact matches are around 0.5%.
    node = plate_waste.PlateWasteNode(edge_model.EdgeModel.from_file(), rng=random.Random(161))
    exact = total = 0
    for _ in range(2000):
        event = node.next_event()
        exact += event["estimated_waste_grams"] == node.last_true_grams
        total += 1
    assert exact / total < 0.05


def test_events_without_edge_inference_are_still_valid_for_older_producers():
    # Backward compatibility of the additive change: a 1.0.0 event, as the
    # simulator produced before, must still validate.
    node = plate_waste.PlateWasteNode(edge_model.EdgeModel.from_file(), rng=random.Random(171))
    event = node.next_event()
    del event["edge_inference"]
    event["schema_version"] = "1.0.0"
    jsonschema.validate(event, SCHEMA)


@pytest.mark.parametrize("damage", [
    lambda e: e["edge_inference"].__setitem__("model_sha256", "not-a-hash"),
    lambda e: e["edge_inference"].__setitem__("model_version", "latest"),
    lambda e: e["edge_inference"].__setitem__("ood_score", -1.0),
    lambda e: e["edge_inference"].__setitem__("inference_latency_ms", -0.1),
    lambda e: e["edge_inference"].__setitem__("out_of_distribution", "yes"),
    lambda e: e["edge_inference"].__setitem__("surprise", 1),
    lambda e: e["edge_inference"].pop("model_id"),
], ids=["bad-hash", "bad-version", "negative-distance", "negative-latency", "non-boolean-flag", "unknown-field", "missing-model-id"])
def test_the_schema_rejects_a_malformed_edge_inference(damage):
    event = plate_waste.PlateWasteNode(edge_model.EdgeModel.from_file(), rng=random.Random(181)).next_event()
    damage(event)
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(event, SCHEMA)


# ------------------------------------------------------------------ startup

def test_a_corrupt_model_stops_the_node_at_startup_instead_of_running_on(tmp_path, monkeypatch):
    # Simulator.run_forever logs and continues past errors raised while
    # generating an event, so the check must happen in main(), before the loop.
    artifact = copy.deepcopy(ARTIFACT)
    artifact["layers"][0]["bias"][0] += 1.0
    bad = tmp_path / "model.json"
    bad.write_text(json.dumps(artifact))
    monkeypatch.setenv("EDGE_MODEL_PATH", str(bad))
    with pytest.raises(edge_model.ModelIntegrityError):
        plate_waste.main()
