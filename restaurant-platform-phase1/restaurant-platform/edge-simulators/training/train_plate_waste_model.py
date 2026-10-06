"""
Offline trainer for the plate-waste node's model. Not part of the node image:
scikit-learn is needed here and deliberately not on the device.

    pip install -r edge-simulators/training/requirements.txt
    python edge-simulators/training/train_plate_waste_model.py

Draws a training set from the same sensor simulation the node runs
(edge_ai/sensor.py), fits a small MLP, stores its weights as int8, calibrates
the out-of-distribution guard on held-out data, measures everything it later
claims, and writes edge_ai/plate_waste_edge_model.json.

    python edge-simulators/training/train_plate_waste_model.py --add-shift-monitor

adds the shift monitor to an existing artifact WITHOUT retraining: the weights
stay byte for byte as they were, only the monitor's calibration, the version,
the hash and the card change. That is how version 1.1.0 was made from 1.0.0;
`--add-flatline-monitor` does the same for the flatline monitor (1.2.0 from 1.1.0).

Every random choice is seeded, so the same library versions reproduce the same
artifact. Different scikit-learn versions can differ in the last bits, which
is why the committed artifact (and its hash) is the source of truth and the
test suite checks the trainer reproduces the *quality*, not the bytes.
"""
import argparse
import json
import random
import sys
from pathlib import Path

import numpy as np
from sklearn.neural_network import MLPRegressor

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from edge_ai import model as edge_model  # noqa: E402
from edge_ai import sensor  # noqa: E402

MODEL_ID = "plate-waste-edge-regressor"
MODEL_VERSION = "1.2.0"
SHIFT_MONITOR_VERSION = "1.1.0"  # the version `--add-shift-monitor` produces
HIDDEN = (16, 8)
TRAIN_SAMPLES = 40_000
HOLDOUT_SAMPLES = 20_000
TRAIN_SEED, HOLDOUT_SEED, FIT_SEED = 20261004, 20261005, 7
OUTPUT_SCALE_G = 100.0  # the network predicts grams / 100
OOD_QUANTILE = 0.999  # the guard is calibrated to flag about 0.1% of in-distribution readings
DRIFT_WINDOW = 50  # readings; about six minutes at the simulator's default rate
DRIFT_QUANTILE = 0.999  # of rolling window means on clean data
DRIFT_CALIBRATION_READINGS = 300_000
SHIFT_WINDOW = 30  # readings; about four minutes at the simulator's default rate
SHIFT_QUANTILE = 0.999  # of rolling window scores on clean data
FLATLINE_WINDOW = 30  # readings
FLATLINE_QUANTILE = 0.001  # the LOWER tail of clean window scores: a flatline is a score that is too small
DRIFT_ONSET_TRIALS = 200
TO_GO_RATE = 0.15  # the simulator's own rate


def draw(n: int, seed: int, lens_fouling: float = 0.0):
    """n (features, true grams) pairs from the node's own sensor simulation."""
    rng = random.Random(seed)
    features, grams = [], []
    for _ in range(n):
        g = sensor.true_waste_grams(rng, to_go=rng.random() < TO_GO_RATE)
        features.append(sensor.feature_vector(sensor.read_sensors(g, rng, lens_fouling)))
        grams.append(g)
    return np.asarray(features), np.asarray(grams)


def rmse(estimate, truth) -> float:
    return float(np.sqrt(np.mean((np.asarray(estimate) - np.asarray(truth)) ** 2)))


def quantise(weights: np.ndarray):
    scale = float(np.max(np.abs(weights)) / 127.0)
    return np.clip(np.round(weights / scale), -127, 127).astype(np.int8), scale


def build(seed_override: int | None = None) -> dict:
    x_train, y_train = draw(TRAIN_SAMPLES, TRAIN_SEED)
    mean, std = x_train.mean(axis=0), x_train.std(axis=0)
    z_train = (x_train - mean) / std

    net = MLPRegressor(
        hidden_layer_sizes=HIDDEN, activation="relu", solver="adam", max_iter=400,
        early_stopping=True, n_iter_no_change=15, random_state=FIT_SEED if seed_override is None else seed_override,
    )
    net.fit(z_train, y_train / OUTPUT_SCALE_G)

    layers = []
    for i, (weights, bias) in enumerate(zip(net.coefs_, net.intercepts_)):
        q, scale = quantise(weights)
        layers.append({
            "weights_int8": q.tolist(),
            "weight_scale": scale,
            "bias": [float(b) for b in bias],
            "activation": "relu" if i < len(net.coefs_) - 1 else "linear",
        })

    covariance = np.cov(z_train, rowvar=False)
    ood = {
        "mean": z_train.mean(axis=0).tolist(), "precision": np.linalg.inv(covariance).tolist(),
        "threshold": 0.0, "drift_window": DRIFT_WINDOW, "drift_threshold": 0.0,
        "shift_window": SHIFT_WINDOW, "shift_threshold": 0.0,
        "flatline_window": FLATLINE_WINDOW, "flatline_threshold": 0.0,
    }

    artifact = {
        "model_id": MODEL_ID,
        "model_version": MODEL_VERSION,
        "feature_names": list(sensor.FEATURE_NAMES),
        "input_mean": mean.tolist(),
        "input_std": std.tolist(),
        "layers": layers,
        "output_scale_g": OUTPUT_SCALE_G,
        "ood": ood,
    }

    # Calibrate the guard on data the model never saw, using the deployed code path.
    provisional = edge_model.EdgeModel({**artifact, "weights_sha256": edge_model.behaviour_hash(artifact)})
    x_hold, y_hold = draw(HOLDOUT_SAMPLES, HOLDOUT_SEED)
    _, hold_distance = provisional.predict_batch(x_hold)
    ood["threshold"] = round(float(np.quantile(hold_distance, OOD_QUANTILE)), 3)
    artifact["weights_sha256"] = edge_model.behaviour_hash(artifact)
    deployed = edge_model.EdgeModel(artifact)

    # Calibrate the drift monitor on a long clean stream, through the deployed
    # monitor class and the same rounding the node applies to each distance.
    calibration_scores = rolling_scores(deployed, draw(DRIFT_CALIBRATION_READINGS, HOLDOUT_SEED + 1)[0])
    ood["drift_threshold"] = round(float(np.quantile(calibration_scores, DRIFT_QUANTILE)), 3)
    artifact["weights_sha256"] = edge_model.behaviour_hash(artifact)
    deployed = edge_model.EdgeModel(artifact)
    calibrate_shift_monitor(artifact)
    calibrate_flatline_monitor(artifact)
    deployed = edge_model.EdgeModel(artifact)

    estimate, distance = deployed.predict_batch(x_hold)
    float_estimate = float_forward(net, (x_hold - mean) / std)

    card = {
        "training_samples": TRAIN_SAMPLES,
        "holdout_samples": HOLDOUT_SAMPLES,
        "seeds": {"train": TRAIN_SEED, "holdout": HOLDOUT_SEED, "fit": FIT_SEED},
        "holdout_rmse_g": round(rmse(estimate, y_hold), 2),
        "holdout_rmse_g_float32_weights": round(rmse(float_estimate, y_hold), 2),
        "holdout_rmse_g_scale_channel_alone": round(rmse(np.maximum(x_hold[:, 0], 0), y_hold), 2),
        "holdout_ood_flag_rate": round(float(np.mean(distance > deployed.ood_threshold)), 4),
    }
    card.update(drift_card(deployed))
    artifact["card"] = card
    return artifact


def drift_card(deployed) -> dict:
    """Everything the card says about the two monitors, measured through the deployed code. Needs no network."""
    clean = draw(DRIFT_CALIBRATION_READINGS, HOLDOUT_SEED + 2)[0]
    clean_scores = rolling_scores(deployed, clean)
    clean_shift = shift_scores(deployed, draw(DRIFT_CALIBRATION_READINGS, HOLDOUT_SEED + 4)[0])
    card = {
        "drift_monitor": {
            "window": DRIFT_WINDOW,
            "threshold": deployed.drift_threshold,
            "clean_time_in_alarm": round(float(np.mean(clean_scores > deployed.drift_threshold)), 4),
        },
        "shift_monitor": {
            "window": SHIFT_WINDOW,
            "threshold": deployed.shift_threshold,
            "clean_time_in_alarm": round(float(np.mean(clean_shift > deployed.shift_threshold)), 4),
        },
        "drift_response": {},
    }
    if deployed.flatline_window is not None:
        clean_flatline = flatline_scores(deployed, draw(DRIFT_CALIBRATION_READINGS, HOLDOUT_SEED + 6)[0])
        card["flatline_monitor"] = {
            "window": FLATLINE_WINDOW,
            "threshold": deployed.flatline_threshold,
            "clean_time_in_alarm": round(float(np.mean(clean_flatline < deployed.flatline_threshold)), 4),
        }
        card["fault_response"] = {}
        for k, fault in enumerate(sensor.FAULTS):
            x_fault = sensor.inject_fault(draw(5_000, HOLDOUT_SEED + 200 + k)[0], fault)
            _, dist_f = deployed.predict_batch(x_fault)
            card["fault_response"][fault] = {
                "per_reading_flag_rate": round(float(np.mean(dist_f > deployed.ood_threshold)), 4),
                "spread_time_in_alarm": round(float(np.mean(rolling_scores(deployed, x_fault) > deployed.drift_threshold)), 4),
                "shift_time_in_alarm": round(float(np.mean(shift_scores(deployed, x_fault) > deployed.shift_threshold)), 4),
                "flatline_time_in_alarm": round(float(np.mean(flatline_scores(deployed, x_fault) < deployed.flatline_threshold)), 4),
            }
    for fouling in (0.1, 0.15, 0.2, 0.3, 0.4, 0.6, 0.8):
        x_drift, y_drift = draw(5_000, HOLDOUT_SEED + int(round(fouling * 100)), lens_fouling=fouling)
        est_d, dist_d = deployed.predict_batch(x_drift)
        row = {
            "rmse_g": round(rmse(est_d, y_drift), 2),
            "ood_flag_rate": round(float(np.mean(dist_d > deployed.ood_threshold)), 4),
            "drift_time_in_alarm": round(float(np.mean(rolling_scores(deployed, x_drift) > deployed.drift_threshold)), 4),
            "shift_time_in_alarm": round(float(np.mean(shift_scores(deployed, x_drift) > deployed.shift_threshold)), 4),
        }
        for label, kind in (("drift", "spread"), ("shift", "shift"), ("either", "either")):
            delays = detection_delays(deployed, fouling, monitor=kind)
            found = [d for d in delays if d is not None]
            row[f"{label}_detected_fraction"] = round(len(found) / len(delays), 3)
            row[f"{label}_median_delay_readings"] = float(np.median(found)) if found else None
        card["drift_response"][f"{fouling:g}"] = row
    return card


def shift_scores(model, features: np.ndarray) -> np.ndarray:
    """The shift monitor's score after each reading once its window is full, exactly as the node computes it."""
    monitor = model.new_shift_monitor()
    scores = []
    for deviation in model.deviations(features):
        score, _ = monitor.update(deviation)
        if score is not None:
            scores.append(score)
    return np.asarray(scores)


def calibrate_shift_monitor(artifact: dict) -> None:
    """Set the shift threshold from a long clean stream, then refresh the hash. Weights are not touched."""
    artifact["ood"]["shift_window"] = SHIFT_WINDOW
    artifact["ood"]["shift_threshold"] = 0.0
    artifact["weights_sha256"] = edge_model.behaviour_hash(artifact)
    provisional = edge_model.EdgeModel(artifact)
    clean = shift_scores(provisional, draw(DRIFT_CALIBRATION_READINGS, HOLDOUT_SEED + 3)[0])
    artifact["ood"]["shift_threshold"] = round(float(np.quantile(clean, SHIFT_QUANTILE)), 3)
    artifact["weights_sha256"] = edge_model.behaviour_hash(artifact)


def flatline_scores(model, features: np.ndarray) -> np.ndarray:
    """The flatline monitor's score after each reading once its window is full, exactly as the node computes it."""
    monitor = model.new_flatline_monitor()
    scores = []
    for deviation in model.deviations(features):
        score, _ = monitor.update(deviation)
        if score is not None:
            scores.append(score)
    return np.asarray(scores)


def calibrate_flatline_monitor(artifact: dict) -> None:
    """Set the flatline threshold at the lower tail of a long clean stream, then refresh the hash. Weights are not touched."""
    artifact["ood"]["flatline_window"] = FLATLINE_WINDOW
    artifact["ood"]["flatline_threshold"] = 0.0
    artifact["weights_sha256"] = edge_model.behaviour_hash(artifact)
    provisional = edge_model.EdgeModel(artifact)
    clean = flatline_scores(provisional, draw(DRIFT_CALIBRATION_READINGS, HOLDOUT_SEED + 5)[0])
    artifact["ood"]["flatline_threshold"] = round(float(np.quantile(clean, FLATLINE_QUANTILE)), 3)
    artifact["weights_sha256"] = edge_model.behaviour_hash(artifact)


def add_flatline_monitor(path: Path) -> dict:
    """Upgrade an artifact that has the shift monitor: same weights, flatline monitor added, new version and hash."""
    artifact = json.loads(Path(path).read_text())
    if artifact["weights_sha256"] != edge_model.behaviour_hash(artifact):
        raise edge_model.ModelIntegrityError("refusing to upgrade an artifact that does not match its own hash")
    weights_before = json.dumps(artifact["layers"], sort_keys=True)
    calibrate_flatline_monitor(artifact)
    artifact["model_version"] = MODEL_VERSION
    artifact["weights_sha256"] = edge_model.behaviour_hash(artifact)
    assert json.dumps(artifact["layers"], sort_keys=True) == weights_before, "the upgrade must not change the weights"
    artifact["card"].update(drift_card(edge_model.EdgeModel(artifact)))
    return artifact


def add_shift_monitor(path: Path) -> dict:
    """Upgrade an existing artifact in place of retraining: same weights, new monitor, new version and hash."""
    artifact = json.loads(Path(path).read_text())
    # An artifact without the monitor cannot be loaded as it is, so its own hash is checked directly.
    if artifact["weights_sha256"] != edge_model.behaviour_hash(artifact):
        raise edge_model.ModelIntegrityError("refusing to upgrade an artifact that does not match its own hash")
    weights_before = json.dumps(artifact["layers"], sort_keys=True)
    calibrate_shift_monitor(artifact)
    artifact["model_version"] = SHIFT_MONITOR_VERSION
    artifact["weights_sha256"] = edge_model.behaviour_hash(artifact)
    assert json.dumps(artifact["layers"], sort_keys=True) == weights_before, "the upgrade must not change the weights"
    deployed = edge_model.EdgeModel(artifact)
    artifact["card"].update(drift_card(deployed))
    return artifact


def rolling_scores(model, features: np.ndarray) -> np.ndarray:
    """The drift monitor's score after each reading once its window is full, exactly as the node computes it."""
    _, distance = model.predict_batch(features)
    monitor = model.new_drift_monitor()
    scores = []
    for d in np.round(distance, 3):
        score, _ = monitor.update(float(d))
        if score is not None:
            scores.append(score)
    return np.asarray(scores)


def detection_delays(model, fouling: float, horizon: int = 300, monitor: str = "spread"):
    """
    Readings from the onset of a fault until the monitor first alarms, over
    many trials (None where it never did within the horizon). Each trial runs
    clean until the drift window is full, then the lens fouls. `monitor` is
    "spread" (the rolling mean of squared distance), "shift" (the window-mean
    test) or "either" (the node's drift_suspected). An alarm before onset is
    a false alarm and is counted as no detection.
    """
    delays = []
    for trial in range(DRIFT_ONSET_TRIALS):
        x_clean, _ = draw(DRIFT_WINDOW, 90_000 + trial)
        x_fouled, _ = draw(horizon, 190_000 + trial, lens_fouling=fouling)
        x = np.vstack([x_clean, x_fouled])
        _, d = model.predict_batch(x)
        deviations = model.deviations(x)
        spread, shift = model.new_drift_monitor(), model.new_shift_monitor()
        found = None
        for i, dist in enumerate(np.round(d, 3)):
            alarm = False
            if monitor in ("spread", "either"):
                alarm |= spread.update(float(dist))[1]
            if monitor in ("shift", "either"):
                alarm |= shift.update(deviations[i])[1]
            if alarm and i >= DRIFT_WINDOW:
                found = i - DRIFT_WINDOW + 1
                break
            if alarm and i < DRIFT_WINDOW:
                break  # a false alarm before onset; counted as no detection
        delays.append(found)
    return delays


def float_forward(net: MLPRegressor, z: np.ndarray) -> np.ndarray:
    return np.maximum(net.predict(z) * OUTPUT_SCALE_G, 0.0)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default=str(edge_model.DEFAULT_MODEL_PATH))
    parser.add_argument("--add-shift-monitor", action="store_true",
                        help="add the shift monitor to the artifact at --out without retraining its weights")
    parser.add_argument("--add-flatline-monitor", action="store_true",
                        help="add the flatline monitor to the artifact at --out (which must have the shift monitor) without retraining")
    args = parser.parse_args()
    if args.add_shift_monitor and args.add_flatline_monitor:
        parser.error("add one monitor at a time: shift first, then flatline")
    artifact = (add_shift_monitor(args.out) if args.add_shift_monitor
                else add_flatline_monitor(args.out) if args.add_flatline_monitor else build())
    text = json.dumps(artifact, separators=(",", ":"), allow_nan=False)  # a NaN here would be silently invalid JSON
    Path(args.out).write_text(text + "\n")
    print(f"wrote {args.out}: {len(text)} bytes, sha256 {artifact['weights_sha256']}")
    print(json.dumps(artifact["card"], indent=2))


if __name__ == "__main__":
    main()
