"""
The plate-waste node's on-device model.

What runs here is the whole inference path of a small edge model, with no
machine-learning framework on the device: a 4-16-8-1 network whose weights
were trained offline (edge-simulators/training/) and are stored as int8, plus
an out-of-distribution guard. It needs numpy and nothing else.

    sensor channels --standardise--> MLP (int8 weights) --> grams
                    \\--Mahalanobis distance from the training data --> ood_score

Three things make this an edge model and not just a function call:

  * It is small and the budgets are checked, not hoped for: the artifact has a
    size ceiling and an inference-latency ceiling (see the constants below,
    enforced by edge-simulators/test_edge_ai.py).
  * It knows when it should not be trusted, at two time scales. The distance
    of each reading from the training data is reported, and readings beyond a
    threshold calibrated on held-out data are flagged `out_of_distribution`.
    That catches gross outliers but, measured, almost none of a slow sensor
    fault (a fouled lens leaves each reading plausible). So a DriftMonitor
    also keeps a rolling mean of the squared distance over the last
    `window` readings and raises `drift_suspected` when it passes a threshold
    calibrated on clean data. That statistic is blunt, though: it reacts to
    how far readings are from the training data and ignores which way, so it
    missed about 40% of the onsets of a mild fault. A second monitor, the
    ShiftMonitor, tests the window's *mean* deviation (a Hotelling T-squared
    on the last `shift_window` readings), which is the shape a slow sensor
    fault actually has, and catches the same mild fault in nearly every trial
    within a few minutes. The two are complementary: the spread monitor
    reacts to added noise far more strongly than the shift monitor does, the
    shift monitor to a small consistent bias the spread monitor mostly misses,
    so the node alarms when either does. Nothing on the node
    can tell an estimate is wrong (there is no ground truth in the field), but
    it can tell that its inputs have stopped looking like anything it was
    trained on.
  * It is accountable. Every estimate carries the model id, version and a
    SHA-256 over everything that determines its behaviour. The loader
    recomputes that hash and refuses to start on a mismatch, so a corrupted
    or hand-edited artifact fails loudly instead of emitting plausible-looking
    numbers.

The weights are stored as int8 to shrink the artifact and are dequantised to
float32 once at load; the arithmetic itself is float32. This is weight-only
quantization, not integer inference.
"""
import collections
import hashlib
import json
import time
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np

DEFAULT_MODEL_PATH = Path(__file__).with_name("plate_waste_edge_model.json")

# Budgets for a constrained node. They are ceilings with headroom over the
# measured values (recorded in the artifact's "card"), set so that a model
# that has grown unreasonably, or an inference path that has become slow,
# fails a test.
MAX_ARTIFACT_BYTES = 16 * 1024
MAX_INFERENCE_P99_MS = 5.0

# The part of the artifact that determines behaviour, and so is hashed.
HASHED_FIELDS = ("feature_names", "input_mean", "input_std", "layers", "output_scale_g", "ood")


def behaviour_hash(artifact: dict) -> str:
    canonical = json.dumps({k: artifact[k] for k in HASHED_FIELDS}, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class ModelIntegrityError(RuntimeError):
    """The artifact does not match the hash it was published with."""


@dataclass(frozen=True)
class Inference:
    grams: float
    ood_score: float
    out_of_distribution: bool
    latency_ms: float
    # The reading's standardised distance from the training mean, per channel: what the ShiftMonitor averages.
    deviation: tuple = field(default=(), compare=False, repr=False)


class EdgeModel:
    def __init__(self, artifact: dict):
        declared = artifact.get("weights_sha256")
        actual = behaviour_hash(artifact)
        if declared != actual:
            raise ModelIntegrityError(
                f"model artifact does not match its declared hash (declared {declared}, computed {actual}); refusing to run it"
            )
        self.model_id = artifact["model_id"]
        self.model_version = artifact["model_version"]
        self.sha256 = actual
        self.feature_names = tuple(artifact["feature_names"])
        self.card = artifact.get("card", {})

        self._mean = np.asarray(artifact["input_mean"], dtype=np.float64)
        self._std = np.asarray(artifact["input_std"], dtype=np.float64)
        self._layers = [
            (
                np.asarray(layer["weights_int8"], dtype=np.int8).astype(np.float32) * np.float32(layer["weight_scale"]),
                np.asarray(layer["bias"], dtype=np.float32),
                layer["activation"],
            )
            for layer in artifact["layers"]
        ]
        self._output_scale = float(artifact["output_scale_g"])
        ood = artifact["ood"]
        self._ood_mean = np.asarray(ood["mean"], dtype=np.float64)
        self._ood_precision = np.asarray(ood["precision"], dtype=np.float64)
        self.ood_threshold = float(ood["threshold"])
        self.drift_window = int(ood["drift_window"])
        self.drift_threshold = float(ood["drift_threshold"])
        self.shift_window = int(ood["shift_window"])
        self.shift_threshold = float(ood["shift_threshold"])

    @classmethod
    def from_file(cls, path=DEFAULT_MODEL_PATH) -> "EdgeModel":
        return cls(json.loads(Path(path).read_text()))

    # ---- the computation, shared by one reading and by whole evaluation sets

    def _standardise(self, features: np.ndarray) -> np.ndarray:
        return (np.asarray(features, dtype=np.float64) - self._mean) / self._std

    def _forward(self, standardised: np.ndarray) -> np.ndarray:
        h = standardised.astype(np.float32)
        for weights, bias, activation in self._layers:
            h = h @ weights + bias
            if activation == "relu":
                h = np.maximum(h, np.float32(0.0))
        return np.maximum(h[..., 0].astype(np.float64) * self._output_scale, 0.0)

    def _distance(self, standardised: np.ndarray) -> np.ndarray:
        delta = standardised - self._ood_mean
        return np.sqrt(np.maximum(np.einsum("...i,ij,...j->...", delta, self._ood_precision, delta), 0.0))

    def predict_batch(self, features: np.ndarray):
        """Estimates and OOD distances for many readings at once (training and evaluation)."""
        z = self._standardise(features)
        return self._forward(z), self._distance(z)

    def deviations(self, features: np.ndarray) -> np.ndarray:
        """Per-channel standardised deviation from the training mean, for many readings at once."""
        return self._standardise(features) - self._ood_mean

    def new_drift_monitor(self) -> "DriftMonitor":
        return DriftMonitor(self.drift_window, self.drift_threshold)

    def new_shift_monitor(self) -> "ShiftMonitor":
        return ShiftMonitor(self.shift_window, self.shift_threshold, self._ood_precision)

    # ---- the one-reading path the node actually runs

    def infer(self, features) -> Inference:
        start = time.perf_counter()
        z = self._standardise(features)
        grams = float(self._forward(z))
        distance = float(self._distance(z))
        deviation = tuple(float(v) for v in z - self._ood_mean)
        latency_ms = (time.perf_counter() - start) * 1000.0
        return Inference(
            grams=round(grams, 1),
            ood_score=round(distance, 3),
            out_of_distribution=distance > self.ood_threshold,
            latency_ms=round(latency_ms, 4),
            deviation=deviation,
        )


class DriftMonitor:
    """
    Rolling mean of the squared OOD distance over the last `window` readings.

    For in-distribution data the squared distance averages about the number of
    features, so a sustained rise means the inputs have moved away from the
    training data even though no single reading looks wrong. Until a full
    window has been seen there is no score and no alarm, because a mean over a
    few readings is too noisy to act on. State is in memory and starts empty
    after a restart, like the other simulators' state.
    """

    def __init__(self, window: int, threshold: float):
        self.window = window
        self.threshold = threshold
        self._squares = collections.deque(maxlen=window)
        self._total = 0.0

    def update(self, ood_score: float):
        """Feed one reading's distance; returns (score or None, drift_suspected)."""
        if len(self._squares) == self.window:
            self._total -= self._squares[0]
        square = ood_score * ood_score
        self._squares.append(square)
        self._total += square
        if len(self._squares) < self.window:
            return None, False
        score = self._total / self.window
        return round(score, 3), score > self.threshold


class ShiftMonitor:
    """
    Hotelling T-squared of the mean deviation over the last `window` readings.

    score = window * mean' P mean, where `mean` is the average of the last
    `window` per-channel deviations from the training data and P is the
    guard's precision matrix. For in-distribution data it follows a
    chi-squared distribution with one degree of freedom per channel, so it
    sits near the number of channels (4) and the calibrated threshold lands
    close to the chi-squared tail point; a sustained bias in any direction
    (a fouling lens dims light, area and height together) adds to it with
    every reading. Until a full window has been seen there is no score and
    no alarm. State is in memory and starts empty after a restart.
    """

    def __init__(self, window: int, threshold: float, precision):
        self.window = window
        self.threshold = threshold
        self._precision = np.asarray(precision, dtype=np.float64)
        self._deviations = collections.deque(maxlen=window)

    def update(self, deviation):
        """Feed one reading's per-channel deviation; returns (score or None, shift_suspected)."""
        self._deviations.append(np.asarray(deviation, dtype=np.float64))
        if len(self._deviations) < self.window:
            return None, False
        mean = np.mean(self._deviations, axis=0)
        score = round(float(self.window * (mean @ self._precision @ mean)), 3)
        return score, score > self.threshold
