"""
Does the node's drift monitoring work on sensor drift the project did not generate?

Every other figure about the monitors (model card, DEF-155) is measured on the project's own simulation of a
fouled lens (RSK-031). This checks the shipped DriftMonitor and ShiftMonitor classes, with the same calibration
recipe (a threshold at the 99.9th percentile of clean windows), on the UCI "Gas Sensor Array Drift Dataset at
Different Concentrations" (Vergara, Rodriguez-Lujan et al.; CC BY 4.0): 13,910 measurements from 16 chemical
sensors over 36 months, in ten time-ordered batches, with real, documented drift.

What this is NOT: a validation of the plate-waste model. The data has none of its channels (a scale, a camera, a
depth estimate, a light sensor) and no leftover-food weights, and no public dataset that I know of does. It checks
the drift-detection METHOD on real drift, with these limits stated up front:
  * the sensors are chemical, the features are the 16 steady-state responses projected onto 4 principal
    components (the node has 4 channels), chosen once and not tuned;
  * readings within a batch are ordered by gas, so a sequential window would alarm on the gas changing; the
    check draws readings in random order, and resamples every batch to the first batch's gas mix (gases 1 to 5;
    gas 6 is absent from batches 3 to 5) so that a change of mix is not mistaken for drift;
  * the drift here is large (a classifier trained on batch 1 is wrong about half the time by batch 2), so it
    cannot say how mild a drift the monitors catch; the dose-response part mixes a controlled fraction of
    batch-2 readings into batch-1 data for that.

    pip install numpy scikit-learn
    python edge-simulators/validation/real_drift_check.py [--cache DIR] [--splits N] [--json out.json]

Downloads the dataset once (about 10 MB) into the cache directory and checks its SHA-256. Not part of CI: it needs
the network. The seeds are fixed, so the same data and library versions give the same numbers.
"""
import argparse
import hashlib
import json
import os
import sys
import urllib.request
import zipfile

import numpy as np
from sklearn.decomposition import PCA
from sklearn.linear_model import LogisticRegression

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from edge_ai import model as edge_model  # noqa: E402

URL = "https://archive.ics.uci.edu/static/public/270/gas+sensor+array+drift+dataset+at+different+concentrations.zip"
SHA256 = "98fe3a30981a222dd4518fbcc3dddd45d5c0ce9b03ef6dc6fe5cf7a04cfbff5e"
STEADY_STATE = list(range(0, 128, 8))  # the first of each sensor's eight features
GASES = [1, 2, 3, 4, 5]
COMPONENTS = 4
SPREAD_WINDOW, SHIFT_WINDOW = 50, 30  # the node's own windows
QUANTILE = 0.999
DOSES = (0.0, 0.05, 0.1, 0.2, 0.3, 0.5, 1.0)
CALIBRATION_READINGS = 30000  # resampled readings the thresholds are calibrated on
STREAM_READINGS = 4000  # resampled readings each batch or dose is judged on


def fetch(cache: str) -> str:
    os.makedirs(cache, exist_ok=True)
    path = os.path.join(cache, "gas_drift.zip")
    if not os.path.exists(path):
        urllib.request.urlretrieve(URL, path)
    digest = hashlib.sha256(open(path, "rb").read()).hexdigest()
    if digest != SHA256:
        raise RuntimeError(f"{path} has SHA-256 {digest}, not the pinned {SHA256}; delete it and re-run, or the dataset changed")
    zipfile.ZipFile(path).extractall(cache)
    return cache


def load_batches(folder: str) -> dict:
    batches = {}
    for n in range(1, 11):
        rows, labels = [], []
        for line in open(os.path.join(folder, f"batch{n}.dat")):
            head, *features = line.split()
            labels.append(int(head.split(";")[0]))
            rows.append([float(f.split(":")[1]) for f in features])
        X, y = np.array(rows), np.array(labels)
        keep = np.isin(y, GASES)
        batches[n] = (X[keep][:, STEADY_STATE], y[keep])
    return batches


def draw(X, y, n, seed, mix):
    """n readings in random order, with each gas in the proportion `mix` (resampled with replacement)."""
    rng = np.random.default_rng(seed)
    picks = []
    for gas, share in zip(GASES, mix):
        pool = np.where(y == gas)[0]
        if len(pool):
            picks.append(rng.choice(pool, int(round(share * n)), replace=True))
    order = np.concatenate(picks)
    rng.shuffle(order)
    return X[order], y[order]


class Guard:
    """The node's pipeline on the projected readings: standardise, deviation from the training mean, Mahalanobis."""

    def __init__(self, X_fit):
        self.mean, self.std = X_fit.mean(0), X_fit.std(0)
        self.pca = PCA(COMPONENTS).fit((X_fit - self.mean) / self.std)
        fitted = self.project(X_fit)
        self.centre = fitted.mean(0)
        self.precision = np.linalg.inv(np.cov(fitted, rowvar=False))

    def project(self, X):
        return self.pca.transform((X - self.mean) / self.std)

    def alarms(self, X):
        """Scores from the shipped monitor classes, one per reading once their windows are full."""
        spread = edge_model.DriftMonitor(SPREAD_WINDOW, 1e18)
        shift = edge_model.ShiftMonitor(SHIFT_WINDOW, 1e18, self.precision)
        spread_scores, shift_scores = [], []
        for deviation in self.project(X) - self.centre:
            distance = float(np.sqrt(max(deviation @ self.precision @ deviation, 0.0)))
            a, _ = spread.update(round(distance, 3))
            b, _ = shift.update(deviation)
            if a is not None:
                spread_scores.append(a)
            if b is not None:
                shift_scores.append(b)
        return np.array(spread_scores), np.array(shift_scores)


def run(batches, splits: int):
    mix = np.array([np.mean(batches[1][1] == g) for g in GASES])
    results = {"clean": [], "batches": {n: [] for n in range(2, 11)}, "doses": {p: [] for p in DOSES}}
    for seed in range(splits):
        X1, y1 = batches[1]
        order = np.random.default_rng(100 + seed).permutation(len(X1))
        fit, calibrate, test = np.array_split(order, [int(0.5 * len(order)), int(0.75 * len(order))])
        guard = Guard(X1[fit])
        spread_cal, shift_cal = guard.alarms(draw(X1[calibrate], y1[calibrate], CALIBRATION_READINGS, seed, mix)[0])
        thresholds = (np.quantile(spread_cal, QUANTILE), np.quantile(shift_cal, QUANTILE))
        classifier = LogisticRegression(max_iter=2000).fit(guard.project(X1[fit]), y1[fit])

        def measure(X, y):
            spread, shift = guard.alarms(X)
            return (float(np.mean(spread > thresholds[0])), float(np.mean(shift > thresholds[1])),
                    float(1.0 - classifier.score(guard.project(X), y)))

        results["clean"].append(measure(*draw(X1[test], y1[test], STREAM_READINGS, 500 + seed, mix)))
        for n in range(2, 11):
            results["batches"][n].append(measure(*draw(*batches[n], STREAM_READINGS, 900 + 10 * n + seed, mix)))
        X2, y2 = batches[2]
        clean_pool = draw(X1[test], y1[test], STREAM_READINGS, 1500 + seed, mix)
        drifted_pool = draw(X2, y2, STREAM_READINGS, 1600 + seed, mix)
        pick = np.random.default_rng(1700 + seed)
        for p in DOSES:
            from_drifted = pick.random(STREAM_READINGS) < p
            X = np.where(from_drifted[:, None], drifted_pool[0], clean_pool[0])
            y = np.where(from_drifted, drifted_pool[1], clean_pool[1])
            results["doses"][p].append(measure(X, y))
    return results


def summarise(results) -> dict:
    mean = lambda rows: [round(float(v), 4) for v in np.mean(rows, axis=0)]
    spread = lambda rows: [round(float(v), 4) for v in np.std(rows, axis=0)]
    return {
        "clean": {"spread_alarm_mean_sd": [mean(results["clean"])[0], spread(results["clean"])[0]],
                  "shift_alarm_mean_sd": [mean(results["clean"])[1], spread(results["clean"])[1]],
                  "classifier_error_mean": mean(results["clean"])[2]},
        "batches": {n: dict(zip(("spread_alarm", "shift_alarm", "classifier_error"), mean(rows))) for n, rows in results["batches"].items()},
        "doses": {p: dict(zip(("spread_alarm", "shift_alarm", "classifier_error"), mean(rows))) for p, rows in results["doses"].items()},
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cache", default=os.path.join(os.path.expanduser("~"), ".cache", "rp-gas-drift"))
    parser.add_argument("--splits", type=int, default=8)
    parser.add_argument("--json")
    args = parser.parse_args()
    summary = summarise(run(load_batches(fetch(args.cache)), args.splits))
    c = summary["clean"]
    print(f"Clean held-out batch-1 readings ({args.splits} random splits): spread monitor in alarm {c['spread_alarm_mean_sd'][0]:.2%} "
          f"(sd {c['spread_alarm_mean_sd'][1]:.2%}), shift monitor {c['shift_alarm_mean_sd'][0]:.2%} (sd {c['shift_alarm_mean_sd'][1]:.2%}); "
          f"classifier error {c['classifier_error_mean']:.1%}; calibrated for 0.1%\n")
    print("Later batches (months of real drift), time in alarm and the error of a classifier trained on batch 1:")
    for n, row in summary["batches"].items():
        print(f"  batch {n:2d}: spread {row['spread_alarm']:6.1%}  shift {row['shift_alarm']:6.1%}  classifier error {row['classifier_error']:6.1%}")
    print("\nDose-response: a fraction of batch-2 readings mixed into clean batch-1 data:")
    for p, row in summary["doses"].items():
        print(f"  {p:4.0%} drifted: spread {row['spread_alarm']:6.1%}  shift {row['shift_alarm']:6.1%}  classifier error {row['classifier_error']:6.1%}")
    if args.json:
        json.dump(summary, open(args.json, "w"), indent=2)


if __name__ == "__main__":
    main()
