"""
Checks that the offline trainer still produces a model of the quality the
committed artifact claims.

It does not compare bytes: scikit-learn's optimiser can differ in the last
bits between versions, so the committed artifact and its hash are the source
of truth. What must stay true is that retraining from scratch gives a model as
good as the one that shipped, with working guards. Run at reduced size so it
takes seconds, not minutes (the full trainer takes about a minute).

Needs scikit-learn (see requirements.txt next to this file); skipped without it.

    pip install -r edge-simulators/training/requirements.txt jsonschema pytest
    cd edge-simulators/training && python -m pytest test_training.py -v
"""
import json
import os
import sys

import numpy as np
import pytest

pytest.importorskip("sklearn")

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.dirname(HERE))

import train_plate_waste_model as trainer  # noqa: E402
from edge_ai import model as edge_model  # noqa: E402


@pytest.fixture(scope="module")
def retrained():
    # Smaller than the real run, same code path.
    saved = (trainer.TRAIN_SAMPLES, trainer.HOLDOUT_SAMPLES, trainer.DRIFT_CALIBRATION_READINGS, trainer.DRIFT_ONSET_TRIALS)
    trainer.TRAIN_SAMPLES, trainer.HOLDOUT_SAMPLES = 15_000, 8_000
    trainer.DRIFT_CALIBRATION_READINGS, trainer.DRIFT_ONSET_TRIALS = 40_000, 20
    try:
        return trainer.build()
    finally:
        trainer.TRAIN_SAMPLES, trainer.HOLDOUT_SAMPLES, trainer.DRIFT_CALIBRATION_READINGS, trainer.DRIFT_ONSET_TRIALS = saved


def test_a_retrained_model_loads_and_is_internally_consistent(retrained):
    loaded = edge_model.EdgeModel(retrained)
    assert loaded.sha256 == retrained["weights_sha256"]
    json.dumps(retrained, allow_nan=False)  # no NaN or infinity anywhere in the artifact


def test_a_retrained_model_is_as_accurate_as_the_one_that_shipped(retrained):
    shipped = json.load(open(edge_model.DEFAULT_MODEL_PATH))["card"]
    card = retrained["card"]
    assert card["holdout_rmse_g"] <= 1.3 * shipped["holdout_rmse_g"]
    assert card["holdout_rmse_g"] <= 0.5 * card["holdout_rmse_g_scale_channel_alone"]
    assert card["holdout_rmse_g"] <= 1.05 * card["holdout_rmse_g_float32_weights"]


def test_a_retrained_guard_is_calibrated_and_catches_a_fouling_lens(retrained):
    card = retrained["card"]
    assert card["holdout_ood_flag_rate"] < 0.005
    assert card["drift_monitor"]["clean_time_in_alarm"] < 0.01
    assert card["drift_response"]["0.4"]["drift_detected_fraction"] >= 0.9
    assert np.isfinite(retrained["ood"]["drift_threshold"]) and retrained["ood"]["drift_threshold"] > 0


def test_the_committed_card_matches_what_the_committed_artifact_does():
    # The card is documentation inside the artifact; this keeps it honest.
    artifact = json.load(open(edge_model.DEFAULT_MODEL_PATH))
    model = edge_model.EdgeModel(artifact)
    features, truth = trainer.draw(8_000, seed=424242)
    estimate, _ = model.predict_batch(features)
    assert abs(trainer.rmse(estimate, truth) - artifact["card"]["holdout_rmse_g"]) <= 0.15 * artifact["card"]["holdout_rmse_g"]
