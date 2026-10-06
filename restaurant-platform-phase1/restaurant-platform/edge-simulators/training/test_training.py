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


def test_a_retrained_model_carries_a_calibrated_shift_monitor_that_beats_the_spread_monitor(retrained):
    card = retrained["card"]
    assert card["shift_monitor"]["clean_time_in_alarm"] < 0.01
    assert np.isfinite(retrained["ood"]["shift_threshold"]) and retrained["ood"]["shift_threshold"] > 0
    mild = card["drift_response"]["0.2"]
    assert mild["shift_detected_fraction"] >= 0.9
    assert mild["shift_detected_fraction"] > mild["drift_detected_fraction"]


def test_the_committed_card_states_what_the_committed_shift_monitor_measured():
    artifact = json.load(open(edge_model.DEFAULT_MODEL_PATH))
    card = artifact["card"]
    assert card["shift_monitor"]["threshold"] == artifact["ood"]["shift_threshold"]
    assert card["shift_monitor"]["window"] == artifact["ood"]["shift_window"]
    assert card["shift_monitor"]["clean_time_in_alarm"] < 0.005
    assert card["drift_response"]["0.2"]["shift_detected_fraction"] >= 0.9
    assert card["drift_response"]["0.2"]["drift_detected_fraction"] < 0.7  # the weakness the shift monitor was added for


@pytest.fixture
def small_sizes(monkeypatch):
    monkeypatch.setattr(trainer, "DRIFT_CALIBRATION_READINGS", 40_000)
    monkeypatch.setattr(trainer, "DRIFT_ONSET_TRIALS", 20)


def legacy_artifact(tmp_path):
    """The committed artifact as it was before the shift monitor: no shift keys, version 1.0.0, its own hash."""
    artifact = json.load(open(edge_model.DEFAULT_MODEL_PATH))
    for key in ("shift_window", "shift_threshold", "flatline_window", "flatline_threshold"):
        del artifact["ood"][key]
    artifact["model_version"] = "1.0.0"
    artifact["weights_sha256"] = edge_model.behaviour_hash(artifact)
    path = tmp_path / "legacy.json"
    path.write_text(json.dumps(artifact))
    return path, artifact


def test_adding_the_shift_monitor_changes_the_monitor_and_the_hash_but_not_the_weights(tmp_path, small_sizes):
    path, legacy = legacy_artifact(tmp_path)
    upgraded = trainer.add_shift_monitor(path)
    assert upgraded["layers"] == legacy["layers"]
    assert upgraded["input_mean"] == legacy["input_mean"] and upgraded["input_std"] == legacy["input_std"]
    assert upgraded["ood"]["mean"] == legacy["ood"]["mean"] and upgraded["ood"]["precision"] == legacy["ood"]["precision"]
    assert upgraded["model_version"] == trainer.SHIFT_MONITOR_VERSION == "1.1.0"
    assert upgraded["weights_sha256"] != legacy["weights_sha256"]
    assert "flatline_window" not in upgraded["ood"], "adding the shift monitor must not add the flatline monitor"
    loaded = edge_model.EdgeModel(upgraded)  # the new hash verifies
    committed = json.load(open(edge_model.DEFAULT_MODEL_PATH))["ood"]["shift_threshold"]
    assert 0.7 * committed < loaded.shift_threshold < 1.4 * committed  # a shorter calibration stream, so only roughly equal


def test_the_upgrade_refuses_an_artifact_that_does_not_match_its_own_hash(tmp_path, small_sizes):
    path, legacy = legacy_artifact(tmp_path)
    legacy["layers"][0]["bias"][0] += 0.5
    path.write_text(json.dumps(legacy))
    with pytest.raises(edge_model.ModelIntegrityError):
        trainer.add_shift_monitor(path)


def test_a_retrained_model_carries_a_calibrated_flatline_monitor_that_catches_a_stuck_sensor(retrained):
    card = retrained["card"]
    assert card["flatline_monitor"]["clean_time_in_alarm"] < 0.01
    assert np.isfinite(retrained["ood"]["flatline_threshold"]) and 0 < retrained["ood"]["flatline_threshold"] < 1
    stuck = card["fault_response"]["stuck-light"]
    assert stuck["flatline_time_in_alarm"] > 0.95
    assert stuck["spread_time_in_alarm"] < 0.05 and stuck["shift_time_in_alarm"] < 0.05  # the gap it exists to fill


def test_the_committed_card_states_what_the_committed_flatline_monitor_measured():
    artifact = json.load(open(edge_model.DEFAULT_MODEL_PATH))
    card = artifact["card"]
    assert card["flatline_monitor"]["threshold"] == artifact["ood"]["flatline_threshold"]
    assert card["flatline_monitor"]["window"] == artifact["ood"]["flatline_window"]
    assert card["flatline_monitor"]["clean_time_in_alarm"] < 0.005
    assert card["fault_response"]["stuck-light"]["flatline_time_in_alarm"] > 0.95
    assert card["fault_response"]["gain-loss-0.7"]["flatline_time_in_alarm"] < 0.5  # the stated limit


def test_adding_the_flatline_monitor_changes_the_monitor_and_the_hash_but_not_the_weights(tmp_path, small_sizes):
    folder = os.path.join(os.path.dirname(HERE), "model_store", "plate-waste-edge-regressor")
    before = json.load(open(os.path.join(folder, "1.1.0.json")))  # has the shift monitor, not the flatline monitor
    path = tmp_path / "before.json"
    path.write_text(json.dumps(before))
    upgraded = trainer.add_flatline_monitor(path)
    assert upgraded["layers"] == before["layers"] and upgraded["ood"]["precision"] == before["ood"]["precision"]
    assert upgraded["ood"]["shift_threshold"] == before["ood"]["shift_threshold"]  # the other monitors are untouched
    assert upgraded["model_version"] == trainer.MODEL_VERSION == "1.2.0"
    assert upgraded["weights_sha256"] != before["weights_sha256"]
    loaded = edge_model.EdgeModel(upgraded)  # the new hash verifies
    committed = json.load(open(edge_model.DEFAULT_MODEL_PATH))["ood"]["flatline_threshold"]
    assert 0.8 * committed < loaded.flatline_threshold < 1.25 * committed  # a shorter calibration stream, so only roughly equal


def test_the_flatline_upgrade_refuses_an_artifact_that_does_not_match_its_own_hash(tmp_path, small_sizes):
    folder = os.path.join(os.path.dirname(HERE), "model_store", "plate-waste-edge-regressor")
    tampered = json.load(open(os.path.join(folder, "1.1.0.json")))
    tampered["layers"][0]["bias"][0] += 0.5
    path = tmp_path / "tampered.json"
    path.write_text(json.dumps(tampered))
    with pytest.raises(edge_model.ModelIntegrityError):
        trainer.add_flatline_monitor(path)
