"""
Tests of the harness in validation/real_drift_check.py, on synthetic data, so a bug in it cannot pass for a finding
about real drift. The real check needs the network and is not run here; its results are in the model card.

The synthetic "batches" mimic the real dataset's shape: five gases of 16 features, a clean first batch, and later
batches whose readings are all moved a little further along the same direction.

    pip install numpy scikit-learn pytest
    cd edge-simulators && python -m pytest test_real_drift_harness.py -v
"""
import os
import sys

import numpy as np
import pytest

pytest.importorskip("sklearn")

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "validation"))

import real_drift_check as check  # noqa: E402


def synthetic_batches(gain_loss_per_batch: float, seed=3):
    """Five gases, 16 features. Later batches lose sensor gain, which is how chemical sensors mostly drift: the
    responses shrink along the directions that already separate the gases (so they stay inside the projection the
    harness keeps; drift in a direction the projection discards would be invisible to it, which is a real limit)."""
    rng = np.random.default_rng(seed)
    centres = rng.normal(5.0, 3.0, (len(check.GASES), 16))  # responses are positive, so losing gain moves their average
    batches = {}
    for n in range(1, 11):
        labels = np.repeat(check.GASES, 120)
        gain = max(0.05, 1.0 - (n - 1) * gain_loss_per_batch)
        batches[n] = (gain * centres[labels - 1] + rng.normal(0, 1.0, (len(labels), 16)), labels)
    return batches


@pytest.fixture(autouse=True)
def small_samples(monkeypatch):
    monkeypatch.setattr(check, "CALIBRATION_READINGS", 8000)
    monkeypatch.setattr(check, "STREAM_READINGS", 1000)


def test_draw_gives_the_requested_gas_mix_in_random_order():
    X, y = synthetic_batches(0.0)[1]
    mix = np.array([0.4, 0.3, 0.1, 0.1, 0.1])
    drawn_X, drawn_y = check.draw(X, y, 2000, seed=1, mix=mix)
    shares = [np.mean(drawn_y == g) for g in check.GASES]
    assert shares == pytest.approx(list(mix), abs=0.01)
    assert not np.all(drawn_y[:100] == drawn_y[0]), "readings came out grouped by gas, which a window would mistake for drift"
    assert drawn_X.shape == (2000, 16)


def test_with_no_drift_the_shift_monitor_keeps_close_to_its_calibrated_false_alarm_rate():
    # The harness is calibrated on one slice of batch 1 and judged on another. With clean data and a
    # reasonable calibration size the shift monitor must stay near its nominal 0.1%, whatever the real data does.
    summary = check.summarise(check.run(synthetic_batches(0.0), splits=2))
    assert summary["clean"]["shift_alarm_mean_sd"][0] < 0.03
    for n in range(2, 11):
        assert summary["batches"][n]["shift_alarm"] < 0.05, f"batch {n} has no drift but alarmed"


def test_drift_that_grows_from_batch_to_batch_raises_the_alarms_and_the_classifiers_error():
    summary = check.summarise(check.run(synthetic_batches(0.08), splits=2))
    shift = [summary["batches"][n]["shift_alarm"] for n in range(2, 11)]
    error = [summary["batches"][n]["classifier_error"] for n in range(2, 11)]
    assert shift[-1] > 0.9 and shift[-1] > shift[0], "the shift monitor missed a large drift"
    assert error[-1] > error[0] >= 0.0, "drift did not make the classifier worse: the ground truth is not measuring drift"
    # With this drift (a loss of gain, a shift of the readings' average) the shift monitor must lead the spread monitor, as it
    # does on the simulated lens: by batch 4 it is alarming most of the time while the spread monitor has barely started.
    # This also pins which column of the summary is which monitor.
    assert summary["batches"][4]["shift_alarm"] > summary["batches"][4]["spread_alarm"] + 0.3


def test_mixing_in_more_drifted_readings_raises_the_alarm_rate():
    # Batch 2, the drifted source, loses a quarter of its gain here, so a fully drifted stream must clearly alarm.
    summary = check.summarise(check.run(synthetic_batches(0.25), splits=2))
    doses = [summary["doses"][p]["shift_alarm"] for p in check.DOSES]
    assert all(later >= earlier - 0.03 for earlier, later in zip(doses, doses[1:])), f"the dose-response is not monotone: {doses}"
    assert doses[0] < 0.05 and doses[-1] > 0.5


def test_the_pinned_dataset_hash_is_a_sha256():
    assert len(check.SHA256) == 64 and set(check.SHA256) <= set("0123456789abcdef")
