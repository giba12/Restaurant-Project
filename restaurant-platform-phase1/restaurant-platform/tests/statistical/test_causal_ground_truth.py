"""
Statistical validation of the causal engine against datasets whose true answer
is known.

Every other test in this project checks that code runs and data flows. This
one checks the thing the whole platform exists for: that the numbers it
reports are *right*. Synthetic data is generated with a planted causal effect
and a confounder that, if ignored, would give the wrong answer; the engine's
real `_run_dowhy` is then asked to recover the planted effect.

Needs DoWhy (Python 3.11 only), so it runs inside the project's own
causal-engine image via run_statistical_tests.sh rather than on a developer's
host Python.

    bash tests/statistical/run_statistical_tests.sh
"""
import os
import sys
import types

import numpy as np
import pandas as pd
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "services"))
sys.path.insert(0, os.path.join(ROOT, "services", "causal-engine"))
os.environ.setdefault("SCHEMA_DIR", os.path.join(ROOT, "schemas"))
if "kafka" not in sys.modules:
    try:
        import kafka  # noqa: F401
    except ImportError:
        sys.modules["kafka"] = types.SimpleNamespace(KafkaConsumer=None, KafkaProducer=None)

pytest.importorskip("dowhy", reason="DoWhy needs Python 3.11; run via run_statistical_tests.sh")
import causal_engine  # noqa: E402

TRUE_WASTE_EFFECT_GRAMS = -90.0
TRUE_STAFFING_EFFECT_MS = -4000.0


# --------------------------------------------------------------- synthetic worlds

def plate_waste_world(n, effect, seed):
    """
    Taking a to-go container reduces waste by `effect` grams. But big portions
    both leave more food AND make a to-go container more likely, so comparing
    to-go plates with the rest, naively, makes to-go look much less helpful
    than it is. Only controlling for portion size recovers the truth.
    """
    rng = np.random.default_rng(seed)
    portion = rng.choice(["small", "regular", "large"], size=n, p=[0.3, 0.4, 0.3])
    dietary = rng.random(n) < 0.15
    p_to_go = pd.Series(portion).map({"small": 0.1, "regular": 0.35, "large": 0.75}).to_numpy()
    to_go = rng.random(n) < p_to_go
    base = pd.Series(portion).map({"small": 40.0, "regular": 90.0, "large": 170.0}).to_numpy()
    waste = base + effect * to_go + np.where(dietary, -10.0, 0.0) + rng.normal(0, 20, n)
    return pd.DataFrame({
        "to_go_container_used": to_go, "estimated_waste_grams": waste,
        "portion_size_variant": portion, "declared_dietary_restriction": dietary,
    })


def staffing_world(n, effect, seed):
    """
    More staff on shift shortens pickup delay by `effect` ms per person. But
    the grill is both understaffed and slower by nature, so a naive comparison
    mixes the station's own slowness into the staffing effect.
    """
    rng = np.random.default_rng(seed)
    station = rng.choice(["station-grill", "station-fry", "station-salad"], size=n)
    staffing = np.where(station == "station-grill", rng.integers(1, 4, n), rng.integers(3, 7, n)).astype(float)
    station_base = pd.Series(station).map({"station-grill": 60000.0, "station-fry": 25000.0, "station-salad": 15000.0}).to_numpy()
    delay = station_base + effect * staffing + rng.normal(0, 3000, n)
    return pd.DataFrame({"pickup_delay_ms": delay, "station_id": station, "staffing_level": staffing})


def run_waste(df):
    spec = causal_engine.TREATMENT_MAP["estimated_waste_grams"]
    return causal_engine._run_dowhy(df, spec["treatment"], spec["outcome"], spec["confounders"])


def run_staffing(df):
    spec = causal_engine.TREATMENT_MAP["pickup_delay_ms"]
    return causal_engine._run_dowhy(df, spec["treatment"], spec["outcome"], spec["confounders"])


# --------------------------------------------------------------- recovering a known effect

def test_the_engine_recovers_a_planted_plate_waste_effect():
    result = run_waste(plate_waste_world(3000, TRUE_WASTE_EFFECT_GRAMS, seed=11))
    assert result["effect_estimate"] == pytest.approx(TRUE_WASTE_EFFECT_GRAMS, abs=10.0)


def test_the_engine_recovers_a_planted_staffing_effect():
    result = run_staffing(staffing_world(3000, TRUE_STAFFING_EFFECT_MS, seed=12))
    assert result["effect_estimate"] == pytest.approx(TRUE_STAFFING_EFFECT_MS, abs=500.0)


def test_the_planted_effect_is_actually_hidden_by_confounding():
    # Guards the test above against being vacuous: if a naive comparison also
    # got the right answer, "the engine recovered the effect" would prove
    # nothing about confounder control.
    df = plate_waste_world(3000, TRUE_WASTE_EFFECT_GRAMS, seed=11)
    naive = df[df.to_go_container_used].estimated_waste_grams.mean() - df[~df.to_go_container_used].estimated_waste_grams.mean()
    assert abs(naive - TRUE_WASTE_EFFECT_GRAMS) > 25.0, f"naive estimate {naive:.1f} is too close to the truth for this test to mean anything"
    adjusted = run_waste(df)["effect_estimate"]
    assert abs(adjusted - TRUE_WASTE_EFFECT_GRAMS) < abs(naive - TRUE_WASTE_EFFECT_GRAMS) / 2


def test_estimates_are_stable_across_independent_samples():
    # One lucky seed proves little. Five independent datasets must all land near the truth.
    estimates = [run_waste(plate_waste_world(2000, TRUE_WASTE_EFFECT_GRAMS, seed=s))["effect_estimate"] for s in range(20, 25)]
    assert all(abs(e - TRUE_WASTE_EFFECT_GRAMS) < 15.0 for e in estimates), estimates


def test_a_strong_effect_passes_its_refutation_test_and_the_flag_is_a_real_bool():
    result = run_waste(plate_waste_world(3000, TRUE_WASTE_EFFECT_GRAMS, seed=11))
    # bool, not numpy.bool_: jsonschema rejects the latter (problem log item 48).
    assert type(result["refutation_passed"]) is bool
    assert result["refutation_passed"] is True


# --------------------------------------------------------------- no effect, no finding

def test_when_there_is_no_effect_the_engine_estimates_roughly_zero():
    result = run_waste(plate_waste_world(3000, 0.0, seed=13))
    assert abs(result["effect_estimate"]) < 10.0


@pytest.mark.slow
def test_the_refutation_gate_rejects_findings_that_are_pure_noise():
    """
    The property the gate exists for: when there is NO true effect, almost no
    finding may be allowed through to narration. Each run is a different
    dataset with a true effect of exactly zero.

    History: this was a strict expected-failure (DEF-106). The old rule passed
    78% to 87% of noise datasets in three measurements (47/60 with a seeded
    permutation placebo; 26/30 and 23/30 with the engine's original unseeded
    default placebo): DoWhy's placebo `new_effect` is the mean of
    100 simulated runs, so it is ~10x quieter than a single estimate and almost
    any noise estimate beat `0.25 * |estimate|`. The gate now requires the
    effect's own regression p-value to be below REFUTATION_ALPHA (0.01), and
    measured 0 of 60 on noise. The bound below (10%) is deliberately generous:
    the expected rate is about 1%, and the datasets are seeded so the result is
    the same on every run.
    """
    runs = 30
    passed = sum(bool(run_waste(plate_waste_world(1500, 0.0, seed=100 + s))["refutation_passed"]) for s in range(runs))
    rate = passed / runs
    print(f"\nrefutation gate passed {passed}/{runs} = {rate:.0%} of pure-noise findings")
    assert rate < 0.10, f"{rate:.0%} of pure-noise findings pass the gate and would be narrated"


@pytest.mark.slow
def test_the_refutation_gate_still_passes_genuine_effects():
    # The other half: a gate that rejected everything would also "reject noise".
    # A modest real effect (-20 g on 1,500 rows) must pass essentially always.
    runs = 10
    passed = sum(bool(run_waste(plate_waste_world(1500, -20.0, seed=300 + s))["refutation_passed"]) for s in range(runs))
    print(f"\nrefutation gate passed {passed}/{runs} genuine -20 g effects")
    assert passed / runs >= 0.9, f"only {passed}/{runs} genuine effects passed the gate"


def test_the_engines_own_info_logs_survive_importing_dowhy():
    # `import dowhy` resets the root logger to WARNING. In production the engine imports it
    # lazily, on its first estimate, AFTER it has configured logging, so before this was pinned
    # its INFO lines -- including the one carrying each finding's refutation p-values --
    # disappeared after the first finding (DEF-132). A fresh interpreter reproduces that real
    # order; inside pytest, DoWhy is already imported before the engine and would hide the bug.
    import subprocess

    probe = (
        "import logging, sys\n"
        f"sys.path[:0] = [{os.path.join(ROOT, 'services')!r}, {os.path.join(ROOT, 'services', 'causal-engine')!r}]\n"
        "import causal_engine\n"
        "assert causal_engine.log.isEnabledFor(logging.INFO), 'INFO disabled before dowhy'\n"
        "import dowhy\n"
        "print('INFO_ENABLED_AFTER_DOWHY=', causal_engine.log.isEnabledFor(logging.INFO))\n"
    )
    result = subprocess.run([sys.executable, "-c", probe], capture_output=True, text=True, env={**os.environ, "SCHEMA_DIR": os.environ["SCHEMA_DIR"]})
    assert result.returncode == 0, result.stderr
    assert "INFO_ENABLED_AFTER_DOWHY= True" in result.stdout, "the engine's INFO logs are silenced once DoWhy is imported"


def test_the_refutation_verdict_is_identical_for_identical_data():
    # Unseeded, the placebo permutations differed on every call, so a finding's
    # verdict (and the measurements about the gate) varied from run to run.
    df = plate_waste_world(1500, 0.0, seed=7)
    first, second = run_waste(df.copy()), run_waste(df.copy())
    for key in ("refutation_passed", "effect_p_value", "placebo_p_value", "effect_estimate"):
        assert first[key] == second[key], f"{key} differs between identical runs: {first[key]!r} vs {second[key]!r}"
    assert isinstance(first["effect_p_value"], float) and isinstance(first["placebo_p_value"], float)


def test_the_gate_decision_follows_the_two_published_conditions():
    # The verdict must equal (effect p < alpha) AND (placebo p >= alpha), so the
    # p-values logged with every finding are enough to audit any decision.
    for effect, seed in ((0.0, 21), (-90.0, 22)):
        result = run_waste(plate_waste_world(1500, effect, seed=seed))
        expected = result["effect_p_value"] < causal_engine.REFUTATION_ALPHA and result["placebo_p_value"] >= causal_engine.REFUTATION_ALPHA
        assert result["refutation_passed"] is expected


# --------------------------------------------------------------- robustness

def test_too_few_rows_is_refused_rather_than_estimated():
    # run_from_anomaly_stream catches this ValueError and skips the anomaly.
    with pytest.raises(ValueError):
        run_waste(plate_waste_world(15, TRUE_WASTE_EFFECT_GRAMS, seed=1))


def test_rows_with_missing_values_are_dropped_not_fatal():
    df = plate_waste_world(2000, TRUE_WASTE_EFFECT_GRAMS, seed=14)
    df.loc[df.sample(frac=0.1, random_state=1).index, "portion_size_variant"] = None
    assert run_waste(df)["effect_estimate"] == pytest.approx(TRUE_WASTE_EFFECT_GRAMS, abs=15.0)


def test_estimates_are_reproducible_for_identical_input():
    df = plate_waste_world(2000, TRUE_WASTE_EFFECT_GRAMS, seed=15)
    assert run_waste(df.copy())["effect_estimate"] == pytest.approx(run_waste(df.copy())["effect_estimate"], abs=1e-6)
