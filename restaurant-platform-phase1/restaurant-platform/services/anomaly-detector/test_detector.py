"""
Behavioural tests for the anomaly detector's statistics: does it stay quiet on
normal data and speak up on abnormal data? No Kafka or database; the kafka and
psycopg2 imports are stubbed and only StationWindow and the severity/event
helpers are exercised. Random data is seeded, so every run is identical.

A detector that is wrong in either direction is useless in production: too
many false alarms and the findings downstream are noise; too few detections
and the platform's whole purpose fails silently.

    pip install numpy scikit-learn pytest jsonschema prometheus-client==0.26.0
    cd services/anomaly-detector && python -m pytest test_detector.py -v
"""
import json
import os
import sys
import types

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
sys.path.insert(0, HERE)
SCHEMAS = os.path.join(os.path.dirname(os.path.dirname(HERE)), "schemas")
os.environ.setdefault("SCHEMA_DIR", SCHEMAS)

sys.modules.setdefault("kafka", types.SimpleNamespace(KafkaConsumer=None, KafkaProducer=None))
sys.modules.setdefault("psycopg2", types.SimpleNamespace())

import jsonschema
import numpy as np
import pytest

import detector

METRIC = "pickup_delay_ms"


def ticket(i, **metrics):
    base = {
        "ticket_id": f"t{i}", "restaurant_id": "rest-001", "station_id": "station-grill", "table_id": "table-01",
        "order_time": "2026-10-01T12:00:00.000Z", "computed_at": "2026-10-01T12:05:00.000Z",
        "time_to_cook_start_ms": 60000, "cook_duration_ms": 300000, "pickup_delay_ms": 30000,
        "service_delay_ms": 45000, "total_ticket_duration_ms": 435000,
    }
    base.update(metrics)
    return base


def baseline_window(rng, n=200, mean=30000.0, std=3000.0):
    window = detector.StationWindow()
    for i in range(n):
        window.add(ticket(i, **{METRIC: float(rng.normal(mean, std))}))
    return window


def test_no_alarm_until_the_baseline_has_enough_history():
    window = detector.StationWindow()
    for i in range(detector.MIN_WINDOW_SIZE - 1):
        window.add(ticket(i, **{METRIC: 30000.0 + i}))
    # Even an absurd value must not alarm before there is a baseline to judge it against.
    assert window.control_limit_check(METRIC, 10_000_000.0) is None


def test_a_typical_value_is_not_flagged():
    window = baseline_window(np.random.default_rng(1))
    assert window.control_limit_check(METRIC, 30000.0) is None


@pytest.mark.parametrize("value", [30000.0 + 6 * 3000.0, 30000.0 - 6 * 3000.0])
def test_a_six_sigma_value_is_flagged_with_bounds_that_contain_the_baseline(value):
    window = baseline_window(np.random.default_rng(2))
    bounds = window.control_limit_check(METRIC, value)
    assert bounds is not None
    lower, upper = bounds
    assert lower < 30000.0 < upper
    assert not (lower <= value <= upper)


def test_false_alarm_rate_on_normal_data_stays_low():
    # 3-sigma limits should flag ~0.3% of normal points. Allow 1.5%: the limits
    # are estimated from a finite window, so they are noisy.
    rng = np.random.default_rng(3)
    window = baseline_window(rng)
    fresh = rng.normal(30000.0, 3000.0, 3000)
    false_alarms = sum(window.control_limit_check(METRIC, float(v)) is not None for v in fresh)
    assert false_alarms / len(fresh) < 0.015, f"{false_alarms}/{len(fresh)} false alarms"


def test_detection_rate_on_a_real_shift_is_high():
    # A genuine 6-sigma shift (what a staffing shortage looks like) must be
    # caught nearly every time, not just sometimes.
    rng = np.random.default_rng(4)
    window = baseline_window(rng)
    shifted = rng.normal(30000.0 + 6 * 3000.0, 3000.0, 500)
    caught = sum(window.control_limit_check(METRIC, float(v)) is not None for v in shifted)
    assert caught / len(shifted) > 0.95, f"only caught {caught}/{len(shifted)}"


def test_the_window_is_bounded_so_memory_cannot_grow_forever():
    window = detector.StationWindow()
    for i in range(detector.WINDOW_SIZE * 3):
        window.add(ticket(i))
    assert len(window.rows) == detector.WINDOW_SIZE


def test_old_history_ages_out_so_the_baseline_can_follow_a_permanent_change():
    window = detector.StationWindow()
    for i in range(detector.WINDOW_SIZE):
        window.add(ticket(i, **{METRIC: 30000.0 + (i % 7) * 100}))
    for i in range(detector.WINDOW_SIZE):  # the restaurant permanently gets slower
        window.add(ticket(1000 + i, **{METRIC: 60000.0 + (i % 7) * 100}))
    assert window.control_limit_check(METRIC, 60300.0) is None, "the new normal should no longer alarm"


def test_tickets_missing_a_metric_are_ignored_not_crashed_on():
    window = detector.StationWindow()
    for i in range(detector.MIN_WINDOW_SIZE + 5):
        window.add(ticket(i, **{METRIC: None if i % 2 else 30000.0 + i}))
    window.control_limit_check(METRIC, 30000.0)  # must not raise


def test_severity_from_deviation_scales_with_distance_beyond_the_limit():
    # Limits 0..100 (span 100). Up to 0.3 spans beyond is low, up to 1.0 is
    # medium, further is high; below the lower limit counts the same way.
    severity = detector._severity_from_deviation
    assert severity(50.0, 0.0, 100.0) == "low"
    assert severity(130.0, 0.0, 100.0) == "low"
    assert severity(131.0, 0.0, 100.0) == "medium"
    assert severity(200.0, 0.0, 100.0) == "medium"
    assert severity(201.0, 0.0, 100.0) == "high"
    assert severity(-101.0, 0.0, 100.0) == "high"


@pytest.mark.parametrize("score,expected", [(-0.4, "high"), (-0.1, "medium"), (-0.01, "low")])
def test_severity_from_isolation_forest_score(score, expected):
    assert detector._severity_from_score(score) == expected


def test_isolation_forest_flags_a_joint_outlier_and_passes_a_typical_ticket():
    rng = np.random.default_rng(5)
    window = detector.StationWindow()
    for i in range(detector.WINDOW_SIZE):
        window.add(ticket(
            i,
            time_to_cook_start_ms=float(rng.normal(60000, 5000)), cook_duration_ms=float(rng.normal(300000, 20000)),
            pickup_delay_ms=float(rng.normal(30000, 3000)), service_delay_ms=float(rng.normal(45000, 4000)),
        ))
    typical = ticket("typical", time_to_cook_start_ms=60000, cook_duration_ms=300000, pickup_delay_ms=30000, service_delay_ms=45000)
    outlier = ticket("outlier", time_to_cook_start_ms=600000, cook_duration_ms=3000000, pickup_delay_ms=300000, service_delay_ms=450000)
    assert window.isolation_forest_check(typical) is None
    score = window.isolation_forest_check(outlier)
    assert score is not None and score < 0, "an extreme joint outlier must score as anomalous (negative)"


@pytest.mark.parametrize("schema_name", ["AnomalyEvent"])
def test_built_events_satisfy_the_published_contract(schema_name):
    schema = json.load(open(os.path.join(SCHEMAS, f"{schema_name}.schema.json")))
    summary = ticket(1)
    jsonschema.validate(detector.build_control_limit_event(METRIC, summary, 99999.0, (1000.0, 2000.0)), schema)
    jsonschema.validate(detector.build_isolation_forest_event(summary, -0.2), schema)
