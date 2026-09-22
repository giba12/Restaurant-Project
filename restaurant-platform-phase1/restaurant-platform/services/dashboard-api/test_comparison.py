"""
Tests for the interactive-vs-simulated comparison: the statistics (hand-computed
expected values) and the endpoint's parameter handling. No database: the two
fetch functions are replaced.

    pip install fastapi httpx pytest
    cd services/dashboard-api && python -m pytest test_comparison.py
"""
import os
import sys
import types
from datetime import datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
sys.path.insert(0, HERE)
sys.modules.setdefault("psycopg2", types.SimpleNamespace())

import pytest
from fastapi.testclient import TestClient

import comparison
import main


# ------------------------------------------------ percentile / stats

def test_percentile_interpolates_like_postgres_percentile_cont():
    assert comparison.percentile([1, 2, 3, 4], 0.5) == 2.5
    assert comparison.percentile([1, 2, 3, 4], 0.9) == pytest.approx(3.7)
    assert comparison.percentile([10, 20, 30], 0.5) == 20
    assert comparison.percentile([7], 0.9) == 7


def test_percentile_of_nothing_is_none_and_ignores_nulls():
    assert comparison.percentile([], 0.5) is None
    assert comparison.percentile([None, None], 0.5) is None
    assert comparison.percentile([None, 5, None, 15], 0.5) == 10


def test_stats_reports_n_median_and_p90_in_whole_ms():
    assert comparison.stats([1000, 2000, 3000, 4000]) == {"n": 4, "median_ms": 2500, "p90_ms": 3700}
    assert comparison.stats([]) == {"n": 0, "median_ms": None, "p90_ms": None}


# ------------------------------------------------ same clock

EVENTS = [
    ("player", "cook_started", 3000), ("player", "cook_started", 5000), ("player", "plated", 4000),
    ("crew", "picked_up_by_server", 10000), ("crew", "picked_up_by_server", 12000), ("crew", "delivered", 8000),
]


def test_same_clock_pools_and_splits_by_stage():
    r = comparison.same_clock(EVENTS)
    assert r["player"] == {"n": 3, "median_ms": 4000, "p90_ms": 4800}
    assert r["crew"] == {"n": 3, "median_ms": 10000, "p90_ms": 11600}
    assert r["player_to_crew_median_ratio"] == 0.4  # the player responded 2.5x faster
    stages = {s["stage"]: s for s in r["by_stage"]}
    assert [s["stage"] for s in r["by_stage"]] == ["cook_started", "plated", "picked_up_by_server", "delivered"]
    assert stages["cook_started"]["player"]["n"] == 2 and stages["cook_started"]["crew"]["n"] == 0
    assert stages["picked_up_by_server"]["crew"]["median_ms"] == 11000 and stages["picked_up_by_server"]["player"]["n"] == 0


def test_same_clock_ratio_is_none_without_both_sides():
    assert comparison.same_clock([("player", "plated", 4000)])["player_to_crew_median_ratio"] is None
    assert comparison.same_clock([("crew", "plated", 4000)])["player_to_crew_median_ratio"] is None
    assert comparison.same_clock([])["player"]["n"] == 0


def test_same_clock_ignores_other_kinds_and_null_elapsed():
    r = comparison.same_clock([("simulated", "plated", 1), ("player", "plated", None), ("player", "plated", 2000)])
    assert r["player"]["n"] == 1 and r["crew"]["n"] == 0


# ------------------------------------------------ vs simulated

def summary(origin, pickup, total=None):
    # (origin, time_to_cook_start, cook_duration, pickup_delay, service_delay, total)
    return (origin, None, None, pickup, None, total)


def test_vs_simulated_medians_and_the_share_of_simulated_that_was_slower():
    rows = [summary("interactive", 10000)] + [summary("simulated", v) for v in (5000, 20000, 30000, 40000)]
    r = comparison.vs_simulated(rows)
    pickup = next(m for m in r["metrics"] if m["metric"] == "pickup_delay_ms")
    assert pickup["interactive"]["median_ms"] == 10000 and pickup["simulated"]["median_ms"] == 25000
    assert pickup["interactive_median_faster_than_pct_of_simulated"] == 75.0  # 3 of 4 simulated were slower
    assert (r["interactive_tickets"], r["simulated_tickets"]) == (1, 4)


def test_vs_simulated_skips_other_origins_and_null_values():
    rows = [summary("vendor_integration", 1), summary("interactive", None), summary("simulated", 9000)]
    r = comparison.vs_simulated(rows)
    pickup = next(m for m in r["metrics"] if m["metric"] == "pickup_delay_ms")
    assert pickup["interactive"]["n"] == 0 and pickup["simulated"]["n"] == 1
    assert pickup["interactive_median_faster_than_pct_of_simulated"] is None
    assert (r["interactive_tickets"], r["simulated_tickets"]) == (1, 1)  # tickets counted, nulls just not measured


def test_empty_inputs_do_not_crash():
    out = comparison.build([], [], {"source_id": None})
    assert out["vs_simulated"]["interactive_tickets"] == 0 and out["same_clock"]["player"]["n"] == 0
    assert all(m["interactive_median_faster_than_pct_of_simulated"] is None for m in out["vs_simulated"]["metrics"])
    assert len(out["notes"]) >= 3


# ------------------------------------------------ the endpoint

@pytest.fixture()
def api(monkeypatch):
    calls = {}

    def events(since, source_id):
        calls["events"] = (since, source_id)
        return EVENTS

    def summaries(since, hours):
        calls["summaries"] = (since, hours)
        return [summary("interactive", 10000), summary("simulated", 30000)]

    monkeypatch.setattr(main, "_fetch_event_rows", events)
    monkeypatch.setattr(main, "_fetch_summary_rows", summaries)
    c = TestClient(main.app)
    c.calls = calls
    return c


def test_endpoint_defaults_to_the_last_24_hours_and_all_players(api):
    r = api.get("/api/comparison")
    assert r.status_code == 200
    body = r.json()
    assert body["scope"]["source_id"] is None and body["scope"]["reference_hours"] == 24
    since, source_id = api.calls["events"]
    assert source_id is None
    assert abs((datetime.now(timezone.utc) - since) - timedelta(hours=24)) < timedelta(seconds=5)
    assert body["same_clock"]["player_to_crew_median_ratio"] == 0.4
    assert body["vs_simulated"]["metrics"][2]["metric"] == "pickup_delay_ms"


def test_endpoint_passes_source_id_since_and_hours_through(api):
    r = api.get("/api/comparison", params={"source_id": "session-ana", "since": "2026-09-21T12:00:00.500Z", "hours": 6})
    assert r.status_code == 200
    since, source_id = api.calls["events"]
    assert source_id == "session-ana" and since == datetime(2026, 9, 21, 12, 0, 0, 500000, tzinfo=timezone.utc)
    assert api.calls["summaries"] == (since, 6)
    assert r.json()["scope"]["since"].startswith("2026-09-21T12:00:00.500")


def test_a_since_without_a_timezone_is_read_as_utc(api):
    api.get("/api/comparison", params={"since": "2026-09-21T12:00:00"})
    assert api.calls["events"][0].tzinfo == timezone.utc


@pytest.mark.parametrize("params", [{"source_id": "Bad Id!"}, {"hours": 0}, {"hours": 100000}, {"since": "yesterday"}])
def test_bad_parameters_are_rejected(api, params):
    assert api.get("/api/comparison", params=params).status_code == 422
