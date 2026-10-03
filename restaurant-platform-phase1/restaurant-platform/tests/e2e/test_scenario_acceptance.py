"""
Acceptance test: the project's stated done condition, automated.

    "An injected scenario produces a correctly attributed finding."

A staffing shortage is injected at a known moment at a known station, then the
platform is checked against what it should have done: slow down the stations
that absorb the load, flag those slowdowns as anomalies (and not at the
station that was removed), and produce a causal finding that carries the
scenario's id so it can be traced back to its cause.

This took a manual 10-minute run on Kubernetes to verify originally. Run here
at the test stack's faster pacing it takes about 8 minutes, hence `slow`.

    bash tests/run_stack_tests.sh acceptance
"""
import os
import re
import time

import pytest

from helpers import compose, sql, sql_int, wait_for

pytestmark = pytest.mark.slow

REMOVED = "station-grill"
BASELINE_SECONDS = int(os.environ.get("ACCEPT_BASELINE_SECONDS", "150"))
SCENARIO_SECONDS = int(os.environ.get("ACCEPT_SCENARIO_SECONDS", "240"))


def mean_pickup_delay_ms(start, end):
    value = sql(f"""SELECT coalesce(avg(pickup_delay_ms), 0)::bigint FROM ticket_timing_summaries
                    WHERE is_complete AND origin = 'simulated' AND computed_at >= '{start}' AND computed_at < '{end}'
                      AND station_id <> '{REMOVED}'""")
    return int(value or 0)


@pytest.fixture(scope="module")
def scenario():
    # Let a baseline build before disturbing anything.
    wait_for(lambda: sql_int("SELECT count(*) FROM ticket_timing_summaries WHERE is_complete AND origin = 'simulated'") >= 40,
             400, description="a baseline of completed tickets")
    time.sleep(BASELINE_SECONDS)
    started = sql("SELECT to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')")
    out = compose("exec", "-T", "scenario-injection-controller", "python", "controller.py", "run",
                  "--scenario-type", "staffing_shortage", "--target", "service-timing",
                  "--duration-seconds", str(SCENARIO_SECONDS), "--stations-removed", REMOVED,
                  timeout=SCENARIO_SECONDS + 120).stdout
    ended = sql("SELECT to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')")
    scenario_id = re.search(r"scenario_injection_id=([0-9a-f-]{36})", out).group(1)
    time.sleep(45)  # let the last in-flight tickets finish and be summarised
    return {"id": scenario_id, "start": started, "end": ended}


def test_the_shortage_measurably_slows_the_stations_that_absorb_it(scenario):
    baseline_start = sql(f"SELECT to_char(timestamptz '{scenario['start']}' - interval '{BASELINE_SECONDS + 60} seconds', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')")
    before = mean_pickup_delay_ms(baseline_start, scenario["start"])
    during = mean_pickup_delay_ms(scenario["start"], scenario["end"])
    print(f"\nmean pickup delay: baseline {before} ms -> during shortage {during} ms ({during / max(before, 1):.1f}x)")
    assert before > 0, "no baseline to compare against"
    assert during > 1.5 * before, f"expected a clear slowdown, got {before} ms -> {during} ms"


def test_anomalies_are_flagged_at_the_loaded_stations_and_not_at_the_removed_one(scenario):
    window = f"detected_at >= '{scenario['start']}' AND detected_at < timestamptz '{scenario['end']}' + interval '60 seconds'"
    loaded = sql_int(f"SELECT count(*) FROM anomaly_events WHERE {window} AND station_id <> '{REMOVED}'")
    at_removed = sql_int(f"SELECT count(*) FROM anomaly_events WHERE {window} AND station_id = '{REMOVED}'")
    print(f"\nanomalies during the scenario: {loaded} at loaded stations, {at_removed} at the removed station")
    assert loaded >= 3, "the platform did not notice the slowdown"
    assert at_removed <= max(1, loaded // 10), "anomalies are not localised: the removed station is being flagged as much as the loaded ones"


def test_a_finding_computed_for_the_scenario_carries_its_id(scenario):
    out = compose(
        "exec", "-T", "causal-engine", "python", "causal_engine.py",
        "--scenario-injection-id", scenario["id"], "--metric-name", "pickup_delay_ms",
        "--window-start", scenario["start"], "--window-end", scenario["end"], "--restaurant-id", "rest-001",
        timeout=300, check=False,
    )
    assert out.returncode == 0, out.stdout + out.stderr
    # Show the engine's own verdict line (effect size, whether the refutation gate passed, and the
    # p-values behind that decision), so a run leaves the evidence and not only pass or fail.
    print("\n" + "\n".join(line for line in (out.stdout + out.stderr).splitlines() if "Emitted CausalFinding" in line))
    row = sql(f"SELECT scenario_injection_id, refutation_passed, effect_estimate_unit FROM causal_findings "
              f"WHERE scenario_injection_id = '{scenario['id']}'")
    assert row, "no finding was stored for the injected scenario"
    scenario_id, refuted, unit = row.split("|")
    assert scenario_id == scenario["id"]
    assert unit == "milliseconds"

    # The review gate, observed live: the running finding-reviewer promotes a
    # finding to narrative-ready if and only if its refutation test passed.
    ready = lambda: sql(f"SELECT narrative_ready FROM causal_findings WHERE scenario_injection_id = '{scenario['id']}'")  # noqa: E731
    if refuted == "t":
        wait_for(lambda: ready() == "t", 120, interval=3, description="the reviewer to promote a refutation-passed finding")
    else:
        time.sleep(15)
        assert ready() == "f", "a finding that failed refutation was promoted to narrative-ready"
