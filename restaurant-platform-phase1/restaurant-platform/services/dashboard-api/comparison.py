"""
Pure statistics for GET /api/comparison: interactive-session timings against
the simulated restaurant, and a player against the crew. No database here --
main.py fetches raw rows and this module turns them into numbers, so it can be
tested exactly.

Two comparisons, both from data the pipeline already stores:

  same_clock   A player's response time against the crew's, from the same
               time window. "Response time" is elapsed_since_previous_stage_ms:
               how long a ticket waited between the previous stage finishing and
               this stage being done. Both are measured the same way on the same
               clock, so the ratio is fair even though the two usually perform
               DIFFERENT stages (a cook does cook/plate, the crew pickup/deliver);
               where both did the same stage, by_stage shows it side by side.

  vs_simulated Interactive tickets against the simulated restaurant's, per
               timing metric, from ticket_timing_summaries.origin. Medians and
               90th percentiles, because the simulated timings have a heavy tail
               (a few stalled tickets) that makes means misleading.

Percentiles use linear interpolation between ranks, the same as Postgres'
percentile_cont, so they can be checked against SQL.
"""
from typing import Iterable, Optional

STAGES_AFTER_ORDER = ["cook_started", "plated", "picked_up_by_server", "delivered"]
SUMMARY_METRICS = [
    "time_to_cook_start_ms",
    "cook_duration_ms",
    "pickup_delay_ms",
    "service_delay_ms",
    "total_ticket_duration_ms",
]

NOTES = [
    "Response time is the wait between the previous stage finishing and this one being done. "
    "Player and crew are measured identically over the same window, but usually do different stages.",
    "Crew response times are largely set by the interactive session's crew-delay settings, so 'you vs the crew' "
    "measures you against that setting.",
    "Simulated timings have a heavy tail (a few stalled tickets), so medians and 90th percentiles "
    "are shown, not means.",
]


def percentile(values: Iterable[float], p: float) -> Optional[float]:
    """p in [0, 1], linear interpolation; None for no data."""
    xs = sorted(v for v in values if v is not None)
    if not xs:
        return None
    if len(xs) == 1:
        return float(xs[0])
    rank = p * (len(xs) - 1)
    lo = int(rank)
    hi = min(lo + 1, len(xs) - 1)
    return xs[lo] + (xs[hi] - xs[lo]) * (rank - lo)


def stats(values: Iterable[float]) -> dict:
    xs = [v for v in values if v is not None]
    med, p90 = percentile(xs, 0.5), percentile(xs, 0.9)
    return {
        "n": len(xs),
        "median_ms": None if med is None else round(med),
        "p90_ms": None if p90 is None else round(p90),
    }


def same_clock(event_rows: Iterable[tuple]) -> dict:
    """event_rows: (source_kind, stage, elapsed_since_previous_stage_ms), source_kind 'player' or 'crew'."""
    by_kind: dict[str, list] = {"player": [], "crew": []}
    by_stage: dict[tuple, list] = {}
    for kind, stage, elapsed in event_rows:
        if kind not in by_kind or elapsed is None:
            continue
        by_kind[kind].append(elapsed)
        by_stage.setdefault((stage, kind), []).append(elapsed)

    player, crew = stats(by_kind["player"]), stats(by_kind["crew"])
    ratio = None
    if player["median_ms"] is not None and crew["median_ms"]:
        ratio = round(player["median_ms"] / crew["median_ms"], 2)
    return {
        "player": player,
        "crew": crew,
        # < 1 means the player responded faster than the crew, > 1 slower
        "player_to_crew_median_ratio": ratio,
        "by_stage": [
            {
                "stage": stage,
                "player": stats(by_stage.get((stage, "player"), [])),
                "crew": stats(by_stage.get((stage, "crew"), [])),
            }
            for stage in STAGES_AFTER_ORDER
        ],
    }


def vs_simulated(summary_rows: Iterable[tuple]) -> dict:
    """summary_rows: (origin, *values in SUMMARY_METRICS order); only 'interactive' and 'simulated' are used."""
    values = {"interactive": {m: [] for m in SUMMARY_METRICS}, "simulated": {m: [] for m in SUMMARY_METRICS}}
    tickets = {"interactive": 0, "simulated": 0}
    for row in summary_rows:
        origin = row[0]
        if origin not in values:
            continue
        tickets[origin] += 1
        for metric, v in zip(SUMMARY_METRICS, row[1:]):
            if v is not None:
                values[origin][metric].append(v)

    metrics = []
    for m in SUMMARY_METRICS:
        inter, sim = stats(values["interactive"][m]), stats(values["simulated"][m])
        beats = None
        if inter["median_ms"] is not None and sim["n"]:
            slower = sum(1 for v in values["simulated"][m] if v > inter["median_ms"])
            beats = round(100.0 * slower / sim["n"], 1)
        metrics.append({
            "metric": m,
            "interactive": inter,
            "simulated": sim,
            # share of simulated tickets that were SLOWER than the interactive median
            "interactive_median_faster_than_pct_of_simulated": beats,
        })
    return {"interactive_tickets": tickets["interactive"], "simulated_tickets": tickets["simulated"], "metrics": metrics}


def build(event_rows: Iterable[tuple], summary_rows: Iterable[tuple], scope: dict) -> dict:
    return {
        "scope": scope,
        "same_clock": same_clock(event_rows),
        "vs_simulated": vs_simulated(summary_rows),
        "notes": NOTES,
    }
