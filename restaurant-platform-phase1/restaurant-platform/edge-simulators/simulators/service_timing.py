import os
import random
import time
from datetime import datetime, timedelta, timezone

from common import scenario as scenario_control
from common import staffing, world
from common.ids import new_event_id
from common.runtime import Simulator

STAGES = ["order_fired", "cook_started", "plated", "picked_up_by_server", "delivered"]

MAX_OPEN_TICKETS = int(os.environ.get("MAX_OPEN_TICKETS", "8"))
MAX_STAGE_AGE_SECONDS = int(os.environ.get("MAX_STAGE_AGE_SECONDS", "300"))
SCENARIO_CONTROL_ENABLED = os.environ.get("SCENARIO_CONTROL_ENABLED", "false").lower() == "true"
# Removing a station from the assignable pool (see _available_stations) only
# changes which station NEW tickets land on -- on its own it has no effect on
# any ticket's own pickup_delay_ms/cook_duration_ms, since a ticket's odds of
# being picked to advance on any given tick are just 1/len(open_tickets),
# independent of which station it's at. Two mechanisms were considered and
# rejected before the one used here:
#   - Weighting ticket selection by station doesn't work: once every open
#     ticket shares the same (reduced) relative weight the selection
#     distribution is uniform again and the effect self-cancels.
#   - A real time.sleep() inside next_event() would block the single
#     simulator thread, freezing every station's event generation, not just
#     the affected ones.
# What does work is letting the backlog grow: more open tickets competing for
# the same fixed tick budget genuinely and sustainedly lowers each one's
# average pick frequency. How large the backlog may grow is set by how many
# people are working: capacity is inversely proportional to the staffing level
# (common/staffing.py), so a staffing shortage -- staff clocked out by the
# staff-shift simulator -- slows the kitchen *because staffing fell*, and the
# causal engine's `staffing_level` treatment is the true cause in this data.
# (Until 2026-10-04 a direct x5 multiplier on this cap, applied while a
# scenario was active, did the slowing without touching staffing at all, so the
# analysed variable and the injected cause were unrelated; DEF-141.)
MIN_OPEN_TICKETS = 2


def _wall_now() -> datetime:
    return datetime.now(timezone.utc)


class TicketLifecycle:
    """
    Tracks open tickets in memory so consecutive events for the same
    ticket_id form a real stage sequence (fired -> ... -> delivered) with
    correctly computed elapsed_since_previous_stage_ms, instead of each
    event being independently random. This in-memory state is
    intentionally simulator-local and not persisted

    Bounded by MAX_OPEN_TICKETS (backlog size) and MAX_STAGE_AGE_SECONDS
    (per-stage staleness), so behavior stays realistic under indefinite
    continuous runtime rather than only over a short-lived test run.
    """

    def __init__(self, active_scenario_getter=None, staffing_getter=None):
        self.open_tickets: dict[str, dict] = {}
        # Callable returning the current staffing level (staff clocked in) or
        # None while it is not yet known; None means the nominal backlog.
        self._staffing_getter = staffing_getter
        # Callable returning the currently active scenario dict (or None),
        # injected by main() when SCENARIO_CONTROL_ENABLED is true. Kept as
        # an optional callback rather than a hard dependency so this class
        # is unchanged/testable when scenario injection is disabled.
        self._active_scenario_getter = active_scenario_getter

    def _active_staffing_shortage(self) -> dict | None:
        if self._active_scenario_getter is None:
            return None
        scenario = self._active_scenario_getter()
        if scenario and scenario.get("scenario_type") == "staffing_shortage":
            return scenario
        return None

    def _available_stations(self) -> list[str]:
        base = [s for s in world.STATIONS if s not in ("station-bar", "station-bussing-01")]
        scenario = self._active_staffing_shortage()
        if scenario is not None:
            removed = set(scenario.get("parameters", {}).get("stations_removed", []))
            reduced = [s for s in base if s not in removed]
            # Never reduce to an empty pool -- that would stall the
            # simulator rather than merely slow it down, which is not
            # the intended perturbation.
            if reduced:
                return reduced
        return base

    def _effective_max_open_tickets(self) -> int:
        """
        MAX_OPEN_TICKETS scaled by the staffing level: capacity is inversely
        proportional to the number of people working the kitchen (see
        common/staffing.py and the module-level comment above).
        """
        level = self._staffing_getter() if self._staffing_getter is not None else None
        return max(MIN_OPEN_TICKETS, int(round(MAX_OPEN_TICKETS * staffing.backlog_factor(level))))

    def _new_ticket(self) -> dict:
        return {
            "table_id": random.choice(world.TABLES),
            "station_id": random.choice(self._available_stations()),
            "stage_index": 0,
            "last_stage_ts": time.monotonic(),
        }

    def _stalest_ticket_id(self) -> str | None:
        """
        Returns the ticket_id that has exceeded MAX_STAGE_AGE_SECONDS at its
        current stage, if any (oldest first). None if no ticket is stale.
        """
        now = time.monotonic()
        stale = [
            (tid, t) for tid, t in self.open_tickets.items()
            if now - t["last_stage_ts"] > MAX_STAGE_AGE_SECONDS
        ]
        if not stale:
            return None
        stale.sort(key=lambda item: item[1]["last_stage_ts"])
        return stale[0][0]

    def next_event(self) -> dict:
        at_capacity = len(self.open_tickets) >= self._effective_max_open_tickets()
        forced_ticket_id = self._stalest_ticket_id()

        if forced_ticket_id is not None:
            # A stale ticket always takes priority over both starting a new
            # ticket and the normal random advance -- this is what actually
            # bounds elapsed_since_previous_stage_ms, rather than leaving it
            # to chance whether a stale ticket ever gets picked again.
            ticket_id = forced_ticket_id
        elif not self.open_tickets or (not at_capacity and random.random() < 0.4):
            ticket_id = new_event_id()
            self.open_tickets[ticket_id] = self._new_ticket()
        else:
            # Uniform selection among whatever's currently open. During an
            # active staffing_shortage this pool is much larger (see
            # _effective_max_open_tickets), so any given ticket's odds of
            # being the one picked -- and therefore its real wall-clock
            # wait until its next stage event -- are correspondingly lower.
            ticket_id = random.choice(list(self.open_tickets.keys()))

        ticket = self.open_tickets[ticket_id]
        stage = STAGES[ticket["stage_index"]]

        # A ticket's events are stamped from ONE wall-clock reading (its first), plus the monotonic time that has passed since,
        # never from a fresh wall-clock reading per event. Stamping each event from the wall clock while measuring elapsed time on
        # the monotonic clock let a step of the wall clock (WSL2's time resync did 1.2 to 1.5 s backwards in a test run) leave
        # every later event of an open ticket stamped earlier than its own elapsed time implied, which the aggregator rightly
        # refuses to turn into a duration (DEF-168). The stamps now always agree with `elapsed_since_previous_stage_ms`.
        now_mono = time.monotonic()
        if ticket["stage_index"] == 0:
            elapsed_ms = None
            stamp = _wall_now()
        else:
            elapsed = now_mono - ticket["last_stage_ts"]
            elapsed_ms = int(elapsed * 1000)
            stamp = ticket["last_stage_wall"] + timedelta(milliseconds=elapsed_ms)

        event = {
            "event_id": new_event_id(),
            "event_type": "ServiceTimingEvent",
            "schema_version": world.SCHEMA_VERSION,
            "source_id": "sim-ticket-timer-01",
            "source_kind": "simulated",
            "timestamp": stamp.isoformat(),
            "restaurant_id": world.RESTAURANT_ID,
            "ticket_id": ticket_id,
            "table_id": ticket["table_id"],
            "station_id": ticket["station_id"],
            "stage": stage,
            "elapsed_since_previous_stage_ms": elapsed_ms,
        }

        if stage == "delivered":
            del self.open_tickets[ticket_id]
        else:
            ticket["stage_index"] += 1
            ticket["last_stage_ts"] = now_mono
            ticket["last_stage_wall"] = stamp

        return event


def main():
    active_scenario_getter = (
        scenario_control.make_scenario_getter("service-timing-scenario-control", (None, "service-timing", "all"))
        if SCENARIO_CONTROL_ENABLED
        else None
    )
    level = staffing.StaffingLevel()
    lifecycle = TicketLifecycle(active_scenario_getter=active_scenario_getter, staffing_getter=level)
    sim = Simulator(
        sensor_type="service-timing",
        schema_filename="ServiceTimingEvent.schema.json",
        mqtt_topic="sensors/service-timing",
    )
    # Staffing is shared simulated-world state, published (retained) by the
    # staff-shift simulator; this one only listens.
    sim.subscribe(staffing.STAFFING_TOPIC, level.on_message)
    sim.run_forever(lifecycle.next_event)


if __name__ == "__main__":
    main()
