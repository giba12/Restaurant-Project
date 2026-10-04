import os
import random

from common import scenario as scenario_control
from common import staffing, world
from common.ids import new_event_id, now_iso
from common.runtime import Simulator

SCENARIO_CONTROL_ENABLED = os.environ.get("SCENARIO_CONTROL_ENABLED", "false").lower() == "true"
# While a staffing shortage is injected, at most this many staff may be
# clocked in: the simulator clocks the surplus out and stops clocking anyone in
# until the scenario ends. The kitchen then slows because of the lower staffing
# level (common/staffing.py), which is what makes the shortage a real
# intervention on the variable the causal engine analyses (DEF-141).
SHORTAGE_MAX_CLOCKED_IN = int(os.environ.get("SHORTAGE_MAX_CLOCKED_IN", "2"))

SCHEDULE_DEVIATION = ["as_scheduled", "as_scheduled", "as_scheduled", "early", "late", "unscheduled_cover"]


class ShiftState:
    """
    Tracks which staff are currently clocked in so shift_action sequences
    are plausible (no clock_out without a prior clock_in, no
    station_reassign for someone off shift). Reset on pod restart, which
    is acceptable for a Phase 3 simulator.

    During an injected staffing shortage the number clocked in is held at or
    below SHORTAGE_MAX_CLOCKED_IN (see the constant's comment); outside one,
    behaviour is the original random walk.
    """

    def __init__(self, active_scenario_getter=None):
        self.clocked_in: set[str] = set()
        self._active_scenario_getter = active_scenario_getter

    def _shortage_active(self) -> bool:
        if self._active_scenario_getter is None:
            return False
        scenario = self._active_scenario_getter()
        return bool(scenario and scenario.get("scenario_type") == "staffing_shortage")

    def next_event(self) -> dict:
        clocked_in_staff = [s for s in world.STAFF_ROSTER if s["staff_id"] in self.clocked_in]
        clocked_out_staff = [s for s in world.STAFF_ROSTER if s["staff_id"] not in self.clocked_in]
        shortage = self._shortage_active()

        if shortage and len(clocked_in_staff) > SHORTAGE_MAX_CLOCKED_IN:
            # Clock the surplus out, one per event.
            staff = random.choice(clocked_in_staff)
            action = "clock_out"
            self.clocked_in.discard(staff["staff_id"])
            station_id = None
        elif shortage and clocked_in_staff and len(clocked_in_staff) >= SHORTAGE_MAX_CLOCKED_IN:
            # At the cap: anyone still in may take a break or move, but nobody clocks in or out.
            staff = random.choice(clocked_in_staff)
            action = random.choice(["break_start", "break_end", "station_reassign"])
            station_id = random.choice(world.STATIONS) if action == "station_reassign" else None
        elif not clocked_in_staff or (clocked_out_staff and random.random() < 0.35):
            staff = random.choice(clocked_out_staff)
            action = "clock_in"
            self.clocked_in.add(staff["staff_id"])
            station_id = None
        else:
            staff = random.choice(clocked_in_staff)
            action = random.choices(
                ["clock_out", "break_start", "break_end", "station_reassign"],
                weights=[0.25, 0.20, 0.20, 0.35],
            )[0]
            if action == "clock_out":
                self.clocked_in.discard(staff["staff_id"])
            station_id = random.choice(world.STATIONS) if action == "station_reassign" else None

        return {
            "event_id": new_event_id(),
            "event_type": "StaffShiftEvent",
            "schema_version": world.SCHEMA_VERSION,
            "source_id": "sim-staffing-sensor-01",
            "source_kind": "simulated",
            "timestamp": now_iso(),
            "restaurant_id": world.RESTAURANT_ID,
            "staff_id": staff["staff_id"],
            "role": staff["role"],
            "shift_action": action,
            "station_id": station_id,
            "scheduled_vs_actual": random.choice(SCHEDULE_DEVIATION),
        }


def main():
    active_scenario_getter = (
        scenario_control.make_scenario_getter("staff-shift-scenario-control", (None, "staff-shift", "all"))
        if SCENARIO_CONTROL_ENABLED
        else None
    )
    state = ShiftState(active_scenario_getter=active_scenario_getter)
    sim = Simulator(
        sensor_type="staff-shift",
        schema_filename="StaffShiftEvent.schema.json",
        mqtt_topic="sensors/staff-shift",
    )

    def next_event():
        event = state.next_event()
        # Staffing is shared simulated-world state: this simulator is the source
        # of truth, and the timing simulator reads it (common/staffing.py).
        sim.publish_state(staffing.STAFFING_TOPIC, staffing.encode(len(state.clocked_in)))
        return event

    sim.run_forever(next_event)


if __name__ == "__main__":
    main()
