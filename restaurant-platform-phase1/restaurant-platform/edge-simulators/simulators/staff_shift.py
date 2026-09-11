import random

from common import world
from common.ids import new_event_id, now_iso
from common.runtime import Simulator

SCHEDULE_DEVIATION = ["as_scheduled", "as_scheduled", "as_scheduled", "early", "late", "unscheduled_cover"]


class ShiftState:
    """
    Tracks which staff are currently clocked in so shift_action sequences
    are plausible (no clock_out without a prior clock_in, no
    station_reassign for someone off shift). Reset on pod restart, which
    is acceptable for a Phase 3 simulator.
    """

    def __init__(self):
        self.clocked_in: set[str] = set()

    def next_event(self) -> dict:
        clocked_in_staff = [s for s in world.STAFF_ROSTER if s["staff_id"] in self.clocked_in]
        clocked_out_staff = [s for s in world.STAFF_ROSTER if s["staff_id"] not in self.clocked_in]

        if not clocked_in_staff or (clocked_out_staff and random.random() < 0.35):
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
    state = ShiftState()
    sim = Simulator(
        sensor_type="staff-shift",
        schema_filename="StaffShiftEvent.schema.json",
        mqtt_topic="sensors/staff-shift",
    )
    sim.run_forever(state.next_event)


if __name__ == "__main__":
    main()
