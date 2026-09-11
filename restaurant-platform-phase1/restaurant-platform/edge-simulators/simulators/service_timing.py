import random
import time

from common import world
from common.ids import new_event_id, now_iso
from common.runtime import Simulator

STAGES = ["fired", "started", "plated", "expo_hold", "delivered"]


class TicketLifecycle:
    """
    Tracks open tickets in memory so consecutive events for the same
    ticket_id form a real stage sequence (fired -> ... -> delivered) with
    correctly computed elapsed_since_previous_stage_ms, instead of each
    event being independently random. This in-memory state is
    intentionally simulator-local and not persisted -- a pod restart
    starting fresh tickets is acceptable for a Phase 3 edge simulator.
    """

    def __init__(self):
        self.open_tickets: dict[str, dict] = {}

    def _new_ticket(self) -> dict:
        return {
            "table_id": random.choice(world.TABLES),
            "station_id": random.choice(
                [s for s in world.STATIONS if s not in ("station-bar", "station-bussing-01")]
            ),
            "stage_index": 0,
            "last_stage_ts": time.monotonic(),
        }

    def next_event(self) -> dict:
        # Start a new ticket if none open, or with some probability even
        # when others are open (kitchen runs multiple tickets concurrently).
        if not self.open_tickets or random.random() < 0.4:
            ticket_id = new_event_id()
            self.open_tickets[ticket_id] = self._new_ticket()
        else:
            ticket_id = random.choice(list(self.open_tickets.keys()))

        ticket = self.open_tickets[ticket_id]
        stage = STAGES[ticket["stage_index"]]

        if ticket["stage_index"] == 0:
            elapsed_ms = None
        else:
            elapsed_ms = int((time.monotonic() - ticket["last_stage_ts"]) * 1000)

        event = {
            "event_id": new_event_id(),
            "event_type": "ServiceTimingEvent",
            "schema_version": world.SCHEMA_VERSION,
            "source_id": "sim-ticket-timer-01",
            "source_kind": "simulated",
            "timestamp": now_iso(),
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
            ticket["last_stage_ts"] = time.monotonic()

        return event


def main():
    lifecycle = TicketLifecycle()
    sim = Simulator(
        sensor_type="service-timing",
        schema_filename="ServiceTimingEvent.schema.json",
        mqtt_topic="sensors/service-timing",
    )
    sim.run_forever(lifecycle.next_event)


if __name__ == "__main__":
    main()