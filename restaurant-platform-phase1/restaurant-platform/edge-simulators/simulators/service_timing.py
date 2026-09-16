import os
import random
import time

from common import world
from common.ids import new_event_id, now_iso
from common.runtime import Simulator

STAGES = ["order_fired", "cook_started", "plated", "picked_up_by_server", "delivered"]

MAX_OPEN_TICKETS = int(os.environ.get("MAX_OPEN_TICKETS", "8"))
MAX_STAGE_AGE_SECONDS = int(os.environ.get("MAX_STAGE_AGE_SECONDS", "300"))
SCENARIO_CONTROL_ENABLED = os.environ.get("SCENARIO_CONTROL_ENABLED", "false").lower() == "true"

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

    def __init__(self, active_scenario_getter=None):
        self.open_tickets: dict[str, dict] = {}
        # Callable returning the currently active scenario dict (or None),
        # injected by main() when SCENARIO_CONTROL_ENABLED is true. Kept as
        # an optional callback rather than a hard dependency so this class
        # is unchanged/testable when scenario injection is disabled.
        self._active_scenario_getter = active_scenario_getter

    def _available_stations(self) -> list[str]:
        base = [s for s in world.STATIONS if s not in ("station-bar", "station-bussing-01")]
        if self._active_scenario_getter is not None:
            scenario = self._active_scenario_getter()
            if scenario and scenario.get("scenario_type") == "staffing_shortage":
                removed = set(scenario.get("parameters", {}).get("stations_removed", []))
                reduced = [s for s in base if s not in removed]
                # Never reduce to an empty pool -- that would stall the
                # simulator rather than merely slow it down, which is not
                # the intended perturbation.
                if reduced:
                    return reduced
        return base

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
        at_capacity = len(self.open_tickets) >= MAX_OPEN_TICKETS
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


def _make_scenario_getter():
    """
    Only constructed when SCENARIO_CONTROL_ENABLED is true. Runs a
    background thread consuming 'scenario-control-events' and exposes the
    single most recent still-active scenario via a plain function, so
    TicketLifecycle does not need any Kafka-client knowledge itself.
    Import is deferred into this function so kafka-python is not a hard
    dependency of this module when scenario control is disabled.
    """
    import json
    import threading
    from kafka import KafkaConsumer

    state = {"active": None}

    def _run():
        consumer = KafkaConsumer(
            "scenario-control-events",
            bootstrap_servers=os.environ.get(
                "KAFKA_BOOTSTRAP_SERVERS",
                "restaurant-platform-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092",
            ),
            api_version=(2, 8, 0),  # required -- automatic negotiation fails against Kafka 4.3.1
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            group_id="service-timing-scenario-control",
        )
        for msg in consumer:
            control = msg.value
            if control.get("target") not in (None, "service-timing", "all"):
                continue
            if control.get("action") == "start":
                state["active"] = control
            elif control.get("action") == "end":
                if state["active"] and state["active"].get("scenario_injection_id") == control.get(
                    "scenario_injection_id"
                ):
                    state["active"] = None

    threading.Thread(target=_run, daemon=True).start()
    return lambda: state["active"]


def main():
    active_scenario_getter = _make_scenario_getter() if SCENARIO_CONTROL_ENABLED else None
    lifecycle = TicketLifecycle(active_scenario_getter=active_scenario_getter)
    sim = Simulator(
        sensor_type="service-timing",
        schema_filename="ServiceTimingEvent.schema.json",
        mqtt_topic="sensors/service-timing",
    )
    sim.run_forever(lifecycle.next_event)


if __name__ == "__main__":
    main()