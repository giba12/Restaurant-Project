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
# Removing a station from the assignable pool (see _available_stations)
# only changes which station NEW tickets land on -- on its own it has no
# effect on any ticket's own pickup_delay_ms/cook_duration_ms, since a
# ticket's odds of being picked to advance on any given tick are just
# 1/len(open_tickets), independent of which station it's at. Two other
# mechanisms were considered and rejected before this one:
#   - Weighting ticket selection by station doesn't work: once every open
#     ticket shares the same (reduced) relative weight -- which happens
#     quickly once pre-scenario tickets at the removed station drain out
#     -- the selection distribution is uniform again and the effect
#     self-cancels. It can only ever produce a brief transient, not a
#     sustained one.
#   - A real time.sleep() inside next_event() would block the single
#     simulator thread entirely, freezing every station's event
#     generation, not just the affected ones -- far too heavy-handed, and
#     not how a real short-staffed kitchen behaves (other stations keep
#     moving).
# Instead: let backlog grow. Raising the effective open-ticket cap during
# a shortage means more tickets compete for the same fixed tick budget,
# which genuinely and sustainedly lowers each one's average pick
# frequency -- exactly what "the same staff now covering a bigger queue"
# should look like, and it doesn't self-cancel because the elevated cap
# persists for the scenario's whole duration, not just until some
# transient population drains.
STAFFING_SHORTAGE_BACKLOG_MULTIPLIER = float(os.environ.get("STAFFING_SHORTAGE_BACKLOG_MULTIPLIER", "5"))

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
        MAX_OPEN_TICKETS, scaled up by STAFFING_SHORTAGE_BACKLOG_MULTIPLIER
        while a staffing_shortage scenario is active -- see the module-level
        comment on that constant for why this (backlog growth), rather than
        selection weighting or a blocking sleep, is the actual causal link
        between an injected shortage and an observable timing metric.
        """
        if self._active_staffing_shortage() is not None:
            return int(MAX_OPEN_TICKETS * STAFFING_SHORTAGE_BACKLOG_MULTIPLIER)
        return MAX_OPEN_TICKETS

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
    import ssl
    import threading
    from kafka import KafkaConsumer

    state = {"active": None}

    # Same opt-in TLS pattern as services/phase5_common.py's KAFKA_TLS_KWARGS
    # (kept local rather than imported -- this package is deliberately
    # separate from services/, see phase5_common.py's own docstring):
    # defaults to today's plaintext behavior, {}; a k8s chart switches this
    # simulator over by setting KAFKA_SECURITY_PROTOCOL=SSL and mounting
    # Strimzi's CA cert at KAFKA_SSL_CAFILE's path.
    #
    # ssl_context, not ssl_cafile: kafka-python 2.0.2's own internal
    # SSLContext construction fails the handshake against this broker
    # outright, for reasons that don't trace to the cert, hostname, or
    # network path -- confirmed live by hand-rolling the same handshake with
    # plain ssl.create_default_context(), which negotiates TLSv1.3
    # successfully. ssl_context sidesteps kafka-python's own construction
    # entirely (see services/phase5_common.py's longer note on this).
    _security_protocol = os.environ.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
    _tls_kwargs = (
        {"security_protocol": _security_protocol,
         "ssl_context": ssl.create_default_context(
             cafile=os.environ.get("KAFKA_SSL_CAFILE", "/etc/kafka-tls/ca.crt"))}
        if _security_protocol != "PLAINTEXT"
        else {}
    )

    def _run():
        consumer = KafkaConsumer(
            "scenario-control-events",
            # Dead in practice: k8s/edge-simulators's chart always sets this
            # explicitly (see services/phase5_common.py's identical note on
            # why this is :9093, not the removed plaintext :9092, since
            # 2026-09-25).
            bootstrap_servers=os.environ.get(
                "KAFKA_BOOTSTRAP_SERVERS",
                "restaurant-platform-kafka-kafka-bootstrap.kafka.svc.cluster.local:9093",
            ),
            api_version=(2, 8, 0),  # required -- automatic negotiation fails against Kafka 4.3.1
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            group_id="service-timing-scenario-control",
            **_tls_kwargs,
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