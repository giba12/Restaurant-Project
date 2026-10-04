"""
ticket-timing-aggregator

Consumes the raw 'service-timing-events' Kafka topic (the same topic the
Phase 4 storage-consumer reads), maintains an in-memory per-ticket_id state
machine across the five ServiceTimingEvent stages, and on every event:
  1. Upserts the ticket's row in ticket_timing_summaries (TimescaleDB).
  2. Publishes the resulting TicketTimingSummary to the
     'ticket-timing-summaries' Kafka topic, for the anomaly-detector to
     consume without also having to read raw stage events.

This is a derived-computation consumer, not an edge simulator -- it has no
MQTT dependency and does not validate against ServiceTimingEvent.schema.json
(that validation already happened at the producer, per the Phase 3 design;
re-validating identical data a second time here would be redundant, not
defensive). It DOES validate its own output against
TicketTimingSummary.schema.json before publishing, since that is this
service's own contract to the rest of Phase 5.

In-memory recovery note: on restart, this service has no memory of tickets
that were mid-sequence before the restart. Kafka consumer-group offsets are
committed only after a successful DB upsert + Kafka publish, so no event is
silently dropped on restart -- but a ticket whose order_fired event was
processed before a restart, and whose later-stage events arrive after,
would (incorrectly) be treated as starting fresh at whatever stage arrives
first post-restart. This is an accepted gap for this revision (consistent
with service_timing.py's own producer-side in-memory-state trade-off), not
a bug requiring a durable-checkpoint fix before Phase 5 is otherwise usable.
"""
import json
import logging
import os
import sys

from kafka import KafkaConsumer, KafkaProducer
import jsonschema

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import phase5_common as common

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ticket-timing-aggregator")

RAW_TOPIC = "service-timing-events"
SUMMARY_TOPIC = "ticket-timing-summaries"
SOURCE_ID = "timing-aggregator-01"

# A ticket touched by a human-driven session ('player') or its automated crew
# ('crew') is "interactive" -- from its very first event, since the crew fires
# the ticket. Downstream, interactive tickets are kept out of the baselines
# built for everything else (see anomaly-detector).
INTERACTIVE_KINDS = {"player", "crew"}


def origin_of(raw_event: dict) -> str:
    """'interactive' for player/crew events, otherwise the event's own source_kind."""
    kind = raw_event.get("source_kind", "simulated")
    return "interactive" if kind in INTERACTIVE_KINDS else kind


# stage -> (field written on entry, field name of the *previous* stage's
# timestamp used to compute the corresponding duration field)
STAGE_FIELD_MAP = {
    "order_fired": "order_time",
    "cook_started": "cook_started_time",
    "plated": "plated_time",
    "picked_up_by_server": "picked_up_time",
    "delivered": "delivered_time",
}


def _duration_ms(start_iso, end_iso):
    if not start_iso or not end_iso:
        return None
    import datetime

    try:
        start = datetime.datetime.fromisoformat(start_iso.replace("Z", "+00:00"))
        end = datetime.datetime.fromisoformat(end_iso.replace("Z", "+00:00"))
    except ValueError:
        return None
    millis = int((end - start).total_seconds() * 1000)
    if millis < 0:
        # Never a negative duration. Producers stamp events with their own wall
        # clocks, which step backwards (NTP; WSL2 resynchronising a busy host)
        # and are never perfectly synchronised across real edge devices. A
        # negative interval is impossible, so the honest value is "unknown":
        # the schema allows null here, and returning the negative number made
        # the summary fail its own schema, which this service treats as fatal,
        # so one clock step crashed the pod and dropped every ticket in flight.
        log.warning("event timestamps went backwards by %d ms (%s then %s); duration left unknown", -millis, start_iso, end_iso)
        return None
    return millis


class TicketState:
    """In-memory per-ticket accumulator, keyed by ticket_id."""

    def __init__(self):
        self.tickets: dict[str, dict] = {}

    def apply(self, raw_event: dict) -> dict:
        ticket_id = raw_event["ticket_id"]
        state = self.tickets.setdefault(
            ticket_id,
            {
                "ticket_id": ticket_id,
                "restaurant_id": raw_event.get("restaurant_id", common.RESTAURANT_ID),
                "station_id": raw_event.get("station_id"),
                "table_id": raw_event.get("table_id"),
                "origin": origin_of(raw_event),
                "order_time": None,
                "cook_started_time": None,
                "plated_time": None,
                "picked_up_time": None,
                "delivered_time": None,
            },
        )
        # Station/table can be set on any stage event; keep the latest
        # non-null value rather than only the first.
        if raw_event.get("station_id"):
            state["station_id"] = raw_event["station_id"]
        if raw_event.get("table_id"):
            state["table_id"] = raw_event["table_id"]

        # Sticky: one interactive event makes the whole ticket interactive.
        if origin_of(raw_event) == "interactive":
            state["origin"] = "interactive"

        field = STAGE_FIELD_MAP.get(raw_event["stage"])
        if field is not None:
            state[field] = raw_event["timestamp"]

        if state["order_time"] is None:
            # This ticket's order_fired event was never observed by this
            # consumer instance -- e.g. the ticket was already mid-sequence
            # when this pod (re)started, per the in-memory recovery gap
            # documented above. TicketTimingSummary.schema.json requires
            # order_time as a non-null string, so there is no valid summary
            # to emit yet; in practice this ticket simply never gets a
            # summary, which is preferable to crashing the consumer on
            # every such ticket. Still drop it from memory on delivery so
            # it doesn't accumulate forever.
            if raw_event["stage"] == "delivered":
                del self.tickets[ticket_id]
            return None

        is_complete = state["delivered_time"] is not None
        summary = {
            "summary_id": common.new_event_id(),
            "event_type": "TicketTimingSummary",
            "schema_version": common.SCHEMA_VERSION,
            "source_id": SOURCE_ID,
            "computed_at": common.now_iso(),
            "restaurant_id": state["restaurant_id"],
            "ticket_id": ticket_id,
            "station_id": state["station_id"],
            "table_id": state["table_id"],
            "origin": state["origin"],
            "order_time": state["order_time"],
            "cook_started_time": state["cook_started_time"],
            "plated_time": state["plated_time"],
            "picked_up_time": state["picked_up_time"],
            "delivered_time": state["delivered_time"],
            "time_to_cook_start_ms": _duration_ms(state["order_time"], state["cook_started_time"]),
            "cook_duration_ms": _duration_ms(state["cook_started_time"], state["plated_time"]),
            "pickup_delay_ms": _duration_ms(state["plated_time"], state["picked_up_time"]),
            "service_delay_ms": _duration_ms(state["picked_up_time"], state["delivered_time"]),
            "total_ticket_duration_ms": _duration_ms(
                state["order_time"], state["delivered_time"] or state["picked_up_time"]
            ),
            "is_complete": is_complete,
        }

        if is_complete:
            # Ticket lifecycle finished -- drop from memory. If a duplicate
            # or out-of-order event for this ticket_id arrives afterward
            # (should not happen given Kafka's per-key ordering guarantee
            # within a partition, but not impossible across a rebalance),
            # it will start a fresh (incorrect) state rather than erroring.
            # Accepted for this revision; see the recovery-gap note above.
            del self.tickets[ticket_id]

        return summary


def upsert_summary(conn, summary: dict):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ticket_timing_summaries (
                ticket_id, summary_id, event_type, schema_version, source_id,
                computed_at, restaurant_id, station_id, table_id, origin,
                order_time, cook_started_time, plated_time, picked_up_time, delivered_time,
                time_to_cook_start_ms, cook_duration_ms, pickup_delay_ms,
                service_delay_ms, total_ticket_duration_ms, is_complete, updated_at
            ) VALUES (
                %(ticket_id)s, %(summary_id)s, %(event_type)s, %(schema_version)s, %(source_id)s,
                %(computed_at)s, %(restaurant_id)s, %(station_id)s, %(table_id)s, %(origin)s,
                %(order_time)s, %(cook_started_time)s, %(plated_time)s, %(picked_up_time)s, %(delivered_time)s,
                %(time_to_cook_start_ms)s, %(cook_duration_ms)s, %(pickup_delay_ms)s,
                %(service_delay_ms)s, %(total_ticket_duration_ms)s, %(is_complete)s, now()
            )
            ON CONFLICT (ticket_id) DO UPDATE SET
                summary_id = EXCLUDED.summary_id,
                computed_at = EXCLUDED.computed_at,
                station_id = EXCLUDED.station_id,
                table_id = EXCLUDED.table_id,
                origin = EXCLUDED.origin,
                order_time = EXCLUDED.order_time,
                cook_started_time = EXCLUDED.cook_started_time,
                plated_time = EXCLUDED.plated_time,
                picked_up_time = EXCLUDED.picked_up_time,
                delivered_time = EXCLUDED.delivered_time,
                time_to_cook_start_ms = EXCLUDED.time_to_cook_start_ms,
                cook_duration_ms = EXCLUDED.cook_duration_ms,
                pickup_delay_ms = EXCLUDED.pickup_delay_ms,
                service_delay_ms = EXCLUDED.service_delay_ms,
                total_ticket_duration_ms = EXCLUDED.total_ticket_duration_ms,
                is_complete = EXCLUDED.is_complete,
                updated_at = now();
            """,
            summary,
        )
    conn.commit()


def main():
    summary_schema = common.load_schema("TicketTimingSummary.schema.json")

    consumer = KafkaConsumer(
        RAW_TOPIC,
        bootstrap_servers=common.KAFKA_BOOTSTRAP_SERVERS,
        api_version=common.KAFKA_API_VERSION,
        **common.KAFKA_TLS_KWARGS,
        group_id="ticket-timing-aggregator",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        enable_auto_commit=False,
    )
    producer = KafkaProducer(
        bootstrap_servers=common.KAFKA_BOOTSTRAP_SERVERS,
        api_version=common.KAFKA_API_VERSION,
        **common.KAFKA_TLS_KWARGS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )
    conn = common.pg_connect()
    state = TicketState()

    log.info("ticket-timing-aggregator started, consuming %s", RAW_TOPIC)
    for msg in consumer:
        raw_event = msg.value
        try:
            summary = state.apply(raw_event)
            if summary is None:
                consumer.commit()
                continue
            jsonschema.validate(instance=summary, schema=summary_schema)
            upsert_summary(conn, summary)
            producer.send(SUMMARY_TOPIC, key=summary["ticket_id"], value=summary)
            producer.flush()
            consumer.commit()
        except jsonschema.ValidationError:
            # A schema violation here is this service's own bug, not the
            # upstream producer's -- fatal, per the same fail-loud
            # convention used in edge-simulators/common/runtime.py.
            log.exception("TicketTimingSummary failed schema validation, ticket_id=%s", raw_event.get("ticket_id"))
            raise
        except Exception:
            conn.rollback()
            log.exception("Failed processing event for ticket_id=%s; offset not committed", raw_event.get("ticket_id"))
            raise


if __name__ == "__main__":
    main()
