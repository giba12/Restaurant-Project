"""
digital-twin (Phase 6)

Maintains current restaurant state -- which tables are occupied, which
staff are on shift and where, how many open tickets each station is
carrying right now -- by consuming the raw Phase 3 event streams
directly (service-timing-events, staff-shift-events), not Phase 5's
derived tables. This is a live snapshot, not a log: each of the three
twin_*_state tables holds one current row per entity, upserted as events
arrive, mirroring storage-consumer's "one process, several topics" shape
rather than ticket-timing-aggregator's per-ticket accumulation.

This service has no API of its own. The three tables are read directly
by dashboard-api (Phase 7) and can be queried by hand.
"""
import json
import logging
import os
import sys

from kafka import KafkaConsumer

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import phase5_common as common

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("digital-twin")

SERVICE_TIMING_TOPIC = "service-timing-events"
STAFF_SHIFT_TOPIC = "staff-shift-events"


# The same statement as storage/schema/006_twin_open_tickets.sql (a test checks they match):
# run at start-up so a database created before that migration still has the table.
ENSURE_OPEN_TICKETS_SQL = """
CREATE TABLE IF NOT EXISTS twin_open_tickets (
    ticket_id   TEXT PRIMARY KEY,
    station_id  TEXT,
    opened_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_twin_open_tickets_station ON twin_open_tickets (station_id);
"""


def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute(ENSURE_OPEN_TICKETS_SQL)
    conn.commit()


def _set_station_count(cur, station_id: str):
    """Make a station's count the number of open tickets it has: derived, never incremented."""
    cur.execute(
        """
        INSERT INTO twin_station_state (station_id, open_ticket_count, updated_at)
        VALUES (%(station_id)s, (SELECT count(*) FROM twin_open_tickets WHERE station_id = %(station_id)s), now())
        ON CONFLICT (station_id) DO UPDATE SET
            open_ticket_count = EXCLUDED.open_ticket_count, updated_at = now()
        """,
        {"station_id": station_id},
    )


def handle_service_timing(conn, event: dict):
    stage = event["stage"]
    table_id = event.get("table_id")
    station_id = event.get("station_id")
    ticket_id = event["ticket_id"]

    with conn.cursor() as cur:
        if stage == "order_fired":
            if table_id:
                cur.execute(
                    """
                    INSERT INTO twin_table_state (table_id, status, occupied_since, current_ticket_id, updated_at)
                    VALUES (%(table_id)s, 'occupied', %(ts)s, %(ticket_id)s, now())
                    ON CONFLICT (table_id) DO UPDATE SET
                        status = 'occupied', occupied_since = EXCLUDED.occupied_since,
                        current_ticket_id = EXCLUDED.current_ticket_id, updated_at = now()
                    """,
                    {"table_id": table_id, "ts": event["timestamp"], "ticket_id": ticket_id},
                )
            if station_id:
                # A set, not a counter: a redelivered order_fired finds the ticket already
                # there and changes nothing, so at-least-once delivery cannot inflate the count.
                cur.execute(
                    """
                    INSERT INTO twin_open_tickets (ticket_id, station_id, opened_at)
                    VALUES (%(ticket_id)s, %(station_id)s, %(ts)s)
                    ON CONFLICT (ticket_id) DO NOTHING
                    """,
                    {"ticket_id": ticket_id, "station_id": station_id, "ts": event["timestamp"]},
                )
                _set_station_count(cur, station_id)
        elif stage == "delivered":
            if table_id:
                cur.execute(
                    """
                    INSERT INTO twin_table_state (table_id, status, occupied_since, current_ticket_id, updated_at)
                    VALUES (%(table_id)s, 'available', NULL, NULL, now())
                    ON CONFLICT (table_id) DO UPDATE SET
                        status = 'available', occupied_since = NULL,
                        current_ticket_id = NULL, updated_at = now()
                    """,
                    {"table_id": table_id},
                )
            # The station the ticket was opened at, if the twin saw it open; otherwise the event's own.
            cur.execute("DELETE FROM twin_open_tickets WHERE ticket_id = %(ticket_id)s RETURNING station_id", {"ticket_id": ticket_id})
            row = cur.fetchone()
            station_id = (row[0] if row else None) or station_id
            if station_id:
                _set_station_count(cur, station_id)
    conn.commit()


def handle_staff_shift(conn, event: dict):
    staff_id = event["staff_id"]
    role = event.get("role")
    action = event["shift_action"]
    station_id = event.get("station_id")
    ts = event["timestamp"]

    if action == "clock_in":
        status, clocked_in_since = "on_shift", ts
    elif action == "clock_out":
        status, clocked_in_since = "off_shift", None
    elif action == "break_start":
        status, clocked_in_since = "on_break", None
    elif action == "break_end":
        status, clocked_in_since = "on_shift", None
    else:  # station_reassign -- status/clocked_in_since unaffected, only station_id changes
        status, clocked_in_since = None, None

    with conn.cursor() as cur:
        if action == "station_reassign":
            cur.execute(
                """
                INSERT INTO twin_staff_state (staff_id, role, station_id, updated_at)
                VALUES (%(staff_id)s, %(role)s, %(station_id)s, now())
                ON CONFLICT (staff_id) DO UPDATE SET
                    station_id = EXCLUDED.station_id, updated_at = now()
                """,
                {"staff_id": staff_id, "role": role, "station_id": station_id},
            )
        else:
            cur.execute(
                """
                INSERT INTO twin_staff_state (staff_id, role, status, station_id, clocked_in_since, updated_at)
                VALUES (%(staff_id)s, %(role)s, %(status)s, %(station_id)s, %(clocked_in_since)s, now())
                ON CONFLICT (staff_id) DO UPDATE SET
                    role = EXCLUDED.role, status = EXCLUDED.status,
                    station_id = COALESCE(EXCLUDED.station_id, twin_staff_state.station_id),
                    clocked_in_since = EXCLUDED.clocked_in_since, updated_at = now()
                """,
                {
                    "staff_id": staff_id, "role": role, "status": status,
                    "station_id": station_id, "clocked_in_since": clocked_in_since,
                },
            )
    conn.commit()


TOPIC_HANDLER = {
    SERVICE_TIMING_TOPIC: handle_service_timing,
    STAFF_SHIFT_TOPIC: handle_staff_shift,
}


def main():
    consumer = KafkaConsumer(
        *TOPIC_HANDLER.keys(),
        bootstrap_servers=common.KAFKA_BOOTSTRAP_SERVERS,
        api_version=common.KAFKA_API_VERSION,
        **common.KAFKA_TLS_KWARGS,
        group_id="digital-twin",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    conn = common.pg_connect()
    ensure_schema(conn)

    log.info("digital-twin started, consuming %s", list(TOPIC_HANDLER.keys()))
    for msg in consumer:
        event = msg.value
        try:
            TOPIC_HANDLER[msg.topic](conn, event)
            consumer.commit()
        except Exception:
            conn.rollback()
            log.exception("Failed processing %s event; offset not committed", msg.topic)
            raise


if __name__ == "__main__":
    main()
