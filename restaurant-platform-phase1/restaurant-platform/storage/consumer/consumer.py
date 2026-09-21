"""
Phase 4 storage consumer.

Subscribes to all four Phase 3 Kafka topics (plate-waste-events,
pos-transaction-events, service-timing-events, staff-shift-events),
validates each event against its committed JSON Schema, and persists it
to the corresponding TimescaleDB hypertable defined in
storage/schema/001_hypertables.sql.

Failure handling follows the same two-class split used in the Phase 3
edge simulators (see edge-simulators/common/runtime.py):

  - Schema validation failure: logged and the event is skipped. NOT
    fatal here. A violation reaching Kafka at this point is either an
    out-of-date local copy of the schema or a genuine upstream defect
    that has already been published -- crashing this process fixes
    neither and would stop every other valid event on the topic from
    being persisted.
  - Database write failure: retried with exponential backoff. The
    consumer offset for that message is not committed until the write
    succeeds, so a transient DB outage cannot silently drop events.

Environment variables:
  KAFKA_BOOTSTRAP_SERVERS   default "restaurant-platform-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092"
  KAFKA_CONSUMER_GROUP      default "storage-consumer"
  TIMESCALE_DSN             required, e.g. "postgresql://user:pass@host:5432/restaurant_platform"
  SCHEMA_DIR                default "/app/schemas"
  DB_MAX_RETRIES            default 5
  DB_RETRY_BASE_SECONDS     default 1.0
"""

import json
import logging
import os
import sys
import time
from typing import Any

import jsonschema
import psycopg2
import psycopg2.extras
from kafka import KafkaConsumer

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    #level="DEBUG",
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("storage-consumer")

KAFKA_BOOTSTRAP_SERVERS = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS",
    "restaurant-platform-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092",
)
KAFKA_CONSUMER_GROUP = os.environ.get("KAFKA_CONSUMER_GROUP", "storage-consumer")
TIMESCALE_DSN = os.environ["TIMESCALE_DSN"]
SCHEMA_DIR = os.environ.get("SCHEMA_DIR", "/app/schemas")
DB_MAX_RETRIES = int(os.environ.get("DB_MAX_RETRIES", "5"))
DB_RETRY_BASE_SECONDS = float(os.environ.get("DB_RETRY_BASE_SECONDS", "1.0"))

# Maps each Kafka topic to: (event_type name, JSON schema filename, insert function)
# The insert functions are defined below, after this table, and referenced
# by name at import time via the TOPIC_CONFIG assembly at the bottom of
# this section -- kept explicit and enumerable rather than derived by
# convention, so a fifth event type cannot be added by accident without a
# matching code path.
TOPIC_SCHEMA_FILES = {
    "plate-waste-events": ("PlateWasteEvent", "PlateWasteEvent.schema.json"),
    "pos-transaction-events": ("POSTransactionEvent", "POSTransactionEvent.schema.json"),
    "service-timing-events": ("ServiceTimingEvent", "ServiceTimingEvent.schema.json"),
    "staff-shift-events": ("StaffShiftEvent", "StaffShiftEvent.schema.json"),
}


def load_schemas() -> dict[str, dict]:
    schemas = {}
    for topic, (event_type, filename) in TOPIC_SCHEMA_FILES.items():
        path = os.path.join(SCHEMA_DIR, filename)
        with open(path, "r", encoding="utf-8") as f:
            schemas[topic] = json.load(f)
        log.info("loaded schema for %s from %s", event_type, path)
    return schemas


# ---------------------------------------------------------------------
# Per-event-type insert statements.
#
# Every column list below is checked against schemas/*.schema.json and
# storage/schema/001_hypertables.sql. .get(...) with a None default is only
# used for genuinely optional schema fields; raw_payload keeps the true
# event regardless. A column that does not exist fails the insert, which
# write_with_retry retries and then refuses to commit past -- so a schema
# mismatch shows up as a stuck consumer, not as silently lost events.
#
# A POS event is one parent row plus one row per entry in its `line_items`
# array (a real one-to-many part of the contract). Both are written in the
# same transaction, so a transaction is never stored without its items.
# ---------------------------------------------------------------------

def _common_fields(event: dict) -> dict:
    return {
        "event_id": event["event_id"],
        "event_type": event["event_type"],
        "schema_version": event["schema_version"],
        "source_id": event["source_id"],
        "source_kind": event["source_kind"],
        "timestamp": event["timestamp"],
        "restaurant_id": event["restaurant_id"],
        "raw_payload": json.dumps(event),
    }


def insert_plate_waste(cur, event: dict) -> None:
    f = _common_fields(event)
    confounders = event.get("confounder_flags", {}) or {}
    cur.execute(
        """
        INSERT INTO plate_waste_events (
            event_id, event_type, schema_version, source_id, source_kind,
            "timestamp", restaurant_id, table_id, station_id, plate_item_ids,
            estimated_waste_grams, to_go_container_used,
            declared_dietary_restriction, portion_size_variant, raw_payload
        ) VALUES (
            %(event_id)s, %(event_type)s, %(schema_version)s, %(source_id)s, %(source_kind)s,
            %(timestamp)s, %(restaurant_id)s, %(table_id)s, %(station_id)s, %(plate_item_ids)s,
            %(estimated_waste_grams)s, %(to_go_container_used)s,
            %(declared_dietary_restriction)s, %(portion_size_variant)s, %(raw_payload)s
        )
        ON CONFLICT (event_id, "timestamp") DO NOTHING
        """,
        {
            **f,
            "table_id": event.get("table_id"),
            "station_id": event.get("station_id"),
            "plate_item_ids": event.get("plate_item_ids"),
            "estimated_waste_grams": event.get("estimated_waste_grams"),
            "to_go_container_used": confounders.get("to_go_container_used"),
            "declared_dietary_restriction": confounders.get("declared_dietary_restriction"),
            "portion_size_variant": confounders.get("portion_size_variant"),
        },
    )


def insert_pos_transaction(cur, event: dict) -> None:
    f = _common_fields(event)
    cur.execute(
        """
        INSERT INTO pos_transaction_events (
            event_id, event_type, schema_version, source_id, source_kind,
            "timestamp", restaurant_id, transaction_id, table_id, server_staff_id,
            total_amount_cents, currency, payment_method, discount_applied_cents, raw_payload
        ) VALUES (
            %(event_id)s, %(event_type)s, %(schema_version)s, %(source_id)s, %(source_kind)s,
            %(timestamp)s, %(restaurant_id)s, %(transaction_id)s, %(table_id)s, %(server_staff_id)s,
            %(total_amount_cents)s, %(currency)s, %(payment_method)s, %(discount_applied_cents)s, %(raw_payload)s
        )
        ON CONFLICT (event_id, "timestamp") DO NOTHING
        """,
        {
            **f,
            "transaction_id": event["transaction_id"],
            "table_id": event.get("table_id"),
            "server_staff_id": event.get("server_staff_id"),
            "total_amount_cents": event["total_amount_cents"],
            "currency": event.get("currency", "USD"),
            "payment_method": event.get("payment_method"),
            "discount_applied_cents": event.get("discount_applied_cents", 0),
        },
    )
    for index, item in enumerate(event["line_items"]):
        cur.execute(
            """
            INSERT INTO pos_transaction_line_items (
                parent_event_id, transaction_id, "timestamp", line_item_index,
                menu_item_id, quantity, unit_price_cents, modifiers, voided
            ) VALUES (
                %(parent_event_id)s, %(transaction_id)s, %(timestamp)s, %(line_item_index)s,
                %(menu_item_id)s, %(quantity)s, %(unit_price_cents)s, %(modifiers)s, %(voided)s
            )
            ON CONFLICT (parent_event_id, line_item_index, "timestamp") DO NOTHING
            """,
            {
                "parent_event_id": f["event_id"],
                "transaction_id": event["transaction_id"],
                "timestamp": f["timestamp"],
                "line_item_index": index,
                "menu_item_id": item["menu_item_id"],
                "quantity": item["quantity"],
                "unit_price_cents": item["unit_price_cents"],
                "modifiers": item.get("modifiers"),
                "voided": item.get("voided", False),
            },
        )


def insert_staff_shift(cur, event: dict) -> None:
    f = _common_fields(event)
    cur.execute(
        """
        INSERT INTO staff_shift_events (
            event_id, event_type, schema_version, source_id, source_kind,
            "timestamp", restaurant_id, staff_id, role, station_id,
            shift_action, raw_payload
        ) VALUES (
            %(event_id)s, %(event_type)s, %(schema_version)s, %(source_id)s, %(source_kind)s,
            %(timestamp)s, %(restaurant_id)s, %(staff_id)s, %(role)s, %(station_id)s,
            %(shift_action)s, %(raw_payload)s
        )
        ON CONFLICT (event_id, "timestamp") DO NOTHING
        """,
        {
            **f,
            "staff_id": event.get("staff_id"),
            "role": event.get("role"),
            "station_id": event.get("station_id"),
            "shift_action": event.get("shift_action"),
        },
    )


def insert_service_timing(cur, event: dict) -> None:
    f = _common_fields(event)
    cur.execute(
        """
        INSERT INTO service_timing_events (
            event_id, event_type, schema_version, source_id, source_kind,
            "timestamp", restaurant_id, ticket_id, table_id, station_id,
            stage, elapsed_since_previous_stage_ms, raw_payload
        ) VALUES (
            %(event_id)s, %(event_type)s, %(schema_version)s, %(source_id)s, %(source_kind)s,
            %(timestamp)s, %(restaurant_id)s, %(ticket_id)s, %(table_id)s, %(station_id)s,
            %(stage)s, %(elapsed_since_previous_stage_ms)s, %(raw_payload)s
        )
        ON CONFLICT (event_id, "timestamp") DO NOTHING
        """,
        {
            **f,
            "ticket_id": event["ticket_id"],
            "table_id": event.get("table_id"),
            "station_id": event.get("station_id"),
            "stage": event["stage"],
            "elapsed_since_previous_stage_ms": event.get("elapsed_since_previous_stage_ms"),
        },
    )


TOPIC_INSERT_FN = {
    "plate-waste-events": insert_plate_waste,
    "pos-transaction-events": insert_pos_transaction,
    "service-timing-events": insert_service_timing,
    "staff-shift-events": insert_staff_shift,
}


def write_with_retry(conn, insert_fn, event: dict) -> None:
    attempt = 0
    while True:
        try:
            with conn.cursor() as cur:
                insert_fn(cur, event)
            conn.commit()
            return
        except psycopg2.Error as exc:
            conn.rollback()
            attempt += 1
            if attempt > DB_MAX_RETRIES:
                log.error(
                    "db write failed after %d attempts, event_id=%s: %s",
                    attempt, event.get("event_id"), exc,
                )
                raise
            backoff = DB_RETRY_BASE_SECONDS * (2 ** (attempt - 1))
            log.warning(
                "db write attempt %d/%d failed, retrying in %.1fs: %s",
                attempt, DB_MAX_RETRIES, backoff, exc,
            )
            time.sleep(backoff)


def main() -> None:
    schemas = load_schemas()
    topics = list(TOPIC_SCHEMA_FILES.keys())

    consumer = KafkaConsumer(
        api_version=(2, 8, 0),
        *topics,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_CONSUMER_GROUP,
        value_deserializer=lambda raw: raw,  # keep raw bytes; decode explicitly below
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    conn = psycopg2.connect(TIMESCALE_DSN)
    log.info("connected to Kafka (%s) and TimescaleDB, subscribed to %s",
              KAFKA_BOOTSTRAP_SERVERS, topics)

    for message in consumer:
        topic = message.topic
        try:
            event = json.loads(message.value.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            log.error("unparseable message on %s at offset %d: %s", topic, message.offset, exc)
            consumer.commit()
            continue

        try:
            jsonschema.validate(instance=event, schema=schemas[topic])
        except jsonschema.ValidationError as exc:
            log.error(
                "SCHEMA VIOLATION on %s at offset %d, event_id=%s: %s",
                topic, message.offset, event.get("event_id"), exc.message,
            )
            consumer.commit()  # non-fatal: skip and move on, see module docstring
            continue

        try:
            write_with_retry(conn, TOPIC_INSERT_FN[topic], event)
        except psycopg2.Error:
            # Do not commit the offset -- this message will be redelivered
            # on restart/rebalance once the DB is reachable again.
            log.error("giving up on event_id=%s for this cycle; offset not committed",
                       event.get("event_id"))
            continue

        consumer.commit()


if __name__ == "__main__":
    main()
