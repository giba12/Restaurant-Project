"""
Integration tests: the storage consumer's insert logic against a real
TimescaleDB with the real migrations applied.

These exist because this exact seam has failed silently more than once: the
consumer read a field under the wrong key and wrote NULL into every row, then
wrote to columns that did not exist (problem log item 46 and the Phase 7
portability fixes). Unit tests with a fake cursor cannot see that; only a real
database can say "that column is NULL" or "that column does not exist".
"""
import json
import types

import psycopg2
import pytest

import consumer
import factory
from conftest import rows, scalar

SOURCE_KINDS_BY_TABLE = {
    "plate_waste_events": ("simulated", "vendor_integration", "player"),
    "pos_transaction_events": ("simulated", "vendor_integration", "player"),
    "staff_shift_events": ("simulated", "vendor_integration", "player"),
    "service_timing_events": ("simulated", "vendor_integration", "player", "crew"),
}


def store(conn, topic, event):
    consumer.write_with_retry(conn, consumer.TOPIC_INSERT_FN[topic], event)


# --------------------------------------------------------------- data lands in the right columns

def test_plate_waste_confounders_are_stored_not_silently_null(conn):
    # The regression for the NULL-confounders bug: the causal engine's whole
    # input is these three columns.
    event = factory.plate_waste_event(**{
        "confounder_flags.to_go_container_used": True,
        "confounder_flags.declared_dietary_restriction": True,
        "confounder_flags.portion_size_variant": "large",
    })
    store(conn, "plate-waste-events", event)
    to_go, dietary, portion, grams = rows(
        conn, "SELECT to_go_container_used, declared_dietary_restriction, portion_size_variant, estimated_waste_grams FROM plate_waste_events"
    )[0]
    assert (to_go, dietary, portion) == (True, True, "large")
    assert float(grams) == pytest.approx(event["estimated_waste_grams"])


def test_the_edge_inference_block_is_stored_whole_and_queryable(conn):
    # No migration was added for this feature on purpose: raw_payload (JSONB)
    # already keeps the whole event. That only holds if the block survives
    # storage intact and can be queried by path, which the causal-engine filter
    # and the dashboard's edge view both rely on.
    event = factory.plate_waste_event()
    store(conn, "plate-waste-events", event)
    stored, version = rows(conn, "SELECT raw_payload -> 'edge_inference', schema_version FROM plate_waste_events")[0]
    assert stored == event["edge_inference"]
    assert version == "1.1.0"
    sha = scalar(conn, "SELECT raw_payload #>> '{edge_inference,model_sha256}' FROM plate_waste_events")
    assert sha == event["edge_inference"]["model_sha256"]


def test_every_simulator_event_type_is_stored_with_no_unexpected_nulls(conn):
    events = factory.simulator_events_of_every_kind()
    store(conn, "plate-waste-events", events["plate_waste"])
    store(conn, "pos-transaction-events", events["pos_transaction"])
    store(conn, "service-timing-events", events["service_timing"])
    store(conn, "staff-shift-events", events["staff_shift"])

    required = {
        "plate_waste_events": ["event_id", "station_id", "estimated_waste_grams", "portion_size_variant", "to_go_container_used", "declared_dietary_restriction", "raw_payload"],
        "pos_transaction_events": ["event_id", "transaction_id", "total_amount_cents", "currency", "raw_payload"],
        "service_timing_events": ["event_id", "ticket_id", "stage", "raw_payload"],
        "staff_shift_events": ["event_id", "staff_id", "role", "shift_action", "raw_payload"],
    }
    for table, columns in required.items():
        row = dict(zip(columns, rows(conn, f"SELECT {', '.join(columns)} FROM {table}")[0]))
        nulls = [c for c, v in row.items() if v is None]
        assert not nulls, f"{table}: columns unexpectedly NULL: {nulls}"


@pytest.mark.parametrize("topic,make", [
    ("plate-waste-events", factory.plate_waste_event),
    ("pos-transaction-events", factory.pos_transaction_event),
    ("service-timing-events", factory.service_timing_event),
    ("staff-shift-events", factory.staff_shift_event),
])
def test_the_original_event_is_preserved_losslessly_in_raw_payload(conn, topic, make):
    # raw_payload is the safety net: whatever the typed columns miss, the
    # truth is still recoverable (that is how the NULL-confounder rows were backfilled).
    event = make()
    store(conn, topic, event)
    table = {"plate-waste-events": "plate_waste_events", "pos-transaction-events": "pos_transaction_events",
             "service-timing-events": "service_timing_events", "staff-shift-events": "staff_shift_events"}[topic]
    stored = scalar(conn, f"SELECT raw_payload FROM {table}")
    stored = stored if isinstance(stored, dict) else json.loads(stored)
    assert stored == event


# --------------------------------------------------------------- idempotency (at-least-once delivery)

@pytest.mark.parametrize("topic,table,make", [
    ("plate-waste-events", "plate_waste_events", factory.plate_waste_event),
    ("pos-transaction-events", "pos_transaction_events", factory.pos_transaction_event),
    ("service-timing-events", "service_timing_events", factory.service_timing_event),
    ("staff-shift-events", "staff_shift_events", factory.staff_shift_event),
])
def test_redelivering_the_same_event_does_not_create_a_duplicate_row(conn, topic, table, make):
    # Kafka delivers at least once: after a crash or rebalance the same message
    # comes again. Storing it twice would silently skew every analysis.
    event = make()
    for _ in range(3):
        store(conn, topic, event)
    assert scalar(conn, f"SELECT count(*) FROM {table}") == 1


def test_pos_transaction_stores_one_row_per_line_item_and_redelivery_adds_none(conn):
    event = factory.pos_transaction_event()
    expected = len(event["line_items"])
    assert expected >= 1
    store(conn, "pos-transaction-events", event)
    store(conn, "pos-transaction-events", event)
    assert scalar(conn, "SELECT count(*) FROM pos_transaction_line_items") == expected
    indexes = [r[0] for r in rows(conn, "SELECT line_item_index FROM pos_transaction_line_items ORDER BY line_item_index")]
    assert indexes == list(range(expected))


# --------------------------------------------------------------- failure behaviour

def test_a_transaction_is_never_stored_without_its_line_items(conn, monkeypatch):
    # Parent row and line items are written in one transaction. If a line item
    # fails, the parent must roll back too -- no orphaned half-transaction.
    monkeypatch.setattr(consumer, "DB_MAX_RETRIES", 0)
    event = factory.pos_transaction_event()
    event["line_items"][-1]["quantity"] = "not-a-number"  # fails inside the database
    with pytest.raises(psycopg2.Error):
        store(conn, "pos-transaction-events", event)
    assert scalar(conn, "SELECT count(*) FROM pos_transaction_events") == 0
    assert scalar(conn, "SELECT count(*) FROM pos_transaction_line_items") == 0


def test_a_transient_database_failure_is_retried_and_the_event_is_not_lost(conn, monkeypatch):
    monkeypatch.setattr(consumer, "DB_RETRY_BASE_SECONDS", 0.01)
    event = factory.staff_shift_event()
    attempts = {"n": 0}

    def flaky_insert(cur, ev):
        attempts["n"] += 1
        if attempts["n"] <= 2:
            raise psycopg2.OperationalError("simulated connection blip")
        consumer.insert_staff_shift(cur, ev)

    consumer.write_with_retry(conn, flaky_insert, event)
    assert attempts["n"] == 3
    assert scalar(conn, "SELECT count(*) FROM staff_shift_events") == 1


def test_a_permanent_failure_is_raised_not_swallowed(conn, monkeypatch):
    # The caller relies on this to leave the Kafka offset uncommitted so the
    # message is redelivered rather than dropped.
    monkeypatch.setattr(consumer, "DB_MAX_RETRIES", 2)
    monkeypatch.setattr(consumer, "DB_RETRY_BASE_SECONDS", 0.01)

    def always_fails(cur, ev):
        raise psycopg2.OperationalError("database is down")

    with pytest.raises(psycopg2.OperationalError):
        consumer.write_with_retry(conn, always_fails, factory.staff_shift_event())


def test_the_consumer_reconnects_after_the_database_drops_its_connection(conn, dsn, monkeypatch):
    # The consumer opens one long-lived connection and keeps it for days. When
    # the database restarts (maintenance, a failover) that connection is dead
    # for good, and a consumer that cannot replace it sits "running" but stores
    # nothing, so no restart policy ever fires. Found by the resilience layer:
    # after a database outage the consumer logged "giving up ... offset not
    # committed" for every message, forever. The server-side kill below is
    # what a database restart does to a client's connection.
    monkeypatch.setattr(consumer, "DB_RETRY_BASE_SECONDS", 0.01)
    consumers_conn = psycopg2.connect(dsn)
    consumers_conn = consumer.write_with_retry(consumers_conn, consumer.insert_staff_shift, factory.staff_shift_event())

    pid = scalar(consumers_conn, "SELECT pg_backend_pid()")
    scalar(conn, "SELECT pg_terminate_backend(%s)", (pid,))

    consumers_conn = consumer.write_with_retry(consumers_conn, consumer.insert_staff_shift, factory.staff_shift_event())
    assert scalar(conn, "SELECT count(*) FROM staff_shift_events") == 2, "the consumer did not recover after its connection was dropped"
    consumers_conn.close()


# --------------------------------------------------------------- the message loop never skips

class PositionedConsumer:
    """
    A fake Kafka consumer that models what actually matters: a *position*.
    `commit()` records everything fetched so far (as real Kafka does), and
    `seek()` moves the position back. A test drives it like the real loop does:
    fetch the message at the position, advance, hand it to the handler.
    """

    def __init__(self, messages):
        self.messages, self.position, self.committed, self.seeks = messages, 0, 0, []

    def commit(self):
        self.committed = self.position

    def seek(self, partition, offset):
        self.seeks.append((partition.topic, offset))
        self.position = offset

    def drive(self, conn, schemas, max_steps=500):
        steps = 0
        while self.position < len(self.messages):
            message = self.messages[self.position]
            self.position += 1
            conn = consumer.handle_message(self, conn, schemas, message)
            steps += 1
            assert steps <= max_steps, "the consumer is looping without making progress"
        return conn


def kafka_message(offset, event, topic="staff-shift-events"):
    value = event if isinstance(event, bytes) else json.dumps(event).encode()
    return types.SimpleNamespace(topic=topic, partition=0, offset=offset, value=value)


@pytest.fixture()
def schemas():
    return consumer.load_schemas()


# --------------------------------------------------------------- schema validation is compiled once

def test_validation_costs_microseconds_a_message_not_tens_of_milliseconds(schemas):
    # `jsonschema.validate` re-checks the whole schema on every call (measured
    # 30 to 60 ms), which capped ingest throughput; the consumer now compiles
    # each schema once. 300 validations took about 20 ms compiled and would take
    # 9 s or more the old way, so a one-second bound is wide on a loaded
    # machine and still fails the old behaviour decisively.
    import time

    event = factory.plate_waste_event()
    consumer._validate_event("plate-waste-events", event, schemas["plate-waste-events"])  # compile once
    started = time.perf_counter()
    for _ in range(300):
        consumer._validate_event("plate-waste-events", event, schemas["plate-waste-events"])
    assert time.perf_counter() - started < 1.0


def test_the_validator_is_built_once_per_schema_and_rebuilt_when_the_schema_object_changes(schemas):
    import copy

    consumer._validators.clear()
    event = factory.plate_waste_event()
    schema = schemas["plate-waste-events"]
    consumer._validate_event("plate-waste-events", event, schema)
    first = consumer._validators["plate-waste-events"][1]
    consumer._validate_event("plate-waste-events", event, schema)
    assert consumer._validators["plate-waste-events"][1] is first, "rebuilt although the schema object had not changed"

    stricter = copy.deepcopy(schema)
    stricter["properties"]["estimated_waste_grams"]["maximum"] = -1
    with pytest.raises(jsonschema_error()):
        consumer._validate_event("plate-waste-events", event, stricter)  # a different object: must not reuse the stale validator
    assert consumer._validators["plate-waste-events"][0] is stricter


def test_validation_still_rejects_what_the_schema_rejects_and_names_the_problem(schemas):
    event = factory.plate_waste_event()
    event["estimated_waste_grams"] = -5
    with pytest.raises(jsonschema_error()) as excinfo:
        consumer._validate_event("plate-waste-events", event, schemas["plate-waste-events"])
    assert "-5" in excinfo.value.message
    event = factory.plate_waste_event()
    event["edge_inference"]["model_sha256"] = "not-a-hash"
    with pytest.raises(jsonschema_error()):
        consumer._validate_event("plate-waste-events", event, schemas["plate-waste-events"])


def jsonschema_error():
    import jsonschema

    return jsonschema.ValidationError


def test_a_stored_message_is_committed(conn, schemas):
    fake = PositionedConsumer([kafka_message(0, factory.staff_shift_event())])
    fake.drive(conn, schemas)
    assert fake.committed == 1
    assert scalar(conn, "SELECT count(*) FROM staff_shift_events") == 1


def test_a_message_that_cannot_be_stored_is_rewound_to_not_skipped(conn, schemas, monkeypatch):
    # The database is "down" (every insert fails) and the retry budget is zero,
    # so the consumer gives up on the message at once.
    monkeypatch.setattr(consumer, "DB_MAX_RETRIES", 0)
    monkeypatch.setitem(consumer.TOPIC_INSERT_FN, "staff-shift-events",
                        lambda cur, ev: (_ for _ in ()).throw(psycopg2.OperationalError("database is down")))
    fake = PositionedConsumer([kafka_message(7, factory.staff_shift_event())])
    fake.position = 8  # as after fetching offset 7
    consumer.handle_message(fake, conn, schemas, fake.messages[0])
    assert fake.seeks == [("staff-shift-events", 7)], "must rewind to the failed message so it is retried"
    assert fake.committed == 0, "must not commit past a message that was not stored"


def test_a_later_message_can_never_commit_over_one_that_failed_to_store(conn, schemas, monkeypatch):
    # THE regression test. The old loop `continue`d past a message it gave up
    # on; the next message to succeed then committed the consumer's position,
    # which was already past it -- the failed message was lost for good. Here
    # the database fails the first few attempts, then recovers; every message
    # must end up stored, exactly once, in order.
    monkeypatch.setattr(consumer, "DB_MAX_RETRIES", 0)
    failures_left = {"n": 3}
    real_insert = consumer.TOPIC_INSERT_FN["staff-shift-events"]

    def flaky_insert(cur, event):
        if failures_left["n"] > 0:
            failures_left["n"] -= 1
            raise psycopg2.OperationalError("database is down")
        real_insert(cur, event)

    monkeypatch.setitem(consumer.TOPIC_INSERT_FN, "staff-shift-events", flaky_insert)

    events = [factory.staff_shift_event(staff_id=f"staff-{i:03d}") for i in range(6)]
    fake = PositionedConsumer([kafka_message(i, e) for i, e in enumerate(events)])
    fake.drive(conn, schemas)

    stored = [r[0] for r in rows(conn, "SELECT event_id FROM staff_shift_events ORDER BY \"timestamp\"")]
    assert stored == [e["event_id"] for e in events], "a message was lost or reordered"
    assert fake.committed == len(events)


def test_unparseable_and_schema_violating_messages_are_deliberately_skipped_and_committed(conn, schemas):
    # The opposite policy, on purpose: a message that can never be stored (it is
    # not JSON, or breaks the contract) must not block every event behind it.
    bad_contract = factory.staff_shift_event()
    del bad_contract["staff_id"]
    fake = PositionedConsumer([kafka_message(0, b"\xff not json"), kafka_message(1, bad_contract), kafka_message(2, factory.staff_shift_event())])
    fake.drive(conn, schemas)
    assert fake.committed == 3
    assert scalar(conn, "SELECT count(*) FROM staff_shift_events") == 1


# --------------------------------------------------------------- the schema/database contract

@pytest.mark.parametrize("table,kinds", sorted(SOURCE_KINDS_BY_TABLE.items()))
def test_database_accepts_every_source_kind_the_schema_allows_and_rejects_others(conn, monkeypatch, table, kinds):
    # The database CHECK constraints and the JSON Schema enums are maintained
    # by hand in two places. If they drift, valid events start failing to
    # store (or invalid ones start getting in).
    make = {
        "plate_waste_events": ("plate-waste-events", factory.plate_waste_event),
        "pos_transaction_events": ("pos-transaction-events", factory.pos_transaction_event),
        "staff_shift_events": ("staff-shift-events", factory.staff_shift_event),
        "service_timing_events": ("service-timing-events", factory.service_timing_event),
    }[table]
    topic, build = make
    for kind in kinds:
        store(conn, topic, build(source_kind=kind))
    assert scalar(conn, f"SELECT count(*) FROM {table}") == len(kinds)

    monkeypatch.setattr(consumer, "DB_MAX_RETRIES", 0)
    with pytest.raises(psycopg2.Error):
        store(conn, topic, build(source_kind="not-a-real-kind"))


def test_the_database_source_kinds_match_the_json_schemas_exactly(conn):
    import re

    schema_dir = consumer.SCHEMA_DIR
    names = {"plate_waste_events": "PlateWasteEvent", "pos_transaction_events": "POSTransactionEvent",
             "staff_shift_events": "StaffShiftEvent", "service_timing_events": "ServiceTimingEvent"}
    for table, schema_name in names.items():
        schema_kinds = set(json.load(open(f"{schema_dir}/{schema_name}.schema.json"))["properties"]["source_kind"]["enum"])
        definition = scalar(conn, "SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = %s", (f"{table}_source_kind_check",))
        db_kinds = set(re.findall(r"'([a-z_]+)'", definition))
        assert db_kinds == schema_kinds, f"{table}: database allows {db_kinds}, schema allows {schema_kinds}"
