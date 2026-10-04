"""
Integration tests: the digital twin's state machine against a real database.

The twin is the one component that answers "what is happening in the
restaurant right now", so its job is to mirror reality from a stream of
events: tables fill and empty, staff clock in and out, stations carry work.
These tests replay realistic event sequences and check the resulting state.
"""
import factory
import twin
from conftest import rows, scalar


def table_state(conn, table_id):
    r = rows(conn, "SELECT status, current_ticket_id, occupied_since FROM twin_table_state WHERE table_id = %s", (table_id,))
    return r[0] if r else None


def station_count(conn, station_id):
    return scalar(conn, "SELECT open_ticket_count FROM twin_station_state WHERE station_id = %s", (station_id,))


def staff_state(conn, staff_id):
    r = rows(conn, "SELECT status, station_id, clocked_in_since, role FROM twin_staff_state WHERE staff_id = %s", (staff_id,))
    return r[0] if r else None


# ------------------------------------------------------------------ tables and stations

def test_firing_an_order_occupies_the_table_and_loads_the_station(conn):
    event = factory.service_timing_event("order_fired", ticket_id="t1", table_id="table-07", station_id="station-grill")
    twin.handle_service_timing(conn, event)
    status, ticket, since = table_state(conn, "table-07")
    assert (status, ticket) == ("occupied", "t1")
    assert since is not None
    assert station_count(conn, "station-grill") == 1


def test_delivering_the_order_frees_the_table_and_unloads_the_station(conn):
    twin.handle_service_timing(conn, factory.service_timing_event("order_fired", ticket_id="t1", table_id="table-07", station_id="station-grill"))
    twin.handle_service_timing(conn, factory.service_timing_event("delivered", ticket_id="t1", table_id="table-07", station_id="station-grill"))
    assert table_state(conn, "table-07")[:2] == ("available", None)
    assert table_state(conn, "table-07")[2] is None
    assert station_count(conn, "station-grill") == 0


def test_intermediate_stages_do_not_change_table_or_station_state(conn):
    twin.handle_service_timing(conn, factory.service_timing_event("order_fired", ticket_id="t1", table_id="table-07", station_id="station-grill"))
    for stage in ("cook_started", "plated", "picked_up_by_server"):
        twin.handle_service_timing(conn, factory.service_timing_event(stage, ticket_id="t1", table_id="table-07", station_id="station-grill"))
    assert table_state(conn, "table-07")[0] == "occupied"
    assert station_count(conn, "station-grill") == 1


def test_a_busy_station_counts_every_open_ticket(conn):
    for i in range(4):
        twin.handle_service_timing(conn, factory.service_timing_event("order_fired", ticket_id=f"t{i}", table_id=f"table-{i}", station_id="station-grill"))
    assert station_count(conn, "station-grill") == 4
    twin.handle_service_timing(conn, factory.service_timing_event("delivered", ticket_id="t0", table_id="table-0", station_id="station-grill"))
    assert station_count(conn, "station-grill") == 3


def test_the_open_ticket_count_can_never_go_negative(conn):
    # After a restart the twin can see a `delivered` for a ticket whose
    # `order_fired` it never saw. A negative workload would be nonsense.
    twin.handle_service_timing(conn, factory.service_timing_event("delivered", ticket_id="ghost", table_id="table-1", station_id="station-fry"))
    twin.handle_service_timing(conn, factory.service_timing_event("delivered", ticket_id="ghost2", table_id="table-2", station_id="station-fry"))
    assert station_count(conn, "station-fry") == 0


def test_stations_are_tracked_independently(conn):
    twin.handle_service_timing(conn, factory.service_timing_event("order_fired", ticket_id="a", table_id="table-1", station_id="station-grill"))
    twin.handle_service_timing(conn, factory.service_timing_event("order_fired", ticket_id="b", table_id="table-2", station_id="station-fry"))
    twin.handle_service_timing(conn, factory.service_timing_event("delivered", ticket_id="a", table_id="table-1", station_id="station-grill"))
    assert station_count(conn, "station-grill") == 0
    assert station_count(conn, "station-fry") == 1


def test_a_redelivered_order_fired_does_not_double_count(conn):
    # Fixed 2026-10-04 (DEF-107): the count used to be incremented per delivery, so Kafka's
    # at-least-once redelivery inflated it for good. It is now the size of the open-ticket set.
    event = factory.service_timing_event("order_fired", ticket_id="t1", table_id="table-1", station_id="station-grill")
    twin.handle_service_timing(conn, event)
    twin.handle_service_timing(conn, event)
    twin.handle_service_timing(conn, event)
    assert station_count(conn, "station-grill") == 1


def test_a_redelivered_delivered_does_not_undercount_other_tickets(conn):
    for ticket in ("t1", "t2", "t3"):
        twin.handle_service_timing(conn, factory.service_timing_event("order_fired", ticket_id=ticket, table_id=f"table-{ticket}", station_id="station-grill"))
    done = factory.service_timing_event("delivered", ticket_id="t1", table_id="table-t1", station_id="station-grill")
    twin.handle_service_timing(conn, done)
    twin.handle_service_timing(conn, done)  # redelivered: must not take a second ticket off
    assert station_count(conn, "station-grill") == 2


def test_a_delivery_for_a_ticket_the_twin_never_saw_open_leaves_the_count_alone(conn):
    twin.handle_service_timing(conn, factory.service_timing_event("order_fired", ticket_id="t1", table_id="table-1", station_id="station-grill"))
    twin.handle_service_timing(conn, factory.service_timing_event("delivered", ticket_id="stranger", table_id="table-9", station_id="station-grill"))
    assert station_count(conn, "station-grill") == 1, "an unknown ticket's delivery must not cancel a real open ticket"


def test_a_delivery_is_charged_to_the_station_the_ticket_was_opened_at(conn):
    twin.handle_service_timing(conn, factory.service_timing_event("order_fired", ticket_id="t1", table_id="table-1", station_id="station-grill"))
    twin.handle_service_timing(conn, factory.service_timing_event("delivered", ticket_id="t1", table_id="table-1", station_id="station-saute"))
    assert station_count(conn, "station-grill") == 0
    assert station_count(conn, "station-saute") in (None, 0)


def test_the_count_always_equals_the_number_of_open_tickets_whatever_the_delivery_pattern(conn):
    import random

    rng = random.Random(5)
    open_now, expected = [], set()
    for step in range(120):
        if open_now and rng.random() < 0.45:
            ticket = rng.choice(open_now)
            event = factory.service_timing_event("delivered", ticket_id=ticket, table_id="table-1", station_id="station-grill")
            expected.discard(ticket)
            open_now.remove(ticket)
        else:
            ticket = f"t{step}"
            event = factory.service_timing_event("order_fired", ticket_id=ticket, table_id="table-1", station_id="station-grill")
            expected.add(ticket)
            open_now.append(ticket)
        for _ in range(rng.choice([1, 1, 2, 3])):  # each message delivered one to three times
            twin.handle_service_timing(conn, event)
        assert station_count(conn, "station-grill") == len(expected)


def test_the_twin_creates_its_own_table_if_the_migration_never_ran(conn):
    from conftest import scalar

    with conn.cursor() as cur:
        cur.execute("DROP TABLE twin_open_tickets")
    conn.commit()
    twin.ensure_schema(conn)
    assert scalar(conn, "SELECT count(*) FROM twin_open_tickets") == 0
    twin.ensure_schema(conn)  # idempotent


def test_the_twins_own_copy_of_the_table_definition_matches_the_migration():
    import re
    from pathlib import Path

    migration = (Path(__file__).resolve().parents[2] / "storage" / "schema" / "006_twin_open_tickets.sql").read_text()
    statements = lambda sql: re.sub(r"\s+", " ", "\n".join(l for l in sql.splitlines() if not l.strip().startswith("--"))).strip()  # noqa: E731
    assert statements(twin.ENSURE_OPEN_TICKETS_SQL) == statements(migration)


def test_a_reassignment_for_staff_the_twin_never_saw_clock_in_is_recorded_as_on_shift_not_as_nothing(conn):
    # Found on GitHub's runner (2026-10-04): the twin held a staff row with no status. Events
    # published before the MQTT bridge subscribes are lost, and a twin can start mid-stream, so it
    # can meet a reassignment before (or without) the clock-in. A reassignment means the person is
    # working; the row must never have an invalid status.
    twin.handle_staff_shift(conn, factory.staff_shift_event("station_reassign", staff_id="s9", role="line_cook", station_id="station-grill"))
    status, station, since, role = staff_state(conn, "s9")
    assert (status, station, role) == ("on_shift", "station-grill", "line_cook")
    assert since is None, "the start of the shift is unknown, and must not be invented"
    twin.handle_staff_shift(conn, factory.staff_shift_event("clock_in", staff_id="s9", role="line_cook"))
    assert staff_state(conn, "s9")[2] is not None, "a later clock-in supplies the start time"


def test_a_reassignment_keeps_the_status_of_staff_the_twin_already_knows(conn):
    twin.handle_staff_shift(conn, factory.staff_shift_event("clock_in", staff_id="s8", role="line_cook"))
    twin.handle_staff_shift(conn, factory.staff_shift_event("break_start", staff_id="s8", role="line_cook"))
    twin.handle_staff_shift(conn, factory.staff_shift_event("station_reassign", staff_id="s8", role="line_cook", station_id="station-saute"))
    status, station, _, _ = staff_state(conn, "s8")
    assert (status, station) == ("on_break", "station-saute")


# ------------------------------------------------------------------ staff

def test_a_staff_member_goes_through_a_whole_shift(conn):
    twin.handle_staff_shift(conn, factory.staff_shift_event("clock_in", staff_id="s1", role="line_cook", station_id="station-grill"))
    status, station, since, role = staff_state(conn, "s1")
    assert (status, station, role) == ("on_shift", "station-grill", "line_cook")
    assert since is not None

    twin.handle_staff_shift(conn, factory.staff_shift_event("break_start", staff_id="s1", role="line_cook"))
    assert staff_state(conn, "s1")[0] == "on_break"

    twin.handle_staff_shift(conn, factory.staff_shift_event("break_end", staff_id="s1", role="line_cook"))
    assert staff_state(conn, "s1")[0] == "on_shift"

    twin.handle_staff_shift(conn, factory.staff_shift_event("clock_out", staff_id="s1", role="line_cook"))
    status, station, since, _ = staff_state(conn, "s1")
    assert status == "off_shift"
    assert since is None
    assert station == "station-grill", "clocking out must not forget where they last worked"


def test_reassigning_a_station_changes_only_the_station(conn):
    twin.handle_staff_shift(conn, factory.staff_shift_event("clock_in", staff_id="s1", station_id="station-grill"))
    before = staff_state(conn, "s1")
    twin.handle_staff_shift(conn, factory.staff_shift_event("station_reassign", staff_id="s1", station_id="station-fry"))
    after = staff_state(conn, "s1")
    assert after[1] == "station-fry"
    assert after[0] == before[0] == "on_shift"
    assert after[2] == before[2], "reassignment must not reset the clock-in time"


def test_staff_are_tracked_independently(conn):
    twin.handle_staff_shift(conn, factory.staff_shift_event("clock_in", staff_id="s1"))
    twin.handle_staff_shift(conn, factory.staff_shift_event("clock_in", staff_id="s2"))
    twin.handle_staff_shift(conn, factory.staff_shift_event("clock_out", staff_id="s1"))
    assert staff_state(conn, "s1")[0] == "off_shift"
    assert staff_state(conn, "s2")[0] == "on_shift"


def test_replaying_a_staff_event_is_harmless(conn):
    # Unlike ticket counts, staff state is absolute, so redelivery is idempotent.
    event = factory.staff_shift_event("clock_in", staff_id="s1", station_id="station-grill")
    for _ in range(3):
        twin.handle_staff_shift(conn, event)
    assert scalar(conn, "SELECT count(*) FROM twin_staff_state") == 1
    assert staff_state(conn, "s1")[0] == "on_shift"
