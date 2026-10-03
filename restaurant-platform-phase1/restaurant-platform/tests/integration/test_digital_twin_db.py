"""
Integration tests: the digital twin's state machine against a real database.

The twin is the one component that answers "what is happening in the
restaurant right now", so its job is to mirror reality from a stream of
events: tables fill and empty, staff clock in and out, stations carry work.
These tests replay realistic event sequences and check the resulting state.
"""
import pytest

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


@pytest.mark.xfail(
    strict=True,
    reason="KNOWN LIMITATION: open_ticket_count is incremented, not derived, so a Kafka redelivery "
           "of order_fired (at-least-once delivery) inflates it. strict=True: when this is fixed the "
           "test will start passing and CI will demand the marker be removed.",
)
def test_a_redelivered_order_fired_does_not_double_count(conn):
    event = factory.service_timing_event("order_fired", ticket_id="t1", table_id="table-1", station_id="station-grill")
    twin.handle_service_timing(conn, event)
    twin.handle_service_timing(conn, event)
    assert station_count(conn, "station-grill") == 1


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
