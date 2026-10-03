"""
Builds events for the integration tests. Where possible they come from the
real simulators (so a producer change is exercised here too); `override`
lets a test pin the one field it cares about.
"""
import copy
import datetime
import itertools
import uuid

from simulators import plate_waste, pos_transaction, service_timing, staff_shift

_clock = itertools.count()


def ts(offset_seconds=0, base="2026-10-01T12:00:00"):
    start = datetime.datetime.fromisoformat(base).replace(tzinfo=datetime.timezone.utc)
    return (start + datetime.timedelta(seconds=offset_seconds)).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def unique_ts():
    """A strictly increasing timestamp, so ON CONFLICT (event_id, timestamp) never masks a test bug."""
    return ts(next(_clock))


def _apply(event, override):
    event = copy.deepcopy(event)
    for key, value in (override or {}).items():
        if key.startswith("confounder_flags."):
            event["confounder_flags"][key.split(".", 1)[1]] = value
        else:
            event[key] = value
    return event


def plate_waste_event(**override):
    return _apply(plate_waste.generate_event(), override)


def pos_transaction_event(**override):
    return _apply(pos_transaction.generate_event(), override)


def service_timing_event(stage="order_fired", ticket_id=None, **override):
    base = {
        "event_id": str(uuid.uuid4()), "event_type": "ServiceTimingEvent", "schema_version": "1.0.0",
        "source_id": "sim-ticket-timer-01", "source_kind": "simulated", "timestamp": unique_ts(),
        "restaurant_id": "rest-001", "ticket_id": ticket_id or str(uuid.uuid4()), "table_id": "table-01",
        "station_id": "station-grill", "stage": stage, "elapsed_since_previous_stage_ms": None,
    }
    return _apply(base, override)


def staff_shift_event(action="clock_in", staff_id="staff-001", role="line_cook", station_id=None, **override):
    base = {
        "event_id": str(uuid.uuid4()), "event_type": "StaffShiftEvent", "schema_version": "1.0.0",
        "source_id": "sim-staffing-sensor-01", "source_kind": "simulated", "timestamp": unique_ts(),
        "restaurant_id": "rest-001", "staff_id": staff_id, "role": role, "shift_action": action,
        "station_id": station_id, "scheduled_vs_actual": "as_scheduled",
    }
    return _apply(base, override)


def simulator_events_of_every_kind():
    """One event from each of the four real simulators."""
    lifecycle = service_timing.TicketLifecycle()
    return {
        "plate_waste": plate_waste.generate_event(),
        "pos_transaction": pos_transaction.generate_event(),
        "service_timing": lifecycle.next_event(),
        "staff_shift": staff_shift.ShiftState().next_event(),
    }
