"""
Staffing as a real driver in the simulated restaurant.

Until 2026-10-04 the simulators never encoded that staffing affects anything:
the staff-shift simulator drew clock-ins and clock-outs at random, and the
injected "staffing shortage" slowed the kitchen through a direct multiplier on
the timing simulator that never touched the staffing signal. The causal engine
analyses `staffing_level` (staff clocked in, from the staff-shift events)
against pickup delay, so in this data there was no staffing effect for it to
find, and the original refutation gate only "found" one because it passed
noise (DEF-106, DEF-141). The to-go effect on plate waste is encoded in the
data on purpose; this module does the same for staffing.

The relationship, kept deliberately simple and monotone: the kitchen can hold
a backlog proportional to the people working it, and a longer backlog means
each ticket waits longer for its next stage (see service_timing.py). A
shortage is an intervention on staffing itself: the staff-shift simulator
clocks staff out, the staffing level falls, and the kitchen slows because of
the fall.

The staff-shift simulator is the source of truth and shares the current count
with the timing simulator as a retained MQTT message, so a simulator that
starts later still learns the current level immediately. The topic is not one
of the bridged `sensors/...` topics and is never stored.
"""
import json

STAFFING_TOPIC = "sim/world/staffing"

# The level at which the kitchen runs at its nominal backlog. The staff-shift
# simulator settles at about this many clocked in (measured 8.2 over 3,000
# staff events, of a roster of ten).
NOMINAL_STAFFING = 8

# Backlog capacity scales as NOMINAL / staffing, within these bounds, so one
# person cannot make the kitchen infinitely slow and a full house cannot make
# it instant.
MIN_FACTOR = 0.5
MAX_FACTOR = 4.0


def backlog_factor(staffing) -> float:
    """
    How much the kitchen's backlog capacity is scaled at this staffing level:
    1.0 at the nominal level, larger when understaffed, smaller when
    overstaffed. None (the level is not yet known) means nominal.
    """
    if staffing is None:
        return 1.0
    return min(max(NOMINAL_STAFFING / max(staffing, 1), MIN_FACTOR), MAX_FACTOR)


def encode(clocked_in: int) -> str:
    return json.dumps({"clocked_in": clocked_in})


def decode(payload) -> int | None:
    """The staffing level in a retained message, or None if it is not one."""
    try:
        value = json.loads(payload)["clocked_in"]
    except (ValueError, KeyError, TypeError):
        return None
    return value if isinstance(value, int) and value >= 0 else None


class StaffingLevel:
    """The latest staffing level this process has heard, or None until it has heard one."""

    def __init__(self):
        self.value = None

    def on_message(self, payload) -> None:
        level = decode(payload)
        if level is not None:
            self.value = level

    def __call__(self):
        return self.value
