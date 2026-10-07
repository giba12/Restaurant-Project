"""
Tests that staffing is a real driver in the simulated restaurant (DEF-141).

The causal engine analyses `staffing_level` (staff clocked in) against pickup
delay. For that analysis to mean anything the simulated world has to contain
the relationship: fewer people working must make tickets wait longer, and an
injected staffing shortage must act by lowering staffing. These tests check
the world, not the engine: that the relationship exists and has the right
sign, that the shortage intervenes on staffing, and that the plumbing that
carries the staffing level between the two simulators works.

No Kafka, MQTT or database: paho is stubbed, time is a fake clock, and random
choices are seeded.

    pip install numpy jsonschema pytest
    cd edge-simulators && python -m pytest test_world_coupling.py -v
"""
import os
import random
import sys
import threading
import types

import numpy as np
import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
os.environ.setdefault("SCHEMA_DIR", os.path.join(os.path.dirname(HERE), "schemas"))

_paho = types.ModuleType("paho")
_paho_mqtt = types.ModuleType("paho.mqtt")
_paho_mqtt_client = types.ModuleType("paho.mqtt.client")
_paho_mqtt_client.Client = None
_paho_mqtt.client = _paho_mqtt_client
_paho.mqtt = _paho_mqtt
sys.modules.setdefault("paho", _paho)
sys.modules.setdefault("paho.mqtt", _paho_mqtt)
sys.modules.setdefault("paho.mqtt.client", _paho_mqtt_client)

from common import scenario, staffing  # noqa: E402
from common.runtime import Simulator  # noqa: E402
from simulators import service_timing, staff_shift  # noqa: E402

SHORTAGE = {"scenario_injection_id": "s1", "action": "start", "scenario_type": "staffing_shortage",
            "target": "all", "parameters": {"stations_removed": ["station-grill"]}}


class FakeClock:
    """One second per event the simulator generates, so waits are counted in simulator ticks."""

    def __init__(self):
        self.now = 0.0

    def monotonic(self):
        return self.now

    def tick(self):
        self.now += 1.0


@pytest.fixture()
def clock(monkeypatch):
    fake = FakeClock()
    monkeypatch.setattr(service_timing, "time", types.SimpleNamespace(monotonic=fake.monotonic))
    return fake


# ------------------------------------------------------------------ the staffing level and its encoding

def test_backlog_capacity_falls_as_staffing_rises_and_is_nominal_when_unknown():
    assert staffing.backlog_factor(None) == 1.0
    assert staffing.backlog_factor(staffing.NOMINAL_STAFFING) == 1.0
    levels = [staffing.backlog_factor(n) for n in range(1, 11)]
    assert levels == sorted(levels, reverse=True), "more staff must never mean a bigger backlog"
    assert levels[0] > 1.0 > levels[-1]


def test_backlog_capacity_is_bounded_at_both_ends():
    assert staffing.backlog_factor(0) == staffing.MAX_FACTOR      # nobody working is not an infinite backlog
    assert staffing.backlog_factor(1) == staffing.MAX_FACTOR
    assert staffing.backlog_factor(100) == staffing.MIN_FACTOR    # a full house is not an instant kitchen


def test_the_staffing_level_survives_the_wire_and_garbage_is_ignored():
    level = staffing.StaffingLevel()
    assert level() is None
    level.on_message(staffing.encode(5).encode())
    assert level() == 5
    for junk in (b"", b"not json", b"{}", b'{"clocked_in": -1}', b'{"clocked_in": "7"}', b'{"clocked_in": 2.5}'):
        level.on_message(junk)
    assert level() == 5, "a malformed message must not overwrite the last good level"


# ------------------------------------------------------------------ the kitchen responds to staffing

def test_the_kitchen_backlog_follows_the_staffing_level():
    short = service_timing.TicketLifecycle(staffing_getter=lambda: 2)._effective_max_open_tickets()
    nominal = service_timing.TicketLifecycle(staffing_getter=lambda: staffing.NOMINAL_STAFFING)._effective_max_open_tickets()
    full = service_timing.TicketLifecycle(staffing_getter=lambda: 10)._effective_max_open_tickets()
    unknown = service_timing.TicketLifecycle()._effective_max_open_tickets()
    assert short > nominal > full >= service_timing.MIN_OPEN_TICKETS
    assert nominal == unknown == service_timing.MAX_OPEN_TICKETS


def test_an_active_scenario_no_longer_changes_the_kitchen_except_through_staffing():
    # The old direct multiplier slowed the kitchen without touching staffing.
    # The scenario may still move new tickets off a removed station, but the
    # backlog capacity must depend on staffing alone.
    lifecycle = service_timing.TicketLifecycle(active_scenario_getter=lambda: SHORTAGE, staffing_getter=lambda: staffing.NOMINAL_STAFFING)
    assert lifecycle._effective_max_open_tickets() == service_timing.MAX_OPEN_TICKETS
    assert "station-grill" not in lifecycle._available_stations()


def waits_at(level, seed, events=12000):
    """Mean wait, in ticks, between 'plated' and 'picked up' with staffing held at `level`."""
    random.seed(seed)
    clock = FakeClock()
    service_timing.time = types.SimpleNamespace(monotonic=clock.monotonic)
    lifecycle = service_timing.TicketLifecycle(staffing_getter=lambda: level)
    waits = []
    for _ in range(events):
        clock.tick()
        event = lifecycle.next_event()
        if event["stage"] == "picked_up_by_server":
            waits.append(event["elapsed_since_previous_stage_ms"] / 1000.0)
    return float(np.mean(waits))


def test_fewer_people_working_makes_tickets_wait_longer_in_the_simulated_kitchen(monkeypatch):
    original = service_timing.time
    try:
        understaffed, nominal, overstaffed = waits_at(2, 1), waits_at(staffing.NOMINAL_STAFFING, 2), waits_at(10, 3)
    finally:
        service_timing.time = original
    # Measured (ticks): 38.3 at 2 staff, 9.3 at 8 (nominal) and 6.4 at 10. The ordering is the property;
    # requiring understaffed to be at least twice nominal fails a kitchen that ignores staffing.
    assert understaffed > 2 * nominal > 0
    assert nominal > overstaffed


# ------------------------------------------------------------------ the staff respond to a shortage

def drive(state, n):
    return [state.next_event()["shift_action"] for _ in range(n)]


def test_an_injected_shortage_clocks_staff_out_down_to_the_cap_and_holds_it_there():
    random.seed(11)
    active = {"on": False}
    state = staff_shift.ShiftState(active_scenario_getter=lambda: SHORTAGE if active["on"] else None)
    drive(state, 200)  # normal operation fills the roster
    assert len(state.clocked_in) > staff_shift.SHORTAGE_MAX_CLOCKED_IN + 2, "the roster should be well above the cap before the shortage"

    active["on"] = True
    actions = []
    for _ in range(300):
        actions.append(state.next_event()["shift_action"])
    assert len(state.clocked_in) == staff_shift.SHORTAGE_MAX_CLOCKED_IN
    # Once at the cap, nobody clocks in during the shortage.
    settled = actions[40:]
    assert "clock_in" not in settled and "clock_out" not in settled


def test_staffing_recovers_after_the_shortage_ends():
    random.seed(12)
    active = {"on": True}
    state = staff_shift.ShiftState(active_scenario_getter=lambda: SHORTAGE if active["on"] else None)
    drive(state, 200)
    assert len(state.clocked_in) <= staff_shift.SHORTAGE_MAX_CLOCKED_IN
    active["on"] = False
    drive(state, 300)
    assert len(state.clocked_in) >= staffing.NOMINAL_STAFFING - 3


def test_other_scenarios_and_no_scenario_leave_the_staff_alone():
    random.seed(13)
    spike = {"scenario_type": "order_volume_spike", "target": "all", "parameters": {"multiplier": 3.0}}
    for getter in (None, lambda: None, lambda: spike):
        state = staff_shift.ShiftState(active_scenario_getter=getter)
        drive(state, 300)
        assert len(state.clocked_in) > staff_shift.SHORTAGE_MAX_CLOCKED_IN


def test_staff_events_stay_sequentially_plausible_during_a_shortage():
    # No clock-out for someone not clocked in, no clock-in for someone already in.
    random.seed(14)
    state = staff_shift.ShiftState(active_scenario_getter=lambda: SHORTAGE)
    shadow = set()
    for _ in range(400):
        event = state.next_event()
        who, action = event["staff_id"], event["shift_action"]
        if action == "clock_in":
            assert who not in shadow
            shadow.add(who)
        elif action == "clock_out":
            assert who in shadow
            shadow.discard(who)
        else:
            assert who in shadow, f"{action} for someone who is not clocked in"


# ------------------------------------------------------------------ the whole world, as the analysis sees it

def test_in_the_simulated_world_staffing_at_pickup_is_negatively_related_to_pickup_delay():
    # The ground truth the causal engine's staffing treatment needs. Both simulators run
    # together on a fake clock: staff events every tenth tick, a shortage injected for the
    # middle third. Each ticket's pickup wait is paired with the staffing level at pickup,
    # exactly the pair the engine's SQL builds.
    random.seed(21)
    clock = FakeClock()
    original = service_timing.time
    service_timing.time = types.SimpleNamespace(monotonic=clock.monotonic)
    try:
        injected = {"on": False}
        staff = staff_shift.ShiftState(active_scenario_getter=lambda: SHORTAGE if injected["on"] else None)
        lifecycle = service_timing.TicketLifecycle(active_scenario_getter=lambda: SHORTAGE if injected["on"] else None,
                                                   staffing_getter=lambda: len(staff.clocked_in))
        levels, delays, during = [], [], []
        total = 18000
        for i in range(total):
            clock.tick()
            injected["on"] = total // 3 <= i < 2 * total // 3
            if i % 10 == 0:
                staff.next_event()
            event = lifecycle.next_event()
            if event["stage"] == "picked_up_by_server":
                levels.append(len(staff.clocked_in))
                delays.append(event["elapsed_since_previous_stage_ms"] / 1000.0)
                during.append(injected["on"])
    finally:
        service_timing.time = original
    levels, delays, during = np.asarray(levels), np.asarray(delays), np.asarray(during)
    # Measured: staffing 8.3 normally and 2.0 in the shortage; pickup wait 10.2 and 38.3 ticks
    # (3.7x); correlation -0.49 over about 3,600 tickets.
    assert len(delays) > 500
    assert levels[during].mean() < levels[~during].mean() - 2, "the shortage did not lower staffing"
    assert delays[during].mean() > 1.5 * delays[~during].mean(), "the shortage did not slow the kitchen"
    assert np.corrcoef(levels, delays)[0, 1] < -0.3, "staffing and pickup delay are not negatively related"


# ------------------------------------------------------------------ the plumbing

class FakeClient:
    def __init__(self):
        self.subscribed, self.published = [], []

    def subscribe(self, topic, qos=0):
        self.subscribed.append((topic, qos))

    def publish(self, topic, payload, qos=0, retain=False):
        self.published.append((topic, payload, qos, retain))


def bare_simulator():
    sim = Simulator.__new__(Simulator)
    sim._handlers = {}
    sim._connected_callbacks = []
    sim._refused_publishes = {}
    sim._connected_event = threading.Event()
    sim.client = FakeClient()
    sim.log = types.SimpleNamespace(error=lambda *a, **k: None, exception=lambda *a, **k: None)
    return sim


def test_subscriptions_are_established_on_every_connect_not_only_the_first():
    sim = bare_simulator()
    sim.subscribe("sim/x", lambda payload: None)
    assert sim.client.subscribed == [], "must wait for the connection before subscribing"
    ok = types.SimpleNamespace(is_failure=False)
    sim._on_connect(sim.client, None, None, ok)
    sim._on_connect(sim.client, None, None, ok)  # a reconnect with a clean session starts with no subscriptions
    assert sim.client.subscribed == [("sim/x", 1), ("sim/x", 1)]


def test_connect_callbacks_run_on_every_successful_connect_and_a_failing_one_does_not_stop_the_others():
    sim = bare_simulator()
    calls = []
    sim.on_connected(lambda: calls.append("first"))
    sim.on_connected(lambda: 1 / 0)  # must not stop the next one, nor the connection
    sim.on_connected(lambda: calls.append("third"))
    ok = types.SimpleNamespace(is_failure=False)
    sim._on_connect(sim.client, None, None, ok)
    sim._on_connect(sim.client, None, None, ok)  # a reconnect
    assert calls == ["first", "third", "first", "third"]
    assert sim._connected_event.is_set()


def test_a_refused_connection_runs_no_connect_callbacks():
    sim = bare_simulator()
    calls = []
    sim.on_connected(lambda: calls.append("called"))
    sim._on_connect(sim.client, None, None, types.SimpleNamespace(is_failure=True))
    assert calls == [] and not sim._connected_event.is_set()


def test_a_subscription_made_after_connecting_takes_effect_at_once():
    sim = bare_simulator()
    sim._connected_event.set()
    sim.subscribe("sim/y", lambda payload: None)
    assert sim.client.subscribed == [("sim/y", 1)]


def test_messages_reach_their_handler_and_a_failing_handler_does_not_kill_the_client():
    sim = bare_simulator()
    seen = []
    sim.subscribe("sim/x", seen.append)
    sim.subscribe("sim/boom", lambda payload: 1 / 0)
    sim._on_message(None, None, types.SimpleNamespace(topic="sim/x", payload=b"hello"))
    sim._on_message(None, None, types.SimpleNamespace(topic="sim/boom", payload=b"x"))  # must not raise
    sim._on_message(None, None, types.SimpleNamespace(topic="sim/unknown", payload=b"x"))
    assert seen == [b"hello"]


def test_shared_world_state_is_published_retained_so_a_late_subscriber_sees_it():
    sim = bare_simulator()
    sim.publish_state(staffing.STAFFING_TOPIC, staffing.encode(4))
    assert sim.client.published == [(staffing.STAFFING_TOPIC, '{"clocked_in": 4}', 1, True)]


def test_the_staffing_topic_is_not_one_the_pipeline_bridges_or_stores():
    # The Kafka Connect bridge carries the four sensors/... topics; the shared state must stay out of it.
    assert not staffing.STAFFING_TOPIC.startswith("sensors/")


def test_scenario_messages_are_filtered_by_target_and_end_only_the_matching_scenario():
    state = {"active": None}
    targets = (None, "staff-shift", "all")
    scenario.apply(state, {**SHORTAGE, "target": "service-timing"}, targets)
    assert state["active"] is None, "a message for another simulator must be ignored"
    scenario.apply(state, SHORTAGE, targets)
    assert state["active"]["scenario_injection_id"] == "s1"
    scenario.apply(state, {"scenario_injection_id": "other", "action": "end", "target": "all"}, targets)
    assert state["active"] is not None, "ending a different scenario must not end this one"
    scenario.apply(state, {"scenario_injection_id": "s1", "action": "end", "target": "all"}, targets)
    assert state["active"] is None
