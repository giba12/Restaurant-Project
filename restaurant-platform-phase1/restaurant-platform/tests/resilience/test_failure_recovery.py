"""
Resilience tests: break things in the running stack and check the platform
heals itself.

In production nothing stays up forever. A process gets OOM-killed, the
database restarts for maintenance, the broker is rescheduled. "It works when
nothing goes wrong" proves little; what matters is that after each of these
the pipeline comes back on its own, with no data lost and none stored twice.
This is the repeatable version of the one-off broker-kill chaos test
originally done by hand on Kubernetes.

Each test breaks exactly one thing, waits for recovery, then checks the same
invariant: every message ever written to Kafka is stored exactly once.

    bash tests/run_stack_tests.sh resilience
"""
import json
import os
import re
import signal
import time
from datetime import datetime, timedelta, timezone


import stack_fixture as stack
from helpers import compose, container_id, inspect, run, sql, sql_int, wait_for

SIMULATORS = ["edge-sim-plate-waste", "edge-sim-pos-transaction", "edge-sim-service-timing", "edge-sim-staff-shift"]
TOPIC_TABLE = {
    "plate-waste-events": "plate_waste_events",
    "pos-transaction-events": "pos_transaction_events",
    "service-timing-events": "service_timing_events",
    "staff-shift-events": "staff_shift_events",
}
RECOVERY_SECONDS = 240
SETTLE_SECONDS = 15  # events published in the last moments may legitimately still be on their way
SHUTDOWN_WINDOW_ALLOWANCE = 2  # measured 0 in six outages at the 0.25 s poll (5 in six at 1 s); see the test below
# Kafka's listener closes about a second before the gate can see it, and what Connect had accepted but not
# yet produced is discarded when it revokes its tasks: measured 2, 3 and 1 events across three restarts
# (109 to 166 before the gate watched Kafka, 26 on GitHub's first run).
KAFKA_RESTART_ALLOWANCE = 4
# After a Mosquitto restart the simulators hold events 30 s while the connectors follow on their own backoff:
# measured 0 in three restarts (11, 2 and 2 without the hold). The broker's own in-flight messages are the
# one thing that could still go.
MQTT_RESTART_ALLOWANCE = 2
PUBLISHED_LINE = re.compile(r"(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d),\d+ INFO \[[\w-]+\] published \w+ event_id=([0-9a-f-]{36})")


def crash(service):
    """
    Kill a service's process the way the kernel's out-of-memory killer does: a
    SIGKILL delivered to it from outside, with no chance to clean up.

    Not `docker kill`: Podman treats that API call as a deliberate stop and
    will not apply the restart policy (measured: a `--restart=unless-stopped`
    container was restarted after a host-side SIGKILL but not after
    `docker kill`), so using it would test Podman's semantics, not the
    platform's. Where the host cannot signal the process directly (rootful
    Docker), `docker kill` is used instead, and Docker does restart after it.
    """
    pid = int(inspect(service)["State"]["Pid"])
    try:
        os.kill(pid, signal.SIGKILL)
    except OSError:
        run(["docker", "kill", container_id(service)])


def total_rows():
    return sum(sql_int(f"SELECT count(*) FROM {t}") for t in TOPIC_TABLE.values())


def healthy_and_running(service):
    return inspect(service)["State"]["Running"]


def wait_for_growth(description, probe, timeout=RECOVERY_SECONDS, minimum=5):
    """Wait until `probe()` has grown by at least `minimum` from where it is now."""
    start = probe()
    wait_for(lambda: probe() - start >= minimum, timeout, interval=5, description=description)


def pipeline_is_flowing():
    wait_for_growth("rows to keep arriving in the database", total_rows)


def quiesce_and_check_nothing_was_lost_or_duplicated():
    """
    Stop the producers, let every consumer catch up, then compare what is in
    Kafka with what is in the database, topic by topic. Equality means every
    message was stored once: no loss (fewer rows) and no duplication (the
    event_id uniqueness the tables enforce would otherwise hide extra copies).
    """
    compose("stop", *SIMULATORS, timeout=120)
    try:
        def settled():
            if stack.consumer_lag("storage-consumer") != 0:
                return False
            before = {t: stack.end_offset(t) for t in TOPIC_TABLE}
            time.sleep(8)  # the connectors may still be flushing the last MQTT messages
            after = {t: stack.end_offset(t) for t in TOPIC_TABLE}
            return before == after and stack.consumer_lag("storage-consumer") == 0

        wait_for(settled, 300, interval=2, description="the pipeline to drain after producers stopped")
        for topic, table in TOPIC_TABLE.items():
            offsets, stored = stack.end_offset(topic), sql_int(f"SELECT count(*) FROM {table}")
            assert stored == offsets, f"{topic}: {offsets} messages in Kafka but {stored} rows in {table}"
            assert sql_int(f"SELECT count(*) - count(DISTINCT event_id) FROM {table}") == 0
    finally:
        compose("start", *SIMULATORS, timeout=120)


# ------------------------------------------------------------------ what the sensors sent, against what was stored

def utc_now():
    return datetime.now(timezone.utc).replace(microsecond=0)


def published_by_the_simulators(since):
    """
    {event_id: published_at} for every event the simulators logged as published since
    `since` (a UTC datetime). The simulators log an event only after the MQTT broker has
    acknowledged it, so this is a record of what the sensors sent that does not depend on
    anything downstream: the thing "stored once" (Kafka against the database) cannot see.
    """
    published = {}
    for service in SIMULATORS:
        for line in compose("logs", "--no-color", service, timeout=120).stdout.splitlines():
            match = PUBLISHED_LINE.search(line)
            if match:
                at = datetime.strptime(match.group(1), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                if at >= since:
                    published[match.group(2)] = at
    return published


def stored_event_ids(since):
    stamp = since.strftime("%Y-%m-%d %H:%M:%S+00")
    ids = set()
    for table in TOPIC_TABLE.values():
        ids.update(sql(f"SELECT event_id FROM {table} WHERE ingested_at >= '{stamp}'", timeout=120).split())
    return ids


def events_sent_but_never_stored(since):
    settled_before = datetime.now(timezone.utc) - timedelta(seconds=SETTLE_SECONDS)
    sent = {i for i, at in published_by_the_simulators(since).items() if at <= settled_before}
    return sent - stored_event_ids(since), len(sent)


def assert_every_published_event_was_stored(since, allowed_missing=0, timeout=180):
    """
    Every event the simulators logged as published since `since` (settled ones: the last few
    seconds may still be on their way) must be in the database. `allowed_missing` is for the one
    loss that cannot be closed: see test_sensor_events_are_not_lost_while_the_kafka_connect_bridge_is_down.
    """
    outcome = {}

    def settled():
        missing, sent = events_sent_but_never_stored(since)
        outcome.update(missing=len(missing), sent=sent)
        return sent > 0 and len(missing) <= allowed_missing

    try:
        wait_for(settled, timeout, interval=10, description="every event the simulators published to be stored")
    except AssertionError:
        raise AssertionError(f"{outcome.get('missing')} of the {outcome.get('sent')} events the simulators published since "
                             f"{since:%H:%M:%S} UTC never reached the database (allowed: {allowed_missing})") from None


# ------------------------------------------------------------------ the invariant itself

def test_baseline_every_message_is_stored_exactly_once_with_nothing_going_wrong():
    # Validates the invariant checked after every failure below. If this
    # fails, the later failures would be uninterpretable.
    pipeline_is_flowing()
    quiesce_and_check_nothing_was_lost_or_duplicated()


# ------------------------------------------------------------------ a process crashes

def test_a_killed_storage_consumer_is_restarted_by_the_platform_and_loses_nothing():
    pipeline_is_flowing()
    crash("storage-consumer")
    time.sleep(15)  # a backlog builds while it is dead

    wait_for(lambda: healthy_and_running("storage-consumer"), 120, description="the restart policy to bring the consumer back")
    pipeline_is_flowing()
    quiesce_and_check_nothing_was_lost_or_duplicated()


def test_a_killed_aggregator_resumes_producing_ticket_summaries():
    wait_for_growth("complete ticket summaries", lambda: sql_int("SELECT count(*) FROM ticket_timing_summaries WHERE is_complete"), minimum=3)
    crash("ticket-timing-aggregator")
    wait_for(lambda: healthy_and_running("ticket-timing-aggregator"), 120, description="the aggregator to restart")
    # Tickets caught mid-flight by the crash are lost (documented), but new ones must flow.
    wait_for_growth("complete ticket summaries after the crash", lambda: sql_int("SELECT count(*) FROM ticket_timing_summaries WHERE is_complete"), minimum=5)


# ------------------------------------------------------------------ dependencies go away

def test_a_database_outage_is_survived_by_every_service_that_uses_it():
    pipeline_is_flowing()
    summaries_before = sql_int("SELECT count(*) FROM ticket_timing_summaries")

    compose("stop", "timescaledb", timeout=120)
    time.sleep(25)  # longer than a typical restart, long enough to exhaust short retry budgets
    compose("start", "timescaledb", timeout=120)
    wait_for(lambda: stack.healthy("timescaledb"), 180, description="the database to be healthy again")

    # Every database-using service must recover without a human restarting it.
    wait_for_growth("rows reaching the database (storage-consumer)", total_rows)
    wait_for(lambda: sql_int("SELECT count(*) FROM ticket_timing_summaries") > summaries_before + 3, RECOVERY_SECONDS, interval=5,
             description="ticket summaries to resume (ticket-timing-aggregator)")
    wait_for(lambda: sql_int("SELECT count(*) FROM twin_station_state WHERE updated_at > now() - interval '60 seconds'") >= 1,
             RECOVERY_SECONDS, interval=5, description="the digital twin to resume updating")
    status, body = stack.http_get("/api/twin/stations")
    assert status == 200 and isinstance(json.loads(body), list), "the dashboard API did not recover"
    quiesce_and_check_nothing_was_lost_or_duplicated()


def test_a_kafka_restart_is_survived():
    pipeline_is_flowing()
    since = utc_now()
    compose("restart", "kafka", timeout=180)
    wait_for(lambda: stack.healthy("kafka"), 240, description="kafka to be healthy again")
    wait_for(lambda: all(state == "RUNNING" for state in stack.connector_states().values()), 240, interval=5,
             description="the MQTT connectors to be RUNNING again")
    pipeline_is_flowing()
    quiesce_and_check_nothing_was_lost_or_duplicated()
    assert_every_published_event_was_stored(since, allowed_missing=KAFKA_RESTART_ALLOWANCE)


def test_an_mqtt_broker_restart_is_survived():
    # The sensor-facing edge: if simulators or the connectors cannot reconnect,
    # data silently stops (this is the shape of problem log item 44).
    pipeline_is_flowing()
    since = utc_now()
    compose("restart", "mosquitto", timeout=120)
    pipeline_is_flowing()
    states = stack.connector_states()
    assert all(state == "RUNNING" for state in states.values()), states
    assert_every_published_event_was_stored(since, allowed_missing=MQTT_RESTART_ALLOWANCE)


def test_sensor_events_are_not_lost_while_the_kafka_connect_bridge_is_down():
    # The MQTT broker keeps nothing for a subscriber that is not connected, so what the
    # sensors published while Kafka Connect was down or restarting never reached Kafka. Nothing
    # noticed: "stored once" compares Kafka with the database, not the sensors with Kafka.
    # Measured before the fix: 470 of 1,266 events across one 45 s outage and its recovery.
    # The ledger here is the simulators' own log of what they published; they now hold events
    # while their connector is not running (DEF-148).
    #
    # A small loss is allowed for (measured: none in six outages): when Connect shuts down its REST API stops answering
    # slightly before its MQTT consumers stop, so an event published in that sub-second window
    # is still lost.
    pipeline_is_flowing()
    since = utc_now()
    compose("stop", "kafka-connect", timeout=120)
    time.sleep(45)  # long enough for every simulator to publish several events into the outage
    compose("start", "kafka-connect", timeout=240)
    wait_for(lambda: all(state == "RUNNING" for state in stack.connector_states().values()), 300, interval=5,
             description="the connectors to be RUNNING again")
    assert_every_published_event_was_stored(since, allowed_missing=SHUTDOWN_WINDOW_ALLOWANCE)


def test_connector_tasks_that_failed_at_startup_are_restarted_automatically():
    # DEF-137, reproduced on purpose. With the MQTT broker stopped its hostname does not
    # resolve, so Kafka Connect's tasks fail as they start (`UnknownHostException: mosquitto`,
    # the failure a transient DNS fault caused on two real starts). Kafka Connect never
    # restarts a failed task, so before the supervisor existed they stayed FAILED after the
    # broker came back and the pipeline ingested nothing. The supervisor must notice and restart them.
    pipeline_is_flowing()
    compose("stop", "mosquitto", timeout=120)
    compose("restart", "kafka-connect", timeout=240)
    wait_for(lambda: any(state == "FAILED" for state in stack.connector_states().values()), 300, interval=5,
             description="a connector task to fail while the broker's name does not resolve (the precondition of this test)")

    compose("start", "mosquitto", timeout=120)
    wait_for(lambda: all(state == "RUNNING" for state in stack.connector_states().values()), 180, interval=5,
             description="the supervisor to restart the failed tasks")
    pipeline_is_flowing()
    quiesce_and_check_nothing_was_lost_or_duplicated()


# ------------------------------------------------------------------ the whole thing is stopped and restarted

def test_stopping_and_restarting_the_whole_stack_preserves_data():
    from stack_fixture import STACK_SERVICES

    pipeline_is_flowing()
    before = total_rows()
    offsets_before = {t: stack.end_offset(t) for t in TOPIC_TABLE}

    compose("down", timeout=300)  # containers and network go; named volumes stay
    # The simulators' logs go with their containers, so the ledger starts here: everything the
    # new simulators publish from their first moment, including the seconds before Kafka Connect
    # has subscribed, which used to be lost.
    since = utc_now()
    compose("up", "-d", "--no-build", *STACK_SERVICES, timeout=600)
    stack.wait_until_ready.cache_clear()
    stack.wait_until_ready()

    assert total_rows() >= before, "rows were lost across a stop/start"
    for topic, offset in offsets_before.items():
        assert stack.end_offset(topic) >= offset, f"{topic} lost messages across a stop/start"
    pipeline_is_flowing()
    assert_every_published_event_was_stored(since)
