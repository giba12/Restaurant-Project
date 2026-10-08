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


import pytest

import stack_fixture as stack
from helpers import PROJECT, compose, inspect, restart_count, run, sql, sql_int, wait_for

SIMULATORS = ["edge-sim-plate-waste", "edge-sim-pos-transaction", "edge-sim-service-timing", "edge-sim-staff-shift"]
TOPIC_TABLE = {
    "plate-waste-events": "plate_waste_events",
    "pos-transaction-events": "pos_transaction_events",
    "service-timing-events": "service_timing_events",
    "staff-shift-events": "staff_shift_events",
}
RECOVERY_SECONDS = 240
SETTLE_SECONDS = 15  # events published in the last moments may legitimately still be on their way
PUBLISHED_LINE = re.compile(r"(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d),\d+ INFO \[[\w-]+\] published \w+ event_id=([0-9a-f-]{36})")


def crash(service):
    """
    Kill a service's process the way the kernel's out-of-memory killer does: a
    SIGKILL delivered to it from outside, with no chance to clean up.

    Never `docker kill`: both Podman and Docker treat that API call as a deliberate
    stop and do not apply the restart policy. Measured on Podman (a
    `--restart=unless-stopped` container was restarted after a host-side SIGKILL
    but not after `docker kill`) and then on GitHub's Docker Engine, where a
    `docker kill` of Mosquitto left it down for the rest of the run (the bridge
    never reconnected) and four later tests failed because of it (DEF-154). The
    signal is sent to the process itself instead: directly when the host user owns
    it (rootless Podman; on GitHub's runner the UID-1001 services), with `sudo`
    when it belongs to root (Mosquitto on rootful Docker). If neither can
    deliver it, fail rather than test the wrong thing.
    """
    pid = int(inspect(service)["State"]["Pid"])
    try:
        os.kill(pid, signal.SIGKILL)
        return
    except PermissionError:
        pass  # the container's process belongs to another user
    result = run(["sudo", "-n", "kill", "-9", str(pid)], check=False)
    if result.returncode != 0:
        raise RuntimeError(
            f"cannot deliver a real SIGKILL to {service} (pid {pid}): the process is not ours and `sudo -n kill` failed "
            f"({result.stderr.strip()}). `docker kill` is not an acceptable substitute: Docker treats it as a manual "
            "stop and will not restart the container."
        )


@pytest.fixture(autouse=True)
def the_whole_stack_is_up_before_each_test():
    """
    Bring every service back before each test. A test that breaks something and then fails halfway
    (or whose recovery does not happen) must not leave the stack broken for the tests after it: on
    GitHub one dead Mosquitto turned one real failure into five, and hid which later tests were fine.
    `up -d` on a running service does nothing; on a stopped one it starts it.
    """
    from stack_fixture import STACK_SERVICES

    compose("up", "-d", "--no-build", *STACK_SERVICES, timeout=600)
    wait_for(stack.bridge_connected, 240, interval=3, description="the bridge to be connected before the test starts")
    yield


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
            time.sleep(8)  # the bridge may still be forwarding the last MQTT messages
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


def assert_every_published_event_was_stored(since, allowed_missing=0, timeout=240):
    """
    Every event the simulators logged as published since `since` (settled ones: the last few
    seconds may still be on their way) must be in the database. No allowance is ever needed: the
    bridge acknowledges a message to Mosquitto only after Kafka has it, so a message is either in
    Kafka or still held by Mosquitto (DEF-152). `allowed_missing` exists for a test to state a
    loss it has measured; none does.
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
    wait_for(stack.bridge_connected, 240, interval=5, description="the bridge to be connected again")
    pipeline_is_flowing()
    quiesce_and_check_nothing_was_lost_or_duplicated()
    # Zero allowed. Under Kafka Connect this lost 109 to 166 events locally and 26 of 734 on GitHub (DEF-151).
    assert_every_published_event_was_stored(since)


def test_an_mqtt_broker_restart_is_survived():
    # The sensor-facing edge. Mosquitto now persists the bridge's session and its queue, and the bridge
    # reconnects at once; nothing is held or guessed (the Kafka Connect connectors reconnected on their own
    # backoff, 2 to 17 s after the simulators, and lost what was sent in between: DEF-151).
    pipeline_is_flowing()
    since = utc_now()
    compose("restart", "mosquitto", timeout=120)
    wait_for(stack.bridge_connected, 120, interval=3, description="the bridge to reconnect to MQTT")
    pipeline_is_flowing()
    assert_every_published_event_was_stored(since)


def test_a_mosquitto_that_is_killed_hard_loses_nothing():
    # Not a stop (which saves its state) but a SIGKILL, as an out-of-memory kill would be. Mosquitto saves its
    # state every 5 s, so what it had accepted in the last seconds is not on disk: the loss window is the time
    # between accepting a message and handing it to the bridge, a few milliseconds, and after a restart it
    # redelivers whatever the bridge had not acknowledged. Measured: 0 of 2,632 events across five kills.
    pipeline_is_flowing()
    since = utc_now()
    crash("mosquitto")
    wait_for(lambda: healthy_and_running("mosquitto"), 120, description="the restart policy to bring Mosquitto back")
    wait_for(stack.bridge_connected, 120, interval=2, description="the bridge to reconnect")
    pipeline_is_flowing()
    assert_every_published_event_was_stored(since)


# ------------------------------------------------------------------ a disk is lost (the Compose version of losing a PVC)

def volume_of(name):
    """The real name of the project's volume `name`, found by the labels Compose puts on it."""
    found = run(["docker", "volume", "ls", "-q", "--filter", f"label=com.docker.compose.project={PROJECT}",
                 "--filter", f"label=com.docker.compose.volume={name}"]).stdout.split()
    assert len(found) == 1, f"expected one volume {name} in project {PROJECT}, found {found}"
    return found[0]


def lose_the_disk_of(service, volume_name):
    """Remove the service's container and then its volume, as losing a node's local volume does: nothing is stopped politely."""
    volume = volume_of(volume_name)
    compose("rm", "-sf", service, timeout=120)
    run(["docker", "volume", "rm", "-f", volume])
    assert not run(["docker", "volume", "ls", "-q", "--filter", f"name=^{volume}$"]).stdout.split(), "the volume is still there"


def test_a_mosquitto_that_loses_its_disk_comes_back_empty_and_from_then_on_loses_nothing():
    # Mosquitto's volume holds the bridge's persistent session and whatever was queued for it. Losing it is the case the other
    # tests do not reach (a node or volume loss; RSK-035). What cannot be promised: the messages the broker held at that moment,
    # and any published while the new broker had no session to queue them for. What must hold: the new broker accepts the
    # bridge, which subscribes afresh, and from the moment it is subscribed again every event is stored.
    pipeline_is_flowing()
    lose_the_disk_of("mosquitto", "mosquitto-data")
    compose("up", "-d", "--no-build", "mosquitto", timeout=180)
    wait_for(stack.bridge_connected, 240, interval=2, description="the bridge to connect to the empty broker and subscribe again")
    resubscribed = utc_now() + timedelta(seconds=5)
    pipeline_is_flowing()
    assert_every_published_event_was_stored(resubscribed)


def test_a_database_that_loses_its_disk_is_rebuilt_from_kafka_to_exactly_what_was_there():
    # The database is not the record of what the sensors said; Kafka is. With the producers stopped and the consumer caught up,
    # lose the database's volume: the schema is recreated empty by the image's own start-up scripts, the consumer group is
    # rewound to the start of every topic, and the event tables must refill to exactly what they held before the loss (storage is
    # idempotent on event_id, so a replay can only restore, never duplicate). "What they held", not "what Kafka holds": Kafka may
    # hold a message twice (a crash between Kafka's confirmation and the MQTT acknowledgement redelivers it, and the Mosquitto
    # kills earlier in this layer do exactly that), and the table, keyed on event_id, keeps one. Tables derived from the events
    # (summaries, findings, the twin) are not rebuilt by this and are not claimed.
    pipeline_is_flowing()
    compose("stop", *SIMULATORS, timeout=120)
    try:
        def drained():
            if stack.consumer_lag("storage-consumer") != 0:
                return False
            before = {t: stack.end_offset(t) for t in TOPIC_TABLE}
            time.sleep(8)
            return before == {t: stack.end_offset(t) for t in TOPIC_TABLE} and stack.consumer_lag("storage-consumer") == 0

        def counts():
            return {t: sql_int(f"SELECT count(*) FROM {table}") for t, table in TOPIC_TABLE.items()}

        wait_for(drained, 300, interval=2, description="every event to be stored before the disk is lost")
        offsets = {t: stack.end_offset(t) for t in TOPIC_TABLE}
        held = counts()
        assert all(count > 0 for count in held.values()), f"nothing stored for some topic, so this proves nothing: {held}"
        assert all(held[t] <= offsets[t] for t in TOPIC_TABLE), f"more rows than Kafka messages: rows {held}, offsets {offsets}"
        compose("stop", "storage-consumer", timeout=120)

        lose_the_disk_of("timescaledb", "timescaledb-data")
        compose("up", "-d", "--no-build", "timescaledb", timeout=180)
        wait_for(lambda: stack.healthy("timescaledb"), 240, description="the new, empty database to be healthy")
        assert counts() == {t: 0 for t in TOPIC_TABLE}, "the new database was not empty, so the disk was not really lost"

        # Kafka only rewinds a group with no members. A container that was stopped, not shut down politely, may still be a member
        # until its session times out, and the tool then prints an error and exits 0: the consumer would carry on from where it was
        # and replay nothing (on GitHub's faster runner the reset came before the old member had gone). So wait for an empty group,
        # and read what the tool said.
        def group_state():
            return compose("exec", "-T", "kafka", f"{stack.KAFKA_BIN}/kafka-consumer-groups.sh", "--bootstrap-server", "localhost:9092",
                           "--describe", "--group", "storage-consumer", "--state", timeout=60).stdout

        wait_for(lambda: any(word in group_state() for word in ("Empty", "Dead")), 180, interval=3, description="the stopped consumer to leave its group")
        reset = compose("exec", "-T", "kafka", f"{stack.KAFKA_BIN}/kafka-consumer-groups.sh", "--bootstrap-server", "localhost:9092",
                        "--group", "storage-consumer", "--reset-offsets", "--to-earliest", "--all-topics", "--execute", timeout=120)
        said = reset.stdout + reset.stderr
        assert "Error" not in said and "plate-waste-events" in said, f"the offsets were not reset: {said}"
        compose("start", "storage-consumer", timeout=120)
        try:
            wait_for(lambda: counts() == held, 300, interval=5, description="the event tables to refill to what they held")
        except AssertionError:
            raise AssertionError(f"the event tables did not refill to what they held: held {held}, now {counts()}, Kafka offsets {offsets} "
                                 f"(Kafka messages beyond the rows, i.e. duplicates in Kafka: {({t: offsets[t] - held[t] for t in offsets})})") from None
        for table in TOPIC_TABLE.values():
            assert sql_int(f"SELECT count(*) - count(DISTINCT event_id) FROM {table}") == 0
    finally:
        compose("start", *SIMULATORS, timeout=120)
    pipeline_is_flowing()


def test_sensor_events_are_not_lost_while_the_bridge_is_down():
    # The case that started all of this (DEF-148): under Kafka Connect the MQTT broker kept nothing for a
    # connector that was not connected, and 470 of 1,266 events were lost across one 45 s outage. The bridge
    # keeps a persistent session, so Mosquitto holds everything published while it is away. The ledger is the
    # simulators' own log of what they published.
    pipeline_is_flowing()
    since = utc_now()
    compose("stop", "mqtt-kafka-bridge", timeout=120)
    time.sleep(45)  # long enough for every simulator to publish several events into the outage
    compose("start", "mqtt-kafka-bridge", timeout=120)
    wait_for(stack.bridge_connected, 120, interval=3, description="the bridge to reconnect")
    assert_every_published_event_was_stored(since)


def test_a_killed_bridge_is_restarted_by_the_platform_and_loses_nothing():
    pipeline_is_flowing()
    since = utc_now()
    crash("mqtt-kafka-bridge")
    time.sleep(15)  # a backlog builds at the broker while it is dead
    wait_for(lambda: healthy_and_running("mqtt-kafka-bridge"), 120, description="the restart policy to bring the bridge back")
    wait_for(stack.bridge_connected, 120, interval=3, description="the bridge to reconnect")
    assert_every_published_event_was_stored(since)


def test_a_kafka_outage_longer_than_the_bridge_will_wait_loses_nothing():
    # The bridge gives up on a message Kafka will not take (the producer times out after 60 s) and exits; its
    # supervisor starts it again and Mosquitto delivers what was never acknowledged. This is that path, run for
    # real: Kafka stopped for 100 s, so the bridge fails and restarts at least once before Kafka returns.
    pipeline_is_flowing()
    since = utc_now()
    compose("stop", "kafka", timeout=120)
    time.sleep(100)
    compose("start", "kafka", timeout=180)
    # 480 s, not 240: Docker's health state (a JVM started every 10 s) took over 240 s once, on a loaded host
    # with every consumer group rejoining, although Kafka had been up for 30 s and the bridge was already
    # forwarding. The loss check below is what this test is about, not how fast a health probe answers.
    wait_for(lambda: stack.healthy("kafka"), 480, description="kafka to be healthy again")
    wait_for(stack.bridge_connected, 240, interval=5, description="the bridge to be connected again")
    pipeline_is_flowing()
    assert restart_count("mqtt-kafka-bridge") >= 1, "the outage did not outlast the bridge's patience, so this test proved nothing"
    assert_every_published_event_was_stored(since)


# ------------------------------------------------------------------ the whole thing is stopped and restarted

def test_stopping_and_restarting_the_whole_stack_preserves_data():
    from stack_fixture import STACK_SERVICES

    pipeline_is_flowing()
    before = total_rows()
    offsets_before = {t: stack.end_offset(t) for t in TOPIC_TABLE}

    compose("down", timeout=300)  # containers and network go; named volumes stay
    # The simulators' logs go with their containers, so the ledger starts here: everything the
    # new simulators publish from their first moment, including the seconds before the bridge
    # has reconnected, which used to be lost.
    since = utc_now()
    compose("up", "-d", "--no-build", *STACK_SERVICES, timeout=600)
    stack.wait_until_ready.cache_clear()
    stack.wait_until_ready()

    assert total_rows() >= before, "rows were lost across a stop/start"
    for topic, offset in offsets_before.items():
        assert stack.end_offset(topic) >= offset, f"{topic} lost messages across a stop/start"
    pipeline_is_flowing()
    assert_every_published_event_was_stored(since)
