"""
Load and stability tests against the running Docker Compose stack.

A pipeline that works at a trickle can still fall over at volume: a consumer
that processes slower than events arrive builds a backlog forever, a leak that
costs a few MB per thousand events takes a service down in a day, a slow
dashboard makes the whole project feel broken. These tests push realistic
bursts through the real stack and measure what happens.

Numbers are printed (run with -s) so each run leaves a record; the assertions
are deliberately loose floors -- they catch a collapse or a leak, not a 10%
change in speed, which would only make the suite flaky.

    bash tests/run_stack_tests.sh load
"""
import concurrent.futures
import re
import time
import urllib.request
import uuid


import stack_fixture as stack
from helpers import DASHBOARD_PORT, compose, restart_count, run, sql, sql_int, wait_for

BURST_EVENTS = 3000
# A floor, not a target; the measured rate is printed on every run. Measured
# 2026-10-02 at 14.6 events/s end to end (a 3,000-event burst on top of the
# test stack's own ~4.5 events/s of simulator traffic). The ceiling comes from
# the storage consumer committing the database transaction and the Kafka offset
# once per message, on a single thread; batching would raise it. 10 events/s is
# still ~14x the stack's real default traffic (~0.7 events/s), so there is ample
# headroom at this project's scale -- the floor exists to catch a collapse.
# Re-measured 2026-10-03: 11 to 12 events/s while every message paid a 30-60 ms
# schema re-check (DEF-139), then 45 events/s (3,000 events in 66 s) once the
# consumer compiled each schema once. One measurement is not enough to raise
# the floor; the validation cost is guarded directly by
# test_validation_costs_microseconds_a_message_not_tens_of_milliseconds.
MIN_INGEST_EVENTS_PER_SECOND = 10
MAX_MEMORY_GROWTH_MB = 100
# A step that appears once is not a leak. The causal engine imports DoWhy,
# statsmodels and scipy lazily, on its first estimate, which is a measured
# +153 MB step (2026-10-03) that lands inside this test's window whenever the
# first anomaly happens to arrive during it, and not otherwise. Counting that
# as growth made the test pass or fail on timing alone. The engine is allowed
# that one-off import on top of the general bound; every other service is held
# to the general bound.
MEMORY_ALLOWANCE_MB = {"causal-engine": MAX_MEMORY_GROWTH_MB + 160}
PYTHON_SERVICES = ["storage-consumer", "ticket-timing-aggregator", "anomaly-detector", "causal-engine",
                   "finding-reviewer", "digital-twin", "dashboard-api"]

# Runs inside an edge-simulator container: it already has the real event
# generators, kafka-python and the broker address, so the burst is made of real
# (schema-valid) events published straight to Kafka, bypassing MQTT.
BURST_SCRIPT = """
import json, os, sys, time
sys.path.insert(0, "/app")
from kafka import KafkaProducer
from simulators import plate_waste

count, mark = int(sys.argv[1]), sys.argv[2]
producer = KafkaProducer(bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"], api_version=(2, 8, 0),
                         value_serializer=lambda v: json.dumps(v).encode("utf-8"), linger_ms=20)
started = time.time()
for _ in range(count):
    event = plate_waste.generate_event()
    event["source_id"] = mark
    producer.send("plate-waste-events", event)
producer.flush()
print(f"published {count} events in {time.time() - started:.2f}s")
"""


def memory_mb(service):
    out = run(["docker", "stats", "--no-stream", "--format", "{{.MemUsage}}", stack_container(service)]).stdout
    value, unit = re.match(r"([\d.]+)\s*([A-Za-z]+)", out.strip()).groups()
    factor = {"B": 1 / 1048576, "kB": 1 / 1024, "KiB": 1 / 1024, "MB": 1, "MiB": 1, "GB": 1024, "GiB": 1024}[unit]
    return float(value) * factor


def stack_container(service):
    from helpers import container_id

    return container_id(service)


# ------------------------------------------------------------------ data freshness

def test_data_reaches_the_database_within_seconds_of_being_produced():
    # The end-to-end latency a user feels: event timestamp -> row visible.
    wait_for(lambda: sql_int("SELECT count(*) FROM service_timing_events") >= 200, 300, description="200 service-timing rows")
    p95 = float(sql("""
        SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM ingested_at - "timestamp"))
        FROM (SELECT ingested_at, "timestamp" FROM service_timing_events ORDER BY ingested_at DESC LIMIT 200) recent
    """))
    print(f"\nend-to-end latency p95 (event time -> stored): {p95:.2f}s")
    assert p95 < 10.0, f"data is arriving {p95:.1f}s late at the 95th percentile"


# ------------------------------------------------------------------ a burst

def test_a_burst_of_events_is_absorbed_and_fully_stored():
    mark = f"loadtest-{uuid.uuid4().hex[:8]}"
    restarts_before = {s: restart_count(s) for s in PYTHON_SERVICES}
    memory_before = {s: memory_mb(s) for s in PYTHON_SERVICES}

    started = time.time()
    out = compose("exec", "-T", "edge-sim-plate-waste", "python", "-", str(BURST_EVENTS), mark, input_text=BURST_SCRIPT, timeout=300).stdout
    print(f"\n{out.strip()}")

    wait_for(lambda: sql_int(f"SELECT count(*) FROM plate_waste_events WHERE source_id = '{mark}'") >= BURST_EVENTS, 600, interval=2,
             description=f"all {BURST_EVENTS} burst events to be stored")
    elapsed = time.time() - started
    rate = BURST_EVENTS / elapsed
    print(f"burst of {BURST_EVENTS} events fully stored in {elapsed:.1f}s = {rate:.0f} events/s end to end")

    stored = sql_int(f"SELECT count(*) FROM plate_waste_events WHERE source_id = '{mark}'")
    assert stored == BURST_EVENTS, f"{BURST_EVENTS - stored} burst events were lost or duplicated"
    assert rate >= MIN_INGEST_EVENTS_PER_SECOND, f"ingest rate collapsed to {rate:.1f} events/s"

    for service in PYTHON_SERVICES:
        assert restart_count(service) == restarts_before[service], f"{service} crashed under load"
        growth = memory_mb(service) - memory_before[service]
        print(f"  memory growth {service:28s} {growth:+7.1f} MB")
        allowed = MEMORY_ALLOWANCE_MB.get(service, MAX_MEMORY_GROWTH_MB)
        assert growth < allowed, f"{service} grew {growth:.0f} MB under a {BURST_EVENTS}-event burst (allowed {allowed})"


def test_the_backlog_drains_completely_after_a_burst():
    wait_for(lambda: stack.consumer_lag("storage-consumer") == 0, 300, interval=3, description="storage-consumer lag to reach 0")


# ------------------------------------------------------------------ the dashboard under concurrent users

def test_the_dashboard_api_serves_many_concurrent_users_without_errors():
    url = f"http://127.0.0.1:{DASHBOARD_PORT}/api/twin/stations"

    def one_request(_):
        started = time.time()
        with urllib.request.urlopen(url, timeout=15) as response:
            response.read()
            return response.status, time.time() - started

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool:
        results = list(pool.map(one_request, range(400)))
    statuses = [status for status, _ in results]
    latencies = sorted(latency for _, latency in results)
    p95 = latencies[int(len(latencies) * 0.95)]
    print(f"\n400 requests, 20 concurrent: p95 {p95 * 1000:.0f} ms, max {latencies[-1] * 1000:.0f} ms")
    assert statuses.count(200) == len(statuses), f"{len(statuses) - statuses.count(200)} requests failed"
    assert p95 < 2.0, f"p95 latency {p95:.2f}s under 20 concurrent users"
