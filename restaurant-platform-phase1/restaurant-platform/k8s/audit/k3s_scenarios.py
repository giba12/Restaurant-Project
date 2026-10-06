"""
Disturb the live k3s cluster and check that no sensor event was lost (the ledger test, DEF-152).

For each scenario it restarts or kills one part of the ingest path, waits for the pipeline to recover, then
compares what the four simulators logged as published (after Mosquitto's acknowledgement) with what reached the
database. "missing" must be 0. Run it by hand: it briefly interrupts the live stream and can fire alerts.

    python3 k8s/audit/k3s_scenarios.py                 # all six scenarios, about 30 minutes
    python3 k8s/audit/k3s_scenarios.py Mosquitto       # only scenarios whose name contains the word
"""
import os, subprocess, sys, time, datetime as dt
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import k3s_ledger as L

NS = "kafka"
def k(*args, check=True, timeout=900):
    r = subprocess.run(["kubectl", "-n", NS, *args], capture_output=True, text=True, timeout=timeout)
    if check and r.returncode != 0: raise RuntimeError(f"kubectl {args}: {r.stderr[:300]}")
    return r
def now(): return dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
def log(msg): print(f"{now():%H:%M:%S} {msg}", flush=True)

def bridge_healthy():
    return k("exec", "deploy/mqtt-kafka-bridge", "--", "python", "bridge.py", "--check", check=False).returncode == 0
def wait_bridge(timeout=420):
    end = time.time() + timeout
    while time.time() < end:
        if bridge_healthy(): return True
        time.sleep(5)
    return False
def ingest_age():
    r = subprocess.run(["kubectl", "exec", "-i", "-n", NS, "timescaledb-0", "--", "sh", "-c", 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -At'],
                       input="SELECT round(extract(epoch from now()-max(ingested_at))::numeric) FROM pos_transaction_events;", capture_output=True, text=True)
    try: return int(r.stdout.strip())
    except ValueError: return 10**9
def wait_ingesting(timeout=900):
    end = time.time() + timeout
    while time.time() < end:
        if ingest_age() < 30: return True
        time.sleep(10)
    return False

def scenario(name, action, ready):
    log(f"=== {name}")
    since = now()
    action()
    ok = ready()
    log(f"{name}: recovered={ok}")
    time.sleep(90)
    for attempt in range(1, 5):
        pub, missing = L.ledger(since, settle_seconds=45, label=name)
        if not missing: break
        log(f"{name}: {len(missing)} missing on look {attempt}; waiting for in-flight messages")
        time.sleep(90)
    return name, len(pub), len(missing), [(a.strftime('%H:%M:%S'), s) for a, s, _ in missing[:6]]

def bridge_ready(): k("rollout", "status", "deploy/mqtt-kafka-bridge", "--timeout=420s"); return wait_bridge()
def mosquitto_ready(): k("rollout", "status", "deploy/mosquitto", "--timeout=420s"); ok = wait_bridge(); return ok and wait_ingesting()
def kafka_ready():
    k("wait", "--for=condition=Ready", "pod/restaurant-platform-kafka-dev-pool-0", "--timeout=900s")
    return wait_bridge() and wait_ingesting(1200)

def bridge_down_90():
    k("scale", "deploy/mqtt-kafka-bridge", "--replicas=0"); time.sleep(90); k("scale", "deploy/mqtt-kafka-bridge", "--replicas=1")

def pod(app): return k("get", "pod", "-l", f"app={app}", "-o", "jsonpath={.items[0].metadata.name}").stdout.strip()

scenarios = [
 ("bridge restarted (graceful)", lambda: k("rollout", "restart", "deploy/mqtt-kafka-bridge"), bridge_ready),
 ("bridge killed hard (force delete)", lambda: k("delete", "pod", pod("mqtt-kafka-bridge"), "--grace-period=0", "--force"), bridge_ready),
 ("bridge stopped 90 s", bridge_down_90, bridge_ready),
 ("Mosquitto restarted (graceful)", lambda: k("rollout", "restart", "deploy/mosquitto"), mosquitto_ready),
 ("Mosquitto killed hard (force delete)", lambda: k("delete", "pod", pod("mosquitto"), "--grace-period=0", "--force"), mosquitto_ready),
 ("Kafka broker restarted", lambda: k("delete", "pod", "restaurant-platform-kafka-dev-pool-0"), kafka_ready),
]
only = sys.argv[1:] 
results = []
for name, action, ready in scenarios:
    if only and not any(o in name for o in only): continue
    try: results.append(scenario(name, action, ready))
    except Exception as exc:
        log(f"{name}: ERROR {exc}"); results.append((name, -1, -1, [str(exc)[:120]]))
print("\nRESULTS", flush=True)
for r in results: print(f"  {r[0]:42s} published {r[1]:5d}  missing {r[2]:3d}  {r[3] if r[2] else ''}", flush=True)
print("DONE", flush=True)
