"""
What the plate-waste node costs to run, measured inside its own container under the limits it is deployed with.

Meant to be piped into the node's image, so it sees the same Python, numpy and cgroup limits the node does:

    docker run --rm -i --cpus 0.1 --memory 96m --memory-swap 96m -e SCHEMA_DIR=/app/schemas \\
        --entrypoint python local/edge-simulator:1.0 - < edge-simulators/footprint_probe.py

(k8s/edge-simulators/values.yaml gives each simulator 100m CPU and 96Mi.) Prints one JSON object.

What this is and is not: the same code under a CPU and memory ceiling, which shows what the node needs and how it
behaves when the CPU is rationed. It is not a different processor, clock speed or instruction set, so it says
nothing about a microcontroller or a slow ARM core; that needs the hardware.
"""
import json
import os
import random
import sys
import time

sys.path.insert(0, os.getcwd())


def rss_mib():
    with open("/proc/self/status") as f:
        for line in f:
            if line.startswith("VmRSS:"):
                return round(int(line.split()[1]) / 1024, 1)


def cgroup(name):
    for base in ("/sys/fs/cgroup/", "/sys/fs/cgroup/memory/"):
        try:
            return open(base + name).read().strip()
        except OSError:
            continue
    return None


def throttling():
    stats = {}
    for line in (cgroup("cpu.stat") or "").splitlines():
        key, _, value = line.partition(" ")
        stats[key] = int(value)
    return stats


def percentiles(values):
    ordered = sorted(values)
    pick = lambda q: round(ordered[min(len(ordered) - 1, int(q * len(ordered)))], 4)
    return {"p50": pick(0.50), "p95": pick(0.95), "p99": pick(0.99), "max": round(ordered[-1], 4)}


out = {"cpu_max": cgroup("cpu.max"), "memory_max": cgroup("memory.max")}
out["rss_mib_python_alone"] = rss_mib()

# The real node: importing the simulator pulls in paho, jsonschema and the runtime, exactly as in service.
from simulators import plate_waste  # noqa: E402
from edge_ai import model as edge_model  # noqa: E402
from edge_ai import sensor  # noqa: E402

out["rss_mib_after_imports"] = rss_mib()
node = plate_waste.PlateWasteNode(edge_model.EdgeModel.from_file(), rng=random.Random(1), node_id="probe", control_key="probe-key")
out["rss_mib_node_ready"] = rss_mib()
for _ in range(500):
    node.next_event()
out["rss_mib_after_500_events"] = rss_mib()

# Inference as the node really runs it: one reading every few seconds, so the CPU quota has time to refill.
rng = random.Random(2)
vectors = [sensor.feature_vector(sensor.read_sensors(sensor.true_waste_grams(rng, False), rng)) for _ in range(400)]
before = throttling()
paced = []
for v in vectors:
    start = time.perf_counter()
    node.model.infer(v)
    paced.append((time.perf_counter() - start) * 1000.0)
    time.sleep(0.03)
out["inference_ms_paced"] = percentiles(paced)

# The same call back to back: this is where a CPU quota shows, as the container is paused until the next period.
# Run until a fixed amount of CPU time has been used, not a fixed number of calls: a fast machine would otherwise
# finish inside one quota period and show nothing (a GitHub runner is about ten times quicker than a laptop).
burst = []
wall = time.perf_counter()
cpu = time.process_time()
while time.process_time() - cpu < 0.3:
    for v in vectors:
        start = time.perf_counter()
        node.model.infer(v)
        burst.append((time.perf_counter() - start) * 1000.0)
out["inference_ms_burst"] = percentiles(burst)
out["burst_wall_seconds"] = round(time.perf_counter() - wall, 3)
out["burst_cpu_seconds"] = round(time.process_time() - cpu, 3)
after = throttling()
out["cpu_periods_throttled"] = after.get("nr_throttled", 0) - before.get("nr_throttled", 0)

# What taking a model update costs: every check up to the shadow comparison, and the shadow comparison itself.
sys.modules.pop("control", None)
from control import edge_control as control  # noqa: E402

store = control.ModelStore()
statuses = []
node.updater.on_status = statuses.append
command = control.build_set_model(store, "1.0.0", "probe-key", shadow_readings=20, now=time.time())
raw = json.dumps(command).encode()
rss_before = rss_mib()
start = time.perf_counter()
node.updater.handle(raw)
out["update_checks_ms"] = round((time.perf_counter() - start) * 1000.0, 1)
out["update_checks_state"] = statuses[-1]["state"] if statuses else None
shadow = []
for _ in range(20):
    start = time.perf_counter()
    node.next_event()
    shadow.append((time.perf_counter() - start) * 1000.0)
out["event_ms_while_shadowing"] = percentiles(shadow)
out["update_final_state"] = statuses[-1]["state"] if statuses else None
out["rss_mib_update_extra"] = round(rss_mib() - rss_before, 1)

out["rss_mib_final"] = rss_mib()
out["cgroup_memory_peak_mib"] = round(int(cgroup("memory.peak")) / 1048576, 1) if cgroup("memory.peak") else None
out["artifact_bytes"] = os.path.getsize(edge_model.DEFAULT_MODEL_PATH)
print(json.dumps(out, indent=2))
