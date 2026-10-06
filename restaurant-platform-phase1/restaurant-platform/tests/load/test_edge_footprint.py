"""
What the plate-waste node costs to run, measured in its own image under the limits it is deployed with.

The repository's budgets (16 KB artifact, 5 ms inference, 1 MB to load) describe the model. These tests run the real
node image under the CPU and memory limits the Kubernetes chart gives it (100m CPU, 96Mi: k8s/edge-simulators/
values.yaml) and measure the whole process, through edge-simulators/footprint_probe.py piped into the container.

What this is and is not: the same code with the CPU and memory rationed. It shows what the node needs and how it
behaves when the CPU is rationed. It is not a different processor, clock speed or instruction set, so it says nothing
about a microcontroller or a slow ARM core; that takes the hardware. If the container engine cannot apply the limits
(no cgroup delegation), the tests skip, because there is nothing to measure.

    bash tests/run_stack_tests.sh load
"""
import json

import pytest

from helpers import ROOT, inspect, run

CPU = "0.1"
MEMORY_BYTES = 96 * 1024 * 1024


@pytest.fixture(scope="module")
def footprint():
    image = inspect("edge-sim-plate-waste")["Image"]  # the very image the stack is running
    probe = (ROOT / "edge-simulators" / "footprint_probe.py").read_text()
    result = run(["docker", "run", "--rm", "-i", "--cpus", CPU, "--memory", "96m", "--memory-swap", "96m",
                  "-e", "SCHEMA_DIR=/app/schemas", "--entrypoint", "python", image, "-"],
                 input_text=probe, timeout=240)
    found = json.loads(result.stdout[result.stdout.index("{"):])
    print("\nedge node footprint under 100m CPU and 96Mi:", json.dumps(found, indent=2))
    if found["cpu_max"] != "10000 100000" or found["memory_max"] != str(MEMORY_BYTES):
        pytest.skip(f"the engine did not apply the limits (cpu.max={found['cpu_max']}, memory.max={found['memory_max']})")
    return found


def test_the_whole_node_fits_in_well_under_the_charts_memory_limit(footprint):
    # Measured 41-43 MiB resident (cgroup peak 26 MiB) against 96 MiB. Almost all of it is Python and the libraries
    # (about 31 MiB on import); the model is 5 KB. A loose ceiling: it catches a leak or a heavy new import.
    assert footprint["rss_mib_final"] < 70, "the node needs more memory than comfortably fits its 96Mi limit"
    assert footprint["rss_mib_after_500_events"] - footprint["rss_mib_node_ready"] < 5, "memory grew over 500 events"
    assert footprint["cgroup_memory_peak_mib"] < 0.8 * 96


def test_inference_meets_its_latency_budget_at_the_charts_cpu_limit_at_the_nodes_real_pace(footprint):
    # One reading every few seconds, so the CPU quota has time to refill. Measured p99 0.82 ms against 5 ms.
    assert footprint["inference_ms_paced"]["p99"] <= 5.0


def test_a_cpu_quota_pauses_a_burst_which_is_why_the_budget_is_judged_at_the_nodes_real_pace(footprint):
    # Pinned so the limit is known and not rediscovered: back to back, 0.3 s of work (the probe runs until it has used
    # that much CPU, so a fast machine cannot finish inside one quota period) takes about ten times as long as the
    # container is paused for the rest of each 100 ms period. The node never does this on its own, but a burst of work
    # (an update's checks) causes one such pause. Asserting it keeps the explanation honest.
    assert footprint["cpu_periods_throttled"] > 0
    assert footprint["burst_wall_seconds"] > 3 * footprint["burst_cpu_seconds"]


def test_a_model_update_can_be_taken_inside_the_limits_and_promoted(footprint):
    assert footprint["update_checks_state"] == "shadowing"
    assert footprint["update_final_state"] == "applied"
    assert footprint["update_checks_ms"] < 3000, "an update's checks take too long under the CPU limit"
    assert footprint["rss_mib_update_extra"] < 10, "holding a candidate beside the model in service costs too much memory"
