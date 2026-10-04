"""
scenario-injection-controller

Design decision (no Phase 1-4 precedent for this component): perturbation
is implemented as a Kafka control-plane topic ('scenario-control-events'),
consumed directly by edge simulators, rather than by mutating Kubernetes
resources (e.g. scaling a Deployment to 0, editing a ConfigMap) at
injection time. Rationale:
  - A Kafka message is consumable identically whether the perturbation
    target is a simulated pod or, later, a real vendor integration
    reacting to the same control topic -- consistent with this project's
    contract-first requirement extended to the injection mechanism itself.
  - Kubernetes-object mutation would require this controller to hold RBAC
    permissions against Deployments/ConfigMaps, which is a broader
    footprint than a single microservice computing findings should need.
  - A scenario needs a defined start AND end (this project's own done
    condition requires an *attributable* finding, which requires the
    perturbation window to be known precisely) -- a Kafka message with
    explicit action=start/action=end pairs models that directly.

Message envelope (not a committed JSON Schema in this revision -- this is
an internal control-plane message, not one of the four Phase 1 event
contracts or the two Phase 5 derived-record contracts, and does not cross
the boundary the contract-first requirement was written to protect. Flagged
here as a deliberate scope decision, not an oversight):

  {
    "scenario_injection_id": "<uuid>",
    "action": "start" | "end",
    "scenario_type": "staffing_shortage" | "order_volume_spike",
    "target": "service-timing" | "plate-waste" | "pos-transaction" | "staff-shift" | "all",
    "started_at": "<iso8601>",
    "parameters": { ... scenario_type-specific ... }
  }

Consumer-side support status, this revision:
  - service-timing: supported (staffing_shortage: removes the stations
    listed). See edge-simulators/simulators/service_timing.py.
  - staff-shift: supported (staffing_shortage: clocks staff out down to
    SHORTAGE_MAX_CLOCKED_IN and holds it there). See edge-simulators/
    simulators/staff_shift.py. The kitchen's backlog capacity depends on
    the staffing level (edge-simulators/common/staffing.py), so a shortage
    slows the kitchen because staffing fell, which makes `staffing_level`
    the true cause in the data the causal engine analyses (DEF-141).
  - plate-waste, pos-transaction: NOT supported -- no consumption hook.
    A scenario targeting only these is published to Kafka but has no
    observable effect.

Use --target all to reach both the kitchen and the staff with one scenario:
each simulator answers to its own name and to "all".
"""
import argparse
import json
import sys
import time
import uuid

from kafka import KafkaProducer

sys.path.insert(0, __file__.rsplit("/", 2)[0])
import phase5_common as common

CONTROL_TOPIC = "scenario-control-events"

SCENARIO_PARAM_SCHEMAS = {
    "staffing_shortage": {"stations_removed": list},
    "order_volume_spike": {"multiplier": float},
}


def _producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=common.KAFKA_BOOTSTRAP_SERVERS,
        api_version=common.KAFKA_API_VERSION,
        **common.KAFKA_TLS_KWARGS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def start_scenario(scenario_type: str, target: str, parameters: dict) -> str:
    if scenario_type not in SCENARIO_PARAM_SCHEMAS:
        raise ValueError(f"Unknown scenario_type: {scenario_type}")
    scenario_injection_id = str(uuid.uuid4())
    message = {
        "scenario_injection_id": scenario_injection_id,
        "action": "start",
        "scenario_type": scenario_type,
        "target": target,
        "started_at": common.now_iso(),
        "parameters": parameters,
    }
    producer = _producer()
    producer.send(CONTROL_TOPIC, value=message)
    producer.flush()
    print(f"Started scenario_injection_id={scenario_injection_id} "
          f"({scenario_type} -> {target}, parameters={parameters})")
    return scenario_injection_id


def end_scenario(scenario_injection_id: str, target: str):
    message = {
        "scenario_injection_id": scenario_injection_id,
        "action": "end",
        "target": target,
        "ended_at": common.now_iso(),
    }
    producer = _producer()
    producer.send(CONTROL_TOPIC, value=message)
    producer.flush()
    print(f"Ended scenario_injection_id={scenario_injection_id}")


def run_timed_scenario(scenario_type: str, target: str, parameters: dict, duration_seconds: int):
    """
    Convenience path for the Phase 5 done-condition test: inject a known
    scenario for a fixed duration, then end it automatically, printing the
    scenario_injection_id so it can be cross-referenced against whatever
    CausalFinding the causal-engine later produces with a matching
    scenario_injection_id -- this is the actual mechanism for checking the
    done condition ("an injected scenario produces a correctly attributed
    finding, not a raw correlation").
    """
    scenario_injection_id = start_scenario(scenario_type, target, parameters)
    print(f"Running for {duration_seconds}s...")
    time.sleep(duration_seconds)
    end_scenario(scenario_injection_id, target)
    return scenario_injection_id


def main():
    parser = argparse.ArgumentParser(description="Phase 5 scenario-injection controller")
    sub = parser.add_subparsers(dest="command", required=True)

    p_run = sub.add_parser("run", help="Start a scenario, wait, then end it")
    p_run.add_argument("--scenario-type", required=True, choices=list(SCENARIO_PARAM_SCHEMAS.keys()))
    p_run.add_argument("--target", required=True)
    p_run.add_argument("--duration-seconds", type=int, default=300)
    p_run.add_argument("--stations-removed", nargs="*", default=[])
    p_run.add_argument("--multiplier", type=float, default=None)

    p_start = sub.add_parser("start", help="Start a scenario without an automatic end")
    p_start.add_argument("--scenario-type", required=True, choices=list(SCENARIO_PARAM_SCHEMAS.keys()))
    p_start.add_argument("--target", required=True)
    p_start.add_argument("--stations-removed", nargs="*", default=[])
    p_start.add_argument("--multiplier", type=float, default=None)

    p_end = sub.add_parser("end", help="End a previously started scenario")
    p_end.add_argument("--scenario-injection-id", required=True)
    p_end.add_argument("--target", required=True)

    args = parser.parse_args()

    if args.command in ("run", "start"):
        parameters = {}
        if args.scenario_type == "staffing_shortage":
            parameters["stations_removed"] = args.stations_removed
        elif args.scenario_type == "order_volume_spike":
            if args.multiplier is None:
                parser.error("--multiplier is required for order_volume_spike")
            parameters["multiplier"] = args.multiplier

        if args.command == "run":
            run_timed_scenario(args.scenario_type, args.target, parameters, args.duration_seconds)
        else:
            start_scenario(args.scenario_type, args.target, parameters)
    elif args.command == "end":
        end_scenario(args.scenario_injection_id, args.target)


if __name__ == "__main__":
    main()
