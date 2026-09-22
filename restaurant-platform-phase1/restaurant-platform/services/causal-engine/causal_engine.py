"""
causal-engine

Two independent entry points into this module, matching
CausalFinding.schema.json's own documented "two expected ways a
CausalFinding comes to exist":

  1. run_from_anomaly_stream() -- a long-running Kafka consumer on
     'anomaly-events'. On each AnomalyEvent, looks up a treatment/outcome
     pair for that anomaly's metric_name from TREATMENT_MAP below, queries
     TimescaleDB for the underlying rows in the anomaly's window, and runs
     a DoWhy estimate controlling for the confounders configured for that
     treatment/outcome pair.

  2. run_for_scenario(scenario_injection_id, ...) -- a one-shot CLI path
     for validating a specific scenario-injection-controller run directly
     (rather than waiting for the anomaly detector to notice it), writing
     scenario_injection_id onto the resulting CausalFinding so it can be
     checked against the injected scenario's known ground truth. This is
     the primary mechanism for exercising Phase 5's stated done condition.

TREATMENT_MAP is deliberately small and hand-curated in this revision --
one entry (plate-waste confounding) has real backing data end-to-end
(PlateWasteEvent's confounder_flags, per this project's own design
memory). Additional entries should only be added once the corresponding
raw table/columns are confirmed to exist and to actually carry the
described relationship, not spec'd speculatively ahead of the data.

Every finding this module writes has narrative_ready initially set False.
This is a deliberate gate: refutation_passed is populated as part of the
estimate, but a human/automated quality check flipping narrative_ready to
True is treated as a separate, later step, not something this module does
unilaterally on every estimate it happens to produce.
"""
import json
import logging
import os
import sys

import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
import jsonschema

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import phase5_common as common

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("causal-engine")

ANOMALY_TOPIC = "anomaly-events"
FINDING_TOPIC = "causal-findings-events"
NARRATION_TOPIC = "narration-ready-events"
SOURCE_ID = "causal-engine-01"

# metric_name (as emitted by anomaly-detector) -> causal spec.
# 'query' must select at least: the treatment column, the outcome column,
# and every column listed in 'confounders', from the relevant Phase 4
# hypertable(s), scoped to a [%(window_start)s, %(window_end)s] range.
TREATMENT_MAP = {
    "estimated_waste_grams": {
        "treatment": "to_go_container_used",
        "outcome": "estimated_waste_grams",
        # plate_waste_events.declared_dietary_restriction matches
        # PlateWasteEvent.schema.json's own field name directly -- the
        # column was previously misnamed (dietary_restriction_flag) and
        # storage-consumer wrote into it via the wrong confounder_flags
        # key; both were fixed to use the canonical schema name.
        "confounders": ["portion_size_variant", "declared_dietary_restriction"],
        "query": """
            SELECT
                to_go_container_used,
                estimated_waste_grams,
                portion_size_variant,
                declared_dietary_restriction
            FROM plate_waste_events
            WHERE "timestamp" BETWEEN %(window_start)s AND %(window_end)s
              AND to_go_container_used IS NOT NULL
              AND source_kind <> 'player'  -- human-driven sessions are quarantined from analysis
        """,
        "effect_unit": "grams",
    },
    "pickup_delay_ms": {
        "treatment": "staffing_level",
        "outcome": "pickup_delay_ms",
        "confounders": ["station_id"],
        # staffing_level is derived, not a raw column -- approximated here
        # as the count of distinct staff_id clocked in at the relevant
        # station during the window, joined against ticket_timing_summaries
        # by time overlap. This is a coarse proxy, not a validated causal
        # variable -- flagged for review once real staffing-density data
        # volume exists to check it against.
        #
        # Interactive sessions are quarantined from this analysis: their
        # tickets (t.origin) run at a different pace than the simulators',
        # and a player clocking in would raise the staffing count for every
        # ticket -- each would confound the estimate. Staff events from
        # human-driven sources (source_kind 'player') are not counted either.
        "query": """
            SELECT
                t.pickup_delay_ms,
                t.station_id,
                (
                    SELECT COUNT(DISTINCT s.staff_id)
                    FROM staff_shift_events s
                    WHERE s.shift_action = 'clock_in'
                      AND s.source_kind <> 'player'
                      AND s."timestamp" <= t.picked_up_time
                      AND NOT EXISTS (
                          SELECT 1 FROM staff_shift_events s2
                          WHERE s2.staff_id = s.staff_id
                            AND s2.shift_action = 'clock_out'
                            AND s2."timestamp" BETWEEN s."timestamp" AND t.picked_up_time
                      )
                ) AS staffing_level
            FROM ticket_timing_summaries t
            WHERE t.computed_at BETWEEN %(window_start)s AND %(window_end)s
              AND t.pickup_delay_ms IS NOT NULL
              AND t.origin <> 'interactive'
        """,
        "effect_unit": "milliseconds",
    },
}


def _load_data(conn, spec: dict, window_start: str, window_end: str) -> pd.DataFrame:
    df = pd.read_sql_query(spec["query"], conn, params={"window_start": window_start, "window_end": window_end})
    # psycopg2 opens a transaction on the SELECT and holds it until told otherwise. Left
    # open, this long-lived connection sat "idle in transaction" for hours and blocked
    # every schema migration (ALTER TABLE waits for it). End it as soon as the read is done.
    conn.commit()
    return df


def _run_dowhy(df: pd.DataFrame, treatment: str, outcome: str, confounders: list) -> dict:
    """
    Returns {effect_estimate, confidence_interval, method, refutation_passed}.
    Uses backdoor.linear_regression -- appropriate for a continuous outcome
    and a binary or continuous treatment with a small, explicitly-listed
    confounder set; DoWhy's own default estimator. A propensity-score
    method would be preferable for a strongly imbalanced binary treatment
    -- not implemented in this revision, noted as a possible refinement
    once real class balance is observed.
    """
    from dowhy import CausalModel

    df = df.dropna(subset=[treatment, outcome] + confounders)
    if len(df) < 20:
        raise ValueError(f"Insufficient rows ({len(df)}) after dropna for a DoWhy estimate")

    # Categorical confounders (e.g. portion_size_variant, station_id) need
    # numeric encoding for the linear-regression estimator.
    df = pd.get_dummies(df, columns=[c for c in confounders if df[c].dtype == object], drop_first=True)
    encoded_confounders = [
        c for c in df.columns if c not in (treatment, outcome) and any(c.startswith(orig) for orig in confounders)
    ]

    model = CausalModel(
        data=df,
        treatment=treatment,
        outcome=outcome,
        common_causes=encoded_confounders,
    )
    identified_estimand = model.identify_effect(proceed_when_unidentifiable=True)
    estimate = model.estimate_effect(identified_estimand, method_name="backdoor.linear_regression")

    refutation_passed = None
    try:
        refutation = model.refute_estimate(
            identified_estimand, estimate, method_name="placebo_treatment_refuter"
        )
        # A passing refutation should show the placebo effect much closer
        # to zero than the real estimate -- DoWhy does not itself return a
        # boolean, so this applies a fixed threshold rather than parsing
        # the refuter's free-text summary.
        # bool(...) coerces from numpy.bool_ -- jsonschema's 'boolean' type
        # check rejects numpy's bool subclass even though it's truthy.
        refutation_passed = bool(abs(refutation.new_effect) < 0.25 * abs(estimate.value)) if estimate.value else None
    except Exception:
        log.warning("Refutation step failed; refutation_passed left null", exc_info=True)

    return {
        "effect_estimate": float(estimate.value),
        "confidence_interval": None,  # DoWhy's CI extraction depends on estimator internals not exercised here
        "method": "backdoor.linear_regression",
        "refutation_passed": refutation_passed,
    }


def _build_finding(spec: dict, result: dict, restaurant_id: str, triggering_anomaly_id, scenario_injection_id) -> dict:
    return {
        "finding_id": common.new_event_id(),
        "event_type": "CausalFinding",
        "schema_version": common.SCHEMA_VERSION,
        "source_id": SOURCE_ID,
        "computed_at": common.now_iso(),
        "restaurant_id": restaurant_id,
        "triggering_anomaly_id": triggering_anomaly_id,
        "scenario_injection_id": scenario_injection_id,
        "treatment_variable": spec["treatment"],
        "outcome_variable": spec["outcome"],
        "confounders_controlled": spec["confounders"],
        "effect_estimate": result["effect_estimate"],
        "effect_estimate_unit": spec["effect_unit"],
        "confidence_interval": result["confidence_interval"],
        "method": result["method"],
        "refutation_passed": result["refutation_passed"],
        "narrative_ready": False,  # gate flag; flipped true by a separate review step, not here
        "summary_text": None,
    }


def insert_finding(conn, finding: dict):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO causal_findings (
                finding_id, event_type, schema_version, source_id, computed_at, restaurant_id,
                triggering_anomaly_id, scenario_injection_id, treatment_variable, outcome_variable,
                confounders_controlled, effect_estimate, effect_estimate_unit,
                ci_lower, ci_upper, ci_confidence_level, method, refutation_passed,
                narrative_ready, summary_text, raw_payload
            ) VALUES (
                %(finding_id)s, %(event_type)s, %(schema_version)s, %(source_id)s, %(computed_at)s, %(restaurant_id)s,
                %(triggering_anomaly_id)s, %(scenario_injection_id)s, %(treatment_variable)s, %(outcome_variable)s,
                %(confounders_controlled)s, %(effect_estimate)s, %(effect_estimate_unit)s,
                %(ci_lower)s, %(ci_upper)s, %(ci_confidence_level)s, %(method)s, %(refutation_passed)s,
                %(narrative_ready)s, %(summary_text)s, %(raw_payload)s
            );
            """,
            {
                **finding,
                "ci_lower": (finding.get("confidence_interval") or {}).get("lower"),
                "ci_upper": (finding.get("confidence_interval") or {}).get("upper"),
                "ci_confidence_level": (finding.get("confidence_interval") or {}).get("confidence_level"),
                "raw_payload": json.dumps(finding),
            },
        )
    conn.commit()


def process_anomaly(conn, producer, finding_schema, anomaly: dict, scenario_injection_id=None):
    spec = TREATMENT_MAP.get(anomaly["metric_name"])
    if spec is None:
        log.info("No TREATMENT_MAP entry for metric_name=%s; skipping", anomaly["metric_name"])
        return None

    df = _load_data(conn, spec, anomaly["window_start"], anomaly["window_end"])
    result = _run_dowhy(df, spec["treatment"], spec["outcome"], spec["confounders"])
    finding = _build_finding(
        spec, result, anomaly["restaurant_id"], anomaly.get("anomaly_id"), scenario_injection_id
    )
    jsonschema.validate(instance=finding, schema=finding_schema)
    insert_finding(conn, finding)
    producer.send(FINDING_TOPIC, value=finding)
    producer.flush()
    log.info(
        "Emitted CausalFinding %s: %s -> %s, effect=%.3f %s",
        finding["finding_id"], finding["treatment_variable"], finding["outcome_variable"],
        finding["effect_estimate"], finding["effect_estimate_unit"],
    )
    return finding


def run_reviewer():
    """
    The narrative_ready gate, as its own step -- consumes FINDING_TOPIC and
    flips narrative_ready to true in the database for any finding whose
    refutation_passed is true. Deliberately a separate process from
    process_anomaly/run_for_scenario: per this module's own top-of-file
    docstring, "a human/automated quality check flipping narrative_ready to
    True is treated as a separate, later step, not something this module
    does unilaterally on every estimate it happens to produce." This is
    that step, made real rather than left permanently unimplemented.

    Rule is deliberately the simplest one directly supported by this
    project's own stated position (CausalFinding.schema.json's own
    refutation_passed description: "an estimate that hasn't been
    refutation-tested is a weaker basis for a narrated claim") --
    refutation_passed is Phase 5's only existing signal of estimate
    quality, so it's the only thing this rule checks. Not a stand-in for a
    real human review step; revisit once a stronger quality signal exists.

    Also publishes to NARRATION_TOPIC on every flip -- necessary, not
    optional: FINDING_TOPIC messages are published by process_anomaly()
    before this reviewer ever runs, so narrative_ready is always false in
    that stream. Nothing downstream (Phase 6's narrator) can otherwise
    learn when a finding actually becomes ready. No JSON Schema for this
    topic, same precedent as scenario-control-events in
    scenario-injection-controller's own docstring: an internal
    control-plane signal, not one of the committed event contracts.
    """
    consumer = KafkaConsumer(
        FINDING_TOPIC,
        bootstrap_servers=common.KAFKA_BOOTSTRAP_SERVERS,
        api_version=common.KAFKA_API_VERSION,
        group_id="finding-reviewer",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        enable_auto_commit=False,
    )
    producer = KafkaProducer(
        bootstrap_servers=common.KAFKA_BOOTSTRAP_SERVERS,
        api_version=common.KAFKA_API_VERSION,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    conn = common.pg_connect()

    log.info("finding-reviewer started, consuming %s", FINDING_TOPIC)
    for msg in consumer:
        finding = msg.value
        try:
            if finding.get("refutation_passed") is True:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE causal_findings SET narrative_ready = true WHERE finding_id = %(finding_id)s",
                        {"finding_id": finding["finding_id"]},
                    )
                conn.commit()
                producer.send(
                    NARRATION_TOPIC,
                    value={"finding_id": finding["finding_id"], "restaurant_id": finding.get("restaurant_id")},
                )
                producer.flush()
                log.info("finding_id=%s marked narrative_ready=true (refutation_passed=true)", finding["finding_id"])
            else:
                log.info(
                    "finding_id=%s left narrative_ready=false (refutation_passed=%s)",
                    finding["finding_id"], finding.get("refutation_passed"),
                )
            consumer.commit()
        except Exception:
            conn.rollback()
            log.exception("Failed reviewing finding_id=%s; offset not committed", finding.get("finding_id"))
            raise


def run_from_anomaly_stream():
    finding_schema = common.load_schema("CausalFinding.schema.json")
    consumer = KafkaConsumer(
        ANOMALY_TOPIC,
        bootstrap_servers=common.KAFKA_BOOTSTRAP_SERVERS,
        api_version=common.KAFKA_API_VERSION,
        group_id="causal-engine",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        enable_auto_commit=False,
    )
    producer = KafkaProducer(
        bootstrap_servers=common.KAFKA_BOOTSTRAP_SERVERS,
        api_version=common.KAFKA_API_VERSION,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    conn = common.pg_connect()

    log.info("causal-engine started, consuming %s", ANOMALY_TOPIC)
    for msg in consumer:
        anomaly = msg.value
        try:
            process_anomaly(conn, producer, finding_schema, anomaly)
            consumer.commit()
        except ValueError as e:
            # Insufficient data for an estimate is an expected, non-fatal
            # outcome (e.g. anomaly window too narrow) -- log and move on
            # rather than crash-looping the pod over it.
            log.warning("Skipping anomaly_id=%s: %s", anomaly.get("anomaly_id"), e)
            consumer.commit()
        except jsonschema.ValidationError:
            log.exception("CausalFinding failed schema validation")
            raise
        except Exception:
            conn.rollback()
            log.exception("Failed processing anomaly_id=%s; offset not committed", anomaly.get("anomaly_id"))
            raise


def run_for_scenario(scenario_injection_id: str, metric_name: str, window_start: str, window_end: str, restaurant_id: str):
    """One-shot path: python causal_engine.py --scenario-injection-id ... """
    finding_schema = common.load_schema("CausalFinding.schema.json")
    conn = common.pg_connect()
    producer = KafkaProducer(
        bootstrap_servers=common.KAFKA_BOOTSTRAP_SERVERS,
        api_version=common.KAFKA_API_VERSION,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    synthetic_anomaly = {
        "metric_name": metric_name,
        "restaurant_id": restaurant_id,
        "window_start": window_start,
        "window_end": window_end,
        "anomaly_id": None,
    }
    return process_anomaly(conn, producer, finding_schema, synthetic_anomaly, scenario_injection_id=scenario_injection_id)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--review", action="store_true", help="Run the narrative_ready reviewer instead of the anomaly-stream consumer")
    parser.add_argument("--scenario-injection-id")
    parser.add_argument("--metric-name")
    parser.add_argument("--window-start")
    parser.add_argument("--window-end")
    parser.add_argument("--restaurant-id", default=common.RESTAURANT_ID)
    args = parser.parse_args()

    if args.review:
        run_reviewer()
    elif args.scenario_injection_id:
        if not all([args.metric_name, args.window_start, args.window_end]):
            parser.error("--metric-name, --window-start, --window-end are required with --scenario-injection-id")
        run_for_scenario(
            args.scenario_injection_id, args.metric_name, args.window_start, args.window_end, args.restaurant_id
        )
    else:
        run_from_anomaly_stream()
