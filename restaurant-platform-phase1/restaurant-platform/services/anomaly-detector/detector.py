"""
anomaly-detector

Consumes 'ticket-timing-summaries' (only is_complete=true summaries are
used -- an in-progress ticket's duration fields are provisional per the
schema's own description and must not feed detection). Maintains a
per-(station_id, metric_name) rolling window and runs two independent
detection modes against it:

  1. control_limit: mean +/- CONTROL_LIMIT_SIGMA * stddev over the rolling
     window. Simple, interpretable, cheap -- runs on every completed
     ticket.
  2. isolation_forest: scikit-learn IsolationForest refit periodically
     (every ISOLATION_FOREST_REFIT_EVERY completed tickets per station)
     over the window's full feature vector (all tracked metrics jointly,
     not one at a time) -- catches joint/multivariate anomalies that
     control limits on a single metric would miss.

Both modes are independent and can each emit a separate AnomalyEvent for
the same underlying ticket if both flag it -- this is intentional; the
schema's detection_method field exists precisely so downstream consumers
can distinguish which method produced which finding rather than the
detector suppressing one in favor of the other.

Quarantine: a summary whose `origin` is in QUARANTINE_ORIGINS (default
"interactive": tickets from a human-driven session and its crew) is neither
evaluated nor added to any window. Those tickets run at a different pace and
volume from the simulators -- at up to ~90x the rate, a few minutes of play
would otherwise replace a station's whole rolling window and make ordinary
tickets look anomalous. They are still stored (ticket_timing_summaries) and
can be compared against the rest; they just cannot move the baseline.
Summaries with no `origin` (older producers) count as "simulated".

Cold-start behavior: both modes require MIN_WINDOW_SIZE observations
before producing any output for a given (station_id, metric_name) pair.
Before that, tickets are recorded into the window but not evaluated -- an
empty/short window is not grounds for either flagging or suppressing an
anomaly, since there is no real baseline yet.
"""
import collections
import json
import logging
import os
import sys

from kafka import KafkaConsumer, KafkaProducer
import jsonschema
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import phase5_common as common

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("anomaly-detector")

SUMMARY_TOPIC = "ticket-timing-summaries"
ANOMALY_TOPIC = "anomaly-events"
SOURCE_ID = "anomaly-detector-01"

TRACKED_METRICS = ["time_to_cook_start_ms", "cook_duration_ms", "pickup_delay_ms", "service_delay_ms"]

WINDOW_SIZE = int(os.environ.get("ANOMALY_WINDOW_SIZE", "200"))
MIN_WINDOW_SIZE = int(os.environ.get("ANOMALY_MIN_WINDOW_SIZE", "30"))
CONTROL_LIMIT_SIGMA = float(os.environ.get("CONTROL_LIMIT_SIGMA", "3.0"))
ISOLATION_FOREST_REFIT_EVERY = int(os.environ.get("ISOLATION_FOREST_REFIT_EVERY", "20"))
ISOLATION_FOREST_CONTAMINATION = float(os.environ.get("ISOLATION_FOREST_CONTAMINATION", "0.05"))
QUARANTINE_ORIGINS = {o.strip() for o in os.environ.get("QUARANTINE_ORIGINS", "interactive").split(",") if o.strip()}
STATUS_LOG_EVERY = 25  # log window sizes once per this many quarantined tickets


def is_quarantined(summary: dict) -> bool:
    return summary.get("origin", "simulated") in QUARANTINE_ORIGINS


class StationWindow:
    """
    Rolling window of completed-ticket feature vectors for one station_id.
    Backs both detection modes so they share the same underlying data
    rather than each independently re-querying or re-accumulating it.
    """

    def __init__(self):
        self.rows: collections.deque = collections.deque(maxlen=WINDOW_SIZE)
        self._since_last_fit = 0
        self._forest = None

    def add(self, summary: dict):
        self.rows.append(summary)
        self._since_last_fit += 1

    def control_limit_check(self, metric_name: str, value) -> tuple | None:
        values = [r[metric_name] for r in self.rows if r.get(metric_name) is not None]
        if len(values) < MIN_WINDOW_SIZE:
            return None
        mean = float(np.mean(values))
        std = float(np.std(values))
        lower = mean - CONTROL_LIMIT_SIGMA * std
        upper = mean + CONTROL_LIMIT_SIGMA * std
        if value < lower or value > upper:
            return (lower, upper)
        return None

    def isolation_forest_check(self, summary: dict) -> float | None:
        """
        Returns an anomaly score (scikit-learn convention: more negative =
        more anomalous) if this summary is flagged, else None. Refits the
        forest only every ISOLATION_FOREST_REFIT_EVERY calls -- refitting
        on every single ticket is unnecessary compute for a model whose
        baseline should only need to drift slowly.
        """
        if len(self.rows) < MIN_WINDOW_SIZE:
            return None

        matrix = np.array(
            [[r.get(m) for m in TRACKED_METRICS] for r in self.rows if all(r.get(m) is not None for m in TRACKED_METRICS)]
        )
        if matrix.shape[0] < MIN_WINDOW_SIZE:
            return None

        if self._forest is None or self._since_last_fit >= ISOLATION_FOREST_REFIT_EVERY:
            from sklearn.ensemble import IsolationForest

            self._forest = IsolationForest(contamination=ISOLATION_FOREST_CONTAMINATION, random_state=42)
            self._forest.fit(matrix)
            self._since_last_fit = 0

        if any(summary.get(m) is None for m in TRACKED_METRICS):
            return None

        point = np.array([[summary[m] for m in TRACKED_METRICS]])
        prediction = self._forest.predict(point)[0]  # -1 = anomaly, 1 = normal
        score = float(self._forest.decision_function(point)[0])
        return score if prediction == -1 else None


def _severity_from_deviation(value, lower, upper) -> str:
    span = max(upper - lower, 1e-9)
    deviation = max(lower - value, value - upper, 0) / span
    if deviation > 1.0:
        return "high"
    if deviation > 0.3:
        return "medium"
    return "low"


def _severity_from_score(score: float) -> str:
    # sklearn's decision_function: more negative = more anomalous. These
    # thresholds are a starting calibration, not derived from real data --
    # revisit once genuine anomaly volume is observed in Phase 5 testing.
    if score < -0.15:
        return "high"
    if score < -0.05:
        return "medium"
    return "low"


def build_control_limit_event(metric_name, summary, value, bounds) -> dict:
    lower, upper = bounds
    return {
        "anomaly_id": common.new_event_id(),
        "event_type": "AnomalyEvent",
        "schema_version": common.SCHEMA_VERSION,
        "source_id": SOURCE_ID,
        "detected_at": common.now_iso(),
        "restaurant_id": summary["restaurant_id"],
        "detection_method": "control_limit",
        "metric_name": metric_name,
        "scope": {"station_id": summary.get("station_id"), "ticket_id": summary["ticket_id"], "table_id": summary.get("table_id"), "staff_id": None},
        "window_start": summary["order_time"],
        "window_end": summary["computed_at"],
        "observed_value": float(value),
        "expected_range": {"lower": lower, "upper": upper},
        "anomaly_score": None,
        "severity": _severity_from_deviation(value, lower, upper),
        "contributing_event_ids": [],
    }


def build_isolation_forest_event(summary, score) -> dict:
    return {
        "anomaly_id": common.new_event_id(),
        "event_type": "AnomalyEvent",
        "schema_version": common.SCHEMA_VERSION,
        "source_id": SOURCE_ID,
        "detected_at": common.now_iso(),
        "restaurant_id": summary["restaurant_id"],
        "detection_method": "isolation_forest",
        "metric_name": "joint:" + "+".join(TRACKED_METRICS),
        "scope": {"station_id": summary.get("station_id"), "ticket_id": summary["ticket_id"], "table_id": summary.get("table_id"), "staff_id": None},
        "window_start": summary["order_time"],
        "window_end": summary["computed_at"],
        "observed_value": float(summary["total_ticket_duration_ms"] or 0),
        "expected_range": None,
        "anomaly_score": score,
        "severity": _severity_from_score(score),
        "contributing_event_ids": [],
    }


def insert_anomaly(conn, event: dict):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO anomaly_events (
                anomaly_id, event_type, schema_version, source_id, detected_at, restaurant_id,
                detection_method, metric_name, station_id, table_id, staff_id, ticket_id,
                window_start, window_end, observed_value,
                expected_range_lower, expected_range_upper, anomaly_score, severity,
                contributing_event_ids, raw_payload
            ) VALUES (
                %(anomaly_id)s, %(event_type)s, %(schema_version)s, %(source_id)s, %(detected_at)s, %(restaurant_id)s,
                %(detection_method)s, %(metric_name)s, %(station_id)s, %(table_id)s, %(staff_id)s, %(ticket_id)s,
                %(window_start)s, %(window_end)s, %(observed_value)s,
                %(expected_range_lower)s, %(expected_range_upper)s, %(anomaly_score)s, %(severity)s,
                %(contributing_event_ids)s, %(raw_payload)s
            );
            """,
            {
                **event,
                "station_id": event["scope"].get("station_id"),
                "table_id": event["scope"].get("table_id"),
                "staff_id": event["scope"].get("staff_id"),
                "ticket_id": event["scope"].get("ticket_id"),
                "expected_range_lower": (event.get("expected_range") or {}).get("lower"),
                "expected_range_upper": (event.get("expected_range") or {}).get("upper"),
                "contributing_event_ids": event.get("contributing_event_ids") or [],
                "raw_payload": json.dumps(event),
            },
        )
    conn.commit()


def main():
    anomaly_schema = common.load_schema("AnomalyEvent.schema.json")

    consumer = KafkaConsumer(
        SUMMARY_TOPIC,
        bootstrap_servers=common.KAFKA_BOOTSTRAP_SERVERS,
        api_version=common.KAFKA_API_VERSION,
        **common.KAFKA_TLS_KWARGS,
        group_id="anomaly-detector",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        enable_auto_commit=False,
    )
    producer = KafkaProducer(
        bootstrap_servers=common.KAFKA_BOOTSTRAP_SERVERS,
        api_version=common.KAFKA_API_VERSION,
        **common.KAFKA_TLS_KWARGS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    conn = common.pg_connect()
    windows: dict[str, StationWindow] = collections.defaultdict(StationWindow)
    quarantined = 0

    log.info("anomaly-detector started, consuming %s (quarantining origins: %s)",
             SUMMARY_TOPIC, sorted(QUARANTINE_ORIGINS) or "none")
    for msg in consumer:
        summary = msg.value
        try:
            if not summary.get("is_complete"):
                consumer.commit()
                continue

            if is_quarantined(summary):
                quarantined += 1
                if quarantined % STATUS_LOG_EVERY == 1:
                    log.info(
                        "quarantined %d %s ticket(s) so far; baseline windows untouched: %s",
                        quarantined, summary.get("origin"), {s: len(w.rows) for s, w in sorted(windows.items())},
                    )
                consumer.commit()
                continue

            station_id = summary.get("station_id") or "unknown"
            window = windows[station_id]

            events_to_emit = []
            for metric_name in TRACKED_METRICS:
                value = summary.get(metric_name)
                if value is None:
                    continue
                bounds = window.control_limit_check(metric_name, value)
                if bounds is not None:
                    events_to_emit.append(build_control_limit_event(metric_name, summary, value, bounds))

            score = window.isolation_forest_check(summary)
            if score is not None:
                events_to_emit.append(build_isolation_forest_event(summary, score))

            # Window is updated AFTER evaluating this summary against the
            # existing baseline -- an anomalous ticket should be judged
            # against the baseline that preceded it, not one already
            # contaminated by including itself.
            window.add(summary)

            for event in events_to_emit:
                jsonschema.validate(instance=event, schema=anomaly_schema)
                insert_anomaly(conn, event)
                producer.send(ANOMALY_TOPIC, value=event)
            producer.flush()
            consumer.commit()
        except jsonschema.ValidationError:
            log.exception("AnomalyEvent failed schema validation")
            raise
        except Exception:
            conn.rollback()
            log.exception("Failed processing summary for ticket_id=%s; offset not committed", summary.get("ticket_id"))
            raise


if __name__ == "__main__":
    main()
