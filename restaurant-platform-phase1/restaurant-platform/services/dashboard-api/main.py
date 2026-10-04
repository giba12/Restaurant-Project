"""
dashboard-api (Phase 7)

Read-only API over the digital twin's current-state tables and the
Phase 5/6 findings tables, for the React dashboard to call. Unlike
finding-narrator, nothing constrains which tables this service may read
-- it reuses the ordinary restaurant_app role, not a restricted one --
since there's no equivalent "must not see raw data" boundary for a
dashboard the way there is for the LLM narrator.

No write endpoints. This service only ever SELECTs.
"""
import contextlib
import os
import secrets as _secrets
import sys
from datetime import datetime, timedelta, timezone

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import phase5_common as common
import comparison

# Set from a Secret (see k8s/dashboard/templates/deployment.yaml), never a
# chart default -- there is no working placeholder for this the way there
# is for a database password, since the whole point is that nothing except
# nginx's own proxy (see services/dashboard-web/nginx.conf.template) should
# know it. compare_digest, not `==`, so response timing cannot leak how
# many leading characters of a guess were correct.
API_KEY = os.environ.get("API_KEY", "")


def require_api_key(request: Request, x_api_key: str = Header(default="")) -> None:
    if request.url.path == "/api/health":
        return  # unauthenticated on purpose: reveals nothing, lets a plain uptime check work
    if not API_KEY:
        # Fail loud, not open: an unset API_KEY means the deployment forgot to
        # configure one, which must not be indistinguishable from "auth disabled".
        raise HTTPException(status_code=500, detail="server misconfigured: API_KEY is not set")
    if not _secrets.compare_digest(x_api_key, API_KEY):
        raise HTTPException(status_code=401, detail="missing or invalid X-API-Key")


app = FastAPI(title="restaurant-platform dashboard-api", dependencies=[Depends(require_api_key)])

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # local dev (a Vite dev server on a different port) only; browsers
    allow_methods=["GET"],  # calling dashboard-web's own origin never trigger CORS at all,
    allow_headers=["*"],  # since nginx proxies /api/ same-origin -- see nginx.conf.template
)


def _rows_as_dicts(cur) -> list[dict]:
    columns = [desc[0] for desc in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


@app.get("/api/health")
def health():
    return {"status": "ok"}


@app.get("/api/twin/tables")
def twin_tables():
    conn = common.pg_connect()
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM twin_table_state ORDER BY table_id")
        return _rows_as_dicts(cur)


@app.get("/api/twin/staff")
def twin_staff():
    conn = common.pg_connect()
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM twin_staff_state ORDER BY staff_id")
        return _rows_as_dicts(cur)


@app.get("/api/twin/stations")
def twin_stations():
    conn = common.pg_connect()
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM twin_station_state ORDER BY station_id")
        return _rows_as_dicts(cur)


@app.get("/api/findings/narrated")
def narrated_findings(limit: int = 20):
    if not 1 <= limit <= 200:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 200")
    conn = common.pg_connect()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT n.finding_id, n.restaurant_id, n.narrative_text, n.model_used, n.narrated_at,
                   f.treatment_variable, f.outcome_variable, f.effect_estimate, f.effect_estimate_unit,
                   f.refutation_passed
            FROM narrated_findings n
            JOIN causal_findings f ON f.finding_id = n.finding_id
            ORDER BY n.narrated_at DESC
            LIMIT %(limit)s
            """,
            {"limit": limit},
        )
        return _rows_as_dicts(cur)


@app.get("/api/anomalies/summary")
def anomalies_summary():
    conn = common.pg_connect()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT detection_method, severity, count(*) AS count
            FROM anomaly_events
            GROUP BY detection_method, severity
            ORDER BY detection_method, severity
            """
        )
        return _rows_as_dicts(cur)


@app.get("/api/edge/plate-waste")
def edge_plate_waste(minutes: int = Query(default=60, ge=1, le=10080, description="How far back to look.")):
    """
    The plate-waste edge nodes as a fleet: for each node and the exact model it
    ran, how many estimates it produced, how many it flagged untrustworthy, how
    often its drift monitor was alarming, how long inference took, and whether
    it is alarming right now. Ground truth does not exist in the field, so
    health here means the node's own self-assessment, not accuracy.
    """
    with contextlib.closing(common.pg_connect()) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT source_id,
                   raw_payload #>> '{edge_inference,model_id}'      AS model_id,
                   raw_payload #>> '{edge_inference,model_version}' AS model_version,
                   raw_payload #>> '{edge_inference,model_sha256}'  AS model_sha256,
                   count(*)                                         AS readings,
                   count(*) FILTER (WHERE (raw_payload #>> '{edge_inference,out_of_distribution}')::boolean) AS out_of_distribution,
                   count(*) FILTER (WHERE (raw_payload #>> '{edge_inference,drift_suspected}')::boolean)     AS drift_suspected,
                   (percentile_cont(0.5)  WITHIN GROUP (ORDER BY (raw_payload #>> '{edge_inference,inference_latency_ms}')::float))::float AS latency_ms_p50,
                   (percentile_cont(0.95) WITHIN GROUP (ORDER BY (raw_payload #>> '{edge_inference,inference_latency_ms}')::float))::float AS latency_ms_p95,
                   avg(estimated_waste_grams)::float                AS mean_estimated_grams,
                   max("timestamp")                                 AS last_reading_at,
                   (array_agg((raw_payload #>> '{edge_inference,drift_suspected}')::boolean ORDER BY "timestamp" DESC))[1] AS drifting_now
            FROM plate_waste_events
            WHERE "timestamp" >= now() - make_interval(mins => %(minutes)s)
              AND raw_payload ? 'edge_inference'
            GROUP BY source_id, model_id, model_version, model_sha256
            ORDER BY last_reading_at DESC
            """,
            {"minutes": minutes},
        )
        nodes = _rows_as_dicts(cur)
    for node in nodes:
        node["out_of_distribution_rate"] = round(node["out_of_distribution"] / node["readings"], 4)
        node["drift_rate"] = round(node["drift_suspected"] / node["readings"], 4)
    return {"window_minutes": minutes, "nodes": nodes}


def _fetch_event_rows(since: datetime, source_id: str | None) -> list[tuple]:
    """Player and crew stage events since `since` (the player's own, if source_id is given)."""
    with contextlib.closing(common.pg_connect()) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT source_kind, stage, elapsed_since_previous_stage_ms
            FROM service_timing_events
            WHERE source_kind IN ('player', 'crew')
              AND stage <> 'order_fired'
              AND elapsed_since_previous_stage_ms IS NOT NULL
              AND "timestamp" >= %(since)s
              AND (source_kind = 'crew' OR %(source_id)s::text IS NULL OR source_id = %(source_id)s)
            LIMIT 100000
            """,
            {"since": since, "source_id": source_id},
        )
        return cur.fetchall()


def _fetch_summary_rows(since: datetime, hours: int) -> list[tuple]:
    """Completed interactive tickets since `since`, and simulated ones from the last `hours`."""
    with contextlib.closing(common.pg_connect()) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT origin, time_to_cook_start_ms, cook_duration_ms, pickup_delay_ms,
                   service_delay_ms, total_ticket_duration_ms
            FROM ticket_timing_summaries
            WHERE is_complete
              AND ((origin = 'interactive' AND computed_at >= %(since)s)
                OR (origin = 'simulated' AND computed_at >= now() - make_interval(hours => %(hours)s)))
            LIMIT 100000
            """,
            {"since": since, "hours": hours},
        )
        return cur.fetchall()


@app.get("/api/comparison")
def get_comparison(
    source_id: str | None = Query(default=None, pattern=r"^[a-z0-9][a-z0-9-]{0,63}$",
                                  description="Compare this one player's events (the source_id its events carry); default: all players."),
    since: datetime | None = Query(default=None, description="Start of the interactive window, ISO 8601; default: `hours` ago."),
    hours: int = Query(default=24, ge=1, le=720, description="How far back the simulated reference reaches."),
):
    """Interactive timings against the simulated restaurant, and a player against the crew."""
    now = datetime.now(timezone.utc)
    if since is None:
        since = now - timedelta(hours=hours)
    elif since.tzinfo is None:
        since = since.replace(tzinfo=timezone.utc)
    scope = {"source_id": source_id, "since": since.isoformat(), "reference_hours": hours}
    return comparison.build(_fetch_event_rows(since, source_id), _fetch_summary_rows(since, hours), scope)
