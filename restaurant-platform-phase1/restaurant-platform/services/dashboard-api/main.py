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
import os
import sys

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import phase5_common as common

app = FastAPI(title="restaurant-platform dashboard-api")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # local dev / portfolio demo only -- not a public deployment
    allow_methods=["GET"],
    allow_headers=["*"],
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
