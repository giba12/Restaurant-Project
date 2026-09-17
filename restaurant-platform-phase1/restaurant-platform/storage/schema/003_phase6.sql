-- Phase 6: LLM narrator output table, digital-twin current-state tables,
-- and the narrator's own restricted database role.
--
-- \getenv (a psql meta-command, not sent to the server) reads the
-- NARRATOR_PGPASSWORD env var -- set from the narrator-credentials Secret
-- by schema-init-job.yaml -- into the narrator_password psql variable
-- used below. Keeps the schema-init Job's command in plain exec-form
-- (no shell wrapper needed just to expand an env var into a psql -v arg).
\getenv narrator_password NARRATOR_PGPASSWORD
--
-- Design note: narrated_findings is a separate table, not a new column on
-- causal_findings. The narrator's whole reason for having its own DB role
-- (below) is that it must never be able to write into a table it's only
-- supposed to read from -- adding its output as a column on causal_findings
-- would require write access there too, defeating the point.

-- ---------------------------------------------------------------------
-- narrated_findings
--
-- Narrator-owned. One row per finding actually narrated. finding_id
-- logically references causal_findings.finding_id, but no FK constraint
-- is declared across the privilege boundary below -- enforcing one would
-- require the narrator's restricted role to hold REFERENCES on
-- causal_findings, a broader grant than the SELECT-only access it
-- actually needs.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS narrated_findings (
    finding_id      UUID PRIMARY KEY,
    restaurant_id   TEXT NOT NULL,
    narrative_text  TEXT NOT NULL,
    model_used      TEXT NOT NULL,
    narrated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------
-- Digital twin current-state tables.
--
-- Three narrow typed tables rather than one JSONB blob, matching this
-- project's established preference (see storage-consumer's typed columns
-- alongside raw_payload). Each is a single current-state row per entity,
-- upserted by digital-twin as raw Phase 3 events arrive -- not an
-- append-only log, so none of these are hypertables (same reasoning as
-- ticket_timing_summaries in 002_phase5_hypertables.sql).
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS twin_table_state (
    table_id            TEXT PRIMARY KEY,
    status              TEXT,
    occupied_since      TIMESTAMPTZ,
    current_ticket_id   TEXT,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS twin_staff_state (
    staff_id            TEXT PRIMARY KEY,
    role                TEXT,
    status              TEXT,
    station_id          TEXT,
    clocked_in_since    TIMESTAMPTZ,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS twin_station_state (
    station_id          TEXT PRIMARY KEY,
    open_ticket_count   INT NOT NULL DEFAULT 0,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------
-- narrator_app: the narrator's restricted role.
--
-- This is the actual enforcement of "the narrator may only ever read
-- causal_findings" -- a database-level guarantee, not just an
-- application-code convention. Idempotent: password kept in sync with
-- the current narrator-credentials Secret on every re-run, matching this
-- project's dev-only static-password convention (see
-- k8s/timescaledb/values.yaml's credentials.password).
-- ---------------------------------------------------------------------
-- Deliberately NOT a DO $$ ... $$ block: psql's :'var' substitution is
-- skipped entirely inside dollar-quoted strings (it has to be -- a DO/
-- function body's own literal $$ content must survive untouched), so a
-- password reference placed inside one is sent to the server as literal,
-- unsubstituted text and fails as a syntax error. Building the DDL as a
-- plain top-level SELECT and handing it to \gexec keeps the substitution
-- in scope while still being conditional (CREATE the first time, ALTER on
-- every later re-run, keeping the role in sync with the current Secret).
SELECT CASE WHEN EXISTS (SELECT FROM pg_roles WHERE rolname = 'narrator_app')
            THEN 'ALTER ROLE narrator_app WITH PASSWORD ' || quote_literal(:'narrator_password')
            ELSE 'CREATE ROLE narrator_app WITH LOGIN PASSWORD ' || quote_literal(:'narrator_password')
       END
\gexec

GRANT CONNECT ON DATABASE restaurant_platform TO narrator_app;
GRANT USAGE ON SCHEMA public TO narrator_app;
GRANT SELECT ON causal_findings TO narrator_app;
GRANT SELECT, INSERT, UPDATE ON narrated_findings TO narrator_app;
