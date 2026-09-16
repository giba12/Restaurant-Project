-- Phase 5: derived-record tables for the ticket-timing-aggregator,
-- anomaly detector, and causal engine.
--
-- Design note: ticket_timing_summaries is NOT a hypertable. Every other
-- table in this project (001_hypertables.sql) models an append-only event
-- log, where create_hypertable's time-partitioning is appropriate.
-- TicketTimingSummary.schema.json's own description ("updated/finalized as
-- later-stage events arrive") describes mutable per-ticket state, not an
-- append-only log -- the aggregator upserts the same row repeatedly as a
-- ticket progresses through stages. TimescaleDB hypertables support
-- upserts, but the row would be chunk-located by its *original* insert
-- time and updated in place regardless of how computed_at changes on
-- later upserts, which does not match this table's actual write pattern
-- and offers no query benefit here (queries against this table are by
-- ticket_id or station_id, not by time-range scan). Modeled as a plain
-- table instead, PRIMARY KEY (ticket_id).

-- ---------------------------------------------------------------------
-- ticket_timing_summaries
--
-- Written by ticket-timing-aggregator. One row per ticket_id, upserted
-- (ON CONFLICT (ticket_id) DO UPDATE) as each new ServiceTimingEvent
-- stage arrives for that ticket.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ticket_timing_summaries (
    ticket_id                   TEXT PRIMARY KEY,
    summary_id                  UUID NOT NULL,
    event_type                  TEXT NOT NULL DEFAULT 'TicketTimingSummary',
    schema_version              TEXT NOT NULL,
    source_id                   TEXT NOT NULL,
    computed_at                 TIMESTAMPTZ NOT NULL,
    restaurant_id               TEXT NOT NULL,
    station_id                  TEXT,
    table_id                    TEXT,

    order_time                  TIMESTAMPTZ NOT NULL,
    cook_started_time           TIMESTAMPTZ,
    plated_time                 TIMESTAMPTZ,
    picked_up_time              TIMESTAMPTZ,
    delivered_time              TIMESTAMPTZ,

    time_to_cook_start_ms       BIGINT CHECK (time_to_cook_start_ms >= 0),
    cook_duration_ms            BIGINT CHECK (cook_duration_ms >= 0),
    pickup_delay_ms             BIGINT CHECK (pickup_delay_ms >= 0),
    service_delay_ms            BIGINT CHECK (service_delay_ms >= 0),
    total_ticket_duration_ms    BIGINT CHECK (total_ticket_duration_ms >= 0),

    is_complete                 BOOLEAN NOT NULL DEFAULT false,
    updated_at                  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_ticket_summary_station ON ticket_timing_summaries (station_id, computed_at DESC);
CREATE INDEX IF NOT EXISTS idx_ticket_summary_incomplete ON ticket_timing_summaries (is_complete) WHERE is_complete = false;

-- ---------------------------------------------------------------------
-- anomaly_events
--
-- Append-only. Written by anomaly-detector. Hypertable, matches
-- AnomalyEvent.schema.json.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS anomaly_events (
    anomaly_id                  UUID NOT NULL,
    event_type                  TEXT NOT NULL DEFAULT 'AnomalyEvent',
    schema_version               TEXT NOT NULL,
    source_id                   TEXT NOT NULL,
    detected_at                  TIMESTAMPTZ NOT NULL,
    restaurant_id                TEXT NOT NULL,
    detection_method              TEXT NOT NULL
                                 CHECK (detection_method IN ('control_limit', 'isolation_forest')),
    metric_name                  TEXT NOT NULL,
    station_id                   TEXT,
    table_id                    TEXT,
    staff_id                    TEXT,
    ticket_id                   TEXT,
    window_start                 TIMESTAMPTZ NOT NULL,
    window_end                   TIMESTAMPTZ NOT NULL,
    observed_value                DOUBLE PRECISION NOT NULL,
    expected_range_lower           DOUBLE PRECISION,
    expected_range_upper           DOUBLE PRECISION,
    anomaly_score                DOUBLE PRECISION,
    severity                    TEXT NOT NULL
                                 CHECK (severity IN ('low', 'medium', 'high')),
    contributing_event_ids          UUID[],
    raw_payload                  JSONB NOT NULL,
    ingested_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (anomaly_id, detected_at)
);
SELECT create_hypertable('anomaly_events', by_range('detected_at'), if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_anomaly_metric ON anomaly_events (metric_name, detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_anomaly_station ON anomaly_events (station_id, detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_anomaly_severity ON anomaly_events (severity, detected_at DESC);

-- ---------------------------------------------------------------------
-- causal_findings
--
-- Append-only. Written by causal-engine. Hypertable, matches
-- CausalFinding.schema.json. This is the only table the Phase 6 narrator
-- (once built) should read from -- enforced at the application layer
-- (narrator's DB role/connection should not have SELECT on any other
-- table), not by anything in this migration itself; noted here so the
-- constraint isn't forgotten when Phase 6 wiring happens.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS causal_findings (
    finding_id                   UUID NOT NULL,
    event_type                  TEXT NOT NULL DEFAULT 'CausalFinding',
    schema_version               TEXT NOT NULL,
    source_id                   TEXT NOT NULL,
    computed_at                  TIMESTAMPTZ NOT NULL,
    restaurant_id                TEXT NOT NULL,
    triggering_anomaly_id          UUID,
    scenario_injection_id          TEXT,
    treatment_variable             TEXT NOT NULL,
    outcome_variable              TEXT NOT NULL,
    confounders_controlled          TEXT[] NOT NULL CHECK (array_length(confounders_controlled, 1) >= 1),
    effect_estimate               DOUBLE PRECISION NOT NULL,
    effect_estimate_unit           TEXT,
    ci_lower                    DOUBLE PRECISION,
    ci_upper                    DOUBLE PRECISION,
    ci_confidence_level            DOUBLE PRECISION CHECK (ci_confidence_level BETWEEN 0 AND 1),
    method                      TEXT NOT NULL,
    refutation_passed              BOOLEAN,
    narrative_ready               BOOLEAN NOT NULL DEFAULT false,
    summary_text                 TEXT,
    raw_payload                  JSONB NOT NULL,
    ingested_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (finding_id, computed_at)
);
SELECT create_hypertable('causal_findings', by_range('computed_at'), if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_causal_treatment_outcome ON causal_findings (treatment_variable, outcome_variable, computed_at DESC);
CREATE INDEX IF NOT EXISTS idx_causal_scenario ON causal_findings (scenario_injection_id) WHERE scenario_injection_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_causal_narrative_ready ON causal_findings (narrative_ready) WHERE narrative_ready = true;
