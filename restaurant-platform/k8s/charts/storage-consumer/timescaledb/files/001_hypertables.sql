-- Phase 4: TimescaleDB hypertable definitions.
--
-- Field-provenance note (applies to every table below except
-- service_timing_events): this session did not have access to the
-- committed schemas/*.schema.json files. Typed columns for
-- plate_waste_events, pos_transaction_events, and staff_shift_events are
-- inferred from prose in restaurant-platform-implementation-status.md and
-- are marked INFERRED inline. Every table also stores the complete raw
-- event body in raw_payload, so no field is lost if an inferred column is
-- wrong or incomplete -- see PHASE4-DESIGN.md, "Critical caveat: schema
-- inference." Correct the INFERRED columns against the real schema files
-- before Phase 5 depends on them.
--
-- service_timing_events' typed columns ARE confirmed -- taken directly
-- from edge-simulators/simulators/service_timing.py's event construction,
-- which was available in full this session.

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ---------------------------------------------------------------------
-- plate_waste_events (INFERRED field set)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS plate_waste_events (
    event_id                   UUID NOT NULL,
    event_type                 TEXT NOT NULL DEFAULT 'PlateWasteEvent',
    schema_version              TEXT NOT NULL,
    source_id                  TEXT NOT NULL,
    source_kind                TEXT NOT NULL,
    "timestamp"                TIMESTAMPTZ NOT NULL,
    restaurant_id               TEXT NOT NULL,
    table_id                   TEXT,
    station_id                 TEXT,
    menu_item_id                TEXT,               -- INFERRED
    estimated_waste_grams       NUMERIC,             -- confirmed name only (Section 3.3 of status doc)
    to_go_container_used         BOOLEAN,             -- INFERRED, from confounder_flags.to_go_container_used
    dietary_restriction_flag     BOOLEAN,             -- INFERRED, existence only, exact field name unconfirmed
    portion_size_variant         TEXT,               -- INFERRED, existence only, exact field name unconfirmed
    raw_payload                 JSONB NOT NULL,
    ingested_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id, "timestamp")
);
SELECT create_hypertable('plate_waste_events', by_range('timestamp'), if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_plate_waste_station ON plate_waste_events (station_id, "timestamp" DESC);
CREATE INDEX IF NOT EXISTS idx_plate_waste_menu_item ON plate_waste_events (menu_item_id, "timestamp" DESC);

-- ---------------------------------------------------------------------
-- pos_transaction_events (INFERRED field set)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pos_transaction_events (
    event_id                   UUID NOT NULL,
    event_type                 TEXT NOT NULL DEFAULT 'POSTransactionEvent',
    schema_version              TEXT NOT NULL,
    source_id                  TEXT NOT NULL,
    source_kind                TEXT NOT NULL,
    "timestamp"                TIMESTAMPTZ NOT NULL,
    restaurant_id               TEXT NOT NULL,
    table_id                   TEXT,                -- INFERRED
    menu_item_id                TEXT,               -- INFERRED
    staff_id                   TEXT,                -- INFERRED
    quantity                   INTEGER,             -- INFERRED
    unit_price                  NUMERIC,             -- INFERRED
    raw_payload                 JSONB NOT NULL,
    ingested_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id, "timestamp")
);
SELECT create_hypertable('pos_transaction_events', by_range('timestamp'), if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_pos_staff ON pos_transaction_events (staff_id, "timestamp" DESC);
CREATE INDEX IF NOT EXISTS idx_pos_menu_item ON pos_transaction_events (menu_item_id, "timestamp" DESC);

-- ---------------------------------------------------------------------
-- staff_shift_events (INFERRED field set)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staff_shift_events (
    event_id                   UUID NOT NULL,
    event_type                 TEXT NOT NULL DEFAULT 'StaffShiftEvent',
    schema_version              TEXT NOT NULL,
    source_id                  TEXT NOT NULL,
    source_kind                TEXT NOT NULL,
    "timestamp"                TIMESTAMPTZ NOT NULL,
    restaurant_id               TEXT NOT NULL,
    staff_id                   TEXT,                -- INFERRED
    role                       TEXT,                -- INFERRED
    station_id                 TEXT,                -- INFERRED
    shift_event_type             TEXT,               -- INFERRED, e.g. clock_in / clock_out / break_start
    raw_payload                 JSONB NOT NULL,
    ingested_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id, "timestamp")
);
SELECT create_hypertable('staff_shift_events', by_range('timestamp'), if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_staff_shift_staff ON staff_shift_events (staff_id, "timestamp" DESC);

-- ---------------------------------------------------------------------
-- service_timing_events (CONFIRMED field set -- see service_timing.py)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS service_timing_events (
    event_id                            UUID NOT NULL,
    event_type                          TEXT NOT NULL DEFAULT 'ServiceTimingEvent',
    schema_version                       TEXT NOT NULL,
    source_id                           TEXT NOT NULL,
    source_kind                         TEXT NOT NULL,
    "timestamp"                         TIMESTAMPTZ NOT NULL,
    restaurant_id                        TEXT NOT NULL,
    ticket_id                           TEXT NOT NULL,
    table_id                            TEXT,
    station_id                          TEXT,
    stage                               TEXT NOT NULL,   -- fired | started | plated | expo_hold | delivered
    elapsed_since_previous_stage_ms       BIGINT,          -- NULL on the first ("fired") stage of a ticket
    raw_payload                          JSONB NOT NULL,
    ingested_at                          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id, "timestamp")
);
SELECT create_hypertable('service_timing_events', by_range('timestamp'), if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_service_timing_ticket ON service_timing_events (ticket_id, "timestamp" DESC);
CREATE INDEX IF NOT EXISTS idx_service_timing_stage ON service_timing_events (stage, "timestamp" DESC);
