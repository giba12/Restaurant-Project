-- Phase 4: TimescaleDB hypertable definitions.
--
-- Revision note: this version replaces the prior INFERRED field sets with
-- columns checked directly against the four committed schema files
-- (PlateWasteEvent, POSTransactionEvent, StaffShiftEvent,
-- ServiceTimingEvent). Corrections from the previous revision are listed
-- per table below. raw_payload is retained on every table regardless of
-- typed-column confidence, per standing practice.
--
-- UNRESOLVED CONTRACT ISSUE (read before using service_timing_events):
-- ServiceTimingEvent.schema.json's `stage` enum is
-- {order_fired, cook_started, plated, picked_up_by_server, delivered},
-- per the schema's own revision note ("split former 'expo_hold'/
-- 'delivered' ambiguity into distinct 'plated' -> 'picked_up_by_server'
-- -> 'delivered' stages"). The producer code reviewed this session
-- (edge-simulators/simulators/service_timing.py) still emits the
-- pre-revision set {fired, started, plated, expo_hold, delivered} via its
-- STAGES constant. These two sets do not intersect except at "plated" and
-- "delivered". This table's CHECK constraint below enforces the SCHEMA's
-- enum, on the stated basis that the schema is the contract of record
-- and the producer is out of compliance with it -- not the reverse. This
-- means the constraint will currently reject every event actually
-- emitted by service_timing.py in its present form. This is not a
-- resolvable ambiguity to paper over silently; either the schema enum or
-- STAGES must be corrected before a real Kafka-to-storage consumer is
-- pointed at this table. Flagging here rather than in Section 8 of the
-- implementation-status document, since it directly blocks Phase 4
-- ingestion, not just a "for later" item.

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ---------------------------------------------------------------------
-- plate_waste_events
--
-- Corrections vs. prior revision:
--   - menu_item_id removed. Not a schema field; the schema's analogous
--     data is plate_item_ids (array, required), not a single scalar.
--   - dietary_restriction_flag renamed to declared_dietary_restriction,
--     matching confounder_flags.declared_dietary_restriction exactly.
--     The prior name did not exist in the schema under any form.
--   - plate_item_ids added (required by schema; previously absent
--     entirely).
--   - table_id added (present in schema as nullable; previously absent).
--   - image_ref added (present in schema as nullable; previously absent).
--   - portion_size_variant constrained to the schema's actual enum
--     (standard | half | large | unknown); previously typed as
--     unconstrained TEXT.
--   - confounder_flags fields are schema-optional (the parent object
--     itself is not in `required`), so all three remain nullable.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS plate_waste_events (
    event_id                    UUID NOT NULL,
    event_type                  TEXT NOT NULL DEFAULT 'PlateWasteEvent',
    schema_version               TEXT NOT NULL,
    source_id                   TEXT NOT NULL,
    source_kind                  TEXT NOT NULL DEFAULT 'simulated'
                                 CHECK (source_kind IN ('simulated', 'vendor_integration')),
    "timestamp"                 TIMESTAMPTZ NOT NULL,
    restaurant_id                TEXT NOT NULL,
    station_id                   TEXT NOT NULL,
    table_id                     TEXT,
    estimated_waste_grams          NUMERIC NOT NULL CHECK (estimated_waste_grams >= 0),
    plate_item_ids                TEXT[] NOT NULL,
    to_go_container_used           BOOLEAN,
    declared_dietary_restriction    BOOLEAN,
    portion_size_variant           TEXT
                                 CHECK (portion_size_variant IN ('standard', 'half', 'large', 'unknown')),
    image_ref                    TEXT,
    raw_payload                  JSONB NOT NULL,
    ingested_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id, "timestamp")
);
SELECT create_hypertable('plate_waste_events', by_range('timestamp'), if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_plate_waste_station ON plate_waste_events (station_id, "timestamp" DESC);
CREATE INDEX IF NOT EXISTS idx_plate_waste_item_ids ON plate_waste_events USING GIN (plate_item_ids);

-- ---------------------------------------------------------------------
-- pos_transaction_events
--
-- Corrections vs. prior revision:
--   - menu_item_id, quantity, unit_price columns removed from this table.
--     The schema's `line_items` is a required array of objects
--     (minItems 1), not a scalar per-transaction field -- a transaction
--     can contain multiple line items. This is a one-to-many
--     relationship and is modeled below as a separate hypertable,
--     pos_transaction_line_items, rather than flattened incorrectly
--     onto the parent row as the prior revision did.
--   - staff_id renamed to server_staff_id, matching the schema field
--     name exactly (previously an unconfirmed guess).
--   - transaction_id added (required by schema; previously absent).
--   - total_amount_cents added (required by schema; previously absent).
--   - currency added (required by schema, 3-letter pattern; previously
--     absent).
--   - payment_method added (schema enum, optional; previously absent).
--   - discount_applied_cents added (schema field, default 0; previously
--     absent).
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pos_transaction_events (
    event_id                    UUID NOT NULL,
    event_type                  TEXT NOT NULL DEFAULT 'POSTransactionEvent',
    schema_version               TEXT NOT NULL,
    source_id                   TEXT NOT NULL,
    source_kind                  TEXT NOT NULL DEFAULT 'simulated'
                                 CHECK (source_kind IN ('simulated', 'vendor_integration')),
    "timestamp"                 TIMESTAMPTZ NOT NULL,
    restaurant_id                TEXT NOT NULL,
    transaction_id                TEXT NOT NULL,
    table_id                     TEXT,
    server_staff_id                TEXT,
    total_amount_cents             INTEGER NOT NULL CHECK (total_amount_cents >= 0),
    currency                    TEXT NOT NULL DEFAULT 'USD' CHECK (currency ~ '^[A-Z]{3}$'),
    payment_method                TEXT
                                 CHECK (payment_method IN ('card', 'cash', 'mobile_wallet', 'gift_card', 'other')),
    discount_applied_cents          INTEGER NOT NULL DEFAULT 0 CHECK (discount_applied_cents >= 0),
    raw_payload                  JSONB NOT NULL,
    ingested_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id, "timestamp")
);
SELECT create_hypertable('pos_transaction_events', by_range('timestamp'), if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_pos_transaction_id ON pos_transaction_events (transaction_id, "timestamp" DESC);
CREATE INDEX IF NOT EXISTS idx_pos_server_staff ON pos_transaction_events (server_staff_id, "timestamp" DESC);

-- New table: not present in the prior revision. Required to represent
-- POSTransactionEvent.line_items (array, minItems 1) without collapsing
-- a one-to-many relationship into the parent row. No enforced foreign
-- key to pos_transaction_events is declared -- the parent's primary key
-- is composite (event_id, timestamp) for hypertable partitioning
-- reasons, and TimescaleDB does not support foreign keys referencing a
-- hypertable's partitioning column cleanly across chunks. transaction_id
-- and parent_event_id are stored as plain (unenforced) reference columns
-- instead; referential integrity is the ingestion consumer's
-- responsibility, not the database's.
CREATE TABLE IF NOT EXISTS pos_transaction_line_items (
    parent_event_id     UUID NOT NULL,
    transaction_id       TEXT NOT NULL,
    "timestamp"          TIMESTAMPTZ NOT NULL,
    line_item_index       INTEGER NOT NULL,
    menu_item_id          TEXT NOT NULL,
    quantity            INTEGER NOT NULL CHECK (quantity >= 1),
    unit_price_cents       INTEGER NOT NULL CHECK (unit_price_cents >= 0),
    modifiers            TEXT[],
    voided              BOOLEAN NOT NULL DEFAULT false,
    PRIMARY KEY (parent_event_id, line_item_index, "timestamp")
);
SELECT create_hypertable('pos_transaction_line_items', by_range('timestamp'), if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_line_items_transaction ON pos_transaction_line_items (transaction_id, "timestamp" DESC);
CREATE INDEX IF NOT EXISTS idx_line_items_menu_item ON pos_transaction_line_items (menu_item_id, "timestamp" DESC);

-- ---------------------------------------------------------------------
-- staff_shift_events
--
-- Corrections vs. prior revision:
--   - shift_event_type renamed to shift_action, matching the schema
--     field name exactly (previously an unconfirmed guess with a
--     different name than the schema uses).
--   - shift_action constrained to the schema's actual enum
--     (clock_in | clock_out | break_start | break_end |
--     station_reassign); previously unconstrained TEXT.
--   - role constrained to the schema's actual enum (server | line_cook |
--     expo | host | bartender | dishwasher | manager); previously
--     unconstrained TEXT.
--   - scheduled_vs_actual added (schema field, enum, optional;
--     previously absent entirely -- this is a modeled confounder per
--     the schema's own description and should not be dropped silently).
--   - station_id retained as nullable, consistent with the schema
--     ("Relevant for station_reassign; null otherwise").
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staff_shift_events (
    event_id                    UUID NOT NULL,
    event_type                  TEXT NOT NULL DEFAULT 'StaffShiftEvent',
    schema_version               TEXT NOT NULL,
    source_id                   TEXT NOT NULL,
    source_kind                  TEXT NOT NULL DEFAULT 'simulated'
                                 CHECK (source_kind IN ('simulated', 'vendor_integration')),
    "timestamp"                 TIMESTAMPTZ NOT NULL,
    restaurant_id                TEXT NOT NULL,
    staff_id                    TEXT NOT NULL,
    role                       TEXT NOT NULL
                                 CHECK (role IN ('server', 'line_cook', 'expo', 'host', 'bartender', 'dishwasher', 'manager')),
    shift_action                 TEXT NOT NULL
                                 CHECK (shift_action IN ('clock_in', 'clock_out', 'break_start', 'break_end', 'station_reassign')),
    station_id                   TEXT,
    scheduled_vs_actual            TEXT
                                 CHECK (scheduled_vs_actual IN ('as_scheduled', 'early', 'late', 'unscheduled_cover')),
    raw_payload                  JSONB NOT NULL,
    ingested_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id, "timestamp")
);
SELECT create_hypertable('staff_shift_events', by_range('timestamp'), if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_staff_shift_staff ON staff_shift_events (staff_id, "timestamp" DESC);
CREATE INDEX IF NOT EXISTS idx_staff_shift_action ON staff_shift_events (shift_action, "timestamp" DESC);

-- ---------------------------------------------------------------------
-- service_timing_events
--
-- Field set was already confirmed correct in the prior revision (taken
-- directly from service_timing.py's event construction). The one
-- substantive change here is the `stage` CHECK constraint, which now
-- enforces the schema's post-revision enum rather than being
-- unconstrained TEXT. See the file-level comment at the top of this
-- file: this constraint will reject events from the producer code as it
-- currently exists, since that code has not been updated to match the
-- schema's 2026-08-26 revision. The constraint is left in place
-- (not loosened to match the producer) so that the mismatch surfaces as
-- a hard ingestion failure rather than silently persisting
-- schema-noncompliant data.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS service_timing_events (
    event_id                            UUID NOT NULL,
    event_type                          TEXT NOT NULL DEFAULT 'ServiceTimingEvent',
    schema_version                       TEXT NOT NULL,
    source_id                           TEXT NOT NULL,
    source_kind                          TEXT NOT NULL DEFAULT 'simulated'
                                         CHECK (source_kind IN ('simulated', 'vendor_integration')),
    "timestamp"                         TIMESTAMPTZ NOT NULL,
    restaurant_id                        TEXT NOT NULL,
    ticket_id                           TEXT NOT NULL,
    table_id                            TEXT,
    station_id                          TEXT,
    stage                              TEXT NOT NULL
                                         CHECK (stage IN ('order_fired', 'cook_started', 'plated', 'picked_up_by_server', 'delivered')),
    elapsed_since_previous_stage_ms       BIGINT CHECK (elapsed_since_previous_stage_ms >= 0),
    raw_payload                          JSONB NOT NULL,
    ingested_at                          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id, "timestamp")
);
SELECT create_hypertable('service_timing_events', by_range('timestamp'), if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_service_timing_ticket ON service_timing_events (ticket_id, "timestamp" DESC);
CREATE INDEX IF NOT EXISTS idx_service_timing_stage ON service_timing_events (stage, "timestamp" DESC);
