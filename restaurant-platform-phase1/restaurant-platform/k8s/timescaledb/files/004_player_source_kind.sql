-- Version-2 (game) support: widen source_kind to accept 'player'.
--
-- A human-driven source (the game client, via services/game-bridge) is a
-- third case of the same "swap the source, keep the contract" rule that
-- already covers 'simulated' and 'vendor_integration'. The matching change
-- to the four schemas/*Event.schema.json enums is in the same commit.
--
-- Written as a standalone, idempotent migration rather than an edit to
-- 001_hypertables.sql because CREATE TABLE IF NOT EXISTS never re-applies
-- to a database that already has the tables -- the same reason the
-- earlier column renames needed manual ALTERs on the live k3s DB. Safe to
-- run on: a fresh DB (after 001), a DB whose 001 already had the CHECK
-- (the Compose path), and a DB built from an older 001 with no CHECK at
-- all (the k3s path before this change). All converge to the same state.

ALTER TABLE plate_waste_events     DROP CONSTRAINT IF EXISTS plate_waste_events_source_kind_check;
ALTER TABLE pos_transaction_events DROP CONSTRAINT IF EXISTS pos_transaction_events_source_kind_check;
ALTER TABLE service_timing_events  DROP CONSTRAINT IF EXISTS service_timing_events_source_kind_check;
ALTER TABLE staff_shift_events     DROP CONSTRAINT IF EXISTS staff_shift_events_source_kind_check;

ALTER TABLE plate_waste_events     ADD CONSTRAINT plate_waste_events_source_kind_check
    CHECK (source_kind IN ('simulated', 'vendor_integration', 'player'));
ALTER TABLE pos_transaction_events ADD CONSTRAINT pos_transaction_events_source_kind_check
    CHECK (source_kind IN ('simulated', 'vendor_integration', 'player'));
ALTER TABLE service_timing_events  ADD CONSTRAINT service_timing_events_source_kind_check
    CHECK (source_kind IN ('simulated', 'vendor_integration', 'player'));
ALTER TABLE staff_shift_events     ADD CONSTRAINT staff_shift_events_source_kind_check
    CHECK (source_kind IN ('simulated', 'vendor_integration', 'player'));
