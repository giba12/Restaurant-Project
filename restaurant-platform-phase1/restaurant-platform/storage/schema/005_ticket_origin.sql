-- Ticket origin, and the 'crew' source kind.
--
-- Interactive sessions (a human working a station, with an automated crew
-- covering the other stations' work) run at a different pace and volume
-- from the simulators. Their tickets must be recognisable so anything that
-- builds a baseline from ticket timings can keep them out of it.
--
--   1. service_timing_events may carry source_kind 'crew' (the automated
--      participants), alongside 'simulated', 'vendor_integration' and 'player'.
--   2. ticket_timing_summaries gets an `origin` column: 'interactive' if any
--      event on the ticket is 'player' or 'crew', else the events' own kind.
--
-- Idempotent, like 004: constraints are dropped and re-added, the column and
-- index use IF NOT EXISTS. Existing rows default to 'simulated'; a database
-- that already holds interactive tickets needs the one-off backfill in the
-- interactive client's directory (see its README).

ALTER TABLE service_timing_events DROP CONSTRAINT IF EXISTS service_timing_events_source_kind_check;
ALTER TABLE service_timing_events ADD CONSTRAINT service_timing_events_source_kind_check
    CHECK (source_kind IN ('simulated', 'vendor_integration', 'player', 'crew'));

ALTER TABLE ticket_timing_summaries ADD COLUMN IF NOT EXISTS origin TEXT NOT NULL DEFAULT 'simulated';
ALTER TABLE ticket_timing_summaries DROP CONSTRAINT IF EXISTS ticket_timing_summaries_origin_check;
ALTER TABLE ticket_timing_summaries ADD CONSTRAINT ticket_timing_summaries_origin_check
    CHECK (origin IN ('simulated', 'vendor_integration', 'interactive'));
CREATE INDEX IF NOT EXISTS idx_ticket_summary_origin ON ticket_timing_summaries (origin, computed_at DESC);
