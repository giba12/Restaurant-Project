-- One-time repair for a k3s database created before the raw-table
-- corrections (see k8s/realign/realign-live-cluster.sh, which runs this).
--
-- 001_hypertables.sql is all IF NOT EXISTS, so it never alters a table that
-- already exists. A database built from the original 001 therefore keeps
-- the original column layout forever, and the current 001 then fails on it
-- (it indexes plate_item_ids, which the old table lacks). The raw event
-- tables hold simulated, regenerable data, so the repair is to drop them
-- and let the current 001 recreate them.
--
-- Guarded: it only acts if a table still has a legacy column, so it is safe
-- to run against an already-correct database (it reports and changes nothing).
-- All five tables are dropped together, or none are.
--
-- DROP TABLE needs an exclusive lock, and a table another session has read
-- inside a still-open transaction cannot be locked. The pipeline services
-- (psycopg2, no autocommit) do exactly that: they read, then sit "idle in
-- transaction", so a plain DROP waits forever. The script stops those
-- services first; the statement below clears anything still connected (a
-- pod that has not finished terminating, an orphaned earlier attempt), and
-- lock_timeout makes any remaining blocker an error instead of a hang.
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = current_database()
  AND pid <> pg_backend_pid()
  AND backend_type = 'client backend';
SET lock_timeout = '30s';

DO $$
DECLARE
    legacy boolean;
BEGIN
    SELECT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND ((table_name = 'plate_waste_events'    AND column_name = 'menu_item_id')
            OR (table_name = 'pos_transaction_events' AND column_name IN ('staff_id', 'menu_item_id', 'quantity', 'unit_price')))
    ) INTO legacy;

    IF legacy THEN
        RAISE NOTICE 'legacy raw-table layout found: dropping plate_waste_events, pos_transaction_events, pos_transaction_line_items, staff_shift_events, service_timing_events';
        -- One statement per table: TimescaleDB refuses a single DROP TABLE
        -- that names several hypertables ("cannot drop a hypertable along
        -- with other objects").
        DROP TABLE IF EXISTS plate_waste_events;
        DROP TABLE IF EXISTS pos_transaction_events;
        DROP TABLE IF EXISTS pos_transaction_line_items;
        DROP TABLE IF EXISTS staff_shift_events;
        DROP TABLE IF EXISTS service_timing_events;
    ELSE
        RAISE NOTICE 'raw tables already have the current layout: nothing to do';
    END IF;
END
$$;
