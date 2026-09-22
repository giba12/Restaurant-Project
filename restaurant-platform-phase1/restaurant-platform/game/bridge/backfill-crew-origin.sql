-- One-off backfill for a database that recorded interactive-session tickets
-- BEFORE migration 005 (storage/schema/005_ticket_origin.sql). Run it after
-- applying 005; a fresh database has nothing to backfill.
--
--   1. Events the crew published before it had its own source_kind were
--      tagged 'simulated'. Re-tag them 'crew' (typed column and stored payload).
--   2. Mark every ticket that has any 'player' or 'crew' event as interactive.
--
-- Idempotent: re-running changes nothing.
--
--   docker compose exec -T timescaledb psql -U restaurant_app -d restaurant_platform \
--     < game/bridge/backfill-crew-origin.sql

UPDATE service_timing_events
   SET source_kind = 'crew',
       raw_payload = jsonb_set(raw_payload, '{source_kind}', '"crew"')
 WHERE source_id = 'game-crew' AND source_kind = 'simulated';

UPDATE ticket_timing_summaries
   SET origin = 'interactive'
 WHERE origin <> 'interactive'
   AND ticket_id IN (SELECT DISTINCT ticket_id FROM service_timing_events
                      WHERE source_kind IN ('player', 'crew'));
