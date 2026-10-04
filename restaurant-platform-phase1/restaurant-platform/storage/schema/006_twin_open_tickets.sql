-- The digital twin's set of open tickets.
--
-- twin_station_state.open_ticket_count used to be incremented on every
-- `order_fired` and decremented on every `delivered`. Kafka delivers at least
-- once, so a redelivered `order_fired` inflated a station's count permanently
-- (DEF-107). The count is now derived from this set: a ticket is in it from
-- its first `order_fired` until its `delivered`, whatever the number of
-- deliveries, and a station's count is the number of its rows here.
--
-- Idempotent, like 004 and 005. Tickets that were open before this migration
-- are not in the set, so a station's count drops to the number of tickets
-- opened since; the previous (possibly inflated) numbers are not carried over.
--
-- The digital twin also runs this same statement at start-up, so a Compose
-- volume created before this migration existed (Postgres only runs its
-- initdb scripts on an empty data directory) does not leave the twin without
-- its table. tests/integration checks the two copies are identical.

CREATE TABLE IF NOT EXISTS twin_open_tickets (
    ticket_id   TEXT PRIMARY KEY,
    station_id  TEXT,
    opened_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_twin_open_tickets_station ON twin_open_tickets (station_id);
