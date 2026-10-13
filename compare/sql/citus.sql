-- Citus columnar access method on a single node: column-oriented stripes with
-- per-chunk min/max for skipping, no index.
\timing on
CREATE EXTENSION IF NOT EXISTS citus;
-- columnar has been its own extension since Citus 11.1
CREATE EXTENSION IF NOT EXISTS citus_columnar;
CREATE TABLE events (
    tenant_id int, user_id bigint, event_type text, country text,
    ts timestamptz, duration_ms int, revenue double precision
) USING columnar;
\echo == ingest
COPY events FROM '/data/events/events_by_ts.csv' WITH (FORMAT csv, HEADER true);
VACUUM ANALYZE events;
SELECT pg_total_relation_size('events') AS bytes;
