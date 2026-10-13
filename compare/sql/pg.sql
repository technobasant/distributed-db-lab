-- Plain PostgreSQL: monthly range partitions and the index an OLTP-minded
-- team would add for per-tenant reads.
\timing on
CREATE TABLE events (
    tenant_id int, user_id bigint, event_type text, country text,
    ts timestamptz, duration_ms int, revenue double precision
) PARTITION BY RANGE (ts);
CREATE TABLE events_2026_07 PARTITION OF events FOR VALUES FROM ('2026-07-01+00') TO ('2026-08-01+00');
CREATE TABLE events_2026_08 PARTITION OF events FOR VALUES FROM ('2026-08-01+00') TO ('2026-09-01+00');
CREATE TABLE events_2026_09 PARTITION OF events FOR VALUES FROM ('2026-09-01+00') TO ('2026-10-01+00');
\echo == ingest
COPY events FROM '/data/events/events_by_ts.csv' WITH (FORMAT csv, HEADER true);
\echo == index
CREATE INDEX ON events (tenant_id, ts);
VACUUM ANALYZE events;
SELECT sum(pg_total_relation_size(inhrelid)) AS bytes FROM pg_inherits WHERE inhparent = 'events'::regclass;
