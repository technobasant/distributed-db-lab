-- TimescaleDB: weekly chunks, then every chunk converted to the columnstore,
-- segmented by tenant and ordered by time.
\timing on
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE TABLE events (
    tenant_id int, user_id bigint, event_type text, country text,
    ts timestamptz NOT NULL, duration_ms int, revenue double precision
);
SELECT create_hypertable('events', by_range('ts', INTERVAL '7 days'));
\echo == ingest
COPY events FROM '/data/events/events_by_ts.csv' WITH (FORMAT csv, HEADER true);
\echo == compress
ALTER TABLE events SET (timescaledb.enable_columnstore = true,
                        timescaledb.segmentby = 'tenant_id', timescaledb.orderby = 'ts');
SELECT count(compress_chunk(c)) FROM show_chunks('events') c;
VACUUM ANALYZE events;
SELECT hypertable_size('events') AS bytes;
