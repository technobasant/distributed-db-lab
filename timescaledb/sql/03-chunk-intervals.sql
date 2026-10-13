-- The same entity-led data at three chunk intervals. ticks_entity_pk (1 day)
-- already exists; build 1-hour and 7-day copies from it.
\timing on
DROP TABLE IF EXISTS ticks_1h, ticks_7d CASCADE;
CREATE TABLE ticks_1h (LIKE ticks_entity_pk INCLUDING ALL);
CREATE TABLE ticks_7d (LIKE ticks_entity_pk INCLUDING ALL);
SELECT create_hypertable('ticks_1h', by_range('ts', INTERVAL '1 hour'));
SELECT create_hypertable('ticks_7d', by_range('ts', INTERVAL '7 days'));
\echo == INSERT 1-hour chunks
INSERT INTO ticks_1h SELECT * FROM ticks_entity_pk;
\echo == INSERT 7-day chunks
INSERT INTO ticks_7d SELECT * FROM ticks_entity_pk;
VACUUM ANALYZE ticks_1h;
VACUUM ANALYZE ticks_7d;
SELECT h AS hypertable, (SELECT count(*) FROM show_chunks(h)) AS chunks,
       pg_size_pretty(hypertable_size(h)) AS size
FROM unnest(ARRAY['ticks_1h', 'ticks_entity_pk', 'ticks_7d']::regclass[]) AS h;
