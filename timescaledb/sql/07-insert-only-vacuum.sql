-- Insert-only tables and the visibility map.
--
-- An index-only scan skips the heap only for pages the visibility map marks
-- all-visible, and only VACUUM sets those bits. A table that is never updated
-- or deleted from used to be vacuumed only for wraparound; PostgreSQL 13 added
-- insert-triggered autovacuum, governed by autovacuum_vacuum_insert_threshold
-- and autovacuum_vacuum_insert_scale_factor (default 0.2 — a fifth of the
-- table must be new before it fires). The audit this rebuilds found a
-- multi-terabyte insert-only hypertable with no autovacuum on record.
\timing on
DROP TABLE IF EXISTS readings;
CREATE TABLE readings (
    id        bigint GENERATED ALWAYS AS IDENTITY,
    entity_id int NOT NULL,
    ts        timestamptz NOT NULL,
    value     double precision NOT NULL
) WITH (autovacuum_enabled = false);
CREATE INDEX readings_entity_ts ON readings (entity_id, ts);

INSERT INTO readings (entity_id, ts, value)
SELECT 1 + (g % 100), timestamptz '2026-07-01+00' + g * INTERVAL '1 second', random()
FROM generate_series(1, 5000000) g;
ANALYZE readings;

\echo == fraction of pages all-visible before VACUUM
SELECT relpages, relallvisible, round(relallvisible::numeric / nullif(relpages, 0), 3) AS all_visible
FROM pg_class WHERE relname = 'readings';
\echo == index-only scan before VACUUM (watch Heap Fetches)
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
SELECT entity_id, ts FROM readings WHERE entity_id = 42 AND ts >= '2026-07-20+00';

VACUUM readings;
\echo == after VACUUM
SELECT relpages, relallvisible, round(relallvisible::numeric / nullif(relpages, 0), 3) AS all_visible
FROM pg_class WHERE relname = 'readings';
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
SELECT entity_id, ts FROM readings WHERE entity_id = 42 AND ts >= '2026-07-20+00';

\echo == insert-triggered autovacuum: default scale factor vs a per-table override
-- add 10% more rows twice; with the default 0.2 nothing fires, with 0.01 it does
ALTER TABLE readings SET (autovacuum_enabled = true);
SELECT pg_stat_reset_single_table_counters('readings'::regclass);
INSERT INTO readings (entity_id, ts, value)
SELECT 1 + (g % 100), timestamptz '2026-08-01+00' + g * INTERVAL '1 second', random()
FROM generate_series(1, 500000) g;
SELECT pg_sleep(20);
SELECT 'default 0.2' AS setting, n_ins_since_vacuum, last_autovacuum IS NOT NULL AS autovacuumed
FROM pg_stat_user_tables WHERE relname = 'readings';

ALTER TABLE readings SET (autovacuum_vacuum_insert_scale_factor = 0.01,
                          autovacuum_vacuum_insert_threshold = 10000);
INSERT INTO readings (entity_id, ts, value)
SELECT 1 + (g % 100), timestamptz '2026-08-10+00' + g * INTERVAL '1 second', random()
FROM generate_series(1, 500000) g;
SELECT pg_sleep(20);
SELECT 'per-table 0.01' AS setting, n_ins_since_vacuum, last_autovacuum IS NOT NULL AS autovacuumed
FROM pg_stat_user_tables WHERE relname = 'readings';
SELECT relpages, relallvisible, round(relallvisible::numeric / nullif(relpages, 0), 3) AS all_visible
FROM pg_class WHERE relname = 'readings';
