-- Two hypertables over the same ticks, differing only in key order.
--
-- ticks_time_pk   PRIMARY KEY (ts, entity_id, metric)   -- led by time
-- ticks_entity_pk PRIMARY KEY (entity_id, metric, ts)   -- led by what queries filter on
--
-- A time-led key looks natural for time-series and is what most tutorials
-- create. The audit this rebuilds found exactly that key on a multi-terabyte
-- hypertable whose API filtered by entity first, with the index outweighing
-- the heap. Same data, both orders, measured.
\timing on
CREATE EXTENSION IF NOT EXISTS timescaledb;
DROP TABLE IF EXISTS ticks_time_pk, ticks_entity_pk CASCADE;

CREATE TABLE ticks_time_pk (
    ts        timestamptz NOT NULL,
    entity_id int         NOT NULL,
    metric    text        NOT NULL,
    value     double precision NOT NULL,
    PRIMARY KEY (ts, entity_id, metric)
);
CREATE TABLE ticks_entity_pk (LIKE ticks_time_pk INCLUDING DEFAULTS);
ALTER TABLE ticks_entity_pk ADD PRIMARY KEY (entity_id, metric, ts);

SELECT create_hypertable('ticks_time_pk', by_range('ts', INTERVAL '1 day'));
SELECT create_hypertable('ticks_entity_pk', by_range('ts', INTERVAL '1 day'));

\echo == COPY into ticks_time_pk
COPY ticks_time_pk FROM '/data/ticks/ticks.csv' WITH (FORMAT csv, HEADER true);
\echo == COPY into ticks_entity_pk
COPY ticks_entity_pk FROM '/data/ticks/ticks.csv' WITH (FORMAT csv, HEADER true);
VACUUM ANALYZE ticks_time_pk;
VACUUM ANALYZE ticks_entity_pk;
