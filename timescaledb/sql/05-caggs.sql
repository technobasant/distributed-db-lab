-- Hierarchical continuous aggregates: 1-minute candles from raw ticks, hourly
-- candles from the 1-minute ones, daily from hourly. Each level reads the one
-- below it, not the raw data.
\timing on
DROP MATERIALIZED VIEW IF EXISTS candles_1d, candles_1h, candles_1m CASCADE;

CREATE MATERIALIZED VIEW candles_1m WITH (timescaledb.continuous) AS
SELECT time_bucket('1 minute', ts) AS bucket, entity_id,
       first(value, ts) AS open, max(value) AS high, min(value) AS low,
       last(value, ts) AS close, count(*) AS ticks
FROM ticks_entity_pk WHERE metric = 'price'
GROUP BY 1, 2 WITH NO DATA;

CREATE MATERIALIZED VIEW candles_1h WITH (timescaledb.continuous) AS
SELECT time_bucket('1 hour', bucket) AS bucket, entity_id,
       first(open, bucket) AS open, max(high) AS high, min(low) AS low,
       last(close, bucket) AS close, sum(ticks) AS ticks
FROM candles_1m GROUP BY 1, 2 WITH NO DATA;

CREATE MATERIALIZED VIEW candles_1d WITH (timescaledb.continuous) AS
SELECT time_bucket('1 day', bucket) AS bucket, entity_id,
       first(open, bucket) AS open, max(high) AS high, min(low) AS low,
       last(close, bucket) AS close, sum(ticks) AS ticks
FROM candles_1h GROUP BY 1, 2 WITH NO DATA;

\echo == refresh 1m from raw
CALL refresh_continuous_aggregate('candles_1m', NULL, '2026-07-13+00');
\echo == refresh 1h from 1m
CALL refresh_continuous_aggregate('candles_1h', NULL, '2026-07-13+00');
\echo == refresh 1d from 1h
CALL refresh_continuous_aggregate('candles_1d', NULL, '2026-07-13+00');

SELECT 'candles_1m' AS view, count(*) FROM candles_1m
UNION ALL SELECT 'candles_1h', count(*) FROM candles_1h
UNION ALL SELECT 'candles_1d', count(*) FROM candles_1d;

-- the hierarchy must agree with the raw data it summarises
SELECT d.entity_id, d.bucket::date,
       d.high = r.high AND d.low = r.low AND d.ticks = r.ticks AS matches_raw
FROM candles_1d d
JOIN (SELECT time_bucket('1 day', ts) AS bucket, entity_id, max(value) AS high,
             min(value) AS low, count(*) AS ticks
      FROM ticks_entity_pk WHERE metric = 'price' AND entity_id = 7 GROUP BY 1, 2) r
  USING (entity_id, bucket)
ORDER BY 2 LIMIT 3;
