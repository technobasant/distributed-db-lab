-- Columnstore (compression): segment by what queries filter on, order by time.
-- Chunks older than the last day are converted; the newest stays row-based
-- for inserts, the way a live feed would run it.
\timing on
ALTER TABLE ticks_entity_pk SET (
    timescaledb.enable_columnstore = true,
    timescaledb.segmentby = 'entity_id, metric',
    timescaledb.orderby = 'ts DESC'
);
SELECT count(compress_chunk(c))
FROM show_chunks('ticks_entity_pk', older_than => '2026-07-12+00'::timestamptz) AS c;
SELECT pg_size_pretty(before_compression_total_bytes) AS before,
       pg_size_pretty(after_compression_total_bytes)  AS after,
       before_compression_total_bytes AS before_bytes,
       after_compression_total_bytes  AS after_bytes,
       round(before_compression_total_bytes::numeric
             / nullif(after_compression_total_bytes, 0), 1) AS ratio,
       number_compressed_chunks, total_chunks
FROM hypertable_compression_stats('ticks_entity_pk');
