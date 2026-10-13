-- Heap vs index bytes per hypertable, and the ratio the audit cared about.
SELECT h AS hypertable,
       s.table_bytes, s.index_bytes, s.total_bytes,
       round(s.index_bytes::numeric / nullif(s.table_bytes, 0), 2) AS index_to_heap,
       (SELECT count(*) FROM show_chunks(h)) AS chunks
FROM unnest(ARRAY['ticks_time_pk', 'ticks_entity_pk']::regclass[]) AS h,
     LATERAL hypertable_detailed_size(h) s;
