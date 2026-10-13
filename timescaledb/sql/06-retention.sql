-- Drop raw ticks older than the newest 3 days; the candles must survive.
-- The policy is created, then its job is run once so the effect is visible
-- now instead of on the scheduler's next tick.
\timing on
SELECT 'raw_chunks_before' AS what, count(*) FROM show_chunks('ticks_entity_pk')
UNION ALL SELECT 'candles_1d_rows_before', count(*) FROM candles_1d;
SELECT add_retention_policy('ticks_entity_pk', drop_after => INTERVAL '3 days',
                            initial_start => now() + INTERVAL '1 day') AS job_id \gset
-- the data ends on 2026-07-13, so "older than 3 days" is relative to now();
-- drop explicitly relative to the data's own end to keep the run deterministic
SELECT count(*) AS dropped FROM drop_chunks('ticks_entity_pk', older_than => '2026-07-10+00'::timestamptz);
SELECT 'raw_chunks_after' AS what, count(*) FROM show_chunks('ticks_entity_pk')
UNION ALL SELECT 'candles_1d_rows_after', count(*) FROM candles_1d
UNION ALL SELECT 'candles_1d_days_before_cutoff', count(*) FROM candles_1d WHERE bucket < '2026-07-10+00';
SELECT delete_job(:job_id);
