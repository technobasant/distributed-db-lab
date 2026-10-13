#!/usr/bin/env bash
# ClickHouse part of the lab. Run from the repository root:
#
#   clickhouse/scripts/ch.sh up | schema | load | variants | mv | parts | failover | down
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"
COMPOSE=(docker compose -f clickhouse/docker-compose.yml)
BENCH=("${COMPOSE[@]}" --profile tools run --rm -T bench)
URL="http://ch-s1r1:8123"
OUT="results/clickhouse"
PY="${PY:-.venv/bin/python}"
mkdir -p "$OUT"

chq() { "${COMPOSE[@]}" exec -T ch-s1r1 clickhouse-client "$@"; }
now() { chq --query "SELECT toUnixTimestamp64Milli(now64(3)) / 1000"; }
secs() { "$PY" -c "print(round($2 - $1, 1))"; }
lat() { # label, sql, runs
  echo "\"$1\":"
  "${BENCH[@]}" tools/bench.py latency --clickhouse "$URL" --sql "$2" --runs "${3:-30}" --warmup 3 --tenants 2..2000
}

cmd_up() {
  "${COMPOSE[@]}" up -d keeper ch-s1r1 ch-s1r2 ch-s2r1 ch-s2r2
  for s in ch-s1r1 ch-s1r2 ch-s2r1 ch-s2r2; do
    for _ in $(seq 1 60); do [ "$("${COMPOSE[@]}" ps --format '{{.Health}}' "$s")" = healthy ] && break; sleep 2; done
  done
  chq --query "SELECT version()" | tee "$OUT/version.txt"
}

cmd_schema() { chq --multiquery < clickhouse/sql/01-schema.sql; }

cmd_load() {
  local t0 t1
  t0=$(now)
  "${COMPOSE[@]}" exec -T ch-s1r1 sh -c 'clickhouse-client --date_time_input_format=best_effort \
      --distributed_foreground_insert=1 \
      --query "INSERT INTO events FORMAT CSVWithNames" < /data/events/events.csv'
  t1=$(now)
  chq --query "SYSTEM SYNC REPLICA ON CLUSTER lab events_local" >/dev/null 2>&1 || true
  chq --format TSVWithNames --query "
    SELECT hostName() AS host, count() AS rows FROM clusterAllReplicas('lab', default.events_local)
    GROUP BY host ORDER BY host" | tee "$OUT/load-per-replica.tsv"
  local rows
  rows=$(chq --query "SELECT count() FROM events")
  "$PY" -c "import json; s=$t1-$t0; print(json.dumps({'rows': $rows, 'seconds': round(s,1), 'rows_per_s': round($rows/s)}, indent=2))" \
    | tee "$OUT/load.json"
}

cmd_variants() {
  local t0 t1
  t0=$(now)
  chq --multiquery < clickhouse/sql/02-variants.sql
  t1=$(now)
  echo "{\"variants_build_seconds\": $(secs "$t0" "$t1")}" > "$OUT/variants-build.json"
  chq --format TSVWithNames --query "
    SELECT table, sum(rows) AS rows, sum(data_compressed_bytes) AS compressed,
           sum(data_uncompressed_bytes) AS uncompressed, sum(bytes_on_disk) AS on_disk,
           round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 2) AS ratio
    FROM cluster('lab', system.parts)
    WHERE active AND database = 'default' AND table IN ('events_local', 'events_ts_local', 'events_str_local')
    GROUP BY table ORDER BY table" | tee "$OUT/sizes.tsv"
  chq --format TSVWithNames --query "
    SELECT table, name AS column, sum(data_compressed_bytes) AS compressed
    FROM cluster('lab', system.columns)
    WHERE database = 'default' AND table IN ('events_local', 'events_str_local')
      AND name IN ('event_type', 'country')
    GROUP BY table, name ORDER BY name, table" | tee "$OUT/column-sizes.tsv"
  local q_tenant="SELECT toDate(ts) AS d, count() FROM %T% WHERE tenant_id = %(tenant)s GROUP BY d SETTINGS optimize_skip_unused_shards = 1"
  local q_group="SELECT event_type, country, count() FROM %T% GROUP BY event_type, country"
  {
    echo '{'
    lat orderby_tenant_first "${q_tenant//%T%/events}"; echo ','
    lat orderby_ts_only "${q_tenant//%T%/events_ts}"; echo ','
    lat lowcardinality_groupby "${q_group//%T%/events}" 15; echo ','
    lat string_groupby "${q_group//%T%/events_str}" 15
    echo '}'
  } > "$OUT/variants.json"
  # rows actually read on the shards for one tenant, by sort key
  for t in events events_ts; do
    chq --query "SELECT count() FROM $t WHERE tenant_id = 42 SETTINGS optimize_skip_unused_shards = 1, log_comment = 'readrows_$t' FORMAT Null"
  done
  chq --query "SYSTEM FLUSH LOGS ON CLUSTER lab" >/dev/null
  chq --format TSVWithNames --query "
    SELECT log_comment, sum(read_rows) AS rows_read_on_shards
    FROM clusterAllReplicas('lab', system.query_log)
    WHERE log_comment LIKE 'readrows_%' AND type = 'QueryFinish' AND is_initial_query = 0
    GROUP BY log_comment ORDER BY log_comment" | tee "$OUT/read-rows.tsv"
}

cmd_mv() {
  local raw_t="SELECT toDate(ts) AS d, uniq(user_id) FROM events WHERE tenant_id = %(tenant)s GROUP BY d SETTINGS optimize_skip_unused_shards = 1"
  local mv_t="SELECT day, uniqMerge(users) FROM events_daily WHERE tenant_id = %(tenant)s GROUP BY day SETTINGS optimize_skip_unused_shards = 1"
  local raw_all="SELECT event_type, count(), sum(revenue) FROM events GROUP BY event_type"
  local mv_all="SELECT event_type, countMerge(events), sumMerge(revenue) FROM events_daily GROUP BY event_type"
  {
    echo '{'
    lat tenant_daily_users_raw "$raw_t"; echo ','
    lat tenant_daily_users_mv "$mv_t"; echo ','
    lat all_event_totals_raw "$raw_all" 15; echo ','
    lat all_event_totals_mv "$mv_all" 15
    echo '}'
  } > "$OUT/mv.json"
  chq --format TSVWithNames --query "
    SELECT (SELECT count() FROM events WHERE tenant_id = 42) AS raw_count,
           (SELECT countMerge(events) FROM events_daily WHERE tenant_id = 42) AS mv_count,
           (SELECT count() FROM cluster('lab', default.events_daily_local)) AS mv_rows,
           (SELECT count() FROM events) AS raw_rows" | tee "$OUT/mv-check.tsv"
}

cmd_parts() {
  # 300 small inserts of 1,000 rows, sent as data (async_insert only applies to
  # INSERT ... FORMAT, never to INSERT ... SELECT — the first version of this
  # step got that wrong and measured nothing). Parts written come from part_log.
  chq --multiquery --query "
    DROP TABLE IF EXISTS parts_sync; DROP TABLE IF EXISTS parts_async;
    CREATE TABLE parts_sync  (tenant_id UInt32, ts DateTime, v Float64) ENGINE = MergeTree ORDER BY (tenant_id, ts);
    CREATE TABLE parts_async (tenant_id UInt32, ts DateTime, v Float64) ENGINE = MergeTree ORDER BY (tenant_id, ts);"
  chq --query "SELECT tenant_id, ts, revenue FROM events_local LIMIT 300000 FORMAT CSV" \
    > "results/raw/parts-batch.csv"
  "${COMPOSE[@]}" cp results/raw/parts-batch.csv ch-s1r1:/tmp/batch.csv >/dev/null
  local t0 t1 t2 t3
  t0=$(now)
  # shellcheck disable=SC2016  # expands inside the container, not here
  "${COMPOSE[@]}" exec -T ch-s1r1 bash -c 'for i in $(seq 0 299); do
      sed -n "$((i * 1000 + 1)),$((i * 1000 + 1000))p" /tmp/batch.csv |
      clickhouse-client --query "INSERT INTO parts_sync FORMAT CSV"; done'
  t1=$(now)
  t2=$(now)
  # shellcheck disable=SC2016
  "${COMPOSE[@]}" exec -T ch-s1r1 bash -c 'for i in $(seq 0 299); do
      sed -n "$((i * 1000 + 1)),$((i * 1000 + 1000))p" /tmp/batch.csv |
      clickhouse-client --async_insert=1 --wait_for_async_insert=0 --query "INSERT INTO parts_async FORMAT CSV"; done'
  t3=$(now)
  chq --query "SYSTEM FLUSH ASYNC INSERT QUEUE" >/dev/null
  chq --query "SYSTEM FLUSH LOGS" >/dev/null
  chq --format TSVWithNames --query "
    SELECT table, countIf(event_type = 'NewPart') AS parts_written,
           countIf(event_type = 'MergeParts') AS merges,
           sumIf(rows, event_type = 'NewPart') AS rows_written
    FROM system.part_log
    WHERE database = 'default' AND table IN ('parts_sync', 'parts_async')
      AND table_uuid IN (SELECT uuid FROM system.tables
                         WHERE database = 'default' AND name IN ('parts_sync', 'parts_async'))
    GROUP BY table ORDER BY table" | tee "$OUT/parts-log.tsv"
  cat > "$OUT/parts.json" <<JSON
{"inserts": 300, "rows_per_insert": 1000,
 "sync_seconds": $(secs "$t0" "$t1"), "async_seconds": $(secs "$t2" "$t3"),
 "rows_sync": $(chq --query "SELECT count() FROM parts_sync"),
 "rows_async": $(chq --query "SELECT count() FROM parts_async")}
JSON
  cat "$OUT/parts.json"
}

cmd_failover() {
  # Take one replica of shard 1 down for a minute while writes and reads keep
  # going through the Distributed table, then bring it back and time the
  # catch-up. parallel_distributed_insert_select=0 keeps INSERT ... SELECT ...
  # LIMIT exact: since 25.7 the default (2) runs it per shard, LIMIT included.
  local before; before=$(chq --query "SELECT count() FROM events")
  local s1r2_before
  s1r2_before=$("${COMPOSE[@]}" exec -T ch-s1r2 clickhouse-client --query "SELECT count() FROM events_local")
  local t_kill; t_kill=$(now)
  "${COMPOSE[@]}" kill ch-s1r2 >/dev/null
  # shellcheck disable=SC2016  # expands inside the container, not here
  "${COMPOSE[@]}" exec -T ch-s1r1 bash -c 'ok=0; err=0; for i in $(seq 1 40); do
      if clickhouse-client --distributed_foreground_insert=1 --parallel_distributed_insert_select=0 \
         --query "INSERT INTO events SELECT * FROM events_ts ORDER BY tenant_id, ts LIMIT 50000 OFFSET $((i * 50000))" 2>/dev/null;
      then ok=$((ok+1)); else err=$((err+1)); fi; done; echo "{\"inserts_ok\": $ok, \"inserts_failed\": $err}"' \
      > "$OUT/failover-inserts.json"
  "${BENCH[@]}" tools/bench.py latency --clickhouse "$URL" --runs 30 --warmup 0 \
      --sql "SELECT count() FROM events" > "$OUT/failover-reads-while-down.json" \
      || echo '{"error": "reads failed"}' > "$OUT/failover-reads-while-down.json"
  local behind; behind=$(chq --query "SELECT count() FROM events_local")
  while [ "$("$PY" -c "print(int($(now) - $t_kill < 60))")" = 1 ]; do sleep 1; done
  local t_start; t_start=$(now)
  "${COMPOSE[@]}" start ch-s1r2 >/dev/null
  for _ in $(seq 1 120); do
    "${COMPOSE[@]}" exec -T ch-s1r2 clickhouse-client --query "SELECT 1" >/dev/null 2>&1 && break; sleep 0.5
  done
  local t_up; t_up=$(now)
  local r1 r2 q
  while true; do
    q=$("${COMPOSE[@]}" exec -T ch-s1r2 clickhouse-client --query \
        "SELECT queue_size FROM system.replicas WHERE table = 'events_local'" 2>/dev/null || echo -1)
    r1=$(chq --query "SELECT count() FROM events_local")
    r2=$("${COMPOSE[@]}" exec -T ch-s1r2 clickhouse-client --query "SELECT count() FROM events_local" 2>/dev/null || echo -1)
    [ "$r1" = "$r2" ] && [ "$q" = 0 ] && break
    sleep 0.5
  done
  local t_synced; t_synced=$(now)
  local after; after=$(chq --query "SELECT count() FROM events")
  cat > "$OUT/failover.json" <<JSON
{"rows_before": $before, "rows_after": $after, "rows_expected_after": $((before + 2000000)),
 "inserts": $(cat "$OUT/failover-inserts.json"),
 "replica_down_seconds": $(secs "$t_kill" "$t_start"),
 "rows_replica_missed_while_down": $((behind - s1r2_before)),
 "restart_to_serving_seconds": $(secs "$t_start" "$t_up"),
 "serving_to_caught_up_seconds": $(secs "$t_up" "$t_synced"),
 "s1r1_rows": $r1, "s1r2_rows": $r2}
JSON
  cat "$OUT/failover.json"
}

cmd_down() { "${COMPOSE[@]}" down -v; }

"cmd_${1:?usage: ch.sh up|schema|load|variants|mv|parts|failover|down}"
