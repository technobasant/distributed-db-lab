#!/usr/bin/env bash
# TimescaleDB part of the lab. Run from the repository root:
#
#   timescaledb/scripts/tsdb.sh up | load | keys | chunks | compress | caggs | retention | vacuum | down
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"
COMPOSE=(docker compose -f timescaledb/docker-compose.yml)
BENCH=("${COMPOSE[@]}" --profile tools run --rm -T bench)
DSN="postgresql://postgres:lab-dev-only@tsdb:5432/postgres"
OUT="results/timescaledb"
PY="${PY:-.venv/bin/python}"
mkdir -p "$OUT"

psql_t() { "${COMPOSE[@]}" exec -T tsdb psql -U postgres -X -v ON_ERROR_STOP=1 "$@"; }
now() { psql_t -A -t -c "SELECT extract(epoch FROM clock_timestamp())"; }
secs() { "$PY" -c "print(round($2 - $1, 1))"; }

# entity-filtered queries (what an API serving one instrument asks) and one
# time-filtered query across every entity (what a dashboard asks)
Q_ENTITY_RECENT="SELECT ts, value FROM %T% WHERE entity_id = %(tenant)s AND metric = 'price' AND ts >= '2026-07-12 18:00+00'"
Q_ENTITY_RANGE="SELECT time_bucket('1 hour', ts) AS h, avg(value) FROM %T% WHERE entity_id = %(tenant)s AND metric = 'price' AND ts >= '2026-07-10+00' GROUP BY 1"
Q_ALL_RECENT="SELECT entity_id, last(value, ts) FROM %T% WHERE metric = 'price' AND ts >= '2026-07-12 23:45+00' GROUP BY 1"

lat() { # table, label, query-template, runs
  local sql="${3//%T%/$1}"
  echo "\"$2\":"
  "${BENCH[@]}" tools/bench.py latency --dsn "$DSN" --sql "$sql" --runs "${4:-50}" --warmup 5 --tenants 1..100
}

cmd_up() {
  "${COMPOSE[@]}" up -d tsdb
  for _ in $(seq 1 60); do
    [ "$("${COMPOSE[@]}" ps --format '{{.Health}}' tsdb)" = healthy ] && break; sleep 2
  done
}

cmd_load() {
  local t0 t1
  psql_t -c "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'" > "$OUT/version.txt" 2>/dev/null || true
  t0=$(now)
  psql_t -f /sql/01-schema-and-load.sql | tee "$OUT/load.log"
  t1=$(now)
  echo "load seconds (both tables + vacuum analyze): $(secs "$t0" "$t1")" | tee -a "$OUT/load.log"
  psql_t -A -F $'\t' -f /sql/02-sizes.sql | tee "$OUT/sizes.tsv"
}

cmd_keys() {
  {
    echo '{'
    lat ticks_time_pk time_pk_entity_recent "$Q_ENTITY_RECENT"; echo ','
    lat ticks_time_pk time_pk_entity_range "$Q_ENTITY_RANGE"; echo ','
    lat ticks_time_pk time_pk_all_recent "$Q_ALL_RECENT"; echo ','
    lat ticks_entity_pk entity_pk_entity_recent "$Q_ENTITY_RECENT"; echo ','
    lat ticks_entity_pk entity_pk_entity_range "$Q_ENTITY_RANGE"; echo ','
    lat ticks_entity_pk entity_pk_all_recent "$Q_ALL_RECENT"; echo ','
  } > "$OUT/keys.json.part"
  local q42="${Q_ENTITY_RECENT//%(tenant)s/42}"
  psql_t > "$OUT/keys-explain.txt" <<SQL
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) ${q42//%T%/ticks_time_pk};
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) ${q42//%T%/ticks_entity_pk};
SQL
  # the usual repair: keep the time-led key and bolt on an entity index
  local t0 t1
  t0=$(now)
  psql_t -c "CREATE INDEX ticks_time_pk_entity_idx ON ticks_time_pk (entity_id, metric, ts);"
  t1=$(now)
  psql_t -c "VACUUM ANALYZE ticks_time_pk;"
  {
    cat "$OUT/keys.json.part"
    lat ticks_time_pk time_pk_plus_index_entity_recent "$Q_ENTITY_RECENT"; echo ','
    lat ticks_time_pk time_pk_plus_index_entity_range "$Q_ENTITY_RANGE"
    echo ", \"extra_index_build_seconds\": $(secs "$t0" "$t1")"
    echo '}'
  } > "$OUT/keys.json"
  rm -f "$OUT/keys.json.part"
  psql_t -A -F $'\t' -f /sql/02-sizes.sql | tee "$OUT/sizes-after-extra-index.tsv"
}

cmd_chunks() {
  psql_t -f /sql/03-chunk-intervals.sql | tee "$OUT/chunks.log"
  {
    echo '{'
    first=1
    for t in ticks_1h ticks_entity_pk ticks_7d; do
      for q in recent range; do
        [ $first = 1 ] || echo ','; first=0
        tmpl=$Q_ENTITY_RECENT; [ $q = range ] && tmpl=$Q_ENTITY_RANGE
        echo "\"${t}_${q}\":"
        "${BENCH[@]}" tools/bench.py explain --dsn "$DSN" --sql "${tmpl//%T%/$t}" --runs 25
      done
    done
    echo '}'
  } > "$OUT/chunks.json"
}

cmd_compress() {
  {
    echo '{'
    lat ticks_entity_pk uncompressed_entity_range "$Q_ENTITY_RANGE" 30; echo ','
    lat ticks_entity_pk uncompressed_all_recent "$Q_ALL_RECENT" 30; echo ','
  } > "$OUT/compress.json.part"
  local t0 t1
  t0=$(now)
  psql_t -f /sql/04-compress.sql | tee "$OUT/compress.log"
  t1=$(now)
  {
    cat "$OUT/compress.json.part"
    lat ticks_entity_pk compressed_entity_range "$Q_ENTITY_RANGE" 30; echo ','
    lat ticks_entity_pk compressed_all_recent "$Q_ALL_RECENT" 30
    echo ", \"compress_seconds\": $(secs "$t0" "$t1")"
    echo '}'
  } > "$OUT/compress.json"
  rm -f "$OUT/compress.json.part"
}

cmd_caggs() {
  local t0 t1
  t0=$(now)
  psql_t -f /sql/05-caggs.sql | tee "$OUT/caggs.log"
  t1=$(now)
  local raw="SELECT time_bucket('1 day', ts) d, first(value, ts), max(value), min(value), last(value, ts) FROM ticks_entity_pk WHERE entity_id = %(tenant)s AND metric = 'price' GROUP BY 1"
  local cagg="SELECT bucket, open, high, low, close FROM candles_1d WHERE entity_id = %(tenant)s ORDER BY 1"
  {
    echo '{"daily_candles_from_raw":'
    "${BENCH[@]}" tools/bench.py latency --dsn "$DSN" --sql "$raw" --runs 30 --warmup 3 --tenants 1..100
    echo ', "daily_candles_from_cagg":'
    "${BENCH[@]}" tools/bench.py latency --dsn "$DSN" --sql "$cagg" --runs 30 --warmup 3 --tenants 1..100
    echo ", \"build_seconds_all_three\": $(secs "$t0" "$t1")"
    echo '}'
  } > "$OUT/caggs.json"
}

cmd_retention() { psql_t -f /sql/06-retention.sql | tee "$OUT/retention.log"; }

cmd_vacuum() { psql_t -f /sql/07-insert-only-vacuum.sql 2>&1 | tee "$OUT/vacuum.log"; }

cmd_down() { "${COMPOSE[@]}" down -v; }

"cmd_${1:?usage: tsdb.sh up|load|keys|chunks|compress|caggs|retention|vacuum|down}"
