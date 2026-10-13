#!/usr/bin/env bash
# Load one time-ordered dataset into each engine in turn and measure ingest,
# on-disk size and four queries, warm (median of 5 after a warm-up run) and
# cold (container restarted and the Docker VM page cache dropped before each).
#
#   compare/scripts/compare.sh data | pg | citus | tsdb | ch | all
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"
COMPOSE=(docker compose -f compare/docker-compose.yml)
BENCH=("${COMPOSE[@]}" --profile tools run --rm -T bench)
OUT="results/compare"
PY="${PY:-.venv/bin/python}"
mkdir -p "$OUT"

# Postgres-dialect and ClickHouse-dialect versions of the same four questions
PG_Q1="SELECT event_type, count(*) FROM events GROUP BY 1"
PG_Q2="SELECT date_trunc('day', ts) AS d, count(*) FROM events WHERE tenant_id = %(tenant)s GROUP BY 1"
PG_Q3="SELECT tenant_id, count(DISTINCT user_id) AS u FROM events WHERE ts >= '2026-08-03+00' AND ts < '2026-08-10+00' GROUP BY 1 ORDER BY u DESC LIMIT 10"
PG_Q4="SELECT country, sum(revenue) FROM events WHERE event_type = 'purchase' AND ts >= '2026-08-30+00' GROUP BY 1"
CH_Q1="SELECT event_type, count() FROM events GROUP BY 1"
CH_Q2="SELECT toDate(ts) AS d, count() FROM events WHERE tenant_id = %(tenant)s GROUP BY d"
CH_Q3="SELECT tenant_id, uniqExact(user_id) AS u FROM events WHERE ts >= '2026-08-03 00:00:00' AND ts < '2026-08-10 00:00:00' GROUP BY tenant_id ORDER BY u DESC LIMIT 10"
CH_Q4="SELECT country, sum(revenue) FROM events WHERE event_type = 'purchase' AND ts >= '2026-08-30 00:00:00' GROUP BY country"

wait_healthy() {
  for _ in $(seq 1 90); do
    [ "$("${COMPOSE[@]}" --profile "$1" ps --format '{{.Health}}' "$1" 2>/dev/null)" = healthy ] && break; sleep 2
  done
  # The Postgres images run initdb and setup scripts on a temporary server and
  # then restart it, so one healthy probe can land on the server that is about
  # to go away. Require three consecutive answers.
  [ "$1" = ch ] && return
  local ok=0
  for _ in $(seq 1 90); do
    if "${COMPOSE[@]}" exec -T "$1" psql -U postgres -X -A -t -c "SELECT 1" >/dev/null 2>&1; then
      ok=$((ok + 1)); [ "$ok" -ge 3 ] && return
    else ok=0; fi
    sleep 2
  done
}
drop_caches() { docker run --rm --privileged alpine sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches' >/dev/null; }
now() { "$PY" -c 'import time; print(time.time())'; }

cmd_data() {
  [ -f data/events/events.csv ] || "$PY" tools/datagen.py events --scale lab --out data/events
  # events arrive in time order in real feeds; load every engine in that order
  { head -1 data/events/events.csv; tail -n +2 data/events/events.csv | LC_ALL=C sort -t, -k5,5 -S 1G; } \
    > data/events/events_by_ts.csv
  wc -l data/events/events_by_ts.csv
}

query_set() { # engine, --dsn|--clickhouse, target, q1..q4
  local engine=$1 flag=$2 target=$3; shift 3
  local i=1 first=1
  echo '{'
  for q in "$@"; do
    [ $first = 1 ] || echo ','; first=0
    echo "\"q$i\": {\"warm\":"
    "${BENCH[@]}" tools/bench.py latency "$flag" "$target" --sql "$q" --runs 5 --warmup 1 --tenants 2..2000
    echo ', "cold":'
    "${COMPOSE[@]}" --profile "$engine" restart "$engine" >/dev/null
    wait_healthy "$engine"
    drop_caches
    "${BENCH[@]}" tools/bench.py latency "$flag" "$target" --sql "$q" --runs 1 --warmup 0 --tenants 2..2000
    echo '}'
    i=$((i + 1))
  done
  echo '}'
}

run_pg_engine() { # name
  local e=$1
  "${COMPOSE[@]}" --profile "$e" up -d "$e"; wait_healthy "$e"
  local t0 t1
  t0=$(now)
  "${COMPOSE[@]}" exec -T "$e" psql -U postgres -X -v ON_ERROR_STOP=1 -f "/sql/$e.sql" > "$OUT/$e-load.log" 2>&1
  t1=$(now)
  query_set "$e" --dsn "postgresql://postgres:lab-dev-only@$e:5432/postgres" \
    "$PG_Q1" "$PG_Q2" "$PG_Q3" "$PG_Q4" > "$OUT/$e-queries.json"
  "$PY" - "$e" "$t0" "$t1" <<'PY'
import json, re, sys
e, t0, t1 = sys.argv[1], float(sys.argv[2]), float(sys.argv[3])
log = open(f"results/compare/{e}-load.log").read()
def after(marker):
    # the longest statement after the marker and before the next one is the
    # step being measured (an ALTER TABLE before compress_chunk takes 3 ms)
    m = re.search(re.escape(marker) + r"(.*?)(?:\n== |\Z)", log, re.S)
    times = [float(t) for t in re.findall(r"Time: ([\d.]+) ms", m.group(1))] if m else []
    return round(max(times) / 1000, 1) if times else None
size = int(re.findall(r"^\s*(\d{6,})\s*$", log, re.M)[-1])
out = {"engine": e, "copy_seconds": after("== ingest"), "compress_seconds": after("== compress"),
       "index_seconds": after("== index"), "script_seconds": round(t1 - t0, 1), "bytes": size}
rows = 10_000_000
if out["copy_seconds"]:
    out["ingest_rows_per_s"] = round(rows / out["copy_seconds"])
json.dump(out, open(f"results/compare/{e}-load.json", "w"), indent=2)
print(json.dumps(out))
PY
  "${COMPOSE[@]}" --profile "$e" down -v >/dev/null
}

cmd_pg() { run_pg_engine pg; }
cmd_citus() { run_pg_engine citus; }
cmd_tsdb() { run_pg_engine tsdb; }

cmd_ch() {
  "${COMPOSE[@]}" --profile ch up -d ch; wait_healthy ch
  "${COMPOSE[@]}" exec -T ch clickhouse-client --multiquery < compare/sql/ch.sql
  local t0 t1
  t0=$(now)
  "${COMPOSE[@]}" exec -T ch sh -c 'clickhouse-client --date_time_input_format=best_effort \
      --query "INSERT INTO events FORMAT CSVWithNames" < /data/events/events_by_ts.csv'
  t1=$(now)
  "${COMPOSE[@]}" exec -T ch clickhouse-client --query "OPTIMIZE TABLE events FINAL" || true
  local bytes
  bytes=$("${COMPOSE[@]}" exec -T ch clickhouse-client --query \
    "SELECT sum(bytes_on_disk) FROM system.parts WHERE table = 'events' AND active")
  query_set ch --clickhouse "http://ch:8123" "$CH_Q1" "$CH_Q2" "$CH_Q3" "$CH_Q4" > "$OUT/ch-queries.json"
  "$PY" -c "import json; s=$t1-$t0; d={'engine':'ch','copy_seconds':round(s,1),'bytes':$bytes,'ingest_rows_per_s':round(10_000_000/s)}; json.dump(d, open('$OUT/ch-load.json','w'), indent=2); print(json.dumps(d))"
  "${COMPOSE[@]}" --profile ch down -v >/dev/null
}

cmd_all() { cmd_pg; cmd_citus; cmd_tsdb; cmd_ch; }

"cmd_${1:?usage: compare.sh data|pg|citus|tsdb|ch|all}"
