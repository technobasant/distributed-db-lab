#!/usr/bin/env bash
# Citus part of the lab. One subcommand per step; each writes its evidence to
# results/citus/. Run from the repository root:
#
#   citus/scripts/citus.sh up | cluster | load | naive | plan | migrate | verify
#   citus/scripts/citus.sh queries | colocation | rebalance | skew | pool | down
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"
COMPOSE=(docker compose -f citus/docker-compose.yml)
OUT="results/citus"
RAW="results/raw"
PY="${PY:-.venv/bin/python}"
# Benchmarks and the planner run in a container on the cluster's network: the
# host path on the measuring machine added ~3 ms to every median (MEASUREMENTS.md).
BENCH=("${COMPOSE[@]}" --profile tools run --rm -T bench)
DSN="postgresql://postgres:lab-dev-only@coordinator:5432/postgres"
mkdir -p "$OUT" "$RAW"

psql_c() { "${COMPOSE[@]}" exec -T coordinator psql -U postgres -X -v ON_ERROR_STOP=1 "$@"; }
# wall-clock from the database server, the same VM clock the load generator uses
now() { psql_c -A -t -c "SELECT extract(epoch FROM clock_timestamp())"; }

wait_healthy() {
  for svc in "$@"; do
    for _ in $(seq 1 60); do
      [ "$("${COMPOSE[@]}" ps --format '{{.Health}}' "$svc" 2>/dev/null)" = "healthy" ] && break
      sleep 2
    done
  done
}

cmd_up() {
  "${COMPOSE[@]}" up -d coordinator worker1 worker2
  wait_healthy coordinator worker1 worker2
}

cmd_cluster() {
  # the extension must exist on a worker before the coordinator registers it
  for w in worker1 worker2; do
    "${COMPOSE[@]}" exec -T "$w" psql -U postgres -X -c "CREATE EXTENSION IF NOT EXISTS citus;" >/dev/null
  done
  psql_c <<'SQL'
CREATE EXTENSION IF NOT EXISTS citus;
SELECT citus_set_coordinator_host('coordinator', 5432);
SELECT citus_add_node('worker1', 5432);
SELECT citus_add_node('worker2', 5432);
SELECT nodename, nodeport, noderole FROM pg_dist_node ORDER BY nodeid;
SELECT citus_version();
SQL
}

cmd_load() {
  psql_c -f /sql/01-legacy-schema.sql >/dev/null
  psql_c -f /sql/02-load.sql | tee "$OUT/load.log"
}

cmd_naive() {
  # expected to fail: every ERROR here is a failure class the planner fixes
  "${COMPOSE[@]}" exec -T coordinator psql -U postgres -X -f /sql/03-naive-attempt.sql \
    > "$OUT/naive-attempt.log" 2>&1 || true
  grep -E "^(==|ERROR|DETAIL|HINT)" "$OUT/naive-attempt.log" || true
}

cmd_plan() {
  # tables at or under this many rows with no tenant key become reference tables;
  # CI's tiny dataset lowers it so the large-table paths still run
  "${BENCH[@]}" tools/plan_distribution.py --dsn "$DSN" --report "$OUT/plan.json" --sql "$OUT/plan.sql" \
    --reference-max-rows "${REFERENCE_MAX_ROWS:-10000}"
}

cmd_migrate() {
  local t0 t1
  t0=$(now)
  "${COMPOSE[@]}" exec -T coordinator psql -U postgres -X -v ON_ERROR_STOP=1 -q \
    < "$OUT/plan.sql" > "$OUT/migrate.log" 2>&1
  t1=$(now)
  "$PY" - "$t0" "$t1" > "$OUT/migrate.json" <<'PY'
import json, sys
t0, t1 = map(float, sys.argv[1:3])
print(json.dumps({"seconds": round(t1 - t0, 1)}, indent=2))
PY
  cat "$OUT/migrate.json"
}

cmd_verify() {
  psql_c -A -F $'\t' <<'SQL' | tee "$OUT/verify.tsv"
SELECT table_name, citus_table_type, distribution_column, colocation_id, shard_count
FROM citus_tables ORDER BY citus_table_type, table_name;
SELECT 'events' AS t, count(*) FROM events
UNION ALL SELECT 'events_null_tenant', count(*) FROM events WHERE tenant_id = 0
UNION ALL SELECT 'users', count(*) FROM users
UNION ALL SELECT 'orders', count(*) FROM orders
UNION ALL SELECT 'AuditLog', count(*) FROM "AuditLog"
UNION ALL SELECT 'webhook_deliveries', count(*) FROM webhook_deliveries;
SQL
}

cmd_queries() {
  local tenant_router="SELECT count(*), sum(value) FROM events WHERE tenant_id = %(tenant)s AND ts >= '2026-08-01'"
  local multi_shard="SELECT event_type, count(*), avg(value) FROM events GROUP BY 1"
  local tenant_join="SELECT u.country_code, count(*) FROM events e JOIN users u ON u.tenant_id = e.tenant_id AND u.id = e.user_id WHERE e.tenant_id = %(tenant)s GROUP BY 1"
  psql_c > "$OUT/plans-explain.txt" <<SQL
EXPLAIN (COSTS OFF) SELECT count(*), sum(value) FROM events WHERE tenant_id = 42 AND ts >= '2026-08-01';
EXPLAIN (COSTS OFF) $multi_shard;
EXPLAIN (COSTS OFF) SELECT u.country_code, count(*) FROM events e JOIN users u ON u.tenant_id = e.tenant_id AND u.id = e.user_id WHERE e.tenant_id = 42 GROUP BY 1;
SQL
  {
    echo '{'
    echo '"router_single_tenant":'; "${BENCH[@]}" tools/bench.py latency --dsn "$DSN" --sql "$tenant_router" --runs 200 --warmup 20
    echo ', "router_colocated_join":'; "${BENCH[@]}" tools/bench.py latency --dsn "$DSN" --sql "$tenant_join" --runs 200 --warmup 20
    echo ', "multi_shard_aggregate":'; "${BENCH[@]}" tools/bench.py latency --dsn "$DSN" --sql "$multi_shard" --runs 20 --warmup 3
    echo '}'
  } > "$OUT/queries.json"
  grep -E "Task Count|Custom Scan" "$OUT/plans-explain.txt" || true
  cat "$OUT/queries.json"
}

cmd_colocation() {
  # the same join, once co-located and once against a copy distributed by
  # user_id: Citus refuses until repartition joins are enabled, then pays for it
  psql_c <<'SQL' > "$OUT/colocation.log" 2>&1 || true
DROP TABLE IF EXISTS events_by_user;
CREATE TABLE events_by_user AS SELECT * FROM events WHERE tenant_id BETWEEN 1 AND 400;
SELECT create_distributed_table('events_by_user', 'user_id', colocate_with => 'none');
\set ON_ERROR_STOP off
SELECT count(*) FROM events_by_user e JOIN orders o ON o.user_id = e.user_id WHERE o.status = 'refunded';
SQL
  cat "$OUT/colocation.log"
  local q_colo="SELECT count(*) FROM events e JOIN orders o ON o.tenant_id = e.tenant_id AND o.user_id = e.user_id WHERE o.status = 'refunded' AND e.tenant_id BETWEEN 1 AND 400"
  local q_repart="SELECT count(*) FROM events_by_user e JOIN orders o ON o.user_id = e.user_id WHERE o.status = 'refunded'"
  {
    echo '{"colocated_join":'; "${BENCH[@]}" tools/bench.py latency --dsn "$DSN" --sql "$q_colo" --runs 7 --warmup 1
    echo ', "repartition_join":'; "${BENCH[@]}" tools/bench.py latency --dsn "$DSN" --sql "$q_repart" --runs 7 --warmup 1 \
      --set "SET citus.enable_repartition_joins = on"
    echo '}'
  } > "$OUT/colocation.json"
  # a CTAS copy has no primary key, and one table without a replica identity
  # blocks the online rebalancer for the whole cluster (found the hard way)
  psql_c -c "DROP TABLE IF EXISTS events_by_user;" >/dev/null
  cat "$OUT/colocation.json"
}

shard_sizes() {
  psql_c -A -F $'\t' -c "
    SELECT nodename, count(*) AS shards, pg_size_pretty(sum(shard_size)) AS size, sum(shard_size) AS bytes
    FROM citus_shards WHERE table_name::text = 'events' GROUP BY 1 ORDER BY 1;"
}

cmd_rebalance() {
  "${COMPOSE[@]}" --profile scale up -d worker3
  wait_healthy worker3
  "${COMPOSE[@]}" exec -T worker3 psql -U postgres -X -c "CREATE EXTENSION IF NOT EXISTS citus;" >/dev/null
  shard_sizes > "$OUT/rebalance-before.tsv"
  psql_c -c "SELECT citus_add_node('worker3', 5432) WHERE NOT EXISTS
             (SELECT 1 FROM pg_dist_node WHERE nodename = 'worker3');"
  local seconds="${LOAD_SECONDS:-240}"
  "${BENCH[@]}" tools/bench.py load --dsn "$DSN" --threads 8 --seconds "$seconds" --out "$RAW/rebalance-load.csv" \
    > "$OUT/rebalance-load-run.json" &
  local load_pid=$!
  sleep 30   # a baseline window under load before anything moves
  local t0 t1
  t0=$(now)
  psql_c -c "SELECT citus_rebalance_start();"
  while true; do
    state=$(psql_c -A -t -c "SELECT state FROM citus_rebalance_status() LIMIT 1;")
    case "$state" in finished|failed|cancelled) break ;; esac
    sleep 2
  done
  t1=$(now)
  psql_c -c "SELECT * FROM citus_rebalance_status();" > "$OUT/rebalance-status.txt"
  wait "$load_pid"
  shard_sizes > "$OUT/rebalance-after.tsv"
  printf '{"start": %s, "end": %s}\n' "$t0" "$t1" > "$OUT/rebalance-marks.json"
  "${BENCH[@]}" tools/bench.py summarize --load "$RAW/rebalance-load.csv" --marks "$OUT/rebalance-marks.json" \
    > "$OUT/rebalance-summary.json"
  echo "rebalance state: $state, seconds: $("$PY" -c "print(round($t1-$t0,1))")"
  cat "$OUT/rebalance-before.tsv" "$OUT/rebalance-after.tsv"
}

node_bytes() {
  psql_c -A -F $'\t' -c "SELECT nodename, count(DISTINCT shardid) FILTER (WHERE table_name::text = 'events') AS event_shards,
    sum(shard_size) AS bytes FROM citus_shards WHERE nodename <> 'coordinator' GROUP BY 1 ORDER BY 1;"
}

cmd_skew() {
  {
    echo "## node bytes after the count-based rebalance"
    node_bytes
    echo "## events shards: largest, median, and the whale's"
    psql_c -A -F $'\t' -c "
      WITH s AS (SELECT shardid, nodename, shard_size FROM citus_shards WHERE table_name::text = 'events')
      SELECT 'largest', max(shard_size) FROM s
      UNION ALL SELECT 'median', percentile_disc(0.5) WITHIN GROUP (ORDER BY shard_size) FROM s
      UNION ALL SELECT 'whale (tenant 1)', shard_size FROM s
        WHERE shardid = get_shard_id_for_distribution_column('events', 1);"
    echo "## moves each strategy would make now"
    psql_c -A -F $'\t' -c "
      SELECT 'by_shard_count', count(*) FROM get_rebalance_table_shards_plan(rebalance_strategy => 'by_shard_count')
      UNION ALL SELECT 'by_disk_size', count(*) FROM get_rebalance_table_shards_plan(rebalance_strategy => 'by_disk_size');"
  } | tee "$OUT/skew-before.tsv"

  # give the whale its own shard group: the shard holding tenant 1 is split
  # into three, tenant 1 alone in the middle one, for every co-located table
  local t0 t1
  t0=$(now)
  psql_c -c "SELECT isolate_tenant_to_new_shard('tenants', 1, cascade_option => 'CASCADE');" | tee "$OUT/isolate.log"
  t1=$(now)
  echo "isolate seconds: $("$PY" -c "print(round($t1 - $t0, 1))")" | tee -a "$OUT/isolate.log"
  psql_c -A -F $'\t' -c "
    SELECT shardid, nodename, shard_size FROM citus_shards
    WHERE table_name::text = 'events' AND shardid = get_shard_id_for_distribution_column('events', 1);" \
    | tee -a "$OUT/isolate.log"

  t0=$(now)
  psql_c -c "SELECT citus_rebalance_start(rebalance_strategy => 'by_disk_size');" -c "SELECT citus_rebalance_wait();"
  t1=$(now)
  {
    echo "## by_disk_size rebalance seconds: $("$PY" -c "print(round($t1 - $t0, 1))")"
    psql_c -A -F $'\t' -c "SELECT state, jsonb_array_length(coalesce(details->'tasks', '[]')) FROM citus_rebalance_status() ORDER BY job_id DESC LIMIT 1;"
    echo "## node bytes after isolating the whale and rebalancing by disk size"
    node_bytes
  } | tee "$OUT/skew-after.tsv"
}

cmd_pool() {
  "${COMPOSE[@]}" --profile pool up -d pgbouncer
  sleep 3
  local pooled="postgresql://postgres:lab-dev-only@localhost:6440/postgres"
  {
    echo '{'
    first=1
    for threads in 16 64 128; do
      for target in direct pooled; do
        dsn=$DSN; [ "$target" = pooled ] && dsn=$pooled
        [ $first = 1 ] || echo ','
        first=0
        echo "\"${target}_${threads}\":"
        "${BENCH[@]}" tools/bench.py load --dsn "$dsn" --threads "$threads" --seconds 45 \
          --out "$RAW/pool-$target-$threads.csv" >/dev/null
        "${BENCH[@]}" tools/bench.py summarize --load "$RAW/pool-$target-$threads.csv"
      done
    done
    echo '}'
  } > "$OUT/pool.json"
  cat "$OUT/pool.json"
}

cmd_down() { "${COMPOSE[@]}" --profile scale --profile pool down -v; }

"cmd_${1:?usage: citus.sh up|cluster|load|naive|plan|migrate|verify|queries|colocation|rebalance|skew|pool|down}"
