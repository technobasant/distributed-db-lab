-- Event analytics on the cluster. Replicated tables per shard, a Distributed
-- table in front, sharded by tenant so one tenant's rows live on one shard —
-- the same placement idea as Citus, implemented with a sharding expression.

CREATE TABLE IF NOT EXISTS events_local ON CLUSTER lab
(
    tenant_id   UInt32,
    user_id     UInt64,
    event_type  LowCardinality(String),
    country     LowCardinality(String),
    ts          DateTime,
    duration_ms UInt32,
    revenue     Float64
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/events_local', '{replica}')
PARTITION BY toYYYYMM(ts)
ORDER BY (tenant_id, event_type, ts);

CREATE TABLE IF NOT EXISTS events ON CLUSTER lab AS events_local
ENGINE = Distributed(lab, default, events_local, intHash64(tenant_id));

-- daily rollup maintained on insert: one row per (day, tenant, event type)
CREATE TABLE IF NOT EXISTS events_daily_local ON CLUSTER lab
(
    day        Date,
    tenant_id  UInt32,
    event_type LowCardinality(String),
    events     AggregateFunction(count),
    users      AggregateFunction(uniq, UInt64),
    revenue    AggregateFunction(sum, Float64)
)
ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/events_daily_local', '{replica}')
PARTITION BY toYYYYMM(day)
ORDER BY (tenant_id, event_type, day);

CREATE MATERIALIZED VIEW IF NOT EXISTS events_daily_mv ON CLUSTER lab TO events_daily_local AS
SELECT toDate(ts) AS day, tenant_id, event_type,
       countState() AS events, uniqState(user_id) AS users, sumState(revenue) AS revenue
FROM events_local GROUP BY day, tenant_id, event_type;

CREATE TABLE IF NOT EXISTS events_daily ON CLUSTER lab AS events_daily_local
ENGINE = Distributed(lab, default, events_daily_local, intHash64(tenant_id));
