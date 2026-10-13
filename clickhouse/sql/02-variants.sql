-- The same rows under two design choices, to measure what each one buys.
--   events_ts_local   ORDER BY (ts)        — the "it's time-series" default
--   events_str_local  String, not LowCardinality(String), for the low-cardinality columns
CREATE TABLE IF NOT EXISTS events_ts_local ON CLUSTER lab AS events_local
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/events_ts_local', '{replica}')
PARTITION BY toYYYYMM(ts) ORDER BY (ts);
CREATE TABLE IF NOT EXISTS events_ts ON CLUSTER lab AS events_ts_local
ENGINE = Distributed(lab, default, events_ts_local, intHash64(tenant_id));

CREATE TABLE IF NOT EXISTS events_str_local ON CLUSTER lab
(
    tenant_id UInt32, user_id UInt64, event_type String, country String,
    ts DateTime, duration_ms UInt32, revenue Float64
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/events_str_local', '{replica}')
PARTITION BY toYYYYMM(ts) ORDER BY (tenant_id, event_type, ts);
CREATE TABLE IF NOT EXISTS events_str ON CLUSTER lab AS events_str_local
ENGINE = Distributed(lab, default, events_str_local, intHash64(tenant_id));

INSERT INTO events_ts  SELECT * FROM events SETTINGS distributed_foreground_insert = 1;
INSERT INTO events_str SELECT * FROM events SETTINGS distributed_foreground_insert = 1;
OPTIMIZE TABLE events_ts_local  ON CLUSTER lab FINAL;
OPTIMIZE TABLE events_str_local ON CLUSTER lab FINAL;
OPTIMIZE TABLE events_local     ON CLUSTER lab FINAL;
