-- ClickHouse single node: MergeTree sorted by tenant, then event type, then time.
CREATE TABLE events
(
    tenant_id UInt32, user_id UInt64, event_type LowCardinality(String),
    country LowCardinality(String), ts DateTime, duration_ms UInt32, revenue Float64
)
ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY (tenant_id, event_type, ts);
