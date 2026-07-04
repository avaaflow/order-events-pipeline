CREATE DATABASE IF NOT EXISTS orders;

CREATE TABLE IF NOT EXISTS orders.events_raw
(
    event_id          UUID,
    order_id          UUID,
    user_id           UInt32,
    restaurant_id     UInt32,
    event_type        LowCardinality(String),
    timestamp         DateTime64(3, 'Asia/Tehran'),
    amount            UInt32,
    city              LowCardinality(String),
    delivery_time_sec Nullable(UInt32),
    ingested_at       DateTime64(3, 'Asia/Tehran') DEFAULT now64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (city, restaurant_id, timestamp)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS orders.events_hourly
(
    hour              DateTime,
    city              LowCardinality(String),
    event_type        LowCardinality(String),
    event_count       UInt64,
    total_amount      UInt64,
    avg_delivery_sec  Nullable(Float64)
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (city, event_type, hour);

CREATE MATERIALIZED VIEW IF NOT EXISTS orders.events_hourly_mv
TO orders.events_hourly
AS
SELECT
    toStartOfHour(timestamp) AS hour,
    city,
    event_type,
    count() AS event_count,
    sum(amount) AS total_amount,
    avg(delivery_time_sec) AS avg_delivery_sec
FROM orders.events_raw
GROUP BY hour, city, event_type;
