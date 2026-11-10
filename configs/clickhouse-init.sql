-- ClickHouse initialization script

CREATE DATABASE IF NOT EXISTS events;

CREATE TABLE IF NOT EXISTS events.raw (
    id String,
    timestamp DateTime64(3),
    type String,
    payload String,
    metadata String,
    received_at DateTime64(3) DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toDate(timestamp)
ORDER BY (type, timestamp)
TTL timestamp + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS events.enriched (
    id String,
    timestamp DateTime64(3),
    type String,
    payload String,
    metadata String,
    enriched_at DateTime64(3) DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toDate(timestamp)
ORDER BY (type, timestamp)
TTL timestamp + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS events.dlq (
    id String,
    timestamp DateTime64(3),
    type String,
    payload String,
    metadata String,
    error String,
    original_topic String,
    failed_at DateTime64(3) DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toDate(failed_at)
ORDER BY failed_at
TTL failed_at + INTERVAL 7 DAY
SETTINGS index_granularity = 8192;

-- Create views for common queries
CREATE VIEW IF NOT EXISTS events.raw_latest AS
SELECT * FROM events.raw
ORDER BY received_at DESC
LIMIT 100;

CREATE VIEW IF NOT EXISTS events.enriched_latest AS
SELECT * FROM events.enriched
ORDER BY enriched_at DESC
LIMIT 100;

-- Grant permissions
GRANT SELECT ON events.* TO default;