CREATE TABLE IF NOT EXISTS raw_events
(
    event_id UUID,
    user_id String,
    movie_id String,
    event_type LowCardinality(String),
    timestamp DateTime64(3, 'UTC'),
    device_type LowCardinality(String),
    session_id String,
    progress_seconds Int32
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (toDate(timestamp), event_id);

CREATE TABLE IF NOT EXISTS kafka_events
(
    event_id String,
    user_id String,
    movie_id String,
    event_type String,
    timestamp DateTime64(3, 'UTC'),
    device_type String,
    session_id String,
    progress_seconds Int32
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092,kafka2:9092,kafka3:9092',
         kafka_topic_list = 'movie-events',
         kafka_group_name = 'cinema-clickhouse',
         kafka_format = 'AvroConfluent',
         format_avro_schema_registry_url = 'http://registry:8081',
         kafka_num_consumers = 1,
         kafka_flush_interval_ms = 1000;

CREATE MATERIALIZED VIEW IF NOT EXISTS ingest_events TO raw_events AS
SELECT toUUID(event_id) AS event_id, user_id, movie_id, event_type,
       timestamp, device_type, session_id, progress_seconds
FROM kafka_events;

CREATE TABLE IF NOT EXISTS daily_metrics
(
    day Date,
    metric LowCardinality(String),
    dimension String,
    value Nullable(Float64),
    computed_at DateTime64(6, 'UTC')
)
ENGINE = MergeTree
PARTITION BY day
ORDER BY (day, metric, dimension);

CREATE TABLE IF NOT EXISTS metrics_stage AS daily_metrics;
