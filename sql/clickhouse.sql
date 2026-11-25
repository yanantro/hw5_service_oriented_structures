CREATE DATABASE IF NOT EXISTS cinema;
CREATE TABLE IF NOT EXISTS cinema.events
(event_id String, user_id String, movie_id String, event_type String, timestamp DateTime64(3, 'UTC'), device_type String, session_id String, progress_seconds Int32, app_version String)
ENGINE = ReplacingMergeTree PARTITION BY toYYYYMM(timestamp) ORDER BY (toDate(timestamp), user_id, event_id);
CREATE TABLE IF NOT EXISTS cinema.kafka_events
(event_id String, user_id String, movie_id String, event_type String, timestamp DateTime64(3, 'UTC'), device_type String, session_id String, progress_seconds Int32, app_version String)
ENGINE = Kafka SETTINGS kafka_broker_list = 'kafka1:9092,kafka2:9092,kafka3:9092', kafka_topic_list = 'movie-events', kafka_group_name = 'clickhouse-cinema', kafka_format = 'AvroConfluent', kafka_num_consumers = 3, kafka_thread_per_consumer = 1, format_avro_schema_registry_url = 'http://schema-registry:8081', input_format_avro_allow_missing_fields = 1;
CREATE MATERIALIZED VIEW IF NOT EXISTS cinema.ingest TO cinema.events AS SELECT * FROM cinema.kafka_events;
CREATE TABLE IF NOT EXISTS cinema.metrics
(date Date, metric String, dimension String, value Nullable(Float64), calculated_at DateTime64(6, 'UTC'))
ENGINE = MergeTree PARTITION BY date ORDER BY (metric, dimension);
CREATE TABLE IF NOT EXISTS cinema.metrics_stage AS cinema.metrics;
