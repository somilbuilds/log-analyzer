-- ============================================================
-- Hive External Table over Parquet data written by Spark
-- ============================================================
-- These tables point to Parquet files in HDFS produced by the
-- Spark Structured Streaming job, partitioned by date and hour.
-- ============================================================

-- Create database
CREATE DATABASE IF NOT EXISTS web_analytics;
USE web_analytics;

-- External table over Spark-produced Parquet files
-- The schema matches the JSON fields from the log replayer
CREATE EXTERNAL TABLE IF NOT EXISTS web_logs (
    host        STRING    COMMENT 'Client hostname or IP',
    timestamp   STRING    COMMENT 'Original log timestamp',
    method      STRING    COMMENT 'HTTP method (GET, POST, etc.)',
    path        STRING    COMMENT 'Requested URL path',
    protocol    STRING    COMMENT 'HTTP protocol version',
    status      INT       COMMENT 'HTTP status code',
    bytes       INT       COMMENT 'Response size in bytes',
    raw         STRING    COMMENT 'Original raw log line',
    replay_timestamp STRING COMMENT 'Wall-clock time when replayed',
    process_time STRING   COMMENT 'Spark processing timestamp'
)
PARTITIONED BY (
    date     STRING  COMMENT 'Partition: date (YYYY-MM-DD)',
    hour_val INT     COMMENT 'Partition: hour (0-23)'
)
STORED AS PARQUET
LOCATION '/user/spark/web_logs';

-- Repair partitions (discover existing partitions in HDFS)
MSCK REPAIR TABLE web_logs;
