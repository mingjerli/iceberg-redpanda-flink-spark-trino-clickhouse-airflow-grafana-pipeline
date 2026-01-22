-- =============================================================================
-- Flink SQL: Metadata Tables Setup
-- =============================================================================
-- Creates metadata tables for incremental tracking and checkpointing
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Create Iceberg Catalog Connection
-- -----------------------------------------------------------------------------
CREATE CATALOG iceberg_catalog WITH (
    'type' = 'iceberg',
    'catalog-type' = 'rest',
    'uri' = 'http://iceberg-rest:8181',
    'warehouse' = 's3a://warehouse/',
    'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
    's3.endpoint' = 'http://minio:9000',
    's3.path-style-access' = 'true',
    's3.access-key-id' = 'admin',
    's3.secret-access-key' = 'admin123456'
);

-- Use the Iceberg catalog
USE CATALOG iceberg_catalog;

-- Create Metadata Database
CREATE DATABASE IF NOT EXISTS metadata
COMMENT 'Metadata tables for tracking and watermarks';

USE metadata;

-- -----------------------------------------------------------------------------
-- Incremental Watermarks Table
-- -----------------------------------------------------------------------------
-- Tracks the last successfully processed timestamp for each source table.
-- Used by incremental loading jobs to know where to resume processing.
CREATE TABLE IF NOT EXISTS incremental_watermarks (
    `source_table`          STRING      COMMENT 'Full table name (e.g., raw.shopify_orders)',
    `pipeline_name`         STRING      COMMENT 'Pipeline or job name that processes this table',
    `last_sync_timestamp`   TIMESTAMP(3) COMMENT 'Timestamp of last successfully synced record',
    `last_event_id`         STRING      COMMENT 'Last processed event/webhook ID (for deduplication)',
    `records_processed`     BIGINT      COMMENT 'Number of records processed in last batch',
    `bytes_processed`       BIGINT      COMMENT 'Approximate bytes processed in last batch',
    `status`                STRING      COMMENT 'Last run status: completed, failed, running',
    `error_message`         STRING      COMMENT 'Error message if status is failed',
    `updated_at`            TIMESTAMP(3) COMMENT 'When this watermark was last updated',
    `updated_by`            STRING      COMMENT 'Job/process that updated this watermark',
    PRIMARY KEY (`source_table`, `pipeline_name`) NOT ENFORCED
) WITH (
    'format-version' = '2',
    'write.upsert.enabled' = 'true'
);

-- -----------------------------------------------------------------------------
-- Load Checkpoints Table
-- -----------------------------------------------------------------------------
-- Tracks individual batch loads to enable idempotent reprocessing.
CREATE TABLE IF NOT EXISTS load_checkpoints (
    `source_table`          STRING      COMMENT 'Full table name being loaded',
    `batch_start_time`      TIMESTAMP(3) COMMENT 'Start of the time window for this batch',
    `batch_end_time`        TIMESTAMP(3) COMMENT 'End of the time window for this batch',
    `pipeline_name`         STRING      COMMENT 'Pipeline or job name',
    `run_id`                STRING      COMMENT 'Unique run/execution ID',
    `status`                STRING      COMMENT 'Batch status: pending, running, completed, failed',
    `records_processed`     BIGINT      COMMENT 'Number of records in this batch',
    `snapshot_id`           BIGINT      COMMENT 'Iceberg snapshot ID created by this batch',
    `started_at`            TIMESTAMP(3) COMMENT 'When batch processing started',
    `completed_at`          TIMESTAMP(3) COMMENT 'When batch processing completed',
    `error_message`         STRING      COMMENT 'Error message if status is failed',
    `retry_count`           INT         COMMENT 'Number of retry attempts',
    PRIMARY KEY (`source_table`, `batch_start_time`, `batch_end_time`, `pipeline_name`) NOT ENFORCED
) WITH (
    'format-version' = '2',
    'write.upsert.enabled' = 'true'
);
