-- =============================================================================
-- Incremental Watermarks - Metadata Table
-- =============================================================================
-- Tracks the last successfully processed timestamp for each source table.
-- Used by incremental loading jobs to know where to resume processing.
--
-- This table is upserted by Spark/Flink jobs after each successful load.
-- =============================================================================

CREATE TABLE IF NOT EXISTS metadata.incremental_watermarks (
    -- Composite key
    source_table                    STRING          COMMENT 'Full table name (e.g., raw.shopify_orders)',
    pipeline_name                   STRING          COMMENT 'Pipeline or job name that processes this table',

    -- Watermark values
    last_sync_timestamp             TIMESTAMP       COMMENT 'Timestamp of last successfully synced record',
    last_event_id                   STRING          COMMENT 'Last processed event/webhook ID (for deduplication)',

    -- Processing stats
    records_processed               BIGINT          COMMENT 'Number of records processed in last batch',
    bytes_processed                 BIGINT          COMMENT 'Approximate bytes processed in last batch',

    -- Status
    status                          STRING          COMMENT 'Last run status: completed, failed, running',
    error_message                   STRING          COMMENT 'Error message if status is failed',

    -- Metadata
    updated_at                      TIMESTAMP       COMMENT 'When this watermark was last updated',
    updated_by                      STRING          COMMENT 'Job/process that updated this watermark',

    -- Primary key for upserts
    PRIMARY KEY (source_table, pipeline_name) NOT ENFORCED
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.upsert.enabled' = 'true',
    'write.parquet.compression-codec' = 'zstd'
);

COMMENT ON TABLE metadata.incremental_watermarks IS 'Tracks incremental processing watermarks for all pipeline jobs.';


-- =============================================================================
-- Load Checkpoints - Idempotency Tracking
-- =============================================================================
-- Tracks individual batch loads to enable idempotent reprocessing.
-- A batch is identified by table, time range, and job run.
-- =============================================================================

CREATE TABLE IF NOT EXISTS metadata.load_checkpoints (
    -- Composite key
    source_table                    STRING          COMMENT 'Full table name being loaded',
    batch_start_time                TIMESTAMP       COMMENT 'Start of the time window for this batch',
    batch_end_time                  TIMESTAMP       COMMENT 'End of the time window for this batch',

    -- Processing info
    pipeline_name                   STRING          COMMENT 'Pipeline or job name',
    run_id                          STRING          COMMENT 'Unique run/execution ID',

    -- Status
    status                          STRING          COMMENT 'Batch status: pending, running, completed, failed',
    records_processed               BIGINT          COMMENT 'Number of records in this batch',
    snapshot_id                     BIGINT          COMMENT 'Iceberg snapshot ID created by this batch',

    -- Timing
    started_at                      TIMESTAMP       COMMENT 'When batch processing started',
    completed_at                    TIMESTAMP       COMMENT 'When batch processing completed',

    -- Error tracking
    error_message                   STRING          COMMENT 'Error message if status is failed',
    retry_count                     INT             COMMENT 'Number of retry attempts',

    -- Primary key for idempotency checks
    PRIMARY KEY (source_table, batch_start_time, batch_end_time, pipeline_name) NOT ENFORCED
)
USING iceberg
PARTITIONED BY (months(batch_start_time))
TBLPROPERTIES (
    'format-version' = '2',
    'write.upsert.enabled' = 'true',
    'write.parquet.compression-codec' = 'zstd'
);

COMMENT ON TABLE metadata.load_checkpoints IS 'Tracks individual batch loads for idempotent reprocessing.';
