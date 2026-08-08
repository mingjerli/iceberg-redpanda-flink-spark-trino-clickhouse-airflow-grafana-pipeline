-- =============================================================================
-- Analytics: ga4_engagement_metrics
-- =============================================================================
-- Daily rollup of GA4 web engagement, aggregated from staging.stg_ga4_sessions.
-- One row per calendar day; the grain for the marts engagement dashboard.
--
-- Note: This file is reference documentation only. The actual CREATE TABLE
-- statement is inline in jobs/spark/analytics_incremental.py.
-- =============================================================================

CREATE TABLE IF NOT EXISTS analytics.ga4_engagement_metrics (
    -- Grain
    metric_date                 DATE            COMMENT 'Session date (one row per day)',

    -- Volume
    total_sessions              BIGINT          COMMENT 'Sessions started on this date',
    total_users                 BIGINT          COMMENT 'Distinct client_id values',
    total_page_views            BIGINT          COMMENT 'page_view events across all sessions',

    -- Engagement
    engaged_sessions            BIGINT          COMMENT 'Sessions with >10s engagement, 2+ page views, or a conversion',
    bounced_sessions            BIGINT          COMMENT 'Single-page-view sessions that were not engaged',
    avg_session_duration_sec    DECIMAL(10, 2)  COMMENT 'Mean session_end - session_start, in seconds',
    avg_engagement_time_ms      BIGINT          COMMENT 'Mean total engagement time per session',

    -- Conversions
    total_conversions           BIGINT          COMMENT 'Conversion events (purchase, sign_up)',
    total_conversion_value      DECIMAL(18, 2)  COMMENT 'Summed value of conversion events',

    -- Rates (0.0 to 1.0)
    engagement_rate             DECIMAL(5, 4)   COMMENT 'engaged_sessions / total_sessions',
    bounce_rate                 DECIMAL(5, 4)   COMMENT 'bounced_sessions / total_sessions',
    conversion_rate             DECIMAL(5, 4)   COMMENT 'Sessions with a conversion / total_sessions',
    avg_events_per_session      DECIMAL(10, 2)  COMMENT 'Mean event_count per session',

    -- Metadata
    _computed_at                TIMESTAMP       COMMENT 'Computation time',
    _version                    INT             COMMENT 'Schema version'
)
USING iceberg
PARTITIONED BY (months(metric_date))
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);
