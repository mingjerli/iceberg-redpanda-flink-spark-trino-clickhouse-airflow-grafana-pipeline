-- =============================================================================
-- Analytics: ga4_engagement_by_channel
-- =============================================================================
-- Daily GA4 engagement split by acquisition channel, aggregated from
-- staging.stg_ga4_sessions. Same measures as ga4_engagement_metrics, one grain
-- finer: (metric_date, channel_group, traffic_source, traffic_medium).
--
-- channel_group is derived in staging and takes one of: direct, organic_search,
-- paid_search, social, email, referral, sms, other.
--
-- Note: This file is reference documentation only. The actual CREATE TABLE
-- statement is inline in jobs/spark/analytics_incremental.py.
-- =============================================================================

CREATE TABLE IF NOT EXISTS analytics.ga4_engagement_by_channel (
    -- Grain
    metric_date                 DATE            COMMENT 'Session date',
    channel_group               STRING          COMMENT 'Derived channel grouping, e.g. organic_search',
    traffic_source              STRING          COMMENT 'Raw traffic source, e.g. google',
    traffic_medium              STRING          COMMENT 'Raw traffic medium, e.g. organic',

    -- Volume
    total_sessions              BIGINT          COMMENT 'Sessions attributed to this channel',
    engaged_sessions            BIGINT          COMMENT 'Sessions meeting the engagement criteria',
    bounced_sessions            BIGINT          COMMENT 'Single-page-view sessions that were not engaged',
    avg_session_duration_sec    DECIMAL(10, 2)  COMMENT 'Mean session duration, in seconds',

    -- Conversions
    total_conversions           BIGINT          COMMENT 'Conversion events from this channel',
    total_conversion_value      DECIMAL(18, 2)  COMMENT 'Summed conversion value',

    -- Rates (0.0 to 1.0)
    engagement_rate             DECIMAL(5, 4)   COMMENT 'engaged_sessions / total_sessions',
    bounce_rate                 DECIMAL(5, 4)   COMMENT 'bounced_sessions / total_sessions',
    conversion_rate             DECIMAL(5, 4)   COMMENT 'Sessions with a conversion / total_sessions',

    -- Metadata
    _computed_at                TIMESTAMP       COMMENT 'Computation time',
    _version                    INT             COMMENT 'Schema version'
)
USING iceberg
PARTITIONED BY (channel_group, months(metric_date))
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);
