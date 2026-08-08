-- =============================================================================
-- Marts: ga4_engagement_dashboard
-- =============================================================================
-- Dashboard-ready daily web engagement, built from analytics.ga4_engagement_metrics
-- with device/channel splits and top dimensions joined in from
-- staging.stg_ga4_sessions. One row per day; backs the "GA4 Web Analytics"
-- section of the Grafana batch_business dashboard.
--
-- Note: This file is reference documentation only. The actual CREATE TABLE
-- statement is inline in jobs/spark/marts_incremental.py.
-- =============================================================================

CREATE TABLE IF NOT EXISTS marts.ga4_engagement_dashboard (
    -- Grain
    metric_date                 DATE            COMMENT 'Session date (one row per day)',

    -- Headline volume
    total_sessions              BIGINT          COMMENT 'Sessions started on this date',
    total_users                 BIGINT          COMMENT 'Distinct client_id values',
    total_page_views            BIGINT          COMMENT 'page_view events across all sessions',
    engaged_sessions            BIGINT          COMMENT 'Sessions meeting the engagement criteria',
    bounced_sessions            BIGINT          COMMENT 'Single-page-view sessions that were not engaged',

    -- Conversions
    total_conversions           BIGINT          COMMENT 'Conversion events (purchase, sign_up)',
    total_conversion_value      DECIMAL(18, 2)  COMMENT 'Summed conversion value',

    -- Rates (0.0 to 1.0)
    engagement_rate             DECIMAL(5, 4)   COMMENT 'engaged_sessions / total_sessions',
    bounce_rate                 DECIMAL(5, 4)   COMMENT 'bounced_sessions / total_sessions',
    conversion_rate             DECIMAL(5, 4)   COMMENT 'Sessions with a conversion / total_sessions',
    avg_session_duration_sec    DECIMAL(10, 2)  COMMENT 'Mean session duration, in seconds',

    -- Trend (7-day trailing window, inclusive of the current day)
    sessions_7day_ma            DECIMAL(18, 2)  COMMENT '7-day moving average of total_sessions',
    engagement_rate_7day_ma     DECIMAL(5, 4)   COMMENT '7-day moving average of engagement_rate',

    -- Top dimensions for the day (single highest-session value)
    top_traffic_source          STRING          COMMENT 'Traffic source with the most sessions',
    top_device_category         STRING          COMMENT 'Device category with the most sessions',

    -- Device split
    desktop_sessions            BIGINT          COMMENT 'Sessions on desktop',
    mobile_sessions             BIGINT          COMMENT 'Sessions on mobile',
    tablet_sessions             BIGINT          COMMENT 'Sessions on tablet',

    -- Channel split
    organic_sessions            BIGINT          COMMENT 'Sessions from organic_search',
    paid_sessions               BIGINT          COMMENT 'Sessions from paid_search',
    direct_sessions             BIGINT          COMMENT 'Sessions from direct',
    social_sessions             BIGINT          COMMENT 'Sessions from social',

    -- Appended after the splits, matching the column order the job creates
    top_country                 STRING          COMMENT 'Country with the most sessions',

    -- Metadata
    _computed_at                TIMESTAMP       COMMENT 'Build time',
    _version                    INT             COMMENT 'Schema version'
)
USING iceberg
PARTITIONED BY (months(metric_date))
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);
