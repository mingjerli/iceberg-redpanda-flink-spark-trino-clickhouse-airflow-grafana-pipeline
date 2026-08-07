-- =============================================================================
-- Analytics: ga4_page_performance
-- =============================================================================
-- Page-level engagement, aggregated from staging.stg_ga4_events.
-- Grain: one row per (metric_date, page_location) with attribution dimensions
-- carried from the session the page view belonged to.
--
-- Note: This file is reference documentation only. The actual CREATE TABLE
-- statement is inline in jobs/spark/analytics_incremental.py.
-- =============================================================================

CREATE TABLE IF NOT EXISTS analytics.ga4_page_performance (
    -- Grain
    metric_date                 DATE            COMMENT 'Event date',
    page_location               STRING          COMMENT 'Full page URL',
    page_title                  STRING          COMMENT 'Page title as reported by GA4',

    -- Volume
    page_views                  BIGINT          COMMENT 'page_view events for this page',
    unique_visitors             BIGINT          COMMENT 'Distinct client_id values that viewed the page',
    unique_sessions             BIGINT          COMMENT 'Distinct sessions that included the page',

    -- Engagement
    avg_engagement_time_ms      BIGINT          COMMENT 'Mean engagement time on the page',
    entrances                   BIGINT          COMMENT 'Sessions whose landing page was this page',
    exits                       BIGINT          COMMENT 'Sessions whose exit page was this page',
    bounces                     BIGINT          COMMENT 'Entrances that bounced',
    bounce_rate                 DECIMAL(5, 4)   COMMENT 'bounces / entrances',
    avg_time_on_page_sec        DECIMAL(10, 2)  COMMENT 'Mean time on page, in seconds',

    -- Attribution (from the owning session)
    traffic_source              STRING          COMMENT 'Session traffic source, e.g. google',
    traffic_medium              STRING          COMMENT 'Session traffic medium, e.g. organic',
    device_category             STRING          COMMENT 'desktop, mobile, or tablet',
    geo_country                 STRING          COMMENT 'Session country',

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
