-- =============================================================================
-- Marts: campaign_dashboard
-- =============================================================================
-- Denormalized campaign scorecard for BI tools and Grafana dashboards.
-- Mirrors analytics.campaign_metrics with added send_month partition key.
--
-- Note: This file is reference documentation only. The actual CREATE TABLE
-- statement is inline in jobs/spark/marts_incremental.py.
-- =============================================================================

CREATE TABLE IF NOT EXISTS marts.campaign_dashboard (
    -- Campaign identification
    campaign_id                 STRING          COMMENT 'Mailchimp campaign ID',
    campaign_type               STRING          COMMENT 'Campaign type',
    subject_line                STRING          COMMENT 'Email subject line',
    send_time                   TIMESTAMP       COMMENT 'Campaign send time',
    send_month                  STRING          COMMENT 'Send month (YYYY-MM) for partitioning',
    list_id                     STRING          COMMENT 'Audience/list ID',
    is_sms                      BOOLEAN         COMMENT 'Whether campaign is SMS type',
    is_automated                BOOLEAN         COMMENT 'Whether campaign is automation type',

    -- Volume
    total_sent                  INT             COMMENT 'Total sent',
    total_delivered              INT             COMMENT 'Total delivered',

    -- Engagement
    total_opens                 BIGINT          COMMENT 'Total opens',
    unique_opens                INT             COMMENT 'Unique opens',
    total_clicks                BIGINT          COMMENT 'Total clicks',
    unique_clicks               INT             COMMENT 'Unique clicks',

    -- Negative
    total_bounces               INT             COMMENT 'Total bounces',
    hard_bounces                BIGINT          COMMENT 'Hard bounces',
    soft_bounces                BIGINT          COMMENT 'Soft bounces',
    total_unsubscribes          INT             COMMENT 'Unsubscribes',

    -- SMS
    sms_sent                    BIGINT          COMMENT 'SMS sent',
    sms_clicks                  BIGINT          COMMENT 'SMS clicks',

    -- Rates
    delivery_rate               DECIMAL(5, 4)   COMMENT 'Delivery rate',
    open_rate                   DECIMAL(5, 4)   COMMENT 'Open rate',
    click_rate                  DECIMAL(5, 4)   COMMENT 'Click rate',
    click_to_open_rate          DECIMAL(5, 4)   COMMENT 'Click-to-open rate',
    bounce_rate                 DECIMAL(5, 4)   COMMENT 'Bounce rate',
    unsubscribe_rate            DECIMAL(5, 4)   COMMENT 'Unsubscribe rate',
    sms_click_rate              DECIMAL(5, 4)   COMMENT 'SMS click rate',

    -- Scoring
    engagement_score            DECIMAL(7, 2)   COMMENT 'Engagement score (-75 to +100)',
    performance_tier            STRING          COMMENT 'Performance tier: excellent, good, average, poor',

    -- Metadata
    _computed_at                TIMESTAMP       COMMENT 'Computation time'
)
USING iceberg
PARTITIONED BY (send_month)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);
