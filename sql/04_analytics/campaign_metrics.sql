-- =============================================================================
-- Analytics: campaign_metrics
-- =============================================================================
-- Per-campaign performance metrics joining campaigns with aggregated events.
-- Includes engagement scoring and performance tiering.
--
-- Note: This file is reference documentation only. The actual CREATE TABLE
-- statement is inline in jobs/spark/analytics_incremental.py.
-- =============================================================================

CREATE TABLE IF NOT EXISTS analytics.campaign_metrics (
    -- Campaign identification
    campaign_id                 STRING          COMMENT 'Mailchimp campaign ID',
    campaign_type               STRING          COMMENT 'Campaign type: regular, plaintext, absplit, rss, variate, automation, sms',
    subject_line                STRING          COMMENT 'Email subject line',
    send_time                   TIMESTAMP       COMMENT 'Campaign send time',
    list_id                     STRING          COMMENT 'Audience/list ID',
    is_sms                      BOOLEAN         COMMENT 'Whether campaign is SMS type',
    is_automated                BOOLEAN         COMMENT 'Whether campaign is automation type',

    -- Volume metrics
    total_sent                  INT             COMMENT 'Total emails/messages sent',
    total_delivered              INT             COMMENT 'Total delivered (sent - bounces)',

    -- Engagement counts
    total_opens                 BIGINT          COMMENT 'Total opens (including repeats, from events)',
    unique_opens                INT             COMMENT 'Unique opens (from campaign metadata)',
    total_clicks                BIGINT          COMMENT 'Total clicks (including repeats, from events)',
    unique_clicks               INT             COMMENT 'Unique clicks (from campaign metadata)',

    -- Negative events
    total_bounces               INT             COMMENT 'Total bounces',
    hard_bounces                BIGINT          COMMENT 'Hard bounces (from events)',
    soft_bounces                BIGINT          COMMENT 'Soft bounces (from events)',
    total_unsubscribes          INT             COMMENT 'Unsubscribes triggered by campaign',

    -- SMS metrics
    sms_sent                    BIGINT          COMMENT 'SMS messages sent (from events)',
    sms_clicks                  BIGINT          COMMENT 'SMS link clicks (from events)',

    -- Rates (0.0 to 1.0)
    delivery_rate               DECIMAL(5, 4)   COMMENT 'Delivered / sent',
    open_rate                   DECIMAL(5, 4)   COMMENT 'Unique opens / sent',
    click_rate                  DECIMAL(5, 4)   COMMENT 'Unique clicks / sent',
    click_to_open_rate          DECIMAL(5, 4)   COMMENT 'Unique clicks / unique opens',
    bounce_rate                 DECIMAL(5, 4)   COMMENT 'Bounces / sent',
    unsubscribe_rate            DECIMAL(5, 4)   COMMENT 'Unsubscribes / sent',
    sms_click_rate              DECIMAL(5, 4)   COMMENT 'SMS clicks / SMS sent',

    -- Scoring
    engagement_score            DECIMAL(7, 2)   COMMENT 'Weighted engagement score (-75 to +100)',
    performance_tier            STRING          COMMENT 'Performance tier: excellent, good, average, poor',

    -- Metadata
    _computed_at                TIMESTAMP       COMMENT 'Computation time'
)
USING iceberg
PARTITIONED BY (months(send_time))
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);
