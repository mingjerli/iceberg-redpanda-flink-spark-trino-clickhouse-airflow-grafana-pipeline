-- =============================================================================
-- Mailchimp Campaigns - Staging Table
-- =============================================================================
-- Cleaned and enriched campaign data from raw.mailchimp_campaigns.
-- Adds derived metrics: click_to_open_rate, is_sms, is_automated.
--
-- Note: This file is reference documentation only. The actual CREATE TABLE
-- statement is inline in jobs/spark/staging_batch.py.
-- =============================================================================

CREATE TABLE IF NOT EXISTS staging.stg_mailchimp_campaigns (
    -- Raw ID
    _raw_id                         STRING          COMMENT 'Raw record identifier (campaign_id)',

    -- Campaign identification
    campaign_id                     STRING          COMMENT 'Mailchimp campaign ID (10-char hex)',
    campaign_type                   STRING          COMMENT 'Campaign type: regular, plaintext, absplit, rss, variate, automation, sms',
    status                          STRING          COMMENT 'Campaign status: save, paused, schedule, sending, sent',
    list_id                         STRING          COMMENT 'Audience/list ID',

    -- Campaign content
    subject_line                    STRING          COMMENT 'Email subject line',
    preview_text                    STRING          COMMENT 'Preview text shown in inbox',
    from_name                       STRING          COMMENT 'Sender display name',
    from_email                      STRING          COMMENT 'Sender email (normalized to lowercase)',
    reply_to                        STRING          COMMENT 'Reply-to email (normalized to lowercase)',
    send_time                       TIMESTAMP       COMMENT 'Campaign send time',
    content_type                    STRING          COMMENT 'Content type: template, html, url, multichannel',

    -- Metrics
    emails_sent                     INT             COMMENT 'Total emails sent',
    opens                           INT             COMMENT 'Total opens (including repeats)',
    unique_opens                    INT             COMMENT 'Unique opens',
    clicks                          INT             COMMENT 'Total clicks (including repeats)',
    unique_clicks                   INT             COMMENT 'Unique clicks',
    unsubscribes                    INT             COMMENT 'Unsubscribes triggered by campaign',
    bounces                         INT             COMMENT 'Total bounces',
    open_rate                       DECIMAL(5, 4)   COMMENT 'Unique opens / emails sent',
    click_rate                      DECIMAL(5, 4)   COMMENT 'Unique clicks / emails sent',

    -- Derived fields
    click_to_open_rate              DECIMAL(5, 4)   COMMENT 'Unique clicks / unique opens (NULL if no opens)',
    is_sms                          BOOLEAN         COMMENT 'Whether campaign is SMS type',
    is_automated                    BOOLEAN         COMMENT 'Whether campaign is automation type',

    -- Configuration
    settings                        STRING          COMMENT 'Campaign settings JSON',
    tracking                        STRING          COMMENT 'Tracking configuration JSON',

    -- Metadata
    _webhook_received_at            TIMESTAMP       COMMENT 'When webhook was received',
    _webhook_event_type             STRING          COMMENT 'Mailchimp webhook type',
    _loaded_at                      TIMESTAMP       COMMENT 'When loaded into raw',
    _staged_at                      TIMESTAMP       COMMENT 'When staged'
)
USING iceberg
PARTITIONED BY (months(send_time))
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);
