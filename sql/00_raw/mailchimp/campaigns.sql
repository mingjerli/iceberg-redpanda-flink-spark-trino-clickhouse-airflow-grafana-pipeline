-- =============================================================================
-- Mailchimp Campaigns - Raw Iceberg Table
-- =============================================================================
-- Stores raw campaign webhook events from Mailchimp. This is an append-only
-- table that receives data from Flink streaming jobs.
--
-- Webhook events: campaign
-- Source: Mailchimp Webhook API
--
-- Note: This file is reference documentation only. The actual CREATE TABLE
-- statement is inline in the Flink SQL job (mailchimp_campaigns_full.sql).
-- =============================================================================

CREATE TABLE IF NOT EXISTS raw.mailchimp_campaigns (
    -- Campaign identification
    campaign_id                     STRING          COMMENT 'Mailchimp campaign ID (10-char hex)',
    campaign_type                   STRING          COMMENT 'Campaign type: regular, plaintext, absplit, rss, variate, automation, sms',
    status                          STRING          COMMENT 'Campaign status: save, paused, schedule, sending, sent',
    list_id                         STRING          COMMENT 'Audience/list ID (10-char alphanumeric)',

    -- Campaign content
    subject_line                    STRING          COMMENT 'Email subject line',
    preview_text                    STRING          COMMENT 'Preview text shown in inbox',
    from_name                       STRING          COMMENT 'Sender display name',
    from_email                      STRING          COMMENT 'Sender email address',
    reply_to                        STRING          COMMENT 'Reply-to email address',
    send_time                       TIMESTAMP       COMMENT 'Campaign send time',
    content_type                    STRING          COMMENT 'Content type: template, html, url, multichannel',

    -- Aggregate metrics (populated for sent campaigns)
    emails_sent                     INT             COMMENT 'Total emails sent',
    opens                           INT             COMMENT 'Total opens (including repeats)',
    unique_opens                    INT             COMMENT 'Unique opens',
    clicks                          INT             COMMENT 'Total clicks (including repeats)',
    unique_clicks                   INT             COMMENT 'Unique clicks',
    unsubscribes                    INT             COMMENT 'Unsubscribes triggered by campaign',
    bounces                         INT             COMMENT 'Total bounces',
    open_rate                       DECIMAL(5, 4)   COMMENT 'Unique opens / emails sent',
    click_rate                      DECIMAL(5, 4)   COMMENT 'Unique clicks / emails sent',

    -- Configuration (stored as JSON strings)
    settings                        STRING          COMMENT 'Additional campaign settings JSON',
    tracking                        STRING          COMMENT 'Tracking configuration JSON: {opens, html_clicks, text_clicks, google_analytics}',

    -- Ingestion metadata
    _webhook_received_at            TIMESTAMP       COMMENT 'When webhook was received by ingestion API',
    _webhook_event_type             STRING          COMMENT 'Mailchimp webhook type field (campaign)',
    _loaded_at                      TIMESTAMP       COMMENT 'When record was loaded into Iceberg'
)
USING iceberg
PARTITIONED BY (months(send_time))
TBLPROPERTIES (
    'format-version' = '2',
    'write.upsert.enabled' = 'false',
    'write.parquet.compression-codec' = 'zstd'
);

COMMENT ON TABLE raw.mailchimp_campaigns IS 'Raw Mailchimp campaign webhook events. Append-only, partitioned by send month.';
