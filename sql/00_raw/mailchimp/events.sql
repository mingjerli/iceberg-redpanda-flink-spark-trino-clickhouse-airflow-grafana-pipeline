-- =============================================================================
-- Mailchimp Events - Raw Iceberg Table
-- =============================================================================
-- Stores raw engagement events from Mailchimp. This is an append-only table
-- that receives data from Flink streaming jobs.
--
-- Webhook events: sent, open, click, bounce, unsub, abuse, sms_sent, sms_click
-- Source: Mailchimp Webhook API
--
-- Note: This file is reference documentation only. The actual CREATE TABLE
-- statement is inline in the Flink SQL job (mailchimp_events_full.sql).
-- =============================================================================

CREATE TABLE IF NOT EXISTS raw.mailchimp_events (
    -- Event identification
    event_id                        STRING          COMMENT 'Unique event ID',
    campaign_id                     STRING          COMMENT 'Associated campaign ID (10-char hex)',
    email_id                        STRING          COMMENT 'Unique member-campaign combination ID',
    email_address                   STRING          COMMENT 'Subscriber email address (PII)',

    -- Event details
    action                          STRING          COMMENT 'Event action: sent, open, click, bounce, unsub, abuse, sms_sent, sms_click',
    event_timestamp                 TIMESTAMP       COMMENT 'When event occurred',
    url                             STRING          COMMENT 'Clicked URL (for click/sms_click events, NULL otherwise)',
    ip                              STRING          COMMENT 'IP address (for open/click events, NULL otherwise, PII)',
    user_agent                      STRING          COMMENT 'Browser/client user agent (for open/click events)',

    -- Geolocation (stored as JSON string)
    location                        STRING          COMMENT 'Geolocation JSON: {latitude, longitude, country_code, region}',

    -- Bounce info
    bounce_type                     STRING          COMMENT 'Bounce type: hard, soft (for bounce events, NULL otherwise)',

    -- List reference
    list_id                         STRING          COMMENT 'Audience/list ID',

    -- Ingestion metadata
    _webhook_received_at            TIMESTAMP       COMMENT 'When webhook was received by ingestion API',
    _webhook_event_type             STRING          COMMENT 'Mailchimp webhook type field (matches action)',
    _loaded_at                      TIMESTAMP       COMMENT 'When record was loaded into Iceberg'
)
USING iceberg
PARTITIONED BY (months(event_timestamp))
TBLPROPERTIES (
    'format-version' = '2',
    'write.upsert.enabled' = 'false',
    'write.parquet.compression-codec' = 'zstd'
);

COMMENT ON TABLE raw.mailchimp_events IS 'Raw Mailchimp engagement events. Append-only, partitioned by event month.';
