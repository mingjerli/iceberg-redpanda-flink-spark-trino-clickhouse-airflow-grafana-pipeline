-- =============================================================================
-- Mailchimp Subscribers - Raw Iceberg Table
-- =============================================================================
-- Stores raw subscriber webhook events from Mailchimp. This is an append-only
-- table that receives data from Flink streaming jobs.
--
-- Webhook events: subscribe, unsubscribe, profile, upemail, cleaned
-- Source: Mailchimp Webhook API
--
-- Note: This file is reference documentation only. The actual CREATE TABLE
-- statement is inline in the Flink SQL job (mailchimp_subscribers_full.sql).
-- Note: Subscribers are mutable (status changes, profile updates). Raw layer
-- is append-only; dedup happens in staging via row_number().
-- =============================================================================

CREATE TABLE IF NOT EXISTS raw.mailchimp_subscribers (
    -- Subscriber identification
    subscriber_id                   STRING          COMMENT 'MD5 hash of lowercased email (Mailchimp convention)',
    email_address                   STRING          COMMENT 'Subscriber email address (PII)',
    email_type                      STRING          COMMENT 'Preferred email format: html, text',
    status                          STRING          COMMENT 'Subscription status: subscribed, unsubscribed, cleaned, pending, transactional',

    -- Profile data (stored as JSON strings)
    merge_fields                    STRING          COMMENT 'Merge fields JSON: {FNAME, LNAME, PHONE, ...} (PII)',
    stats                           STRING          COMMENT 'Engagement stats JSON: {avg_open_rate, avg_click_rate}',

    -- List reference
    list_id                         STRING          COMMENT 'Audience/list ID',

    -- Tags (stored as JSON string)
    tags                            STRING          COMMENT 'Array of tag objects JSON: [{id, name}]',

    -- Signup tracking
    ip_signup                       STRING          COMMENT 'IP address at signup (PII)',
    timestamp_signup                TIMESTAMP       COMMENT 'Signup timestamp',
    ip_opt                          STRING          COMMENT 'IP address at opt-in confirmation (PII)',
    timestamp_opt                   TIMESTAMP       COMMENT 'Opt-in confirmation timestamp',
    last_changed                    TIMESTAMP       COMMENT 'Last profile change timestamp',

    -- Preferences
    language                        STRING          COMMENT 'Subscriber language preference (ISO 639-1)',
    vip                             BOOLEAN         COMMENT 'VIP status flag',
    source                          STRING          COMMENT 'How subscriber was added: API, import, popup, landing_page',

    -- SMS
    phone                           STRING          COMMENT 'SMS-enabled phone number in E.164 format (PII)',
    sms_status                      STRING          COMMENT 'SMS subscription status: subscribed, unsubscribed, non_subscribed',

    -- Ingestion metadata
    _webhook_received_at            TIMESTAMP       COMMENT 'When webhook was received by ingestion API',
    _webhook_event_type             STRING          COMMENT 'Mailchimp webhook type field (subscribe, unsubscribe, etc.)',
    _loaded_at                      TIMESTAMP       COMMENT 'When record was loaded into Iceberg'
)
USING iceberg
PARTITIONED BY (months(timestamp_signup))
TBLPROPERTIES (
    'format-version' = '2',
    'write.upsert.enabled' = 'false',
    'write.parquet.compression-codec' = 'zstd'
);

COMMENT ON TABLE raw.mailchimp_subscribers IS 'Raw Mailchimp subscriber webhook events. Append-only, partitioned by signup month.';
