-- =============================================================================
-- Mailchimp Events - Staging Table
-- =============================================================================
-- Cleaned and enriched engagement events from raw.mailchimp_events.
-- Adds derived fields: email_normalized, location extraction, event
-- classification flags.
--
-- Note: This file is reference documentation only. The actual CREATE TABLE
-- statement is inline in jobs/spark/staging_batch.py.
-- =============================================================================

CREATE TABLE IF NOT EXISTS staging.stg_mailchimp_events (
    -- Raw ID
    _raw_id                         STRING          COMMENT 'Raw record identifier (event_id)',

    -- Event identification
    event_id                        STRING          COMMENT 'Unique event ID',
    campaign_id                     STRING          COMMENT 'Associated campaign ID',
    email_id_token                  STRING          COMMENT 'Tokenized unique member-campaign combination ID (pii/registry.py class: mailchimp_id)',
    email_address_token             STRING          COMMENT 'Tokenized subscriber email address (class: email)',
    email_normalized_token          STRING          COMMENT 'Tokenized normalized email, for matching (class: email)',

    -- Event details
    action                          STRING          COMMENT 'Event action: sent, open, click, bounce, unsub, abuse, sms_sent, sms_click',
    event_timestamp                 TIMESTAMP       COMMENT 'When event occurred',
    event_date                      DATE            COMMENT 'Event date (derived from event_timestamp)',
    url                             STRING          COMMENT 'Clicked URL (for click events)',
    ip                              STRING          COMMENT 'IP address (PII)',
    user_agent                      STRING          COMMENT 'Browser/client user agent',

    -- Location (extracted from JSON)
    location                        STRING          COMMENT 'Full location JSON',
    location_country                STRING          COMMENT 'Country code (extracted from location JSON)',
    location_region                 STRING          COMMENT 'Region/state (extracted from location JSON)',

    -- Bounce info
    bounce_type                     STRING          COMMENT 'Bounce type: hard, soft',

    -- List reference
    list_id                         STRING          COMMENT 'Audience/list ID',

    -- Derived classification flags
    is_sms_event                    BOOLEAN         COMMENT 'Whether event is SMS-related (sms_sent, sms_click)',
    is_positive_engagement          BOOLEAN         COMMENT 'Whether event is positive engagement (open, click, sms_click)',
    is_negative_event               BOOLEAN         COMMENT 'Whether event is negative (bounce, unsub, abuse)',

    -- Metadata
    _webhook_received_at            TIMESTAMP       COMMENT 'When webhook was received',
    _webhook_event_type             STRING          COMMENT 'Mailchimp webhook type',
    _loaded_at                      TIMESTAMP       COMMENT 'When loaded into raw',
    _staged_at                      TIMESTAMP       COMMENT 'When staged'
)
USING iceberg
PARTITIONED BY (months(event_timestamp))
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);
