-- =============================================================================
-- Mailchimp Subscribers - Staging Table
-- =============================================================================
-- Cleaned and enriched subscriber data from raw.mailchimp_subscribers.
-- Includes within-batch dedup (row_number by subscriber_id ordered by
-- _loaded_at DESC), merge_fields extraction, and entity resolution fields.
--
-- Note: This file is reference documentation only. The actual CREATE TABLE
-- statement is inline in jobs/spark/staging_batch.py.
-- =============================================================================

CREATE TABLE IF NOT EXISTS staging.stg_mailchimp_subscribers (
    -- Raw ID
    _raw_id                         STRING          COMMENT 'Raw record identifier (subscriber_id)',

    -- Subscriber identification
    subscriber_id                   STRING          COMMENT 'MD5 hash of lowercased email (Mailchimp convention)',
    email_address                   STRING          COMMENT 'Subscriber email address (PII)',
    email_normalized                STRING          COMMENT 'Lowercased trimmed email for entity resolution',
    email_type                      STRING          COMMENT 'Preferred email format: html, text',
    status                          STRING          COMMENT 'Subscription status: subscribed, unsubscribed, cleaned, pending, transactional',

    -- Profile data (extracted from merge_fields JSON)
    first_name                      STRING          COMMENT 'First name (from merge_fields.FNAME, PII)',
    last_name                       STRING          COMMENT 'Last name (from merge_fields.LNAME, PII)',
    full_name                       STRING          COMMENT 'Concatenated first + last name (for entity resolution)',
    phone                           STRING          COMMENT 'Phone number in E.164 format (PII)',
    phone_normalized                STRING          COMMENT 'Digits-only phone for entity resolution matching',

    -- Raw JSON fields (preserved for downstream flexibility)
    merge_fields                    STRING          COMMENT 'All merge fields as JSON',
    stats                           STRING          COMMENT 'Engagement stats as JSON',

    -- Extracted stats
    avg_open_rate                   DECIMAL(5, 4)   COMMENT 'Average open rate (from stats JSON)',
    avg_click_rate                  DECIMAL(5, 4)   COMMENT 'Average click rate (from stats JSON)',

    -- List and tags
    list_id                         STRING          COMMENT 'Audience/list ID',
    tags                            STRING          COMMENT 'Tags as JSON array',

    -- Signup tracking
    ip_signup                       STRING          COMMENT 'IP address at signup (PII)',
    signup_timestamp                TIMESTAMP       COMMENT 'Signup timestamp',
    ip_opt                          STRING          COMMENT 'IP at opt-in confirmation (PII)',
    timestamp_opt                   TIMESTAMP       COMMENT 'Opt-in confirmation timestamp',
    last_changed                    TIMESTAMP       COMMENT 'Last profile change timestamp',

    -- Preferences
    language                        STRING          COMMENT 'Language preference (ISO 639-1)',
    vip                             BOOLEAN         COMMENT 'VIP status flag',
    source                          STRING          COMMENT 'How subscriber was added: API, import, popup, landing_page',
    sms_status                      STRING          COMMENT 'SMS subscription status',

    -- Derived flags
    has_sms                         BOOLEAN         COMMENT 'Has phone AND sms_status = subscribed',
    is_active                       BOOLEAN         COMMENT 'status = subscribed',
    days_since_signup               INT             COMMENT 'Days since signup_timestamp',

    -- Metadata
    _webhook_received_at            TIMESTAMP       COMMENT 'When webhook was received',
    _webhook_event_type             STRING          COMMENT 'Mailchimp webhook type',
    _loaded_at                      TIMESTAMP       COMMENT 'When loaded into raw',
    _staged_at                      TIMESTAMP       COMMENT 'When staged'
)
USING iceberg
PARTITIONED BY (months(signup_timestamp))
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);
