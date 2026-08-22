-- =============================================================================
-- Staging: stg_hubspot_contacts
-- =============================================================================
-- Cleans and standardizes raw HubSpot contacts for downstream processing.
-- Source: raw.hubspot_contacts
-- Target: staging.stg_hubspot_contacts
-- =============================================================================

CREATE TABLE IF NOT EXISTS staging.stg_hubspot_contacts (
    contact_id                  STRING          COMMENT 'HubSpot contact ID',
    email_token                 STRING          COMMENT 'Tokenized email (pii/registry.py class: email)',
    first_name_token            STRING          COMMENT 'Tokenized first name (class: name)',
    last_name_token             STRING          COMMENT 'Tokenized last name (class: name)',
    full_name_token             STRING          COMMENT 'Tokenized full name (class: name)',
    phone_token                 STRING          COMMENT 'Tokenized primary phone (class: phone)',
    mobile_phone_token          STRING          COMMENT 'Tokenized mobile phone (class: phone)',
    last_name_prefix_token      STRING          COMMENT 'Tokenized 3-char last-name prefix, for name_zip blocking (class: name_prefix)',

    -- Address
    address_token                STRING          COMMENT 'Tokenized street address (class: address)',
    city                        STRING          COMMENT 'City',
    state                       STRING          COMMENT 'State/region',
    zip                         STRING          COMMENT 'Postal code',
    country                     STRING          COMMENT 'Country',

    -- Company
    company                     STRING          COMMENT 'Company name',
    job_title                   STRING          COMMENT 'Job title',
    associated_company_id       STRING          COMMENT 'Associated company ID',

    -- Lifecycle
    lifecycle_stage             STRING          COMMENT 'Raw lifecycle stage',
    lifecycle_stage_normalized  STRING          COMMENT 'Normalized: prospect, qualified, customer',
    lead_status                 STRING          COMMENT 'Lead status',

    -- Analytics
    analytics_source            STRING          COMMENT 'Original traffic source',
    first_page_seen             STRING          COMMENT 'First page URL',
    page_views                  BIGINT          COMMENT 'Total page views',
    sessions                    BIGINT          COMMENT 'Number of sessions',
    avg_pages_per_session       DECIMAL(10, 2)  COMMENT 'Pages per session',

    -- Marketing
    email_optout                BOOLEAN         COMMENT 'Email opt-out status',
    is_marketable               BOOLEAN         COMMENT 'Can receive marketing emails',

    -- Derived flags
    has_company                 BOOLEAN         COMMENT 'Has associated company',
    has_phone                   BOOLEAN         COMMENT 'Has phone number',
    is_customer                 BOOLEAN         COMMENT 'Lifecycle = customer',
    is_engaged                  BOOLEAN         COMMENT 'High engagement flag',

    -- Website
    website                     STRING          COMMENT 'Contact website',

    -- Timestamps
    created_at                  TIMESTAMP       COMMENT 'Contact creation time',
    updated_at                  TIMESTAMP       COMMENT 'Last update time',

    -- Metadata
    _raw_id                     STRING          COMMENT 'Reference to raw table ID',
    _webhook_subscription_type  STRING          COMMENT 'Webhook subscription type',
    _loaded_at                  TIMESTAMP       COMMENT 'Raw layer load time',
    _staged_at                  TIMESTAMP       COMMENT 'Staging layer process time'
)
USING iceberg
PARTITIONED BY (months(created_at))
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);
