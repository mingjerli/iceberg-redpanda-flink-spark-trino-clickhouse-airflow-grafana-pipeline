-- =============================================================================
-- HubSpot Contacts - Raw Iceberg Table
-- =============================================================================
-- Stores raw contact webhook events from HubSpot. This is an append-only table
-- that receives data from Flink streaming jobs.
--
-- Webhook events: contact.creation, contact.propertyChange
-- Source: HubSpot CRM API v3 (webhook format)
--
-- Note: HubSpot webhooks send minimal data. Full object is fetched via API.
-- =============================================================================

CREATE TABLE IF NOT EXISTS raw.hubspot_contacts (
    -- Primary identifier (HubSpot uses string IDs)
    id                              STRING          COMMENT 'HubSpot object ID (hs_object_id)',
    hs_object_id                    STRING          COMMENT 'HubSpot object ID (duplicate for compatibility)',

    -- All properties as JSON (HubSpot's flexible schema)
    properties                      STRING          COMMENT 'All contact properties as JSON (may contain PII)',

    -- Core PII fields (extracted from properties)
    email                           STRING          COMMENT 'Primary email address (unique identifier, PII)',
    firstname                       STRING          COMMENT 'Contact first name (PII)',
    lastname                        STRING          COMMENT 'Contact last name (PII)',
    phone                           STRING          COMMENT 'Primary phone number (PII)',
    mobilephone                     STRING          COMMENT 'Mobile phone number (PII)',

    -- Address (PII)
    address                         STRING          COMMENT 'Street address (PII)',
    city                            STRING          COMMENT 'City',
    state                           STRING          COMMENT 'State/region',
    zip                             STRING          COMMENT 'Postal/ZIP code (PII)',
    country                         STRING          COMMENT 'Country',

    -- Company info
    company                         STRING          COMMENT 'Company name (text, not association)',
    jobtitle                        STRING          COMMENT 'Job title',
    associatedcompanyid             STRING          COMMENT 'Associated company ID',

    -- Lifecycle
    lifecyclestage                  STRING          COMMENT 'Lifecycle stage: subscriber, lead, mql, sql, opportunity, customer, evangelist, other',
    hs_lead_status                  STRING          COMMENT 'Lead status',

    -- Web/Marketing
    website                         STRING          COMMENT 'Website URL',
    hs_analytics_source             STRING          COMMENT 'Original traffic source',
    hs_analytics_first_url          STRING          COMMENT 'First page seen',
    hs_analytics_num_page_views     BIGINT          COMMENT 'Total page views',
    hs_analytics_num_visits         BIGINT          COMMENT 'Number of sessions',
    hs_email_optout                 BOOLEAN         COMMENT 'Opted out of all email',

    -- Timestamps
    createdate                      TIMESTAMP       COMMENT 'Record creation timestamp',
    lastmodifieddate                TIMESTAMP       COMMENT 'Last modified timestamp',

    -- Ingestion metadata
    _webhook_received_at            TIMESTAMP       COMMENT 'When webhook was received by ingestion API',
    _webhook_subscription_type      STRING          COMMENT 'Webhook subscription type (contact.creation, etc.)',
    _loaded_at                      TIMESTAMP       COMMENT 'When record was loaded into Iceberg'
)
USING iceberg
PARTITIONED BY (months(createdate))
TBLPROPERTIES (
    'format-version' = '2',
    'write.upsert.enabled' = 'false',
    'write.parquet.compression-codec' = 'zstd'
);

COMMENT ON TABLE raw.hubspot_contacts IS 'Raw HubSpot contact webhook events. Append-only, partitioned by month.';
