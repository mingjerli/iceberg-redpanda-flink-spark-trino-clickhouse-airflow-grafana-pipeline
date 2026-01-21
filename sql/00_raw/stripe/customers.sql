-- =============================================================================
-- Stripe Customers - Raw Iceberg Table
-- =============================================================================
-- Stores raw customer webhook events from Stripe. This is an append-only table
-- that receives data from Flink streaming jobs.
--
-- Webhook events: customer.created, customer.updated
-- Source: Stripe REST API (webhook format)
-- =============================================================================

CREATE TABLE IF NOT EXISTS raw.stripe_customers (
    -- Primary identifier (Stripe uses string IDs)
    id                              STRING          COMMENT 'Unique customer identifier (cus_xxx)',
    object                          STRING          COMMENT 'Object type, always customer',

    -- PII fields
    email                           STRING          COMMENT 'Customer email address (PII, max 512 chars)',
    name                            STRING          COMMENT 'Customer full name (PII, max 256 chars)',
    phone                           STRING          COMMENT 'Customer phone number (PII, max 20 chars)',
    address                         STRING          COMMENT 'Customer address object as JSON (PII)',
    shipping                        STRING          COMMENT 'Shipping information as JSON (PII)',

    -- Financial
    balance                         BIGINT          COMMENT 'Customer balance in cents (can be negative)',
    currency                        STRING          COMMENT 'Default currency for customer (ISO 4217)',
    default_source                  STRING          COMMENT 'Default payment source ID',
    delinquent                      BOOLEAN         COMMENT 'Whether customer has unpaid invoices',

    -- Billing
    invoice_prefix                  STRING          COMMENT 'Prefix for generating invoice numbers',
    invoice_settings                STRING          COMMENT 'Default invoice settings as JSON',
    next_invoice_sequence           BIGINT          COMMENT 'Next invoice number suffix',

    -- Tax
    tax_exempt                      STRING          COMMENT 'Tax exemption status: none, exempt, reverse',

    -- Preferences
    preferred_locales               STRING          COMMENT 'Customer preferred languages as JSON array',

    -- Discount
    discount                        STRING          COMMENT 'Current discount active on customer as JSON',

    -- Additional
    description                     STRING          COMMENT 'Arbitrary description',
    livemode                        BOOLEAN         COMMENT 'True for live mode, false for test mode',
    metadata                        STRING          COMMENT 'Key-value pairs for custom data as JSON',
    test_clock                      STRING          COMMENT 'Test clock ID if applicable',

    -- Timestamps (Stripe uses Unix timestamps)
    created                         TIMESTAMP       COMMENT 'Customer creation timestamp',

    -- Ingestion metadata
    _webhook_received_at            TIMESTAMP       COMMENT 'When webhook was received by ingestion API',
    _webhook_event_id               STRING          COMMENT 'Stripe event ID (evt_xxx)',
    _loaded_at                      TIMESTAMP       COMMENT 'When record was loaded into Iceberg'
)
USING iceberg
PARTITIONED BY (months(created))
TBLPROPERTIES (
    'format-version' = '2',
    'write.upsert.enabled' = 'false',
    'write.parquet.compression-codec' = 'zstd'
);

COMMENT ON TABLE raw.stripe_customers IS 'Raw Stripe customer webhook events. Append-only, partitioned by month.';
