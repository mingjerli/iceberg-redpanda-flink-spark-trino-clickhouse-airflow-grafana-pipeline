-- =============================================================================
-- Shopify Customers - Raw Iceberg Table
-- =============================================================================
-- Stores raw customer webhook events from Shopify. This is an append-only table
-- that receives data from Flink streaming jobs.
--
-- Webhook topics: customers/create, customers/update
-- Source: Shopify REST Admin API (webhook format)
-- =============================================================================

CREATE TABLE IF NOT EXISTS raw.shopify_customers (
    -- Primary identifier
    id                              BIGINT          COMMENT 'Unique customer identifier',
    admin_graphql_api_id            STRING          COMMENT 'GraphQL API ID',

    -- PII fields
    email                           STRING          COMMENT 'Customer email address (PII)',
    first_name                      STRING          COMMENT 'Customer first name (PII)',
    last_name                       STRING          COMMENT 'Customer last name (PII)',
    phone                           STRING          COMMENT 'Customer phone number in E.164 format (PII)',

    -- Addresses (PII)
    default_address                 STRING          COMMENT 'Default address object as JSON',
    addresses                       STRING          COMMENT 'Array of address objects as JSON',

    -- Marketing
    accepts_marketing               BOOLEAN         COMMENT 'Marketing opt-in status',
    accepts_marketing_updated_at    TIMESTAMP       COMMENT 'When marketing preference was last updated',
    marketing_opt_in_level          STRING          COMMENT 'Marketing opt-in level: single_opt_in, confirmed_opt_in, unknown',

    -- Financial
    currency                        STRING          COMMENT 'Customer preferred currency',
    total_spent                     DECIMAL(18, 2)  COMMENT 'Lifetime spend amount',
    orders_count                    BIGINT          COMMENT 'Total number of orders placed',

    -- Status
    state                           STRING          COMMENT 'Customer state: enabled, disabled, invited, declined',
    verified_email                  BOOLEAN         COMMENT 'Whether email is verified',
    tax_exempt                      BOOLEAN         COMMENT 'Whether customer is tax exempt',
    tax_exemptions                  STRING          COMMENT 'Array of tax exemption types as JSON',

    -- Additional
    note                            STRING          COMMENT 'Note about the customer',
    tags                            STRING          COMMENT 'Comma-separated customer tags',

    -- Timestamps
    created_at                      TIMESTAMP       COMMENT 'Customer creation timestamp',
    updated_at                      TIMESTAMP       COMMENT 'Last update timestamp',

    -- Ingestion metadata
    _webhook_received_at            TIMESTAMP       COMMENT 'When webhook was received by ingestion API',
    _webhook_topic                  STRING          COMMENT 'Webhook topic (customers/create, customers/update)',
    _loaded_at                      TIMESTAMP       COMMENT 'When record was loaded into Iceberg'
)
USING iceberg
PARTITIONED BY (months(created_at))
TBLPROPERTIES (
    'format-version' = '2',
    'write.upsert.enabled' = 'false',
    'write.parquet.compression-codec' = 'zstd'
);

COMMENT ON TABLE raw.shopify_customers IS 'Raw Shopify customer webhook events. Append-only, partitioned by month.';
