-- =============================================================================
-- Staging: stg_shopify_customers
-- =============================================================================
-- Cleans and standardizes raw Shopify customers for downstream processing.
-- Source: raw.shopify_customers
-- Target: staging.stg_shopify_customers
-- =============================================================================

CREATE TABLE IF NOT EXISTS staging.stg_shopify_customers (
    customer_id                 BIGINT          COMMENT 'Unique customer identifier',
    email                       STRING          COMMENT 'Normalized email (lowercase, trimmed)',
    first_name                  STRING          COMMENT 'First name',
    last_name                   STRING          COMMENT 'Last name',
    full_name                   STRING          COMMENT 'Concatenated full name',
    phone                       STRING          COMMENT 'Phone number',

    -- Address
    address_line1               STRING          COMMENT 'Street address line 1',
    address_line2               STRING          COMMENT 'Street address line 2',
    city                        STRING          COMMENT 'City',
    province                    STRING          COMMENT 'State/province',
    province_code               STRING          COMMENT 'Province ISO code',
    country                     STRING          COMMENT 'Country name',
    country_code                STRING          COMMENT 'Country ISO code',
    zip                         STRING          COMMENT 'Postal/ZIP code',

    -- Financial
    currency                    STRING          COMMENT 'Customer currency',
    total_spent                 DECIMAL(18, 2)  COMMENT 'Lifetime spend amount',
    orders_count                BIGINT          COMMENT 'Total order count',
    avg_order_value             DECIMAL(18, 2)  COMMENT 'Average order value',

    -- Segmentation
    customer_tier               STRING          COMMENT 'Derived: vip, gold, silver, bronze, new',

    -- Marketing
    accepts_marketing           BOOLEAN         COMMENT 'Email marketing consent',
    marketing_opt_in_level      STRING          COMMENT 'Consent level',

    -- Status
    state                       STRING          COMMENT 'Account state: enabled, disabled, etc.',
    is_active                   BOOLEAN         COMMENT 'Whether account is active',
    verified_email              BOOLEAN         COMMENT 'Whether email is verified',
    tax_exempt                  BOOLEAN         COMMENT 'Tax exemption status',

    -- Misc
    note                        STRING          COMMENT 'Customer notes',
    tags                        STRING          COMMENT 'Comma-separated tags',
    address_count               INT             COMMENT 'Number of addresses',

    -- Timestamps
    created_at                  TIMESTAMP       COMMENT 'Customer creation time',
    updated_at                  TIMESTAMP       COMMENT 'Last update time',

    -- Metadata
    _raw_id                     BIGINT          COMMENT 'Reference to raw table ID',
    _webhook_topic              STRING          COMMENT 'Source webhook topic',
    _loaded_at                  TIMESTAMP       COMMENT 'Raw layer load time',
    _staged_at                  TIMESTAMP       COMMENT 'Staging layer process time'
)
USING iceberg
PARTITIONED BY (months(created_at))
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);
