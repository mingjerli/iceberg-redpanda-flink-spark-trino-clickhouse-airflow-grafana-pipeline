-- =============================================================================
-- Core: customers
-- =============================================================================
-- Unified customer view combining data from all sources.
-- This is the single source of truth for "who is a customer".
-- =============================================================================

CREATE TABLE IF NOT EXISTS core.customers (
    customer_id                 STRING          COMMENT 'Unified customer ID (from entity_index)',

    -- Best available identity
    email                       STRING          COMMENT 'Primary email (best available)',
    phone                       STRING          COMMENT 'Primary phone (best available)',
    first_name                  STRING          COMMENT 'First name (best available)',
    last_name                   STRING          COMMENT 'Last name (best available)',
    full_name                   STRING          COMMENT 'Full name (best available)',

    -- Address (best available)
    address_line1               STRING          COMMENT 'Street address',
    city                        STRING          COMMENT 'City',
    state                       STRING          COMMENT 'State/province',
    country                     STRING          COMMENT 'Country',
    country_code                STRING          COMMENT 'Country ISO code',
    postal_code                 STRING          COMMENT 'Postal/ZIP code',

    -- Segmentation
    customer_tier               STRING          COMMENT 'Tier: vip, gold, silver, bronze, new',
    lifecycle_stage             STRING          COMMENT 'Stage: prospect, qualified, customer',

    -- Aggregated metrics
    total_spent                 DECIMAL(18, 2)  COMMENT 'Total lifetime spend',
    orders_count                BIGINT          COMMENT 'Total order count',
    avg_order_value             DECIMAL(18, 2)  COMMENT 'Average order value',

    -- Marketing
    accepts_marketing           BOOLEAN         COMMENT 'Marketing consent',
    is_active                   BOOLEAN         COMMENT 'Active customer flag',

    -- Engagement
    page_views                  BIGINT          COMMENT 'Total page views (from HubSpot)',
    sessions                    BIGINT          COMMENT 'Total sessions (from HubSpot)',
    analytics_source            STRING          COMMENT 'Original acquisition source',

    -- Source attribution
    source_count                INT             COMMENT 'Number of sources with this customer',
    has_shopify                 BOOLEAN         COMMENT 'Has Shopify record',
    has_stripe                  BOOLEAN         COMMENT 'Has Stripe record',
    has_hubspot                 BOOLEAN         COMMENT 'Has HubSpot record',
    primary_source              STRING          COMMENT 'Primary source system',

    -- Source IDs for reference
    shopify_customer_id         BIGINT          COMMENT 'Shopify customer ID',
    stripe_customer_id          STRING          COMMENT 'Stripe customer ID',
    hubspot_contact_id          STRING          COMMENT 'HubSpot contact ID',

    -- Timestamps
    first_seen_at               TIMESTAMP       COMMENT 'Earliest record across sources',
    last_seen_at                TIMESTAMP       COMMENT 'Latest activity across sources',
    created_at                  TIMESTAMP       COMMENT 'Customer record creation',
    updated_at                  TIMESTAMP       COMMENT 'Last update time',

    -- Metadata
    _merged_at                  TIMESTAMP       COMMENT 'When entity resolution ran'
)
USING iceberg
PARTITIONED BY (customer_tier)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);
