-- =============================================================================
-- Staging: stg_shopify_orders
-- =============================================================================
-- Cleans and standardizes raw Shopify orders for downstream processing.
-- Source: raw.shopify_orders
-- Target: staging.stg_shopify_orders
-- =============================================================================

CREATE TABLE IF NOT EXISTS staging.stg_shopify_orders (
    order_id                    BIGINT          COMMENT 'Unique order identifier',
    order_number                BIGINT          COMMENT 'Shop-unique order number',
    customer_id                 BIGINT          COMMENT 'Customer ID',
    customer_email              STRING          COMMENT 'Normalized customer email',
    customer_phone              STRING          COMMENT 'Customer phone number',
    order_name                  STRING          COMMENT 'Order display name',

    -- Financial
    currency                    STRING          COMMENT 'Order currency (uppercase)',
    subtotal_price              DECIMAL(18, 2)  COMMENT 'Subtotal before discounts/tax/shipping',
    total_discounts             DECIMAL(18, 2)  COMMENT 'Total discount amount',
    total_shipping              DECIMAL(18, 2)  COMMENT 'Total shipping amount',
    total_tax                   DECIMAL(18, 2)  COMMENT 'Total tax amount',
    total_price                 DECIMAL(18, 2)  COMMENT 'Final order total',
    total_outstanding           DECIMAL(18, 2)  COMMENT 'Outstanding balance',

    -- Status
    financial_status            STRING          COMMENT 'Payment status',
    fulfillment_status          STRING          COMMENT 'Shipping status',
    order_status                STRING          COMMENT 'Derived: completed, cancelled, processing, etc.',

    -- Counts
    line_item_count             INT             COMMENT 'Number of line items',
    discount_count              INT             COMMENT 'Number of discount codes applied',

    -- Flags
    has_discount                BOOLEAN         COMMENT 'Whether order has discounts',
    is_cancelled                BOOLEAN         COMMENT 'Whether order is cancelled',
    is_test                     BOOLEAN         COMMENT 'Whether this is a test order',
    taxes_included              BOOLEAN         COMMENT 'Whether taxes included in subtotal',
    buyer_accepts_marketing     BOOLEAN         COMMENT 'Marketing consent',

    -- Shipping address
    shipping_country            STRING          COMMENT 'Shipping country name',
    shipping_country_code       STRING          COMMENT 'Shipping country ISO code',
    shipping_province           STRING          COMMENT 'Shipping state/province',
    shipping_city               STRING          COMMENT 'Shipping city',

    -- Attribution
    source_name                 STRING          COMMENT 'Channel source: web, pos, mobile',
    referring_site              STRING          COMMENT 'Referrer URL',

    -- Timestamps
    created_at                  TIMESTAMP       COMMENT 'Order creation time',
    updated_at                  TIMESTAMP       COMMENT 'Last update time',
    processed_at                TIMESTAMP       COMMENT 'Payment processing time',
    cancelled_at                TIMESTAMP       COMMENT 'Cancellation time',

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
