-- =============================================================================
-- Core: orders
-- =============================================================================
-- Unified order view with resolved customer entities.
-- Links orders to unified customer IDs for cross-source analysis.
-- =============================================================================

CREATE TABLE IF NOT EXISTS core.orders (
    order_id                    STRING          COMMENT 'Unique order identifier',
    customer_id                 STRING          COMMENT 'Unified customer ID (from entity_index)',
    source                      STRING          COMMENT 'Source system: shopify',

    -- Order details
    order_number                BIGINT          COMMENT 'Display order number',
    order_name                  STRING          COMMENT 'Order display name',

    -- Financial
    currency                    STRING          COMMENT 'Order currency',
    subtotal_price              DECIMAL(18, 2)  COMMENT 'Subtotal',
    discount_amount             DECIMAL(18, 2)  COMMENT 'Discount amount',
    shipping_amount             DECIMAL(18, 2)  COMMENT 'Shipping amount',
    tax_amount                  DECIMAL(18, 2)  COMMENT 'Tax amount',
    total_price                 DECIMAL(18, 2)  COMMENT 'Total order amount',

    -- Status
    financial_status            STRING          COMMENT 'Payment status',
    fulfillment_status          STRING          COMMENT 'Shipping status',
    order_status                STRING          COMMENT 'Derived status: completed, cancelled, processing, etc.',

    -- Counts
    line_item_count             INT             COMMENT 'Number of items',

    -- Flags
    has_discount                BOOLEAN         COMMENT 'Has discount applied',
    is_cancelled                BOOLEAN         COMMENT 'Is cancelled',
    is_test                     BOOLEAN         COMMENT 'Is test order',

    -- Location
    shipping_country            STRING          COMMENT 'Shipping country',
    shipping_country_code       STRING          COMMENT 'Shipping country code',
    shipping_state              STRING          COMMENT 'Shipping state',
    shipping_city               STRING          COMMENT 'Shipping city',

    -- Attribution
    channel                     STRING          COMMENT 'Sales channel: web, pos, mobile',
    referring_site              STRING          COMMENT 'Referrer URL',

    -- Related payment (if matched to Stripe)
    stripe_charge_id            STRING          COMMENT 'Associated Stripe charge ID',
    payment_method              STRING          COMMENT 'Payment method type',

    -- Timestamps
    order_date                  DATE            COMMENT 'Order date (date only)',
    created_at                  TIMESTAMP       COMMENT 'Order creation time',
    updated_at                  TIMESTAMP       COMMENT 'Last update time',
    processed_at                TIMESTAMP       COMMENT 'Payment processing time',
    cancelled_at                TIMESTAMP       COMMENT 'Cancellation time',

    -- Metadata
    source_order_id             BIGINT          COMMENT 'Original order ID in source',
    staged_at                   TIMESTAMP       COMMENT 'Staging layer process time'
)
USING iceberg
PARTITIONED BY (months(created_at))
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);
