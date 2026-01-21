-- =============================================================================
-- Shopify Orders - Raw Iceberg Table
-- =============================================================================
-- Stores raw order webhook events from Shopify. This is an append-only table
-- that receives data from Flink streaming jobs.
--
-- Webhook topics: orders/create, orders/updated
-- Source: Shopify REST Admin API (webhook format)
-- =============================================================================

CREATE TABLE IF NOT EXISTS raw.shopify_orders (
    -- Primary identifier
    id                              BIGINT          COMMENT 'Unique order identifier',
    admin_graphql_api_id            STRING          COMMENT 'GraphQL API ID',

    -- Order identifiers
    order_number                    BIGINT          COMMENT 'Shop-unique order number',
    number                          BIGINT          COMMENT 'Numeric order number without #',
    name                            STRING          COMMENT 'Order name shown in admin (e.g., #1001)',
    token                           STRING          COMMENT 'Unique token for the order',
    confirmation_number             STRING          COMMENT 'Random alphanumeric identifier',

    -- Customer info
    customer_id                     BIGINT          COMMENT 'Customer ID from nested customer object',
    email                           STRING          COMMENT 'Customer email at time of order (PII)',
    phone                           STRING          COMMENT 'Customer phone number (PII)',
    customer                        STRING          COMMENT 'Nested customer object as JSON (PII)',

    -- Addresses (PII)
    billing_address                 STRING          COMMENT 'Nested billing address object as JSON',
    shipping_address                STRING          COMMENT 'Nested shipping address object as JSON',

    -- Financial
    currency                        STRING          COMMENT 'Three-letter currency code (ISO 4217)',
    presentment_currency            STRING          COMMENT 'Currency shown to customer',
    subtotal_price                  DECIMAL(18, 2)  COMMENT 'Sum of line items before discounts/shipping/tax',
    total_line_items_price          DECIMAL(18, 2)  COMMENT 'Sum of all line item prices',
    total_discounts                 DECIMAL(18, 2)  COMMENT 'Total discount amount applied',
    total_price                     DECIMAL(18, 2)  COMMENT 'Final order total',
    total_tax                       DECIMAL(18, 2)  COMMENT 'Total tax amount',
    total_tip_received              DECIMAL(18, 2)  COMMENT 'Total tip amount',
    total_outstanding               DECIMAL(18, 2)  COMMENT 'Outstanding balance to be paid',
    current_subtotal_price          DECIMAL(18, 2)  COMMENT 'Current subtotal after edits/refunds',
    current_total_price             DECIMAL(18, 2)  COMMENT 'Current total price after edits/refunds',
    current_total_discounts         DECIMAL(18, 2)  COMMENT 'Current total discounts after edits',
    current_total_tax               DECIMAL(18, 2)  COMMENT 'Current total tax after edits/refunds',
    taxes_included                  BOOLEAN         COMMENT 'Whether taxes are included in subtotal',
    estimated_taxes                 BOOLEAN         COMMENT 'Whether taxes are estimated',
    tax_exempt                      BOOLEAN         COMMENT 'Whether order is tax exempt',

    -- Status
    financial_status                STRING          COMMENT 'Payment status: pending, authorized, paid, etc.',
    fulfillment_status              STRING          COMMENT 'Shipping status: fulfilled, partial, restocked, null',
    cancel_reason                   STRING          COMMENT 'Cancellation reason if cancelled',
    cancelled_at                    TIMESTAMP       COMMENT 'When order was cancelled',
    closed_at                       TIMESTAMP       COMMENT 'When order was closed/completed',
    confirmed                       BOOLEAN         COMMENT 'Whether order has been confirmed',

    -- Line items and fulfillment
    line_items                      STRING          COMMENT 'Array of line item objects as JSON',
    fulfillments                    STRING          COMMENT 'Array of fulfillment objects as JSON',
    refunds                         STRING          COMMENT 'Array of refund objects as JSON',
    shipping_lines                  STRING          COMMENT 'Array of shipping line objects as JSON',
    total_shipping_price_set        STRING          COMMENT 'Total shipping in shop and presentment currencies as JSON',

    -- Discounts
    discount_codes                  STRING          COMMENT 'Array of discount codes as JSON',

    -- Tax
    tax_lines                       STRING          COMMENT 'Array of tax line objects as JSON',

    -- Marketing and attribution
    buyer_accepts_marketing         BOOLEAN         COMMENT 'Whether customer consented to marketing',
    landing_site                    STRING          COMMENT 'URL of first page customer landed on',
    landing_site_ref                STRING          COMMENT 'Landing site reference parameter',
    referring_site                  STRING          COMMENT 'URL that referred customer to store',
    source_name                     STRING          COMMENT 'Channel source: web, pos, mobile, api',
    source_identifier               STRING          COMMENT 'POS or third-party order ID',
    source_url                      STRING          COMMENT 'URL where order originated',

    -- Cart/Checkout
    cart_token                      STRING          COMMENT 'Unique token for the cart',
    checkout_id                     BIGINT          COMMENT 'ID of the checkout that created this order',
    checkout_token                  STRING          COMMENT 'Unique token for the checkout',

    -- Additional fields
    app_id                          BIGINT          COMMENT 'ID of the app that created the order',
    browser_ip                      STRING          COMMENT 'IP address of customer browser (PII)',
    customer_locale                 STRING          COMMENT 'Customer locale (e.g., en, fr-CA)',
    note                            STRING          COMMENT 'Customer note on order',
    note_attributes                 STRING          COMMENT 'Additional order attributes as JSON',
    order_status_url                STRING          COMMENT 'URL for customer to check order status',
    payment_gateway_names           STRING          COMMENT 'List of payment gateways used as JSON',
    tags                            STRING          COMMENT 'Comma-separated tags',
    test                            BOOLEAN         COMMENT 'Whether this is a test order',
    total_weight                    BIGINT          COMMENT 'Total weight in grams',
    user_id                         BIGINT          COMMENT 'ID of staff member who created order (POS)',

    -- Timestamps
    created_at                      TIMESTAMP       COMMENT 'Order creation timestamp',
    updated_at                      TIMESTAMP       COMMENT 'Last update timestamp',
    processed_at                    TIMESTAMP       COMMENT 'When payment was processed',

    -- Ingestion metadata
    _webhook_received_at            TIMESTAMP       COMMENT 'When webhook was received by ingestion API',
    _webhook_topic                  STRING          COMMENT 'Webhook topic (orders/create, orders/updated)',
    _loaded_at                      TIMESTAMP       COMMENT 'When record was loaded into Iceberg'
)
USING iceberg
PARTITIONED BY (months(created_at))
TBLPROPERTIES (
    'format-version' = '2',
    'write.upsert.enabled' = 'false',
    'write.parquet.compression-codec' = 'zstd'
);

-- Add table comment
COMMENT ON TABLE raw.shopify_orders IS 'Raw Shopify order webhook events. Append-only, partitioned by month.';
