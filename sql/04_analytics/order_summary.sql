-- =============================================================================
-- Analytics: order_summary
-- =============================================================================
-- Aggregated order metrics by day and dimensions.
-- Enables fast time-series and dimensional analysis.
-- =============================================================================

CREATE TABLE IF NOT EXISTS analytics.order_summary (
    -- Dimensions
    order_date                  DATE            COMMENT 'Order date',
    order_hour                  INT             COMMENT 'Hour of day (0-23)',
    shipping_country            STRING          COMMENT 'Shipping country',
    shipping_country_code       STRING          COMMENT 'Shipping country code',
    shipping_state              STRING          COMMENT 'Shipping state',
    channel                     STRING          COMMENT 'Sales channel: web, pos, mobile',
    source                      STRING          COMMENT 'Source system',

    -- Order counts
    total_orders                BIGINT          COMMENT 'Total order count',
    completed_orders            BIGINT          COMMENT 'Completed orders',
    cancelled_orders            BIGINT          COMMENT 'Cancelled orders',
    refunded_orders             BIGINT          COMMENT 'Refunded orders',
    pending_orders              BIGINT          COMMENT 'Pending orders',

    -- Customer counts
    unique_customers            BIGINT          COMMENT 'Unique customer count',
    new_customers               BIGINT          COMMENT 'First-time customers',
    returning_customers         BIGINT          COMMENT 'Repeat customers',

    -- Revenue metrics
    gross_revenue               DECIMAL(18, 2)  COMMENT 'Gross revenue (all orders)',
    net_revenue                 DECIMAL(18, 2)  COMMENT 'Net revenue (excluding cancelled/refunded)',
    total_discounts             DECIMAL(18, 2)  COMMENT 'Total discount amount',
    total_shipping              DECIMAL(18, 2)  COMMENT 'Total shipping collected',
    total_tax                   DECIMAL(18, 2)  COMMENT 'Total tax collected',

    -- Averages
    avg_order_value             DECIMAL(18, 2)  COMMENT 'Average order value',
    avg_discount_per_order      DECIMAL(18, 2)  COMMENT 'Average discount per order',
    avg_items_per_order         DECIMAL(10, 2)  COMMENT 'Average items per order',

    -- Discount analysis
    orders_with_discount        BIGINT          COMMENT 'Orders with discount applied',
    discount_rate               DECIMAL(5, 4)   COMMENT 'Percentage of orders with discount',

    -- Metadata
    _computed_at                TIMESTAMP       COMMENT 'Computation time',
    _partition_key              STRING          COMMENT 'Partition key for updates'
)
USING iceberg
PARTITIONED BY (order_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);
