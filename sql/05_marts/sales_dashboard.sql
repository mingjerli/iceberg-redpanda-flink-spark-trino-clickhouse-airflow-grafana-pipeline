-- =============================================================================
-- Marts: sales_dashboard
-- =============================================================================
-- Pre-aggregated sales data optimized for the sales dashboard.
-- Daily granularity with key dimensions for filtering.
-- =============================================================================

CREATE TABLE IF NOT EXISTS marts.sales_dashboard (
    -- Time dimensions
    date                        DATE            COMMENT 'Date',
    day_of_week                 INT             COMMENT 'Day of week (1=Mon, 7=Sun)',
    day_name                    STRING          COMMENT 'Day name: Monday, Tuesday, etc.',
    week_of_year                INT             COMMENT 'ISO week number',
    month                       INT             COMMENT 'Month number',
    month_name                  STRING          COMMENT 'Month name: January, February, etc.',
    quarter                     INT             COMMENT 'Quarter (1-4)',
    year                        INT             COMMENT 'Year',
    is_weekend                  BOOLEAN         COMMENT 'Is Saturday or Sunday',

    -- Geographic dimensions
    country                     STRING          COMMENT 'Shipping country',
    country_code                STRING          COMMENT 'Country ISO code',
    region                      STRING          COMMENT 'Region grouping',

    -- Channel dimensions
    channel                     STRING          COMMENT 'Sales channel',
    source                      STRING          COMMENT 'Source system',

    -- Order metrics
    total_orders                BIGINT          COMMENT 'Total orders',
    completed_orders            BIGINT          COMMENT 'Completed orders',
    cancelled_orders            BIGINT          COMMENT 'Cancelled orders',
    pending_orders              BIGINT          COMMENT 'Pending orders',
    order_completion_rate       DECIMAL(5, 4)   COMMENT 'Completion rate',

    -- Revenue metrics
    gross_revenue               DECIMAL(18, 2)  COMMENT 'Gross revenue',
    net_revenue                 DECIMAL(18, 2)  COMMENT 'Net revenue',
    total_refunds               DECIMAL(18, 2)  COMMENT 'Total refunds',
    total_discounts             DECIMAL(18, 2)  COMMENT 'Total discounts given',
    total_shipping_revenue      DECIMAL(18, 2)  COMMENT 'Shipping collected',
    total_tax_collected         DECIMAL(18, 2)  COMMENT 'Tax collected',

    -- Averages
    avg_order_value             DECIMAL(18, 2)  COMMENT 'Average order value',
    avg_items_per_order         DECIMAL(10, 2)  COMMENT 'Average items per order',
    avg_discount_per_order      DECIMAL(18, 2)  COMMENT 'Average discount',

    -- Customer metrics
    total_customers             BIGINT          COMMENT 'Unique customers',
    new_customers               BIGINT          COMMENT 'First-time customers',
    returning_customers         BIGINT          COMMENT 'Repeat customers',
    new_customer_rate           DECIMAL(5, 4)   COMMENT 'New customer percentage',

    -- Payment metrics (from Stripe)
    successful_payments         BIGINT          COMMENT 'Successful payment count',
    failed_payments             BIGINT          COMMENT 'Failed payment count',
    payment_success_rate        DECIMAL(5, 4)   COMMENT 'Payment success rate',

    -- Comparison helpers
    gross_revenue_prev_day      DECIMAL(18, 2)  COMMENT 'Previous day gross revenue',
    gross_revenue_prev_week     DECIMAL(18, 2)  COMMENT 'Same day last week revenue',
    orders_prev_day             BIGINT          COMMENT 'Previous day orders',
    orders_prev_week            BIGINT          COMMENT 'Same day last week orders',

    -- Growth rates
    revenue_day_over_day        DECIMAL(7, 4)   COMMENT 'Revenue DoD growth',
    revenue_week_over_week      DECIMAL(7, 4)   COMMENT 'Revenue WoW growth',
    orders_day_over_day         DECIMAL(7, 4)   COMMENT 'Orders DoD growth',
    orders_week_over_week       DECIMAL(7, 4)   COMMENT 'Orders WoW growth',

    -- Metadata
    _refreshed_at               TIMESTAMP       COMMENT 'Mart refresh time'
)
USING iceberg
PARTITIONED BY (year, month)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);
