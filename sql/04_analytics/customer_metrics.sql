-- =============================================================================
-- Analytics: customer_metrics
-- =============================================================================
-- Pre-computed customer-level metrics for fast dashboard queries.
-- Updated incrementally from core.customers and core.orders.
-- =============================================================================

CREATE TABLE IF NOT EXISTS analytics.customer_metrics (
    customer_id                 STRING          COMMENT 'Unified customer ID',
    email                       STRING          COMMENT 'Customer email',
    full_name                   STRING          COMMENT 'Customer name',

    -- Segmentation
    customer_tier               STRING          COMMENT 'Tier: vip, gold, silver, bronze, new',
    lifecycle_stage             STRING          COMMENT 'Stage: prospect, qualified, customer',
    customer_segment            STRING          COMMENT 'Derived segment: high_value_engaged, high_value, engaged, active, inactive',

    -- Financial metrics
    total_spent                 DECIMAL(18, 2)  COMMENT 'Lifetime spend',
    total_orders                BIGINT          COMMENT 'Total orders',
    avg_order_value             DECIMAL(18, 2)  COMMENT 'Average order value',
    first_order_value           DECIMAL(18, 2)  COMMENT 'First order amount',
    last_order_value            DECIMAL(18, 2)  COMMENT 'Most recent order amount',
    estimated_ltv               DECIMAL(18, 2)  COMMENT 'Estimated lifetime value',

    -- Order timing
    first_order_date            DATE            COMMENT 'Date of first order',
    last_order_date             DATE            COMMENT 'Date of most recent order',
    days_since_first_order      INT             COMMENT 'Days since first order',
    days_since_last_order       INT             COMMENT 'Days since last order (recency)',
    order_frequency_days        DECIMAL(10, 2)  COMMENT 'Average days between orders',

    -- Engagement
    page_views                  BIGINT          COMMENT 'Total page views',
    sessions                    BIGINT          COMMENT 'Total sessions',
    engagement_score            DECIMAL(5, 2)   COMMENT 'Engagement score 0-100',

    -- RFM Analysis
    rfm_recency_score           INT             COMMENT 'Recency score 1-5',
    rfm_frequency_score         INT             COMMENT 'Frequency score 1-5',
    rfm_monetary_score          INT             COMMENT 'Monetary score 1-5',
    rfm_segment                 STRING          COMMENT 'RFM segment: Champions, Loyal, New, At Risk, Lost, etc.',

    -- Marketing
    accepts_marketing           BOOLEAN         COMMENT 'Marketing consent',
    acquisition_source          STRING          COMMENT 'Original acquisition source',

    -- Source info
    source_count                INT             COMMENT 'Number of source systems',
    has_shopify                 BOOLEAN         COMMENT 'Has Shopify data',
    has_hubspot                 BOOLEAN         COMMENT 'Has HubSpot data',

    -- Cohorts
    first_order_cohort          STRING          COMMENT 'First order YYYY-MM cohort',
    signup_cohort               STRING          COMMENT 'Signup YYYY-MM cohort',

    -- Timestamps
    customer_created_at         TIMESTAMP       COMMENT 'Customer creation time',
    customer_updated_at         TIMESTAMP       COMMENT 'Customer last update',
    _computed_at                TIMESTAMP       COMMENT 'Metrics computation time',
    _version                    INT             COMMENT 'Computation version'
)
USING iceberg
PARTITIONED BY (customer_segment)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);
