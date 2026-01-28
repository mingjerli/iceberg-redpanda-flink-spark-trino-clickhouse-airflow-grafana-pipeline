-- =============================================================================
-- ClickHouse Analytics Tables
-- =============================================================================
-- Creates materialized views and tables for real-time OLAP analytics.
-- These tables are populated from Iceberg tables via scheduled sync.
-- =============================================================================

-- Create analytics database
CREATE DATABASE IF NOT EXISTS analytics;

-- =============================================================================
-- Customer Metrics (Materialized View)
-- =============================================================================
CREATE TABLE IF NOT EXISTS analytics.customer_metrics
(
    customer_id String,
    email String,
    full_name String,
    customer_tier LowCardinality(String),
    customer_segment LowCardinality(String),
    total_spent Decimal(18, 2),
    total_orders UInt64,
    avg_order_value Decimal(18, 2),
    first_order_date Date,
    last_order_date Date,
    days_since_last_order UInt32,
    rfm_segment LowCardinality(String),
    accepts_marketing UInt8,
    source_count UInt8,
    _synced_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_synced_at)
ORDER BY customer_id
PARTITION BY customer_segment
SETTINGS index_granularity = 8192;

-- =============================================================================
-- Order Summary (Aggregated by Day)
-- =============================================================================
CREATE TABLE IF NOT EXISTS analytics.order_summary_daily
(
    order_date Date,
    shipping_country LowCardinality(String),
    channel LowCardinality(String),
    total_orders UInt64,
    completed_orders UInt64,
    cancelled_orders UInt64,
    unique_customers UInt64,
    gross_revenue Decimal(18, 2),
    net_revenue Decimal(18, 2),
    total_discounts Decimal(18, 2),
    avg_order_value Decimal(18, 2),
    _synced_at DateTime DEFAULT now()
)
ENGINE = SummingMergeTree()
ORDER BY (order_date, shipping_country, channel)
PARTITION BY toYYYYMM(order_date)
SETTINGS index_granularity = 8192;

-- =============================================================================
-- Payment Metrics
-- =============================================================================
CREATE TABLE IF NOT EXISTS analytics.payment_metrics_daily
(
    payment_date Date,
    card_brand LowCardinality(String),
    billing_country LowCardinality(String),
    total_charges UInt64,
    successful_charges UInt64,
    failed_charges UInt64,
    gross_volume Decimal(18, 2),
    net_volume Decimal(18, 2),
    refunded_volume Decimal(18, 2),
    success_rate Float32,
    refund_rate Float32,
    _synced_at DateTime DEFAULT now()
)
ENGINE = SummingMergeTree()
ORDER BY (payment_date, card_brand, billing_country)
PARTITION BY toYYYYMM(payment_date)
SETTINGS index_granularity = 8192;

-- =============================================================================
-- Real-time Event Stream (for live dashboards)
-- =============================================================================
CREATE TABLE IF NOT EXISTS analytics.events_live
(
    event_time DateTime,
    event_type LowCardinality(String),
    source LowCardinality(String),
    entity_id String,
    amount Decimal(18, 2),
    metadata String
)
ENGINE = MergeTree()
ORDER BY (event_time, event_type)
PARTITION BY toYYYYMMDD(event_time)
TTL event_time + INTERVAL 7 DAY
SETTINGS index_granularity = 8192;

-- =============================================================================
-- Aggregated Views for Dashboards
-- =============================================================================

-- Hourly revenue view
CREATE VIEW IF NOT EXISTS analytics.revenue_hourly AS
SELECT
    toStartOfHour(order_date) as hour,
    sum(gross_revenue) as gross_revenue,
    sum(net_revenue) as net_revenue,
    sum(total_orders) as order_count,
    sum(unique_customers) as customer_count
FROM analytics.order_summary_daily
GROUP BY hour
ORDER BY hour DESC;

-- Customer segment distribution
CREATE VIEW IF NOT EXISTS analytics.customer_segments AS
SELECT
    customer_segment,
    count() as customer_count,
    sum(total_spent) as total_revenue,
    avg(total_orders) as avg_orders,
    avg(days_since_last_order) as avg_recency
FROM analytics.customer_metrics
GROUP BY customer_segment
ORDER BY total_revenue DESC;

-- Payment method performance
CREATE VIEW IF NOT EXISTS analytics.payment_performance AS
SELECT
    card_brand,
    sum(total_charges) as total_charges,
    sum(successful_charges) as successful_charges,
    sum(gross_volume) as gross_volume,
    avg(success_rate) as avg_success_rate
FROM analytics.payment_metrics_daily
GROUP BY card_brand
ORDER BY gross_volume DESC;
