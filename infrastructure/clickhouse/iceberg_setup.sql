-- =============================================================================
-- ClickHouse Iceberg Setup
-- =============================================================================
-- Configures ClickHouse to query Iceberg tables stored in MinIO.
-- ClickHouse 24.3+ supports the iceberg() table function for reading Iceberg tables.
--
-- Usage:
--   docker exec -i iceberg-clickhouse clickhouse-client < iceberg_setup.sql
--
-- Note: ClickHouse reads Iceberg tables directly from S3/MinIO using the iceberg()
-- table function. This is read-only access - writes go through Spark/Flink.
-- =============================================================================

-- Create database for Iceberg views
CREATE DATABASE IF NOT EXISTS iceberg;

-- Enable schema evolution support for Iceberg tables
SET iceberg_engine_ignore_schema_evolution = 1;

-- =============================================================================
-- Raw Layer Views
-- =============================================================================
-- These views provide access to raw Iceberg tables via the iceberg() table function.
-- The iceberg() function reads the metadata.json file to understand the table schema.

CREATE OR REPLACE VIEW iceberg.raw_shopify_orders AS
SELECT *
FROM iceberg('http://minio:9000/warehouse/raw/shopify_orders/',
             'admin', 'admin123');

CREATE OR REPLACE VIEW iceberg.raw_shopify_customers AS
SELECT *
FROM iceberg('http://minio:9000/warehouse/raw/shopify_customers/',
             'admin', 'admin123');

CREATE OR REPLACE VIEW iceberg.raw_stripe_charges AS
SELECT *
FROM iceberg('http://minio:9000/warehouse/raw/stripe_charges/',
             'admin', 'admin123');

CREATE OR REPLACE VIEW iceberg.raw_stripe_customers AS
SELECT *
FROM iceberg('http://minio:9000/warehouse/raw/stripe_customers/',
             'admin', 'admin123');

CREATE OR REPLACE VIEW iceberg.raw_hubspot_contacts AS
SELECT *
FROM iceberg('http://minio:9000/warehouse/raw/hubspot_contacts/',
             'admin', 'admin123');

-- =============================================================================
-- Staging Layer Views
-- =============================================================================

CREATE OR REPLACE VIEW iceberg.stg_shopify_orders AS
SELECT *
FROM iceberg('http://minio:9000/warehouse/staging/stg_shopify_orders/',
             'admin', 'admin123');

CREATE OR REPLACE VIEW iceberg.stg_shopify_customers AS
SELECT *
FROM iceberg('http://minio:9000/warehouse/staging/stg_shopify_customers/',
             'admin', 'admin123');

CREATE OR REPLACE VIEW iceberg.stg_stripe_charges AS
SELECT *
FROM iceberg('http://minio:9000/warehouse/staging/stg_stripe_charges/',
             'admin', 'admin123');

CREATE OR REPLACE VIEW iceberg.stg_stripe_customers AS
SELECT *
FROM iceberg('http://minio:9000/warehouse/staging/stg_stripe_customers/',
             'admin', 'admin123');

CREATE OR REPLACE VIEW iceberg.stg_hubspot_contacts AS
SELECT *
FROM iceberg('http://minio:9000/warehouse/staging/stg_hubspot_contacts/',
             'admin', 'admin123');

-- =============================================================================
-- Semantic Layer Views
-- =============================================================================

CREATE OR REPLACE VIEW iceberg.entity_index AS
SELECT *
FROM iceberg('http://minio:9000/warehouse/semantic/entity_index/',
             'admin', 'admin123');

-- =============================================================================
-- Analytics Layer Views
-- =============================================================================

CREATE OR REPLACE VIEW iceberg.customer_metrics AS
SELECT *
FROM iceberg('http://minio:9000/warehouse/analytics/customer_metrics/',
             'admin', 'admin123');

CREATE OR REPLACE VIEW iceberg.order_summary AS
SELECT *
FROM iceberg('http://minio:9000/warehouse/analytics/order_summary/',
             'admin', 'admin123');

CREATE OR REPLACE VIEW iceberg.payment_metrics AS
SELECT *
FROM iceberg('http://minio:9000/warehouse/analytics/payment_metrics/',
             'admin', 'admin123');

-- =============================================================================
-- Marts Layer Views
-- =============================================================================

CREATE OR REPLACE VIEW iceberg.customer_360 AS
SELECT *
FROM iceberg('http://minio:9000/warehouse/marts/customer_360/',
             'admin', 'admin123');

CREATE OR REPLACE VIEW iceberg.sales_dashboard_daily AS
SELECT *
FROM iceberg('http://minio:9000/warehouse/marts/sales_dashboard_daily/',
             'admin', 'admin123');

-- =============================================================================
-- Verification Queries
-- =============================================================================
-- Run these to verify the setup is working:

-- SELECT 'raw_shopify_orders' AS table_name, count(*) AS row_count FROM iceberg.raw_shopify_orders
-- UNION ALL
-- SELECT 'stg_shopify_orders', count(*) FROM iceberg.stg_shopify_orders
-- UNION ALL
-- SELECT 'entity_index', count(*) FROM iceberg.entity_index
-- UNION ALL
-- SELECT 'customer_metrics', count(*) FROM iceberg.customer_metrics;

-- =============================================================================
-- Sample OLAP Queries
-- =============================================================================
-- These demonstrate ClickHouse's analytical capabilities on Iceberg data.

-- Daily order metrics with running totals
-- SELECT
--     toDate(created_at) AS order_date,
--     count(*) AS orders,
--     sum(total_price) AS revenue,
--     avg(total_price) AS avg_order_value,
--     runningAccumulate(sumState(total_price)) OVER (ORDER BY order_date) AS cumulative_revenue
-- FROM iceberg.stg_shopify_orders
-- GROUP BY order_date
-- ORDER BY order_date;

-- Customer segmentation analysis
-- SELECT
--     customer_tier,
--     count(DISTINCT customer_id) AS customer_count,
--     avg(total_spent) AS avg_total_spent,
--     quantile(0.5)(total_spent) AS median_spent
-- FROM iceberg.stg_shopify_customers
-- GROUP BY customer_tier
-- ORDER BY avg_total_spent DESC;
