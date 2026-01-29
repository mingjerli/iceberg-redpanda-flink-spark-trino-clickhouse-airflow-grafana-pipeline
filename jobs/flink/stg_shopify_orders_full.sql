-- =============================================================================
-- Flink SQL: Staging Shopify Orders Full Pipeline
-- =============================================================================
-- Complete pipeline: setup catalog + streaming staging transform
-- Reads from raw.shopify_orders, transforms, writes to staging.stg_shopify_orders
--
-- This job runs continuously, monitoring the raw table for new data and
-- transforming it to the staging layer in near real-time.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Create Iceberg Catalog Connection
-- -----------------------------------------------------------------------------
CREATE CATALOG iceberg_catalog WITH (
    'type' = 'iceberg',
    'catalog-type' = 'rest',
    'uri' = 'http://iceberg-rest:8181',
    'warehouse' = 's3a://warehouse/',
    'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
    's3.endpoint' = 'http://minio:9000',
    's3.path-style-access' = 'true'
);

USE CATALOG iceberg_catalog;

-- Create staging database if not exists
CREATE DATABASE IF NOT EXISTS staging
COMMENT 'Staging layer - cleaned and typed data from raw sources';

USE staging;

-- -----------------------------------------------------------------------------
-- Create Staging Sink Table
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg_shopify_orders (
    -- Primary keys
    `order_id`                      BIGINT,
    `order_number`                  BIGINT,

    -- Customer identifiers
    `customer_id`                   BIGINT,
    `customer_email`                STRING,
    `customer_phone`                STRING,

    -- Order display info
    `order_name`                    STRING,

    -- Financial
    `currency`                      STRING,
    `subtotal_price`                DECIMAL(18, 2),
    `total_discounts`               DECIMAL(18, 2),
    `total_shipping`                DECIMAL(18, 2),
    `total_tax`                     DECIMAL(18, 2),
    `total_price`                   DECIMAL(18, 2),
    `total_outstanding`             DECIMAL(18, 2),

    -- Current financials
    `current_subtotal_price`        DECIMAL(18, 2),
    `current_total_price`           DECIMAL(18, 2),
    `current_total_tax`             DECIMAL(18, 2),

    -- Status fields
    `financial_status`              STRING,
    `fulfillment_status`            STRING,
    `order_status`                  STRING,

    -- Derived counts and flags
    `line_item_count`               INT,
    `discount_count`                INT,
    `has_discount`                  BOOLEAN,
    `is_cancelled`                  BOOLEAN,
    `is_test`                       BOOLEAN,
    `taxes_included`                BOOLEAN,
    `tax_exempt`                    BOOLEAN,
    `buyer_accepts_marketing`       BOOLEAN,

    -- Shipping address
    `shipping_country`              STRING,
    `shipping_country_code`         STRING,
    `shipping_province`             STRING,
    `shipping_city`                 STRING,

    -- Billing address
    `billing_country`               STRING,
    `billing_country_code`          STRING,

    -- Attribution
    `source_name`                   STRING,
    `referring_site`                STRING,
    `landing_site`                  STRING,

    -- Timestamps
    `created_at`                    TIMESTAMP(3),
    `updated_at`                    TIMESTAMP(3),
    `processed_at`                  TIMESTAMP(3),
    `cancelled_at`                  TIMESTAMP(3),
    `closed_at`                     TIMESTAMP(3),

    -- Metadata
    `_raw_id`                       BIGINT,
    `_webhook_topic`                STRING,
    `_loaded_at`                    TIMESTAMP(3),
    `_staged_at`                    TIMESTAMP(3)
) WITH (
    'format-version' = '2',
    'write.upsert.enabled' = 'false'
);

-- -----------------------------------------------------------------------------
-- Create Source View from Raw Table (Streaming Mode)
-- -----------------------------------------------------------------------------
-- Read from raw.shopify_orders in streaming mode to process new data
-- -----------------------------------------------------------------------------
CREATE TEMPORARY VIEW raw_orders_source AS
SELECT
    `id`,
    `order_number`,
    `customer_id`,
    `email`,
    `phone`,
    `name`,
    `currency`,
    `subtotal_price`,
    `total_discounts`,
    `total_shipping_price_set`,
    `total_tax`,
    `total_price`,
    `total_outstanding`,
    `current_subtotal_price`,
    `current_total_price`,
    `current_total_tax`,
    `financial_status`,
    `fulfillment_status`,
    `line_items`,
    `discount_codes`,
    `cancelled_at`,
    `test`,
    `taxes_included`,
    `tax_exempt`,
    `buyer_accepts_marketing`,
    `shipping_address`,
    `billing_address`,
    `source_name`,
    `referring_site`,
    `landing_site`,
    `created_at`,
    `updated_at`,
    `processed_at`,
    `closed_at`,
    `_webhook_topic`,
    `_loaded_at`
FROM iceberg_catalog.`raw`.shopify_orders
/*+ OPTIONS(
    'streaming' = 'true',
    'monitor-interval' = '10s'
) */;

-- -----------------------------------------------------------------------------
-- Streaming Staging Transform
-- -----------------------------------------------------------------------------
INSERT INTO stg_shopify_orders
SELECT
    -- Primary keys
    `id`                            AS `order_id`,
    `order_number`,

    -- Customer identifiers
    `customer_id`,
    LOWER(TRIM(`email`))            AS `customer_email`,
    `phone`                         AS `customer_phone`,

    -- Order display info
    `name`                          AS `order_name`,

    -- Financial
    UPPER(`currency`)               AS `currency`,
    `subtotal_price`,
    `total_discounts`,
    -- Extract shop money amount from total_shipping_price_set JSON
    CAST(
        COALESCE(
            JSON_VALUE(`total_shipping_price_set`, '$.shop_money.amount'),
            '0'
        ) AS DECIMAL(18, 2)
    )                               AS `total_shipping`,
    `total_tax`,
    `total_price`,
    `total_outstanding`,

    -- Current financials
    `current_subtotal_price`,
    `current_total_price`,
    `current_total_tax`,

    -- Status fields
    `financial_status`,
    `fulfillment_status`,
    -- Derived order status
    CASE
        WHEN `cancelled_at` IS NOT NULL THEN 'cancelled'
        WHEN `financial_status` IN ('refunded', 'partially_refunded') THEN 'refunded'
        WHEN `financial_status` = 'paid' AND `fulfillment_status` = 'fulfilled' THEN 'completed'
        WHEN `financial_status` = 'paid' AND (`fulfillment_status` IS NULL OR `fulfillment_status` = 'partial') THEN 'processing'
        WHEN `financial_status` IN ('pending', 'authorized') THEN 'pending_payment'
        ELSE 'open'
    END                             AS `order_status`,

    -- Derived counts and flags
    -- Count line items from JSON array
    COALESCE(CARDINALITY(CAST(`line_items` AS ARRAY<STRING>)), 0) AS `line_item_count`,
    -- Count discount codes
    COALESCE(CARDINALITY(CAST(`discount_codes` AS ARRAY<STRING>)), 0) AS `discount_count`,
    `total_discounts` > 0           AS `has_discount`,
    `cancelled_at` IS NOT NULL      AS `is_cancelled`,
    COALESCE(`test`, FALSE)         AS `is_test`,
    COALESCE(`taxes_included`, FALSE) AS `taxes_included`,
    COALESCE(`tax_exempt`, FALSE)   AS `tax_exempt`,
    COALESCE(`buyer_accepts_marketing`, FALSE) AS `buyer_accepts_marketing`,

    -- Shipping address (extracted from JSON)
    JSON_VALUE(`shipping_address`, '$.country')      AS `shipping_country`,
    JSON_VALUE(`shipping_address`, '$.country_code') AS `shipping_country_code`,
    JSON_VALUE(`shipping_address`, '$.province')     AS `shipping_province`,
    JSON_VALUE(`shipping_address`, '$.city')         AS `shipping_city`,

    -- Billing address (extracted from JSON)
    JSON_VALUE(`billing_address`, '$.country')       AS `billing_country`,
    JSON_VALUE(`billing_address`, '$.country_code')  AS `billing_country_code`,

    -- Attribution
    `source_name`,
    `referring_site`,
    `landing_site`,

    -- Timestamps
    `created_at`,
    `updated_at`,
    `processed_at`,
    `cancelled_at`,
    `closed_at`,

    -- Metadata
    `id`                            AS `_raw_id`,
    `_webhook_topic`,
    `_loaded_at`,
    CURRENT_TIMESTAMP               AS `_staged_at`

FROM raw_orders_source;
