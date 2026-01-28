-- =============================================================================
-- Flink SQL: Shopify Customers Full Pipeline
-- =============================================================================
-- Complete pipeline: setup catalog + streaming ingestion
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

-- Use the Iceberg catalog
USE CATALOG iceberg_catalog;

-- Create Raw Database
CREATE DATABASE IF NOT EXISTS `raw`
COMMENT 'Raw layer - append-only webhook events';

USE `raw`;

-- -----------------------------------------------------------------------------
-- Create Kafka Source Table
-- -----------------------------------------------------------------------------
CREATE TEMPORARY TABLE shopify_customers_source (
    `id`                              BIGINT,
    `admin_graphql_api_id`            STRING,
    `email`                           STRING,
    `first_name`                      STRING,
    `last_name`                       STRING,
    `phone`                           STRING,
    `default_address`                 STRING,
    `addresses`                       STRING,
    `accepts_marketing`               BOOLEAN,
    `accepts_marketing_updated_at`    STRING,
    `marketing_opt_in_level`          STRING,
    `currency`                        STRING,
    `total_spent`                     STRING,
    `orders_count`                    BIGINT,
    `state`                           STRING,
    `verified_email`                  BOOLEAN,
    `tax_exempt`                      BOOLEAN,
    `tax_exemptions`                  STRING,
    `note`                            STRING,
    `tags`                            STRING,
    `created_at`                      STRING,
    `updated_at`                      STRING,
    `_webhook_received_at`            STRING,
    `_webhook_topic`                  STRING,
    `_source`                         STRING,
    `_event_type`                     STRING,
    -- Kafka metadata
    `event_time` TIMESTAMP(3) METADATA FROM 'timestamp',
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'shopify.customers',
    'properties.bootstrap.servers' = 'redpanda:9092',
    'properties.group.id' = 'flink-shopify-customers-raw',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

-- -----------------------------------------------------------------------------
-- Create Iceberg Sink Table
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS shopify_customers (
    `id`                              BIGINT,
    `admin_graphql_api_id`            STRING,
    `email`                           STRING,
    `first_name`                      STRING,
    `last_name`                       STRING,
    `phone`                           STRING,
    `default_address`                 STRING,
    `addresses`                       STRING,
    `accepts_marketing`               BOOLEAN,
    `accepts_marketing_updated_at`    TIMESTAMP(3),
    `marketing_opt_in_level`          STRING,
    `currency`                        STRING,
    `total_spent`                     DECIMAL(18, 2),
    `orders_count`                    BIGINT,
    `state`                           STRING,
    `verified_email`                  BOOLEAN,
    `tax_exempt`                      BOOLEAN,
    `tax_exemptions`                  STRING,
    `note`                            STRING,
    `tags`                            STRING,
    `created_at`                      TIMESTAMP(3),
    `updated_at`                      TIMESTAMP(3),
    `_webhook_received_at`            TIMESTAMP(3),
    `_webhook_topic`                  STRING,
    `_loaded_at`                      TIMESTAMP(3)
) WITH (
    'format-version' = '2',
    'write.upsert.enabled' = 'false'
);

-- -----------------------------------------------------------------------------
-- Streaming Insert Job
-- -----------------------------------------------------------------------------
INSERT INTO shopify_customers
SELECT
    `id`,
    `admin_graphql_api_id`,
    `email`,
    `first_name`,
    `last_name`,
    `phone`,
    `default_address`,
    `addresses`,
    `accepts_marketing`,
    TO_TIMESTAMP(REPLACE(REPLACE(`accepts_marketing_updated_at`, 'T', ' '), 'Z', '')),
    `marketing_opt_in_level`,
    `currency`,
    CAST(`total_spent` AS DECIMAL(18, 2)),
    `orders_count`,
    `state`,
    `verified_email`,
    `tax_exempt`,
    `tax_exemptions`,
    `note`,
    `tags`,
    TO_TIMESTAMP(REPLACE(REPLACE(`created_at`, 'T', ' '), 'Z', '')),
    TO_TIMESTAMP(REPLACE(REPLACE(`updated_at`, 'T', ' '), 'Z', '')),
    TO_TIMESTAMP(REPLACE(REPLACE(`_webhook_received_at`, 'T', ' '), 'Z', '')),
    `_webhook_topic`,
    CURRENT_TIMESTAMP as `_loaded_at`
FROM shopify_customers_source;
