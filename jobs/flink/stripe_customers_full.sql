-- =============================================================================
-- Flink SQL: Stripe Customers Full Pipeline
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
CREATE TEMPORARY TABLE stripe_customers_source (
    `id`                              STRING,
    `object`                          STRING,
    `email`                           STRING,
    `name`                            STRING,
    `phone`                           STRING,
    `address`                         STRING,
    `shipping`                        STRING,
    `balance`                         BIGINT,
    `currency`                        STRING,
    `default_source`                  STRING,
    `delinquent`                      BOOLEAN,
    `invoice_prefix`                  STRING,
    `invoice_settings`                STRING,
    `next_invoice_sequence`           BIGINT,
    `tax_exempt`                      STRING,
    `preferred_locales`               STRING,
    `discount`                        STRING,
    `description`                     STRING,
    `livemode`                        BOOLEAN,
    `metadata`                        STRING,
    `test_clock`                      STRING,
    `created`                         BIGINT,
    `_webhook_received_at`            STRING,
    `_webhook_event_id`               STRING,
    -- Kafka metadata
    `event_time` TIMESTAMP(3) METADATA FROM 'timestamp',
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'stripe.customers',
    'properties.bootstrap.servers' = 'redpanda:9092',
    'properties.group.id' = 'flink-stripe-customers-raw',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

-- -----------------------------------------------------------------------------
-- Create Iceberg Sink Table
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stripe_customers (
    `id`                              STRING,
    `object`                          STRING,
    `email`                           STRING,
    `name`                            STRING,
    `phone`                           STRING,
    `address`                         STRING,
    `shipping`                        STRING,
    `balance`                         BIGINT,
    `currency`                        STRING,
    `default_source`                  STRING,
    `delinquent`                      BOOLEAN,
    `invoice_prefix`                  STRING,
    `invoice_settings`                STRING,
    `next_invoice_sequence`           BIGINT,
    `tax_exempt`                      STRING,
    `preferred_locales`               STRING,
    `discount`                        STRING,
    `description`                     STRING,
    `livemode`                        BOOLEAN,
    `metadata`                        STRING,
    `test_clock`                      STRING,
    `created`                         TIMESTAMP(3),
    `_webhook_received_at`            TIMESTAMP(3),
    `_webhook_event_id`               STRING,
    `_loaded_at`                      TIMESTAMP(3)
) WITH (
    'format-version' = '2',
    'write.upsert.enabled' = 'false'
);

-- -----------------------------------------------------------------------------
-- Streaming Insert Job
-- -----------------------------------------------------------------------------
INSERT INTO stripe_customers
SELECT
    `id`,
    `object`,
    `email`,
    `name`,
    `phone`,
    `address`,
    `shipping`,
    `balance`,
    `currency`,
    `default_source`,
    `delinquent`,
    `invoice_prefix`,
    `invoice_settings`,
    `next_invoice_sequence`,
    `tax_exempt`,
    `preferred_locales`,
    `discount`,
    `description`,
    `livemode`,
    `metadata`,
    `test_clock`,
    -- Convert Unix timestamp (seconds) to TIMESTAMP
    TO_TIMESTAMP_LTZ(`created`, 0),
    TO_TIMESTAMP(REPLACE(REPLACE(`_webhook_received_at`, 'T', ' '), 'Z', '')),
    `_webhook_event_id`,
    CURRENT_TIMESTAMP as `_loaded_at`
FROM stripe_customers_source;
