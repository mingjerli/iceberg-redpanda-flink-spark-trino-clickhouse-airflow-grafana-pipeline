-- =============================================================================
-- Flink SQL: Stripe Charges Full Pipeline
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
    's3.path-style-access' = 'true',
    's3.access-key-id' = 'admin',
    's3.secret-access-key' = 'admin123456'
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
CREATE TEMPORARY TABLE stripe_charges_source (
    `id`                              STRING,
    `object`                          STRING,
    `amount`                          BIGINT,
    `amount_captured`                 BIGINT,
    `amount_refunded`                 BIGINT,
    `currency`                        STRING,
    `customer`                        STRING,
    `payment_intent`                  STRING,
    `payment_method`                  STRING,
    `payment_method_details`          STRING,
    `billing_details`                 STRING,
    `receipt_email`                   STRING,
    `shipping`                        STRING,
    `status`                          STRING,
    `captured`                        BOOLEAN,
    `paid`                            BOOLEAN,
    `refunded`                        BOOLEAN,
    `disputed`                        BOOLEAN,
    `failure_code`                    STRING,
    `failure_message`                 STRING,
    `failure_balance_transaction`     STRING,
    `outcome`                         STRING,
    `fraud_details`                   STRING,
    `balance_transaction`             STRING,
    `transfer_data`                   STRING,
    `transfer_group`                  STRING,
    `on_behalf_of`                    STRING,
    `source_transfer`                 STRING,
    `invoice`                         STRING,
    `application`                     STRING,
    `application_fee`                 STRING,
    `application_fee_amount`          BIGINT,
    `statement_descriptor`            STRING,
    `statement_descriptor_suffix`     STRING,
    `calculated_statement_descriptor` STRING,
    `receipt_number`                  STRING,
    `receipt_url`                     STRING,
    `refunds`                         STRING,
    `review`                          STRING,
    `source`                          STRING,
    `description`                     STRING,
    `livemode`                        BOOLEAN,
    `metadata`                        STRING,
    `created`                         BIGINT,
    `_webhook_received_at`            STRING,
    `_webhook_event_id`               STRING,
    -- Kafka metadata
    `event_time` TIMESTAMP(3) METADATA FROM 'timestamp',
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'stripe.charges',
    'properties.bootstrap.servers' = 'redpanda:9092',
    'properties.group.id' = 'flink-stripe-charges-raw',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

-- -----------------------------------------------------------------------------
-- Create Iceberg Sink Table
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stripe_charges (
    `id`                              STRING,
    `object`                          STRING,
    `amount`                          BIGINT,
    `amount_captured`                 BIGINT,
    `amount_refunded`                 BIGINT,
    `currency`                        STRING,
    `customer`                        STRING,
    `payment_intent`                  STRING,
    `payment_method`                  STRING,
    `payment_method_details`          STRING,
    `billing_details`                 STRING,
    `receipt_email`                   STRING,
    `shipping`                        STRING,
    `status`                          STRING,
    `captured`                        BOOLEAN,
    `paid`                            BOOLEAN,
    `refunded`                        BOOLEAN,
    `disputed`                        BOOLEAN,
    `failure_code`                    STRING,
    `failure_message`                 STRING,
    `failure_balance_transaction`     STRING,
    `outcome`                         STRING,
    `fraud_details`                   STRING,
    `balance_transaction`             STRING,
    `transfer_data`                   STRING,
    `transfer_group`                  STRING,
    `on_behalf_of`                    STRING,
    `source_transfer`                 STRING,
    `invoice`                         STRING,
    `application`                     STRING,
    `application_fee`                 STRING,
    `application_fee_amount`          BIGINT,
    `statement_descriptor`            STRING,
    `statement_descriptor_suffix`     STRING,
    `calculated_statement_descriptor` STRING,
    `receipt_number`                  STRING,
    `receipt_url`                     STRING,
    `refunds`                         STRING,
    `review`                          STRING,
    `source`                          STRING,
    `description`                     STRING,
    `livemode`                        BOOLEAN,
    `metadata`                        STRING,
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
INSERT INTO stripe_charges
SELECT
    `id`,
    `object`,
    `amount`,
    `amount_captured`,
    `amount_refunded`,
    `currency`,
    `customer`,
    `payment_intent`,
    `payment_method`,
    `payment_method_details`,
    `billing_details`,
    `receipt_email`,
    `shipping`,
    `status`,
    `captured`,
    `paid`,
    `refunded`,
    `disputed`,
    `failure_code`,
    `failure_message`,
    `failure_balance_transaction`,
    `outcome`,
    `fraud_details`,
    `balance_transaction`,
    `transfer_data`,
    `transfer_group`,
    `on_behalf_of`,
    `source_transfer`,
    `invoice`,
    `application`,
    `application_fee`,
    `application_fee_amount`,
    `statement_descriptor`,
    `statement_descriptor_suffix`,
    `calculated_statement_descriptor`,
    `receipt_number`,
    `receipt_url`,
    `refunds`,
    `review`,
    `source`,
    `description`,
    `livemode`,
    `metadata`,
    -- Convert Unix timestamp (seconds) to TIMESTAMP
    TO_TIMESTAMP_LTZ(`created`, 0),
    TO_TIMESTAMP(`_webhook_received_at`),
    `_webhook_event_id`,
    CURRENT_TIMESTAMP as `_loaded_at`
FROM stripe_charges_source;
