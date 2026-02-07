-- =============================================================================
-- Flink SQL: Mailchimp Subscribers Full Pipeline
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
CREATE TEMPORARY TABLE mailchimp_subscribers_source (
    `subscriber_id`                   STRING,
    `email_address`                   STRING,
    `email_type`                      STRING,
    `status`                          STRING,
    `merge_fields`                    STRING,
    `stats`                           STRING,
    `list_id`                         STRING,
    `tags`                            STRING,
    `ip_signup`                       STRING,
    `timestamp_signup`                STRING,
    `ip_opt`                          STRING,
    `timestamp_opt`                   STRING,
    `last_changed`                    STRING,
    `language`                        STRING,
    `vip`                             BOOLEAN,
    `source`                          STRING,
    `phone`                           STRING,
    `sms_status`                      STRING,
    `_webhook_received_at`            STRING,
    `_webhook_event_type`             STRING,
    `_source`                         STRING,
    `_event_type`                     STRING,
    -- Kafka metadata
    `event_time` TIMESTAMP(3) METADATA FROM 'timestamp',
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'mailchimp.subscribers',
    'properties.bootstrap.servers' = 'redpanda:9092',
    'properties.group.id' = 'flink-mailchimp-subscribers-raw',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

-- -----------------------------------------------------------------------------
-- Create Iceberg Sink Table
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mailchimp_subscribers (
    `subscriber_id`                   STRING,
    `email_address`                   STRING,
    `email_type`                      STRING,
    `status`                          STRING,
    `merge_fields`                    STRING,
    `stats`                           STRING,
    `list_id`                         STRING,
    `tags`                            STRING,
    `ip_signup`                       STRING,
    `timestamp_signup`                TIMESTAMP(3),
    `ip_opt`                          STRING,
    `timestamp_opt`                   TIMESTAMP(3),
    `last_changed`                    TIMESTAMP(3),
    `language`                        STRING,
    `vip`                             BOOLEAN,
    `source`                          STRING,
    `phone`                           STRING,
    `sms_status`                      STRING,
    `_webhook_received_at`            TIMESTAMP(3),
    `_webhook_event_type`             STRING,
    `_loaded_at`                      TIMESTAMP(3)
) WITH (
    'format-version' = '2',
    'write.upsert.enabled' = 'false'
);

-- -----------------------------------------------------------------------------
-- Streaming Insert Job
-- -----------------------------------------------------------------------------
INSERT INTO mailchimp_subscribers
SELECT
    `subscriber_id`,
    `email_address`,
    `email_type`,
    `status`,
    `merge_fields`,
    `stats`,
    `list_id`,
    `tags`,
    `ip_signup`,
    TO_TIMESTAMP(REPLACE(REPLACE(`timestamp_signup`, 'T', ' '), 'Z', '')),
    `ip_opt`,
    TO_TIMESTAMP(REPLACE(REPLACE(`timestamp_opt`, 'T', ' '), 'Z', '')),
    TO_TIMESTAMP(REPLACE(REPLACE(`last_changed`, 'T', ' '), 'Z', '')),
    `language`,
    `vip`,
    `source`,
    `phone`,
    `sms_status`,
    TO_TIMESTAMP(REPLACE(REPLACE(`_webhook_received_at`, 'T', ' '), 'Z', '')),
    `_webhook_event_type`,
    CURRENT_TIMESTAMP as `_loaded_at`
FROM mailchimp_subscribers_source;
