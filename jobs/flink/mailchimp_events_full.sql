-- =============================================================================
-- Flink SQL: Mailchimp Events Full Pipeline
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
CREATE TEMPORARY TABLE mailchimp_events_source (
    `event_id`                        STRING,
    `campaign_id`                     STRING,
    `email_id`                        STRING,
    `email_address`                   STRING,
    `action`                          STRING,
    `timestamp`                       STRING,
    `url`                             STRING,
    `ip`                              STRING,
    `user_agent`                      STRING,
    `location`                        STRING,
    `bounce_type`                     STRING,
    `list_id`                         STRING,
    `_webhook_received_at`            STRING,
    `_webhook_event_type`             STRING,
    `_source`                         STRING,
    `_event_type`                     STRING,
    -- Kafka metadata
    `event_time` TIMESTAMP(3) METADATA FROM 'timestamp',
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'mailchimp.events',
    'properties.bootstrap.servers' = 'redpanda:9092',
    'properties.group.id' = 'flink-mailchimp-events-raw',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

-- -----------------------------------------------------------------------------
-- Create Iceberg Sink Table
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mailchimp_events (
    `event_id`                        STRING,
    `campaign_id`                     STRING,
    `email_id`                        STRING,
    `email_address`                   STRING,
    `action`                          STRING,
    `event_timestamp`                 TIMESTAMP(3),
    `url`                             STRING,
    `ip`                              STRING,
    `user_agent`                      STRING,
    `location`                        STRING,
    `bounce_type`                     STRING,
    `list_id`                         STRING,
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
INSERT INTO mailchimp_events
SELECT
    `event_id`,
    `campaign_id`,
    `email_id`,
    `email_address`,
    `action`,
    TO_TIMESTAMP(REPLACE(REPLACE(`timestamp`, 'T', ' '), 'Z', '')),
    `url`,
    `ip`,
    `user_agent`,
    `location`,
    `bounce_type`,
    `list_id`,
    TO_TIMESTAMP(REPLACE(REPLACE(`_webhook_received_at`, 'T', ' '), 'Z', '')),
    `_webhook_event_type`,
    CURRENT_TIMESTAMP as `_loaded_at`
FROM mailchimp_events_source;
