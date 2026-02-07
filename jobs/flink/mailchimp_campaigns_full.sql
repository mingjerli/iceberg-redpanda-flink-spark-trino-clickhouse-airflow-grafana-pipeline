-- =============================================================================
-- Flink SQL: Mailchimp Campaigns Full Pipeline
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
CREATE TEMPORARY TABLE mailchimp_campaigns_source (
    `campaign_id`                     STRING,
    `campaign_type`                   STRING,
    `status`                          STRING,
    `list_id`                         STRING,
    `subject_line`                    STRING,
    `preview_text`                    STRING,
    `from_name`                       STRING,
    `from_email`                      STRING,
    `reply_to`                        STRING,
    `send_time`                       STRING,
    `content_type`                    STRING,
    `emails_sent`                     INT,
    `opens`                           INT,
    `unique_opens`                    INT,
    `clicks`                          INT,
    `unique_clicks`                   INT,
    `unsubscribes`                    INT,
    `bounces`                         INT,
    `open_rate`                       DOUBLE,
    `click_rate`                      DOUBLE,
    `settings`                        STRING,
    `tracking`                        STRING,
    `_webhook_received_at`            STRING,
    `_webhook_event_type`             STRING,
    `_source`                         STRING,
    `_event_type`                     STRING,
    -- Kafka metadata
    `event_time` TIMESTAMP(3) METADATA FROM 'timestamp',
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'mailchimp.campaigns',
    'properties.bootstrap.servers' = 'redpanda:9092',
    'properties.group.id' = 'flink-mailchimp-campaigns-raw',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

-- -----------------------------------------------------------------------------
-- Create Iceberg Sink Table
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mailchimp_campaigns (
    `campaign_id`                     STRING,
    `campaign_type`                   STRING,
    `status`                          STRING,
    `list_id`                         STRING,
    `subject_line`                    STRING,
    `preview_text`                    STRING,
    `from_name`                       STRING,
    `from_email`                      STRING,
    `reply_to`                        STRING,
    `send_time`                       TIMESTAMP(3),
    `content_type`                    STRING,
    `emails_sent`                     INT,
    `opens`                           INT,
    `unique_opens`                    INT,
    `clicks`                          INT,
    `unique_clicks`                   INT,
    `unsubscribes`                    INT,
    `bounces`                         INT,
    `open_rate`                       DECIMAL(5, 4),
    `click_rate`                      DECIMAL(5, 4),
    `settings`                        STRING,
    `tracking`                        STRING,
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
INSERT INTO mailchimp_campaigns
SELECT
    `campaign_id`,
    `campaign_type`,
    `status`,
    `list_id`,
    `subject_line`,
    `preview_text`,
    `from_name`,
    `from_email`,
    `reply_to`,
    TO_TIMESTAMP(REPLACE(REPLACE(`send_time`, 'T', ' '), 'Z', '')),
    `content_type`,
    `emails_sent`,
    `opens`,
    `unique_opens`,
    `clicks`,
    `unique_clicks`,
    `unsubscribes`,
    `bounces`,
    CAST(`open_rate` AS DECIMAL(5, 4)),
    CAST(`click_rate` AS DECIMAL(5, 4)),
    `settings`,
    `tracking`,
    TO_TIMESTAMP(REPLACE(REPLACE(`_webhook_received_at`, 'T', ' '), 'Z', '')),
    `_webhook_event_type`,
    CURRENT_TIMESTAMP as `_loaded_at`
FROM mailchimp_campaigns_source;
