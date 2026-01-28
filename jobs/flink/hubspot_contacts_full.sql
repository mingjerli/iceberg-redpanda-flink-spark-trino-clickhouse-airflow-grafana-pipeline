-- =============================================================================
-- Flink SQL: HubSpot Contacts Full Pipeline
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
CREATE TEMPORARY TABLE hubspot_contacts_source (
    `id`                              STRING,
    `hs_object_id`                    STRING,
    `properties`                      STRING,
    `email`                           STRING,
    `firstname`                       STRING,
    `lastname`                        STRING,
    `phone`                           STRING,
    `mobilephone`                     STRING,
    `address`                         STRING,
    `city`                            STRING,
    `state`                           STRING,
    `zip`                             STRING,
    `country`                         STRING,
    `company`                         STRING,
    `jobtitle`                        STRING,
    `associatedcompanyid`             STRING,
    `lifecyclestage`                  STRING,
    `hs_lead_status`                  STRING,
    `website`                         STRING,
    `hs_analytics_source`             STRING,
    `hs_analytics_first_url`          STRING,
    `hs_analytics_num_page_views`     BIGINT,
    `hs_analytics_num_visits`         BIGINT,
    `hs_email_optout`                 BOOLEAN,
    `createdate`                      STRING,
    `lastmodifieddate`                STRING,
    `_webhook_received_at`            STRING,
    `_webhook_subscription_type`      STRING,
    -- Kafka metadata
    `event_time` TIMESTAMP(3) METADATA FROM 'timestamp',
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'hubspot.contacts',
    'properties.bootstrap.servers' = 'redpanda:9092',
    'properties.group.id' = 'flink-hubspot-contacts-raw',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

-- -----------------------------------------------------------------------------
-- Create Iceberg Sink Table
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hubspot_contacts (
    `id`                              STRING,
    `hs_object_id`                    STRING,
    `properties`                      STRING,
    `email`                           STRING,
    `firstname`                       STRING,
    `lastname`                        STRING,
    `phone`                           STRING,
    `mobilephone`                     STRING,
    `address`                         STRING,
    `city`                            STRING,
    `state`                           STRING,
    `zip`                             STRING,
    `country`                         STRING,
    `company`                         STRING,
    `jobtitle`                        STRING,
    `associatedcompanyid`             STRING,
    `lifecyclestage`                  STRING,
    `hs_lead_status`                  STRING,
    `website`                         STRING,
    `hs_analytics_source`             STRING,
    `hs_analytics_first_url`          STRING,
    `hs_analytics_num_page_views`     BIGINT,
    `hs_analytics_num_visits`         BIGINT,
    `hs_email_optout`                 BOOLEAN,
    `createdate`                      TIMESTAMP(3),
    `lastmodifieddate`                TIMESTAMP(3),
    `_webhook_received_at`            TIMESTAMP(3),
    `_webhook_subscription_type`      STRING,
    `_loaded_at`                      TIMESTAMP(3)
) WITH (
    'format-version' = '2',
    'write.upsert.enabled' = 'false'
);

-- -----------------------------------------------------------------------------
-- Streaming Insert Job
-- -----------------------------------------------------------------------------
INSERT INTO hubspot_contacts
SELECT
    `id`,
    `hs_object_id`,
    `properties`,
    `email`,
    `firstname`,
    `lastname`,
    `phone`,
    `mobilephone`,
    `address`,
    `city`,
    `state`,
    `zip`,
    `country`,
    `company`,
    `jobtitle`,
    `associatedcompanyid`,
    `lifecyclestage`,
    `hs_lead_status`,
    `website`,
    `hs_analytics_source`,
    `hs_analytics_first_url`,
    `hs_analytics_num_page_views`,
    `hs_analytics_num_visits`,
    `hs_email_optout`,
    TO_TIMESTAMP(REPLACE(REPLACE(`createdate`, 'T', ' '), 'Z', '')),
    TO_TIMESTAMP(REPLACE(REPLACE(`lastmodifieddate`, 'T', ' '), 'Z', '')),
    TO_TIMESTAMP(REPLACE(REPLACE(`_webhook_received_at`, 'T', ' '), 'Z', '')),
    `_webhook_subscription_type`,
    CURRENT_TIMESTAMP as `_loaded_at`
FROM hubspot_contacts_source;
