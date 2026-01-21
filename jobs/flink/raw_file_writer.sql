-- =============================================================================
-- Flink SQL: Raw File Writer (JSON Archive)
-- =============================================================================
-- Archives raw webhook events from Redpanda to S3/MinIO as partitioned
-- JSON files. This serves as a backup/archive before Iceberg processing.
--
-- Files are organized as:
--   s3://raw-events/{source}/{entity}/year={YYYY}/month={MM}/day={DD}/
--
-- This job uses filesystem sink with rolling policies.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Shopify Orders Archive
-- -----------------------------------------------------------------------------
CREATE TEMPORARY TABLE shopify_orders_kafka_source (
    `payload` STRING,
    `event_time` TIMESTAMP(3) METADATA FROM 'timestamp',
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'shopify.orders',
    'properties.bootstrap.servers' = 'redpanda:9092',
    'properties.group.id' = 'flink-raw-file-writer-shopify-orders',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'raw'
);

CREATE TEMPORARY TABLE shopify_orders_file_sink (
    `payload` STRING,
    `event_time` TIMESTAMP(3),
    `year` STRING,
    `month` STRING,
    `day` STRING
) PARTITIONED BY (`year`, `month`, `day`)
WITH (
    'connector' = 'filesystem',
    'path' = 's3://raw-events/shopify/orders/',
    'format' = 'raw',
    'sink.rolling-policy.file-size' = '128MB',
    'sink.rolling-policy.rollover-interval' = '15 min',
    'sink.rolling-policy.check-interval' = '1 min',
    'partition.time-extractor.timestamp-pattern' = '$year-$month-$day'
);

INSERT INTO shopify_orders_file_sink
SELECT
    `payload`,
    `event_time`,
    DATE_FORMAT(`event_time`, 'yyyy') as `year`,
    DATE_FORMAT(`event_time`, 'MM') as `month`,
    DATE_FORMAT(`event_time`, 'dd') as `day`
FROM shopify_orders_kafka_source;


-- -----------------------------------------------------------------------------
-- Shopify Customers Archive
-- -----------------------------------------------------------------------------
CREATE TEMPORARY TABLE shopify_customers_kafka_source (
    `payload` STRING,
    `event_time` TIMESTAMP(3) METADATA FROM 'timestamp',
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'shopify.customers',
    'properties.bootstrap.servers' = 'redpanda:9092',
    'properties.group.id' = 'flink-raw-file-writer-shopify-customers',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'raw'
);

CREATE TEMPORARY TABLE shopify_customers_file_sink (
    `payload` STRING,
    `event_time` TIMESTAMP(3),
    `year` STRING,
    `month` STRING,
    `day` STRING
) PARTITIONED BY (`year`, `month`, `day`)
WITH (
    'connector' = 'filesystem',
    'path' = 's3://raw-events/shopify/customers/',
    'format' = 'raw',
    'sink.rolling-policy.file-size' = '128MB',
    'sink.rolling-policy.rollover-interval' = '15 min',
    'sink.rolling-policy.check-interval' = '1 min'
);

INSERT INTO shopify_customers_file_sink
SELECT
    `payload`,
    `event_time`,
    DATE_FORMAT(`event_time`, 'yyyy') as `year`,
    DATE_FORMAT(`event_time`, 'MM') as `month`,
    DATE_FORMAT(`event_time`, 'dd') as `day`
FROM shopify_customers_kafka_source;


-- -----------------------------------------------------------------------------
-- Stripe Customers Archive
-- -----------------------------------------------------------------------------
CREATE TEMPORARY TABLE stripe_customers_kafka_source (
    `payload` STRING,
    `event_time` TIMESTAMP(3) METADATA FROM 'timestamp',
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'stripe.customers',
    'properties.bootstrap.servers' = 'redpanda:9092',
    'properties.group.id' = 'flink-raw-file-writer-stripe-customers',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'raw'
);

CREATE TEMPORARY TABLE stripe_customers_file_sink (
    `payload` STRING,
    `event_time` TIMESTAMP(3),
    `year` STRING,
    `month` STRING,
    `day` STRING
) PARTITIONED BY (`year`, `month`, `day`)
WITH (
    'connector' = 'filesystem',
    'path' = 's3://raw-events/stripe/customers/',
    'format' = 'raw',
    'sink.rolling-policy.file-size' = '128MB',
    'sink.rolling-policy.rollover-interval' = '15 min',
    'sink.rolling-policy.check-interval' = '1 min'
);

INSERT INTO stripe_customers_file_sink
SELECT
    `payload`,
    `event_time`,
    DATE_FORMAT(`event_time`, 'yyyy') as `year`,
    DATE_FORMAT(`event_time`, 'MM') as `month`,
    DATE_FORMAT(`event_time`, 'dd') as `day`
FROM stripe_customers_kafka_source;


-- -----------------------------------------------------------------------------
-- Stripe Charges Archive
-- -----------------------------------------------------------------------------
CREATE TEMPORARY TABLE stripe_charges_kafka_source (
    `payload` STRING,
    `event_time` TIMESTAMP(3) METADATA FROM 'timestamp',
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'stripe.charges',
    'properties.bootstrap.servers' = 'redpanda:9092',
    'properties.group.id' = 'flink-raw-file-writer-stripe-charges',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'raw'
);

CREATE TEMPORARY TABLE stripe_charges_file_sink (
    `payload` STRING,
    `event_time` TIMESTAMP(3),
    `year` STRING,
    `month` STRING,
    `day` STRING
) PARTITIONED BY (`year`, `month`, `day`)
WITH (
    'connector' = 'filesystem',
    'path' = 's3://raw-events/stripe/charges/',
    'format' = 'raw',
    'sink.rolling-policy.file-size' = '128MB',
    'sink.rolling-policy.rollover-interval' = '15 min',
    'sink.rolling-policy.check-interval' = '1 min'
);

INSERT INTO stripe_charges_file_sink
SELECT
    `payload`,
    `event_time`,
    DATE_FORMAT(`event_time`, 'yyyy') as `year`,
    DATE_FORMAT(`event_time`, 'MM') as `month`,
    DATE_FORMAT(`event_time`, 'dd') as `day`
FROM stripe_charges_kafka_source;


-- -----------------------------------------------------------------------------
-- HubSpot Contacts Archive
-- -----------------------------------------------------------------------------
CREATE TEMPORARY TABLE hubspot_contacts_kafka_source (
    `payload` STRING,
    `event_time` TIMESTAMP(3) METADATA FROM 'timestamp',
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'hubspot.contacts',
    'properties.bootstrap.servers' = 'redpanda:9092',
    'properties.group.id' = 'flink-raw-file-writer-hubspot-contacts',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'raw'
);

CREATE TEMPORARY TABLE hubspot_contacts_file_sink (
    `payload` STRING,
    `event_time` TIMESTAMP(3),
    `year` STRING,
    `month` STRING,
    `day` STRING
) PARTITIONED BY (`year`, `month`, `day`)
WITH (
    'connector' = 'filesystem',
    'path' = 's3://raw-events/hubspot/contacts/',
    'format' = 'raw',
    'sink.rolling-policy.file-size' = '128MB',
    'sink.rolling-policy.rollover-interval' = '15 min',
    'sink.rolling-policy.check-interval' = '1 min'
);

INSERT INTO hubspot_contacts_file_sink
SELECT
    `payload`,
    `event_time`,
    DATE_FORMAT(`event_time`, 'yyyy') as `year`,
    DATE_FORMAT(`event_time`, 'MM') as `month`,
    DATE_FORMAT(`event_time`, 'dd') as `day`
FROM hubspot_contacts_kafka_source;
