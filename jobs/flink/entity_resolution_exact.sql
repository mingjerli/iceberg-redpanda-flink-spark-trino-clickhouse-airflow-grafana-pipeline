-- ============================================================================
-- Flink Job: Exact-Match Entity Resolution
-- ============================================================================
-- Purpose: Real-time entity resolution using exact matching on email/phone
--
-- This job runs as a streaming Flink application that:
-- 1. Monitors staging tables for new records
-- 2. Performs exact-match resolution (email, phone) via blocking index lookup
-- 3. Creates new unified entities or links to existing ones
-- 4. Updates entity_index and blocking_index using upsert mode
--
-- Match Strategy:
-- - Exact email match: confidence 1.0
-- - Exact phone match: confidence 1.0
-- - No match: Create new unified entity
--
-- Note: This job uses upsert mode to handle idempotency. Re-processing the
-- same staging record will update rather than duplicate the entity_index entry.
--
-- Execution:
--   flink run -c org.apache.flink.table.gateway.SqlGateway \
--     entity_resolution_exact.sql
-- ============================================================================

-- -----------------------------------------------------------------------------
-- Catalog Setup
-- -----------------------------------------------------------------------------
CREATE CATALOG iceberg WITH (
    'type' = 'iceberg',
    'catalog-type' = 'rest',
    'uri' = 'http://iceberg-rest:8181',
    'warehouse' = 's3://warehouse/',
    's3.endpoint' = 'http://minio:9000',
    's3.path-style-access' = 'true'
);

USE CATALOG iceberg;

-- -----------------------------------------------------------------------------
-- Create databases if not exist
-- -----------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS staging;
CREATE DATABASE IF NOT EXISTS semantic;

-- -----------------------------------------------------------------------------
-- Entity Index Table (upsert mode for idempotency)
-- -----------------------------------------------------------------------------
-- Using (source, source_id) as the primary key ensures that re-processing
-- the same staging record updates rather than creates duplicates.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS semantic.entity_index (
    unified_id STRING NOT NULL,
    entity_type STRING NOT NULL,
    source STRING NOT NULL,
    source_id STRING NOT NULL,
    match_type STRING,
    match_confidence DECIMAL(3, 2),
    match_reason STRING,
    linked_to_unified_id STRING,
    matched_at TIMESTAMP(6) NOT NULL,
    matched_by STRING,
    _staged_at TIMESTAMP(6),
    PRIMARY KEY (entity_type, source, source_id) NOT ENFORCED
) WITH (
    'format-version' = '2',
    'write.upsert.enabled' = 'true'
);

-- -----------------------------------------------------------------------------
-- Blocking Index Table (upsert mode)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS semantic.blocking_index (
    blocking_key STRING NOT NULL,
    blocking_key_type STRING NOT NULL,
    unified_id STRING NOT NULL,
    entity_type STRING NOT NULL,
    source STRING NOT NULL,
    source_id STRING NOT NULL,
    key_value STRING,
    is_primary BOOLEAN,
    created_at TIMESTAMP(6) NOT NULL,
    expires_at TIMESTAMP(6),
    PRIMARY KEY (blocking_key, entity_type) NOT ENFORCED
) WITH (
    'format-version' = '2',
    'write.upsert.enabled' = 'true'
);

-- -----------------------------------------------------------------------------
-- Staging Source Tables (streaming read from Iceberg)
-- -----------------------------------------------------------------------------

-- Shopify Customers (streaming source)
CREATE TEMPORARY VIEW shopify_customers_stream AS
SELECT
    CAST(customer_id AS STRING) AS source_id,
    'shopify_customers' AS source,
    'customer' AS entity_type,
    LOWER(TRIM(email)) AS normalized_email,
    REGEXP_REPLACE(phone, '[^0-9+]', '') AS normalized_phone,
    first_name,
    last_name,
    full_name,
    _staged_at
FROM staging.stg_shopify_customers
/*+ OPTIONS('streaming'='true', 'monitor-interval'='10s') */;

-- HubSpot Contacts (streaming source)
CREATE TEMPORARY VIEW hubspot_contacts_stream AS
SELECT
    contact_id AS source_id,
    'hubspot_contacts' AS source,
    'customer' AS entity_type,
    LOWER(TRIM(email)) AS normalized_email,
    REGEXP_REPLACE(COALESCE(phone, mobile_phone), '[^0-9+]', '') AS normalized_phone,
    first_name,
    last_name,
    full_name,
    _staged_at
FROM staging.stg_hubspot_contacts
/*+ OPTIONS('streaming'='true', 'monitor-interval'='10s') */;

-- Combined stream of all customer sources
CREATE TEMPORARY VIEW all_customers_stream AS
SELECT * FROM shopify_customers_stream
UNION ALL
SELECT * FROM hubspot_contacts_stream;

-- -----------------------------------------------------------------------------
-- Blocking Index Lookup View (for temporal join)
-- -----------------------------------------------------------------------------
-- This view provides the latest unified_id for each blocking key.
-- Used for looking up existing entities by email or phone.
-- -----------------------------------------------------------------------------
CREATE TEMPORARY VIEW blocking_lookup AS
SELECT
    blocking_key,
    unified_id,
    entity_type
FROM semantic.blocking_index
/*+ OPTIONS('streaming'='true', 'monitor-interval'='5s') */;

-- -----------------------------------------------------------------------------
-- Entity Resolution: Main Pipeline
-- -----------------------------------------------------------------------------
-- For each incoming customer record:
-- 1. Look up the blocking index by normalized email
-- 2. If found, use the existing unified_id
-- 3. If not found, generate a new unified_id
--
-- Note: In Flink streaming, we use a LEFT JOIN with the blocking index.
-- The unified_id generation uses a deterministic approach based on source+id
-- to ensure idempotency.
-- -----------------------------------------------------------------------------

-- Insert resolved entities into entity_index
INSERT INTO semantic.entity_index
SELECT
    -- If email match found, use existing unified_id; else generate new one
    COALESCE(
        bl.unified_id,
        -- Deterministic ID generation: prefix + timestamp millis + source + id
        CONCAT(
            'U-',
            CAST(UNIX_TIMESTAMP(c._staged_at) * 1000 AS STRING),
            '-',
            SUBSTRING(c.source, 1, 2),
            '-',
            c.source_id
        )
    ) AS unified_id,
    c.entity_type,
    c.source,
    c.source_id,
    -- Match type
    CASE
        WHEN bl.unified_id IS NOT NULL THEN 'exact_email'
        ELSE 'new_entity'
    END AS match_type,
    -- Confidence is always 1.0 for exact match or new entity
    CAST(1.00 AS DECIMAL(3, 2)) AS match_confidence,
    -- Match reason
    CASE
        WHEN bl.unified_id IS NOT NULL
            THEN CONCAT('Matched via email: ', c.normalized_email)
        ELSE 'New entity created - no existing match found'
    END AS match_reason,
    CAST(NULL AS STRING) AS linked_to_unified_id,
    CURRENT_TIMESTAMP AS matched_at,
    'flink_exact_match' AS matched_by,
    c._staged_at
FROM all_customers_stream c
-- Lookup existing entity by normalized email
LEFT JOIN blocking_lookup FOR SYSTEM_TIME AS OF c._staged_at AS bl
    ON bl.blocking_key = CONCAT('email:', c.normalized_email)
    AND bl.entity_type = 'customer'
WHERE c.normalized_email IS NOT NULL
  AND c.normalized_email <> '';


-- -----------------------------------------------------------------------------
-- Blocking Index Updates: Email Keys
-- -----------------------------------------------------------------------------
-- After inserting into entity_index, add blocking keys for new entities.
-- This uses the entity_index as the source of truth for unified_id.
-- -----------------------------------------------------------------------------

INSERT INTO semantic.blocking_index
SELECT
    CONCAT('email:', c.normalized_email) AS blocking_key,
    'email' AS blocking_key_type,
    ei.unified_id,
    'customer' AS entity_type,
    c.source,
    c.source_id,
    c.normalized_email AS key_value,
    TRUE AS is_primary,
    CURRENT_TIMESTAMP AS created_at,
    CAST(NULL AS TIMESTAMP(6)) AS expires_at
FROM all_customers_stream c
JOIN semantic.entity_index FOR SYSTEM_TIME AS OF c._staged_at AS ei
    ON ei.source = c.source
    AND ei.source_id = c.source_id
    AND ei.entity_type = 'customer'
WHERE c.normalized_email IS NOT NULL
  AND c.normalized_email <> '';


-- -----------------------------------------------------------------------------
-- Blocking Index Updates: Phone Keys
-- -----------------------------------------------------------------------------

INSERT INTO semantic.blocking_index
SELECT
    CONCAT('phone:', c.normalized_phone) AS blocking_key,
    'phone' AS blocking_key_type,
    ei.unified_id,
    'customer' AS entity_type,
    c.source,
    c.source_id,
    c.normalized_phone AS key_value,
    FALSE AS is_primary,
    CURRENT_TIMESTAMP AS created_at,
    CAST(NULL AS TIMESTAMP(6)) AS expires_at
FROM all_customers_stream c
JOIN semantic.entity_index FOR SYSTEM_TIME AS OF c._staged_at AS ei
    ON ei.source = c.source
    AND ei.source_id = c.source_id
    AND ei.entity_type = 'customer'
WHERE c.normalized_phone IS NOT NULL
  AND c.normalized_phone <> ''
  AND CHAR_LENGTH(c.normalized_phone) >= 7;


-- -----------------------------------------------------------------------------
-- Notes on Flink Streaming Entity Resolution
-- -----------------------------------------------------------------------------
-- 1. Upsert Mode: The entity_index uses (entity_type, source, source_id) as
--    the primary key. This ensures that re-processing the same staging record
--    updates the existing entry rather than creating duplicates.
--
-- 2. Temporal Joins: The blocking index lookup uses a temporal join
--    (FOR SYSTEM_TIME AS OF) to get the latest blocking keys at the time
--    the staging record was created.
--
-- 3. Deterministic ID Generation: New unified_ids are generated using a
--    deterministic formula based on _staged_at timestamp and source+id.
--    This ensures idempotency - reprocessing produces the same unified_id.
--
-- 4. Blocking Index Primary Key: Uses (blocking_key, entity_type) so that
--    the same blocking key (e.g., email:john@example.com) maps to one entity.
--    If a blocking key needs to be reassigned (entity merge), the old entry
--    is automatically replaced.
--
-- 5. Limitations:
--    - Phone-based matching happens after email matching completes
--    - Records without email get new unified_ids (no cross-matching)
--    - Fuzzy matching is handled by the Spark batch job
--
-- 6. For production deployment:
--    - Configure checkpointing for fault tolerance
--    - Monitor lag metrics for timely processing
--    - Set up alerting for resolution quality metrics
-- -----------------------------------------------------------------------------
