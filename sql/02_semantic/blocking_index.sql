-- =============================================================================
-- Semantic: blocking_index
-- =============================================================================
-- A prepared index of blocking keys for entity resolution, keyed on
-- tokens rather than plaintext (pii/registry.py) so the table holds no
-- direct identifiers.
--
-- Note: This file is reference documentation only. The actual CREATE TABLE
-- statement is inline in jobs/spark/entity_backfill.py.
--
-- Note: this table is written every run but not read by any pipeline job
-- today. It is a prepared index awaiting a matching strategy that has not
-- been built (DESIGN_PII_MASKING.md Section 8), kept only because
-- tests/test_ga4_entity_resolution.py asserts against it.
-- =============================================================================

CREATE TABLE IF NOT EXISTS semantic.blocking_index (
    blocking_key                STRING NOT NULL COMMENT 'email:<email_token> | phone:<phone_token> | name_zip:<name_prefix_token>_<zip> -- never plaintext',
    blocking_key_type           STRING NOT NULL COMMENT 'Blocking key type: email, phone, name_zip (partition key)',
    unified_id                  STRING NOT NULL COMMENT 'Unified entity ID this row belongs to',
    entity_type                 STRING NOT NULL COMMENT 'Entity type, e.g. customer (partition key)',
    source                      STRING NOT NULL COMMENT 'Source system: shopify_customers, stripe_customers, hubspot_contacts, mailchimp_subscribers',
    source_id                   STRING NOT NULL COMMENT 'Original ID in source system',
    key_value                   STRING          COMMENT 'The raw component(s) the key was built from: the token itself for email/phone, "<name_prefix_token>_<zip>" for name_zip',
    is_primary                  BOOLEAN         COMMENT 'Whether this is the primary blocking key for the row',
    created_at                  TIMESTAMP NOT NULL COMMENT 'Record creation time',
    expires_at                  TIMESTAMP       COMMENT 'Optional expiry for the blocking entry'
)
USING iceberg
PARTITIONED BY (blocking_key_type, entity_type)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

-- =============================================================================
-- Blocking Strategy
-- =============================================================================
-- Three blocking passes, all keyed on tokens (jobs/spark/entity_backfill.py):
--
-- Pass 1: email          blocking_key = concat('email:', email_token)
-- Pass 2: phone          blocking_key = concat('phone:', phone_token)
-- Pass 3: name_zip       blocking_key = concat('name_zip:', last_name_prefix_token, '_', zip)
--                        -- zip stays in the clear (pii/registry.py); the
--                        -- surname component is tokenized because a hash
--                        -- has no meaningful substring, so the prefix is
--                        -- tokenized as its own value (name_prefix class).
-- =============================================================================
