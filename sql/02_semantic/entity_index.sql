-- =============================================================================
-- Semantic: entity_index
-- =============================================================================
-- Maps source system IDs to unified entity IDs for cross-source identity.
-- This is the core entity resolution table.
--
-- Note: This file is reference documentation only. The actual CREATE TABLE
-- statement is inline in jobs/spark/entity_backfill.py.
--
-- No plaintext PII reaches this table. `entity_backfill.py` matches on
-- `email_token` (the tokenized, class-keyed form of every source's email
-- column -- see jobs/spark/pii/registry.py), never on plaintext, so
-- match_reason below carries a token, not an email address.
-- =============================================================================

CREATE TABLE IF NOT EXISTS semantic.entity_index (
    unified_id                  STRING NOT NULL COMMENT 'Unified entity identifier (UUID)',
    entity_type                 STRING NOT NULL COMMENT 'Entity type, e.g. customer (partition key)',
    source                      STRING NOT NULL COMMENT 'Source system: shopify_customers, stripe_customers, hubspot_contacts, mailchimp_subscribers, ga4_sessions',
    source_id                   STRING NOT NULL COMMENT 'Original ID in source system',

    -- Match metadata
    match_type                  STRING          COMMENT 'exact_email or new_entity -- no fuzzy, phone, or ML matching is implemented (see DESIGN_PII_MASKING.md Section 8)',
    match_confidence            DECIMAL(3, 2)   COMMENT 'Match confidence, always 1.0 today (no partial-confidence matching exists)',
    match_reason                STRING          COMMENT 'Human-readable match explanation, e.g. "Matched via email token: tok_..." -- carries the token, never plaintext',
    linked_to_unified_id        STRING          COMMENT 'Entity this was merged into, if any',
    matched_at                  TIMESTAMP NOT NULL COMMENT 'When this match was recorded',
    matched_by                  STRING          COMMENT 'Process that produced the match, e.g. spark_backfill',
    _staged_at                  TIMESTAMP       COMMENT 'Staging layer process time of the source row'
)
USING iceberg
PARTITIONED BY (entity_type)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);
