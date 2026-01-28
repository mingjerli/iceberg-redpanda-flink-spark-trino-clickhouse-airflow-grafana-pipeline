-- =============================================================================
-- Semantic: entity_index
-- =============================================================================
-- Maps source system IDs to unified entity IDs for cross-source identity.
-- This is the core entity resolution table.
-- =============================================================================

CREATE TABLE IF NOT EXISTS semantic.entity_index (
    entity_id                   STRING          COMMENT 'Unified entity identifier (UUID)',
    source                      STRING          COMMENT 'Source system: shopify, stripe, hubspot',
    source_id                   STRING          COMMENT 'Original ID in source system',

    -- Identity fields used for matching
    email                       STRING          COMMENT 'Original email',
    email_normalized            STRING          COMMENT 'Normalized email for matching',
    phone                       STRING          COMMENT 'Original phone',
    phone_normalized            STRING          COMMENT 'Normalized phone for matching',
    first_name                  STRING          COMMENT 'First name',
    last_name                   STRING          COMMENT 'Last name',
    full_name                   STRING          COMMENT 'Full name',

    -- Match metadata
    match_confidence            DECIMAL(5, 2)   COMMENT 'Match confidence score 0-100',
    match_type                  STRING          COMMENT 'Match type: exact_email, exact_phone, fuzzy_name',
    matched_entity_id           STRING          COMMENT 'Entity this was matched to (if merged)',

    -- Timestamps
    created_at                  TIMESTAMP       COMMENT 'Record creation time',
    updated_at                  TIMESTAMP       COMMENT 'Last update time'
)
USING iceberg
PARTITIONED BY (source)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

-- Index for efficient entity lookups
-- Note: In production, consider adding secondary indexes for email_normalized and phone_normalized
