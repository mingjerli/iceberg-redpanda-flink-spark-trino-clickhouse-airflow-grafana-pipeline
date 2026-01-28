-- =============================================================================
-- Semantic: blocking_index
-- =============================================================================
-- Enables efficient entity resolution by grouping records with shared attributes.
-- Instead of comparing all records (O(n^2)), we only compare within blocks.
--
-- Example: Block by email domain means we only compare records with @gmail.com
-- to other @gmail.com records, not to @yahoo.com records.
-- =============================================================================

CREATE TABLE IF NOT EXISTS semantic.blocking_index (
    blocking_key                STRING          COMMENT 'The blocking key value (e.g., gmail.com)',
    blocking_type               STRING          COMMENT 'Type of blocking key: email_domain, phone_area, name_prefix',
    source                      STRING          COMMENT 'Source system: shopify, stripe, hubspot',
    source_id                   STRING          COMMENT 'Original ID in source system',
    entity_id                   STRING          COMMENT 'Resolved entity ID (null if unresolved)',

    -- Additional fields for comparison within block
    email_normalized            STRING          COMMENT 'Normalized email for comparison',
    phone_normalized            STRING          COMMENT 'Normalized phone for comparison',
    name_soundex                STRING          COMMENT 'Soundex of name for fuzzy matching',

    -- Timestamps
    created_at                  TIMESTAMP       COMMENT 'Record creation time'
)
USING iceberg
PARTITIONED BY (blocking_type)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

-- =============================================================================
-- Blocking Strategy
-- =============================================================================
-- We use multiple blocking passes to balance speed and recall:
--
-- Pass 1: Exact email match (blocking_type = 'email_exact')
--   - Most precise, catches ~70-80% of duplicates
--   - blocking_key = normalized email
--
-- Pass 2: Email domain (blocking_type = 'email_domain')
--   - Catches variations (john.doe vs johndoe)
--   - blocking_key = email domain (e.g., 'gmail.com')
--
-- Pass 3: Phone area code (blocking_type = 'phone_area')
--   - Catches records without email
--   - blocking_key = first 3 digits of phone
--
-- Pass 4: Name prefix (blocking_type = 'name_prefix')
--   - Last resort for fuzzy matching
--   - blocking_key = first 3 chars of last name (uppercase)
--   - Requires additional Levenshtein/Jaro-Winkler comparison
-- =============================================================================
