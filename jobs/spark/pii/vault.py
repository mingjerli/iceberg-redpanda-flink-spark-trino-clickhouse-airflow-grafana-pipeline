"""
PII Vault
=========

semantic.pii_vault maps a token back to its normalized plaintext. It is one of
only two places plaintext survives; the other is the raw layer.

The table is insert-only and never issues an UPDATE. Partly the watermark rule
in CLAUDE.md, but mainly because plaintext is immutable by construction: the
token is a pure function of the plaintext, so a matching token can never need a
different value.

The __future__ import is load-bearing: the Spark image runs Python 3.8.
"""
from __future__ import annotations

import logging

from pyspark.sql import Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

logger = logging.getLogger(__name__)

VAULT_TABLE = "iceberg.semantic.pii_vault"

VAULT_DDL = f"""
    CREATE TABLE IF NOT EXISTS {VAULT_TABLE} (
        token          STRING NOT NULL COMMENT 'Deterministic token, primary key',
        pii_class      STRING NOT NULL COMMENT 'email | phone | name | address | name_prefix | mailchimp_id',
        plaintext      STRING NOT NULL COMMENT 'Original normalized value',
        key_version    INT            COMMENT 'Pepper version used to derive token',
        _first_seen_at TIMESTAMP      COMMENT 'When this token was first written',
        _first_source  STRING         COMMENT 'Staging table that first produced it'
    )
    USING iceberg
    PARTITIONED BY (pii_class)
    TBLPROPERTIES (
        'format-version' = '2',
        'write.parquet.compression-codec' = 'zstd'
    )
"""


def create_vault(spark):
    """Create semantic.pii_vault if it does not already exist."""
    spark.sql(VAULT_DDL)


def upsert_vault(spark, vault_df):
    """
    Insert tokens not already present. Returns the number of rows added.

    The source MUST be deduplicated on token before the MERGE. Several staging
    tables legitimately produce the same token for one email address, and a
    MERGE whose source repeats a key raises MERGE_CARDINALITY_VIOLATION.
    """
    if vault_df is None:
        return 0

    create_vault(spark)
    before = spark.table(VAULT_TABLE).count()

    # Reconstruct with explicit non-nullable schema. tokenize_frame returns
    # nullable columns because token_expr and normalize return when().otherwise()
    # expressions which are nullable by construction; .where(token.isNotNull())
    # filters rows but doesn't narrow schema nullability. Use named Row
    # construction to map values by name, preventing silent column swaps if
    # this list is reordered in future edits.
    target_schema = StructType([
        StructField("token", StringType(), False),
        StructField("pii_class", StringType(), False),
        StructField("plaintext", StringType(), False),
        StructField("key_version", IntegerType(), True),
        StructField("_first_source", StringType(), True),
    ])

    vault_df.dropDuplicates(["token"]) \
        .rdd.map(lambda r: Row(
            token=r["token"],
            pii_class=r["pii_class"],
            plaintext=r["plaintext"],
            key_version=r["key_version"],
            _first_source=r["_first_source"],
        )) \
        .toDF(schema=target_schema) \
        .createOrReplaceTempView("pii_vault_updates")

    spark.sql(f"""
        MERGE INTO {VAULT_TABLE} AS target
        USING pii_vault_updates AS source
        ON target.token = source.token
        WHEN NOT MATCHED THEN INSERT (
            token, pii_class, plaintext, key_version, _first_seen_at, _first_source
        ) VALUES (
            source.token, source.pii_class, source.plaintext, source.key_version,
            current_timestamp(), source._first_source
        )
    """)

    added = spark.table(VAULT_TABLE).count() - before
    logger.info(f"Vault: {added} new tokens stored")
    return added


def lookup(spark, tokens):
    """Return vault rows for the given tokens. Unknown tokens yield no row."""
    create_vault(spark)
    if not tokens:
        return spark.table(VAULT_TABLE).limit(0)

    quoted = ", ".join("'" + t.replace("'", "''") + "'" for t in tokens)
    return spark.sql(f"""
        SELECT token, pii_class, plaintext
        FROM {VAULT_TABLE}
        WHERE token IN ({quoted})
    """)
