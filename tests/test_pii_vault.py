"""
Tests for semantic.pii_vault.

The dedup assertion is the important one. Several staging tables legitimately
produce the same token for the same email address -- that is the entire point of
class-keyed tokens -- and feeding a MERGE a source with duplicate keys raises
MERGE_CARDINALITY_VIOLATION. That failure already shipped once in this pipeline,
via marts GA4 sessions.
"""
from __future__ import annotations

from pyspark.sql import Row

from pii.tokenize import tokenize_frame
from pii.vault import VAULT_TABLE, create_vault, lookup, upsert_vault

PEPPER = "test-pepper-do-not-use-in-production"


def _shopify(spark, rows):
    return spark.createDataFrame([Row(**r) for r in rows])


def test_upsert_inserts_new_tokens(spark):
    create_vault(spark)
    spark.sql(f"DELETE FROM {VAULT_TABLE} WHERE 1=1")

    _, vault_df = tokenize_frame(
        _shopify(spark, [{"customer_id": "1", "email": "ada@example.com"}]),
        "stg_shopify_customers", PEPPER,
    )
    upsert_vault(spark, vault_df)

    stored = spark.table(VAULT_TABLE).collect()
    assert len(stored) == 1
    assert stored[0]["plaintext"] == "ada@example.com"
    assert stored[0]["pii_class"] == "email"


def test_upsert_is_idempotent(spark):
    create_vault(spark)
    spark.sql(f"DELETE FROM {VAULT_TABLE} WHERE 1=1")

    _, vault_df = tokenize_frame(
        _shopify(spark, [{"customer_id": "1", "email": "ada@example.com"}]),
        "stg_shopify_customers", PEPPER,
    )
    upsert_vault(spark, vault_df)
    first = spark.table(VAULT_TABLE).count()
    upsert_vault(spark, vault_df)
    second = spark.table(VAULT_TABLE).count()

    assert first == second


def test_duplicate_tokens_in_one_batch_do_not_raise(spark):
    """Two rows with the same email produce one token; MERGE must not see both."""
    create_vault(spark)
    spark.sql(f"DELETE FROM {VAULT_TABLE} WHERE 1=1")

    _, vault_df = tokenize_frame(
        _shopify(spark, [
            {"customer_id": "1", "email": "ada@example.com"},
            {"customer_id": "2", "email": "ADA@example.com"},
        ]),
        "stg_shopify_customers", PEPPER,
    )
    upsert_vault(spark, vault_df)

    assert spark.table(VAULT_TABLE).count() == 1


def test_lookup_returns_plaintext_for_known_tokens(spark):
    create_vault(spark)
    spark.sql(f"DELETE FROM {VAULT_TABLE} WHERE 1=1")

    tokenized, vault_df = tokenize_frame(
        _shopify(spark, [{"customer_id": "1", "email": "ada@example.com"}]),
        "stg_shopify_customers", PEPPER,
    )
    upsert_vault(spark, vault_df)
    token = tokenized.collect()[0]["email_token"]

    result = lookup(spark, [token]).collect()
    assert len(result) == 1
    assert result[0]["plaintext"] == "ada@example.com"


def test_lookup_of_unknown_token_returns_nothing(spark):
    create_vault(spark)
    assert lookup(spark, ["tok_" + "0" * 32]).count() == 0
