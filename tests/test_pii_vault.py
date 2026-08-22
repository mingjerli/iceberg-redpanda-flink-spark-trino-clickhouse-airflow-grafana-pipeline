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
from pii.vault import VAULT_TABLE, create_vault, ensure_namespace, lookup, upsert_vault

PEPPER = "test-pepper-do-not-use-in-production"


def _shopify(spark, rows):
    return spark.createDataFrame([Row(**r) for r in rows])


def test_ensure_namespace_is_idempotent_and_creates_when_missing(spark):
    """`CREATE TABLE` does not create the namespace it targets. On the REST
    catalog production uses, a missing namespace raises
    NoSuchNamespaceException -- which shipped: on a cold start all 8 staging
    jobs with registered PII died with `Namespace semantic does not exist`,
    while the 2 without registered PII succeeded because those return from
    upsert_vault before reaching the vault at all.

    READ THIS BEFORE TRUSTING THIS TEST. It cannot reproduce that failure.
    conftest registers the catalog as `type=hadoop`, which auto-creates a
    namespace as a directory; production is `type=rest`, which does not. So
    the whole class of missing-namespace bug is invisible to this suite no
    matter how the test is written. What is asserted here is only that
    ensure_namespace creates when absent and is safe to call repeatedly. The
    production behaviour is verified by a from-scratch `reset_and_run.sh`.
    """
    spark.sql("DROP NAMESPACE IF EXISTS iceberg.pii_ns_probe CASCADE")

    ensure_namespace(spark, "pii_ns_probe")
    ensure_namespace(spark, "pii_ns_probe")  # must not raise on the second call

    names = {row[0] for row in spark.sql("SHOW NAMESPACES IN iceberg").collect()}
    assert "pii_ns_probe" in names, f"namespace not created; got {names}"

    spark.sql("DROP NAMESPACE IF EXISTS iceberg.pii_ns_probe CASCADE")


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
