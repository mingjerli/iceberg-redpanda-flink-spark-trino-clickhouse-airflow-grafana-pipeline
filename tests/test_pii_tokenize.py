"""
Tests for PII normalizers and the token expression.

The properties asserted here are what downstream joins depend on. A token that
is not deterministic, or that differs per source for the same value, silently
breaks cross-source entity resolution -- silently, because unmatched records are
not an error, they just become separate customers.
"""
from __future__ import annotations

from pyspark.sql import Row
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType

from pii.registry import ADDRESS, EMAIL, NAME, NAME_PREFIX, PHONE, token_column
from pii.tokenize import normalize, token_expr, tokenize_frame

PEPPER = "test-pepper-do-not-use-in-production"


def tokenize_one(spark, value, pii_class):
    """Return the token for a single scalar value, or None."""
    schema = StructType([StructField("v", StringType(), True)])
    df = spark.createDataFrame([Row(v=value)], schema=schema)
    normalized = normalize("v", pii_class)
    return df.select(token_expr(normalized, pii_class, PEPPER).alias("t")).collect()[0]["t"]


def test_token_column_appends_suffix():
    assert token_column("email") == "email_token"


def test_email_tokens_are_deterministic(spark):
    first = tokenize_one(spark, "Ada@Example.COM", EMAIL)
    second = tokenize_one(spark, "Ada@Example.COM", EMAIL)
    assert first == second


def test_email_normalization_matches_entity_backfill(spark):
    """entity_backfill.py:296 matches on lower(trim(email)); tokens must agree."""
    assert tokenize_one(spark, "  Ada@Example.COM ", EMAIL) == tokenize_one(spark, "ada@example.com", EMAIL)


def test_token_has_expected_shape(spark):
    token = tokenize_one(spark, "ada@example.com", EMAIL)
    assert token.startswith("tok_")
    assert len(token) == 36


def test_null_and_blank_produce_null_not_a_token(spark):
    """concat_ws skips nulls, so without an explicit guard a NULL input yields a
    real token -- collapsing every customer with a missing email into one."""
    assert tokenize_one(spark, None, EMAIL) is None
    assert tokenize_one(spark, "   ", EMAIL) is None


def test_same_value_different_class_gives_different_token(spark):
    assert tokenize_one(spark, "ada", NAME) != tokenize_one(spark, "ada", ADDRESS)


def test_phone_normalizer_strips_formatting(spark):
    assert tokenize_one(spark, "+1 (555) 123-4567", PHONE) == tokenize_one(spark, "+15551234567", PHONE)


def test_phone_shorter_than_seven_chars_is_null(spark):
    """The length>=7 guard lives at entity_backfill.py:388 today. Tokens are all
    36 chars, so the guard must move into the normalizer or become a no-op."""
    assert tokenize_one(spark, "12345", PHONE) is None


def test_name_prefix_uses_first_three_characters(spark):
    assert tokenize_one(spark, "Lovelace", NAME_PREFIX) == tokenize_one(spark, "LOVeXXXX", NAME_PREFIX)


def test_tokenize_frame_drops_plaintext_and_emits_vault_rows(spark):
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("email", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("full_name", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("address_line1", StringType(), True),
        StructField("address_line2", StringType(), True),
    ])
    df = spark.createDataFrame([
        Row(customer_id="1", email="Ada@Example.com", first_name="Ada",
            last_name="Lovelace", full_name="Ada Lovelace", phone="+15551234567",
            address_line1="1 Main St", address_line2=None),
    ], schema=schema)
    tokenized, vault = tokenize_frame(df, "stg_shopify_customers", PEPPER)

    for plaintext_column in ("email", "first_name", "last_name", "full_name", "phone", "address_line1"):
        assert plaintext_column not in tokenized.columns
        assert token_column(plaintext_column) in tokenized.columns

    assert "customer_id" in tokenized.columns
    assert "last_name_prefix_token" in tokenized.columns

    classes = {r["pii_class"] for r in vault.collect()}
    assert classes == {EMAIL, NAME, PHONE, ADDRESS, NAME_PREFIX}
    assert vault.filter(col("plaintext").isNull()).count() == 0


def test_full_name_token_is_the_token_of_the_joined_plaintext(spark):
    """
    full_name is built by concatenating first_name and last_name at
    staging_batch.py:346-350. Tokenizing the inputs first would concatenate two
    tokens into a meaningless string, so tokenization must run last.
    """
    df = spark.createDataFrame([
        Row(customer_id="1", first_name="Ada", last_name="Lovelace", full_name="Ada Lovelace"),
    ])
    tokenized, _ = tokenize_frame(df, "stg_shopify_customers", PEPPER)
    row = tokenized.collect()[0]

    assert row["full_name_token"] == tokenize_one(spark, "Ada Lovelace", NAME)
    assert row["full_name_token"] != row["first_name_token"]
    assert row["full_name_token"] != row["last_name_token"]


def test_tokenize_frame_gives_same_email_token_across_sources(spark):
    """Shopify email and GA4 user_id are the same person; class-keyed tokens
    must agree or cross-source entity resolution stops matching.

    GA4 user_id is registered on stg_ga4_events, not stg_ga4_sessions:
    compute_ga4_sessions reads user_id_token straight through from events
    rather than re-deriving it, so stg_ga4_sessions has no registry entry of
    its own (see pii/registry.py)."""
    shopify = spark.createDataFrame([Row(customer_id="1", email="ada@example.com")])
    ga4 = spark.createDataFrame([Row(event_id="e1", user_id="ada@example.com")])

    shopify_token = tokenize_frame(shopify, "stg_shopify_customers", PEPPER)[0].collect()[0]["email_token"]
    ga4_token = tokenize_frame(ga4, "stg_ga4_events", PEPPER)[0].collect()[0]["user_id_token"]

    assert shopify_token == ga4_token
