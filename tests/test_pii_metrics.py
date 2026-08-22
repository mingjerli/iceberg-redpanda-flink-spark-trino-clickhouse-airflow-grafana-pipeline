"""
Tests for the PII metric producers added to close IMPORTANT 5 of the PII
masking fix wave: pipeline_pii_vault_entries and
pipeline_pii_tokenization_null_rate were declared in metrics/registry.py with
no producer anywhere in the codebase. See metrics/pii_metrics.py for why the
two gauges are collected differently (a vault table scan vs. the in-memory
frame tokenize_frame just produced).
"""
from __future__ import annotations

from pyspark.sql import Row
from pyspark.sql.types import StringType, StructField, StructType

from metrics.pii_metrics import collect_null_rate_samples, collect_vault_metrics
from pii.tokenize import tokenize_frame
from pii.vault import VAULT_TABLE, create_vault, upsert_vault

PEPPER = "test-pepper-do-not-use-in-production"


def sample_for(samples, name, **labels):
    matches = [
        s for s in samples
        if s.name == name and all(s.labels.get(k) == v for k, v in labels.items())
    ]
    assert len(matches) == 1, "expected one {}{}, got {}".format(name, labels, len(matches))
    return matches[0]


# ---------------------------------------------------------------------------
# collect_vault_metrics: a scan of semantic.pii_vault's current state.
# ---------------------------------------------------------------------------

def test_vault_metrics_grouped_by_pii_class(spark):
    # semantic.pii_vault accumulates across the whole test session (see
    # test_pii_vault.py's own DELETE-first pattern) -- start from a clean
    # slate so this count assertion is not at the mercy of test order.
    create_vault(spark)
    spark.sql(f"DELETE FROM {VAULT_TABLE} WHERE 1=1")

    df = spark.createDataFrame(
        [Row(customer_id="c1", email="ada@example.com", phone="+15551234567")],
        StructType([
            StructField("customer_id", StringType()),
            StructField("email", StringType()),
            StructField("phone", StringType()),
        ]),
    )
    _, vault_df = tokenize_frame(df, "stg_shopify_customers", PEPPER)
    upsert_vault(spark, vault_df)

    samples = collect_vault_metrics(spark)
    names = {s.name for s in samples}
    assert names == {"pipeline_pii_vault_entries"}

    classes = {s.labels["pii_class"] for s in samples}
    assert {"email", "phone"} <= classes
    assert sample_for(samples, "pipeline_pii_vault_entries", pii_class="email").value == 1.0


def test_vault_metrics_missing_table_returns_empty(spark):
    spark.sql("DROP TABLE IF EXISTS iceberg.semantic.pii_vault")
    assert collect_vault_metrics(spark) == []


# ---------------------------------------------------------------------------
# collect_null_rate_samples: the in-memory frame tokenize_frame produced.
# ---------------------------------------------------------------------------

def _token_frame(spark, rows):
    return spark.createDataFrame(
        rows,
        StructType([
            StructField("email_token", StringType()),
            StructField("phone_token", StringType()),
        ]),
    )


def test_null_rate_computed_per_registered_column(spark):
    df = _token_frame(spark, [
        Row(email_token="tok_a", phone_token="tok_x"),
        Row(email_token=None, phone_token="tok_y"),
        Row(email_token="tok_b", phone_token="tok_z"),
        Row(email_token=None, phone_token=None),
    ])

    samples = collect_null_rate_samples(df, "stg_shopify_customers")

    email = sample_for(
        samples, "pipeline_pii_tokenization_null_rate",
        table="stg_shopify_customers", column="email",
    )
    phone = sample_for(
        samples, "pipeline_pii_tokenization_null_rate",
        table="stg_shopify_customers", column="phone",
    )
    assert email.value == 0.5
    assert phone.value == 0.25


def test_null_rate_labels_use_the_pre_token_column_name(spark):
    """The metric should read `column="email"`, not `column="email_token"` --
    an operator reading the alert wants the name of the thing that broke, not
    the derived column the pipeline happens to store it in."""
    df = _token_frame(spark, [Row(email_token="tok_a", phone_token="tok_x")])
    samples = collect_null_rate_samples(df, "stg_shopify_customers")
    columns = {s.labels["column"] for s in samples}
    assert columns == {"email", "phone"}


def test_table_with_no_registered_pii_returns_empty(spark):
    """stg_mailchimp_campaigns has no PII_FIELDS/PII_DERIVED entry at all."""
    df = spark.createDataFrame(
        [Row(campaign_id="c1")],
        StructType([StructField("campaign_id", StringType())]),
    )
    assert collect_null_rate_samples(df, "stg_mailchimp_campaigns") == []


def test_empty_frame_returns_empty(spark):
    df = _token_frame(spark, [])
    assert collect_null_rate_samples(df, "stg_shopify_customers") == []


def test_samples_render_without_error(spark):
    from metrics.pushgateway import render_exposition

    df = _token_frame(spark, [Row(email_token=None, phone_token="tok_x")])
    body = render_exposition(collect_null_rate_samples(df, "stg_shopify_customers"))
    assert "pipeline_pii_tokenization_null_rate" in body
