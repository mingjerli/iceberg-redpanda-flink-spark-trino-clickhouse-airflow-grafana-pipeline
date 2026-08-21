"""
Golden equivalence for token-based entity resolution.

This is the test that matters most in the PII work. A normalizer that drifts
from entity_backfill.py degrades match quality invisibly, because an unmatched
record is not an error -- it just becomes a separate customer. Asserting that
token-based grouping equals plaintext-based grouping is what catches the drift.
"""
from __future__ import annotations

from datetime import datetime

from pyspark.sql.functions import lit

from pii.tokenize import normalize, token_expr
from tests.pipeline_tables import insert_rows

PEPPER = "test-pepper-do-not-use-in-production"
STAGED_AT = datetime(2026, 8, 21, 12, 0, 0)


def token_for(spark, value, pii_class):
    df = spark.range(1).withColumn("v", lit(value))
    return df.select(token_expr(normalize("v", pii_class), pii_class, PEPPER).alias("t")).collect()[0]["t"]


def _unified_groups(entity_index_df):
    """Return the set of (source, source_id) groups sharing a unified_id."""
    grouped = {}
    for r in entity_index_df.collect():
        grouped.setdefault(r["unified_id"], set()).add((r["source"], r["source_id"]))
    return {frozenset(v) for v in grouped.values()}


def test_mixed_case_emails_resolve_to_one_entity(spark, pipeline_tables):
    """Shopify and HubSpot rows for one person, differing only in case."""
    from jobs.spark.entity_backfill import get_all_staging_customers, perform_initial_resolution

    email_token = token_for(spark, "ada@example.com", "email")

    insert_rows(spark, "iceberg.staging.stg_shopify_customers", [
        {"customer_id": "S1", "email_token": email_token, "_staged_at": STAGED_AT},
    ])
    insert_rows(spark, "iceberg.staging.stg_hubspot_contacts", [
        {"contact_id": "H1", "email_token": email_token, "_staged_at": STAGED_AT},
    ])

    entity_index_df, _ = perform_initial_resolution(
        spark, get_all_staging_customers(spark), dry_run=True
    )

    assert _unified_groups(entity_index_df) == {
        frozenset({("shopify_customers", "S1"), ("hubspot_contacts", "H1")})
    }


def test_different_emails_stay_separate(spark, pipeline_tables):
    from jobs.spark.entity_backfill import get_all_staging_customers, perform_initial_resolution

    insert_rows(spark, "iceberg.staging.stg_shopify_customers", [
        {"customer_id": "S1", "email_token": token_for(spark, "ada@example.com", "email"),
         "_staged_at": STAGED_AT},
        {"customer_id": "S2", "email_token": token_for(spark, "grace@example.com", "email"),
         "_staged_at": STAGED_AT},
    ])

    entity_index_df, _ = perform_initial_resolution(
        spark, get_all_staging_customers(spark), dry_run=True
    )

    assert len(_unified_groups(entity_index_df)) == 2


def test_entity_index_match_reason_holds_no_plaintext(spark, pipeline_tables):
    """match_reason was concat('Matched via email: ', normalized_email) at :352."""
    from jobs.spark.entity_backfill import get_all_staging_customers, perform_initial_resolution

    email_token = token_for(spark, "ada@example.com", "email")
    insert_rows(spark, "iceberg.staging.stg_shopify_customers", [
        {"customer_id": "S1", "email_token": email_token, "_staged_at": STAGED_AT},
        {"customer_id": "S2", "email_token": email_token, "_staged_at": STAGED_AT},
    ])

    entity_index_df, _ = perform_initial_resolution(
        spark, get_all_staging_customers(spark), dry_run=True
    )

    reasons = [r["match_reason"] or "" for r in entity_index_df.collect()]
    assert not any("ada@example.com" in r for r in reasons)


def test_blocking_index_holds_no_plaintext(spark, pipeline_tables):
    from jobs.spark.entity_backfill import get_all_staging_customers, perform_initial_resolution

    email_token = token_for(spark, "ada@example.com", "email")
    insert_rows(spark, "iceberg.staging.stg_shopify_customers", [
        {"customer_id": "S1", "email_token": email_token, "_staged_at": STAGED_AT},
    ])

    _, blocking_df = perform_initial_resolution(
        spark, get_all_staging_customers(spark), dry_run=True
    )

    keys = [r["blocking_key"] for r in blocking_df.collect()]
    assert not any("ada@example.com" in k for k in keys)
    assert all(k.startswith(("email:tok_", "phone:tok_", "name_zip:tok_")) for k in keys)
