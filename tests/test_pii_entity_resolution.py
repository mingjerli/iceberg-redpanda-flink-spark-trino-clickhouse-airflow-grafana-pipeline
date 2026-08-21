"""
Golden equivalence for token-based entity resolution.

This is the test that matters most in the PII work. A normalizer that drifts
from entity_backfill.py degrades match quality invisibly, because an unmatched
record is not an error -- it just becomes a separate customer. Asserting that
token-based grouping equals plaintext-based grouping is what catches the drift.

Also covers two related failure modes that are just as silent: tokenizing the
same identifier twice on its way through a pipeline (GA4 user_id, tokenized in
stage_ga4_events and carried through compute_ga4_sessions), and a plaintext
identifier surviving in an unregistered column even after its registered
sibling is tokenized (Mailchimp subscriber_id vs. _raw_id).
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
    """Shopify and HubSpot rows for one person, written with different case
    and surrounding whitespace -- both must normalize to the same token."""
    from jobs.spark.entity_backfill import get_all_staging_customers, perform_initial_resolution

    insert_rows(spark, "iceberg.staging.stg_shopify_customers", [
        {"customer_id": "S1", "email_token": token_for(spark, "Ada@Example.COM ", "email"),
         "_staged_at": STAGED_AT},
    ])
    insert_rows(spark, "iceberg.staging.stg_hubspot_contacts", [
        {"contact_id": "H1", "email_token": token_for(spark, "ada@example.com", "email"),
         "_staged_at": STAGED_AT},
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


def test_phone_formatting_variants_share_one_blocking_key(spark, pipeline_tables):
    """Differently formatted phone numbers for the same number must normalize
    to one token, producing one shared phone: blocking key."""
    from jobs.spark.entity_backfill import get_all_staging_customers, perform_initial_resolution

    insert_rows(spark, "iceberg.staging.stg_shopify_customers", [
        {"customer_id": "S1", "phone_token": token_for(spark, "+1 (555) 123-4567", "phone"),
         "_staged_at": STAGED_AT},
    ])
    insert_rows(spark, "iceberg.staging.stg_hubspot_contacts", [
        {"contact_id": "H1", "phone_token": token_for(spark, "+15551234567", "phone"),
         "_staged_at": STAGED_AT},
    ])

    _, blocking_df = perform_initial_resolution(
        spark, get_all_staging_customers(spark), dry_run=True
    )

    phone_rows = [r for r in blocking_df.collect() if r["blocking_key_type"] == "phone"]
    assert len(phone_rows) == 2, f"Expected one phone blocking row per source, got {len(phone_rows)}"

    phone_keys = {r["blocking_key"] for r in phone_rows}
    assert len(phone_keys) == 1, \
        f"+1 (555) 123-4567 and +15551234567 must normalize to one shared blocking key, got {phone_keys}"


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
    assert len(reasons) == 2, f"Expected 2 entity_index rows, got {len(reasons)}"
    assert any(email_token in r for r in reasons), \
        "Expected the exact_email match to carry the token in match_reason"
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
    assert keys == [f"email:{email_token}"], \
        f"Expected exactly one email blocking key (no phone/name_zip set), got {keys}"
    assert not any("ada@example.com" in k for k in keys)
    assert all(k.startswith(("email:tok_", "phone:tok_", "name_zip:tok_")) for k in keys)


def test_ga4_user_id_tokenized_exactly_once(spark, pipeline_tables):
    """
    compute_ga4_sessions derives stg_ga4_sessions from stg_ga4_events. user_id
    must be tokenized exactly once, in stage_ga4_events; compute_ga4_sessions
    carries the resulting token straight through. Re-tokenizing it downstream
    would hash an already-tokenized value -- token(token(email)) -- silently
    breaking every GA4 cross-source match.
    """
    from jobs.spark.staging_batch import stage_ga4_events, compute_ga4_sessions

    email = "session_user@example.com"
    expected_token = token_for(spark, email, "email")

    insert_rows(spark, "iceberg.raw.ga4_events", [{
        "_raw_id": "evt_1",
        "client_id": "client_1",
        "user_id": email,
        "event_name": "page_view",
        "event_timestamp": 1_700_000_000_000_000,
        "event_date": "20231114",
        "user_properties": "[]",
        "_loaded_at": STAGED_AT,
        "_source_file": "test.parquet",
    }])

    stage_ga4_events(spark, mode="full")
    events_token = spark.table("iceberg.staging.stg_ga4_events") \
        .select("user_id_token").collect()[0]["user_id_token"]
    assert events_token == expected_token, \
        "stage_ga4_events should tokenize user_id to the independently computed email token"

    compute_ga4_sessions(spark, mode="full")
    session_token = spark.table("iceberg.staging.stg_ga4_sessions") \
        .select("user_id_token").collect()[0]["user_id_token"]

    assert session_token == expected_token, \
        "user_id_token must survive the events->sessions hop unchanged"
    assert session_token != token_for(spark, expected_token, "email"), \
        "session token must not be a second hash of the events token (double tokenization)"


def test_mailchimp_subscriber_raw_id_holds_token_not_plaintext(spark, pipeline_tables):
    """
    _raw_id used to copy the plaintext subscriber_id (MD5(lower(email)),
    itself PII per pii/registry.py) even though tokenize_frame replaces the
    subscriber_id column with subscriber_id_token in the same row.
    staging_batch.py now sets _raw_id from the token so no plaintext
    identifier survives.

    stg_mailchimp_subscribers is also declared (narrower) in
    pipeline_tables.STAGING_TABLE_DDL for entity-resolution tests, so it must
    be dropped first -- the same reason JOB_MANAGED_TABLES exists for
    stg_ga4_events: this test needs the table that stage_mailchimp_subscribers
    itself creates, with its full production schema, not the pre-created one.
    """
    from jobs.spark.staging_batch import stage_mailchimp_subscribers

    spark.sql("DROP TABLE IF EXISTS iceberg.staging.stg_mailchimp_subscribers")

    subscriber_id = "abc123subscriberid"
    expected_token = token_for(spark, subscriber_id, "mailchimp_id")

    insert_rows(spark, "iceberg.raw.mailchimp_subscribers", [{
        "subscriber_id": subscriber_id,
        "email_address": "mailchimp_user@example.com",
        "status": "subscribed",
        "merge_fields": '{"FNAME": "Ada", "LNAME": "Lovelace"}',
        "stats": "{}",
        "timestamp_signup": STAGED_AT,
        "_loaded_at": STAGED_AT,
    }])

    stage_mailchimp_subscribers(spark, mode="full")

    row = spark.table("iceberg.staging.stg_mailchimp_subscribers").collect()[0]

    assert row["_raw_id"] == expected_token, \
        "_raw_id should carry the subscriber_id token, not the plaintext MD5 hash"
    assert row["_raw_id"] == row["subscriber_id_token"]
    assert row["_raw_id"] != subscriber_id
