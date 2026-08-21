"""
Tests for GA4 entity resolution integration (TDD).

Verifies that GA4 users are included in entity resolution and
matched to existing entities via user_id_token (email).

Staging tables carry PII as *_token columns (jobs/spark/pii/tokenize.py), so
every value inserted here is a token computed the same way staging_batch.py
computes it -- not the plaintext email/phone/name a human would type.
"""
from datetime import datetime, timedelta

from pyspark.sql.functions import col, lit

from pii.tokenize import normalize, token_expr
from tests.pipeline_tables import insert_rows

STG_GA4_SESSIONS = "iceberg.staging.stg_ga4_sessions"
STG_SHOPIFY_CUSTOMERS = "iceberg.staging.stg_shopify_customers"
ENTITY_INDEX = "iceberg.semantic.entity_index"
BLOCKING_INDEX = "iceberg.semantic.blocking_index"

PEPPER = "test-pepper-do-not-use-in-production"


def token_for(spark, value, pii_class):
    """Compute the token staging_batch.py would produce for a plaintext value."""
    df = spark.range(1).withColumn("v", lit(value))
    return df.select(token_expr(normalize("v", pii_class), pii_class, PEPPER).alias("t")).collect()[0]["t"]


def test_ga4_included_in_get_all_staging_customers(spark, pipeline_tables):
    """
    Test that get_all_staging_customers includes GA4 users.

    Expected: GA4 users with user_id_token are included in staging customer union.
    """
    token1 = token_for(spark, "customer1@example.com", "email")
    token2 = token_for(spark, "customer2@example.com", "email")

    insert_rows(spark, STG_GA4_SESSIONS, [
        {
            "session_id": "session_001",
            "client_id": "1234567890.1234567890",
            "user_id_token": token1,  # Will match to entity
            "session_start": datetime(2026, 2, 1, 10, 0, 0),
            "session_end": datetime(2026, 2, 1, 10, 15, 0),
            "traffic_source": "google",
            "device_category": "desktop",
            "geo_country": "United States",
            "_staged_at": datetime(2026, 2, 1, 11, 0, 0),
        },
        {
            "session_id": "session_002",
            "client_id": "9876543210.9876543210",
            "user_id_token": token2,
            "session_start": datetime(2026, 2, 1, 11, 0, 0),
            "session_end": datetime(2026, 2, 1, 11, 20, 0),
            "traffic_source": "direct",
            "device_category": "mobile",
            "geo_country": "Canada",
            "_staged_at": datetime(2026, 2, 1, 12, 0, 0),
        },
        {
            "session_id": "session_003",
            "client_id": "5555555555.5555555555",
            "user_id_token": None,  # Anonymous user - should be excluded
            "session_start": datetime(2026, 2, 1, 12, 0, 0),
            "session_end": datetime(2026, 2, 1, 12, 10, 0),
            "traffic_source": "facebook",
            "device_category": "tablet",
            "geo_country": "United Kingdom",
            "_staged_at": datetime(2026, 2, 1, 13, 0, 0),
        },
    ])

    from jobs.spark.entity_backfill import get_all_staging_customers

    all_customers = get_all_staging_customers(spark)
    ga4_customers = all_customers.filter(col("source") == "ga4_sessions").collect()

    # CRITICAL ASSERTION: Should have 2 GA4 customers (excluding anonymous)
    assert len(ga4_customers) == 2, \
        f"Expected 2 GA4 customers with user_id_token, got {len(ga4_customers)}"

    tokens_by_source_id = {c["source_id"]: c["email_token"] for c in ga4_customers}
    assert tokens_by_source_id["1234567890.1234567890"] == token1, \
        "email_token should match user_id_token"
    assert tokens_by_source_id["9876543210.9876543210"] == token2


def test_ga4_entity_resolution_via_email(spark, pipeline_tables):
    """
    Test that GA4 users are matched to existing entities via email token.

    Scenario:
    1. Existing Shopify customer with email: customer@example.com
    2. GA4 session with user_id: customer@example.com
    3. Entity resolution should link them to same unified_id
    """
    email_token = token_for(spark, "customer@example.com", "email")

    insert_rows(spark, STG_SHOPIFY_CUSTOMERS, [{
        "customer_id": "shopify_123",
        "email_token": email_token,
        "first_name_token": token_for(spark, "John", "name"),
        "last_name_token": token_for(spark, "Doe", "name"),
        "full_name_token": token_for(spark, "John Doe", "name"),
        "phone_token": token_for(spark, "555-1234", "phone"),
        "address_line1_token": token_for(spark, "123 Main St", "address"),
        "last_name_prefix_token": token_for(spark, "Doe", "name_prefix"),
        "city": "New York",
        "province": "NY",
        "zip": "10001",
        "country": "US",
        "created_at": datetime(2026, 1, 1, 10, 0, 0),
        "_staged_at": datetime(2026, 1, 1, 11, 0, 0),
    }])

    insert_rows(spark, STG_GA4_SESSIONS, [{
        "session_id": "session_ga4_001",
        "client_id": "9999999999.9999999999",
        "user_id_token": email_token,  # Same email token as Shopify
        "session_start": datetime(2026, 2, 1, 14, 0, 0),
        "session_end": datetime(2026, 2, 1, 14, 30, 0),
        "_staged_at": datetime(2026, 2, 1, 15, 0, 0),
    }])

    from jobs.spark.entity_backfill import get_all_staging_customers, perform_initial_resolution

    staging_data = get_all_staging_customers(spark)
    entity_index_df, _ = perform_initial_resolution(spark, staging_data, dry_run=True)

    entities = entity_index_df.collect()

    shopify_entity = [e for e in entities if e["source"] == "shopify_customers"][0]
    ga4_entity = [e for e in entities if e["source"] == "ga4_sessions"][0]

    # CRITICAL ASSERTION: Both should have same unified_id (matched by email token)
    assert shopify_entity["unified_id"] == ga4_entity["unified_id"], \
        f"Shopify and GA4 should be linked to same unified_id. " \
        f"Shopify: {shopify_entity['unified_id']}, GA4: {ga4_entity['unified_id']}"

    assert ga4_entity["match_type"] == "exact_email", \
        "GA4 should be matched via exact_email"


def test_ga4_anonymous_users_excluded(spark, pipeline_tables):
    """
    Test that GA4 sessions without user_id_token are excluded from entity resolution.

    GA4 sessions with user_id_token=NULL represent anonymous visitors and should
    NOT be included in customer entity resolution.
    """
    now = datetime.now()
    token_user = token_for(spark, "user@example.com", "email")
    token_another = token_for(spark, "another@example.com", "email")

    insert_rows(spark, STG_GA4_SESSIONS, [
        {"session_id": "s1", "client_id": "c1", "user_id_token": token_user, "_staged_at": now},
        {"session_id": "s2", "client_id": "c2", "user_id_token": None, "_staged_at": now},
        {"session_id": "s3", "client_id": "c3", "user_id_token": "", "_staged_at": now},
        {"session_id": "s4", "client_id": "c4", "user_id_token": token_another, "_staged_at": now},
    ])

    from jobs.spark.entity_backfill import get_all_staging_customers
    all_customers = get_all_staging_customers(spark)

    ga4_customers = all_customers.filter(col("source") == "ga4_sessions").collect()

    # Should only include 2 sessions (with valid user_id_token)
    assert len(ga4_customers) == 2, \
        f"Expected 2 GA4 customers (excluding anonymous), got {len(ga4_customers)}"

    tokens = sorted([c["email_token"] for c in ga4_customers])
    assert tokens == sorted([token_another, token_user]), \
        f"Expected specific tokens, got {tokens}"


def test_ga4_blocking_index_no_row_explosion(spark, pipeline_tables):
    """
    CRITICAL TEST: Verify blocking index doesn't explode with multi-session clients.

    Scenario: Client has 10 sessions → should produce 1 blocking key, not 10.
    This tests the CRITICAL FIX for cardinality issue.
    """
    base_time = datetime(2026, 2, 1, 10, 0, 0)
    email_token = token_for(spark, "multi_user@example.com", "email")

    insert_rows(spark, STG_GA4_SESSIONS, [
        {
            "session_id": f"session_{i}",
            "client_id": "multi_session_client_123",
            "user_id_token": email_token,
            "geo_country": "United States",
            "session_start": base_time + timedelta(hours=i),
            "session_end": base_time + timedelta(hours=i, minutes=30),
            "_staged_at": base_time + timedelta(hours=i, minutes=35),
        }
        for i in range(10)
    ])

    insert_rows(spark, ENTITY_INDEX, [{
        "unified_id": "test_unified_id_123",
        "entity_type": "customer",
        "source": "ga4_sessions",
        "source_id": "multi_session_client_123",
        "match_type": "exact_email",
        "match_confidence": 1.0,
        "match_reason": "email match",
        "linked_to_unified_id": None,
        "matched_at": datetime.now(),
        "matched_by": "test",
        "_staged_at": datetime.now(),
    }])

    from jobs.spark.entity_backfill import rebuild_blocking_index
    rebuild_blocking_index(spark, dry_run=False)

    # CRITICAL ASSERTION: Should have exactly 1 blocking key for this email token
    row_count = spark.sql(f"""
        SELECT * FROM {BLOCKING_INDEX}
        WHERE key_value = '{email_token}' AND blocking_key_type = 'email'
    """).count()

    assert row_count == 1, \
        f"CRITICAL: Expected 1 blocking key, got {row_count}. Row explosion detected!"


def test_ga4_latest_session_wins(spark, pipeline_tables):
    """Test that latest session is selected when client has multiple sessions."""
    email_token = token_for(spark, "geo_user@example.com", "email")

    insert_rows(spark, STG_GA4_SESSIONS, [
        {"session_id": "old_session", "client_id": "geo_test_client",
         "user_id_token": email_token, "geo_country": "United States",
         "session_start": datetime(2026, 1, 1, 10, 0, 0),
         "_staged_at": datetime(2026, 1, 1, 11, 0, 0)},
        {"session_id": "new_session", "client_id": "geo_test_client",
         "user_id_token": email_token, "geo_country": "Canada",
         "session_start": datetime(2026, 2, 1, 10, 0, 0),
         "_staged_at": datetime(2026, 2, 1, 11, 0, 0)},
    ])

    insert_rows(spark, ENTITY_INDEX, [{
        "unified_id": "geo_unified_id",
        "entity_type": "customer",
        "source": "ga4_sessions",
        "source_id": "geo_test_client",
        "match_type": "exact_email",
        "match_confidence": 1.0,
        "match_reason": "email match",
        "linked_to_unified_id": None,
        "matched_at": datetime.now(),
        "matched_by": "test",
        "_staged_at": datetime.now(),
    }])

    from jobs.spark.entity_backfill import rebuild_blocking_index
    rebuild_blocking_index(spark, dry_run=False)

    blocking_rows = spark.sql(f"""
        SELECT * FROM {BLOCKING_INDEX}
        WHERE key_value = '{email_token}' AND blocking_key_type = 'email'
    """).collect()

    assert len(blocking_rows) == 1, f"Expected 1 row, got {len(blocking_rows)}"
