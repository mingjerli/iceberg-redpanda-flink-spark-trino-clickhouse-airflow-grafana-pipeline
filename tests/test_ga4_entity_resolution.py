"""
Tests for GA4 entity resolution integration (TDD).

Verifies that GA4 users are included in entity resolution and
matched to existing entities via user_id (email).
"""
import pytest
from pyspark.sql import SparkSession
from datetime import datetime


def test_ga4_included_in_get_all_staging_customers(spark):
    """
    Test that get_all_staging_customers includes GA4 users.

    Expected: GA4 users with user_id are included in staging customer union.
    """
    # Create stg_ga4_sessions table with sample data
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.staging.stg_ga4_sessions (
            session_id STRING,
            client_id STRING,
            user_id STRING,
            session_start TIMESTAMP,
            session_end TIMESTAMP,
            traffic_source STRING,
            device_category STRING,
            geo_country STRING,
            _staged_at TIMESTAMP
        ) USING iceberg
    """)

    # Insert test data with user_id (email for entity resolution)
    test_sessions = [
        {
            "session_id": "session_001",
            "client_id": "1234567890.1234567890",
            "user_id": "customer1@example.com",  # Will match to entity
            "session_start": datetime(2026, 2, 1, 10, 0, 0),
            "session_end": datetime(2026, 2, 1, 10, 15, 0),
            "traffic_source": "google",
            "device_category": "desktop",
            "geo_country": "United States",
            "_staged_at": datetime(2026, 2, 1, 11, 0, 0)
        },
        {
            "session_id": "session_002",
            "client_id": "9876543210.9876543210",
            "user_id": "customer2@example.com",
            "session_start": datetime(2026, 2, 1, 11, 0, 0),
            "session_end": datetime(2026, 2, 1, 11, 20, 0),
            "traffic_source": "direct",
            "device_category": "mobile",
            "geo_country": "Canada",
            "_staged_at": datetime(2026, 2, 1, 12, 0, 0)
        },
        {
            "session_id": "session_003",
            "client_id": "5555555555.5555555555",
            "user_id": None,  # Anonymous user - should be excluded
            "session_start": datetime(2026, 2, 1, 12, 0, 0),
            "session_end": datetime(2026, 2, 1, 12, 10, 0),
            "traffic_source": "facebook",
            "device_category": "tablet",
            "geo_country": "United Kingdom",
            "_staged_at": datetime(2026, 2, 1, 13, 0, 0)
        }
    ]

    import pandas as pd
    df = spark.createDataFrame(pd.DataFrame(test_sessions))
    df.writeTo("iceberg.staging.stg_ga4_sessions").using("iceberg").append()

    # Call get_all_staging_customers (should include GA4)
    from jobs.spark.entity_backfill import get_all_staging_customers

    all_customers = get_all_staging_customers(spark)

    # Filter to GA4 only
    ga4_customers = all_customers.filter(col("source") == "ga4_sessions").collect()

    # CRITICAL ASSERTION: Should have 2 GA4 customers (excluding anonymous)
    assert len(ga4_customers) == 2, \
        f"Expected 2 GA4 customers with user_id, got {len(ga4_customers)}"

    # Verify GA4 customer structure
    ga4_customer_1 = ga4_customers[0]
    assert ga4_customer_1["email"] == "customer1@example.com", "Email should match user_id"
    assert ga4_customer_1["source"] == "ga4_sessions"
    assert ga4_customer_1["source_id"] is not None

    # Cleanup
    spark.sql("DROP TABLE IF EXISTS iceberg.staging.stg_ga4_sessions")


def test_ga4_entity_resolution_via_email(spark):
    """
    Test that GA4 users are matched to existing entities via email.

    Scenario:
    1. Existing Shopify customer with email: customer@example.com
    2. GA4 session with user_id: customer@example.com
    3. Entity resolution should link them to same unified_id
    """
    # Create staging tables
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.staging.stg_shopify_customers (
            customer_id STRING,
            email STRING,
            first_name STRING,
            last_name STRING,
            full_name STRING,
            phone STRING,
            address_line1 STRING,
            city STRING,
            province STRING,
            zip STRING,
            country STRING,
            created_at TIMESTAMP,
            _staged_at TIMESTAMP
        ) USING iceberg
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.staging.stg_ga4_sessions (
            session_id STRING,
            client_id STRING,
            user_id STRING,
            session_start TIMESTAMP,
            session_end TIMESTAMP,
            _staged_at TIMESTAMP
        ) USING iceberg
    """)

    # Insert Shopify customer
    shopify_data = [{
        "customer_id": "shopify_123",
        "email": "customer@example.com",
        "first_name": "John",
        "last_name": "Doe",
        "full_name": "John Doe",
        "phone": "555-1234",
        "address_line1": "123 Main St",
        "city": "New York",
        "province": "NY",
        "zip": "10001",
        "country": "US",
        "created_at": datetime(2026, 1, 1, 10, 0, 0),
        "_staged_at": datetime(2026, 1, 1, 11, 0, 0)
    }]

    import pandas as pd
    shopify_df = spark.createDataFrame(pd.DataFrame(shopify_data))
    shopify_df.writeTo("iceberg.staging.stg_shopify_customers").using("iceberg").append()

    # Insert GA4 session with matching email
    ga4_data = [{
        "session_id": "session_ga4_001",
        "client_id": "9999999999.9999999999",
        "user_id": "customer@example.com",  # Same email as Shopify
        "session_start": datetime(2026, 2, 1, 14, 0, 0),
        "session_end": datetime(2026, 2, 1, 14, 30, 0),
        "_staged_at": datetime(2026, 2, 1, 15, 0, 0)
    }]

    ga4_df = spark.createDataFrame(pd.DataFrame(ga4_data))
    ga4_df.writeTo("iceberg.staging.stg_ga4_sessions").using("iceberg").append()

    # Run entity resolution
    from jobs.spark.entity_backfill import get_all_staging_customers, perform_initial_resolution

    staging_data = get_all_staging_customers(spark)
    entity_index_df, _ = perform_initial_resolution(spark, staging_data, dry_run=True)

    # Get unified_ids for both sources
    entities = entity_index_df.collect()

    shopify_entity = [e for e in entities if e["source"] == "shopify_customers"][0]
    ga4_entity = [e for e in entities if e["source"] == "ga4_sessions"][0]

    # CRITICAL ASSERTION: Both should have same unified_id (matched by email)
    assert shopify_entity["unified_id"] == ga4_entity["unified_id"], \
        f"Shopify and GA4 should be linked to same unified_id. " \
        f"Shopify: {shopify_entity['unified_id']}, GA4: {ga4_entity['unified_id']}"

    # Verify match type
    assert ga4_entity["match_type"] == "exact_email", \
        "GA4 should be matched via exact_email"

    # Cleanup
    spark.sql("DROP TABLE IF EXISTS iceberg.staging.stg_shopify_customers")
    spark.sql("DROP TABLE IF EXISTS iceberg.staging.stg_ga4_sessions")


def test_ga4_anonymous_users_excluded(spark):
    """
    Test that GA4 sessions without user_id are excluded from entity resolution.

    GA4 sessions with user_id=NULL represent anonymous visitors and should
    NOT be included in customer entity resolution.
    """
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.staging.stg_ga4_sessions (
            session_id STRING,
            client_id STRING,
            user_id STRING,
            _staged_at TIMESTAMP
        ) USING iceberg
    """)

    # Insert mix of logged-in and anonymous sessions
    sessions = [
        {"session_id": "s1", "client_id": "c1", "user_id": "user@example.com", "_staged_at": datetime.now()},
        {"session_id": "s2", "client_id": "c2", "user_id": None, "_staged_at": datetime.now()},
        {"session_id": "s3", "client_id": "c3", "user_id": "", "_staged_at": datetime.now()},
        {"session_id": "s4", "client_id": "c4", "user_id": "another@example.com", "_staged_at": datetime.now()},
    ]

    import pandas as pd
    df = spark.createDataFrame(pd.DataFrame(sessions))
    df.writeTo("iceberg.staging.stg_ga4_sessions").using("iceberg").append()

    # Get staging customers
    from jobs.spark.entity_backfill import get_all_staging_customers
    all_customers = get_all_staging_customers(spark)

    ga4_customers = all_customers.filter(col("source") == "ga4_sessions").collect()

    # Should only include 2 sessions (with valid user_id)
    assert len(ga4_customers) == 2, \
        f"Expected 2 GA4 customers (excluding anonymous), got {len(ga4_customers)}"

    # Verify emails
    emails = sorted([c["email"] for c in ga4_customers])
    assert emails == ["another@example.com", "user@example.com"], \
        f"Expected specific emails, got {emails}"

    # Cleanup
    spark.sql("DROP TABLE IF EXISTS iceberg.staging.stg_ga4_sessions")


def test_ga4_blocking_index_no_row_explosion(spark):
    """
    CRITICAL TEST: Verify blocking index doesn't explode with multi-session clients.

    Scenario: Client has 10 sessions → should produce 1 blocking key, not 10.
    This tests the CRITICAL FIX for cardinality issue.
    """
    from datetime import timedelta
    import pandas as pd

    # Create necessary tables
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.semantic.entity_index (
            unified_id STRING, entity_type STRING, source STRING, source_id STRING,
            match_type STRING, match_confidence DECIMAL(3, 2), match_reason STRING,
            linked_to_unified_id STRING, matched_at TIMESTAMP, matched_by STRING,
            _staged_at TIMESTAMP
        ) USING iceberg
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.semantic.blocking_index (
            blocking_key STRING, blocking_key_type STRING, unified_id STRING,
            entity_type STRING, source STRING, source_id STRING, key_value STRING,
            is_primary BOOLEAN, created_at TIMESTAMP, expires_at TIMESTAMP
        ) USING iceberg
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.staging.stg_ga4_sessions (
            session_id STRING, client_id STRING, user_id STRING, geo_country STRING,
            session_start TIMESTAMP, session_end TIMESTAMP, _staged_at TIMESTAMP
        ) USING iceberg
    """)

    # Insert 1 entity with 10 sessions (same client_id)
    base_time = datetime(2026, 2, 1, 10, 0, 0)
    sessions = []
    for i in range(10):
        sessions.append({
            "session_id": f"session_{i}",
            "client_id": "multi_session_client_123",
            "user_id": "multi_user@example.com",
            "geo_country": "United States",
            "session_start": base_time + timedelta(hours=i),
            "session_end": base_time + timedelta(hours=i, minutes=30),
            "_staged_at": base_time + timedelta(hours=i, minutes=35)
        })

    sessions_df = spark.createDataFrame(pd.DataFrame(sessions))
    sessions_df.writeTo("iceberg.staging.stg_ga4_sessions").using("iceberg").append()

    # Insert entity into entity_index
    entity_data = [{
        "unified_id": "test_unified_id_123", "entity_type": "customer",
        "source": "ga4_sessions", "source_id": "multi_session_client_123",
        "match_type": "exact_email", "match_confidence": 1.0,
        "match_reason": "email match", "linked_to_unified_id": None,
        "matched_at": datetime.now(), "matched_by": "test", "_staged_at": datetime.now()
    }]

    entity_df = spark.createDataFrame(pd.DataFrame(entity_data))
    entity_df.writeTo("iceberg.semantic.entity_index").using("iceberg").append()

    # Run rebuild_blocking_index
    from jobs.spark.entity_backfill import rebuild_blocking_index
    rebuild_blocking_index(spark, dry_run=False)

    # CRITICAL ASSERTION: Should have exactly 1 blocking key for this email
    blocking_df = spark.sql("""
        SELECT * FROM iceberg.semantic.blocking_index
        WHERE key_value = 'multi_user@example.com' AND blocking_key_type = 'email'
    """)

    row_count = blocking_df.count()
    assert row_count == 1, f"CRITICAL: Expected 1 blocking key, got {row_count}. Row explosion detected!"

    # Cleanup
    spark.sql("DROP TABLE IF EXISTS iceberg.semantic.entity_index")
    spark.sql("DROP TABLE IF EXISTS iceberg.semantic.blocking_index")
    spark.sql("DROP TABLE IF EXISTS iceberg.staging.stg_ga4_sessions")


def test_ga4_latest_session_wins(spark):
    """Test that latest session is selected when client has multiple sessions."""
    import pandas as pd

    # Create tables  
    spark.sql("""CREATE TABLE IF NOT EXISTS iceberg.semantic.entity_index (
        unified_id STRING, entity_type STRING, source STRING, source_id STRING,
        match_type STRING, match_confidence DECIMAL(3, 2), match_reason STRING,
        linked_to_unified_id STRING, matched_at TIMESTAMP, matched_by STRING,
        _staged_at TIMESTAMP) USING iceberg""")

    spark.sql("""CREATE TABLE IF NOT EXISTS iceberg.semantic.blocking_index (
        blocking_key STRING, blocking_key_type STRING, unified_id STRING, entity_type STRING,
        source STRING, source_id STRING, key_value STRING, is_primary BOOLEAN,
        created_at TIMESTAMP, expires_at TIMESTAMP) USING iceberg""")

    spark.sql("""CREATE TABLE IF NOT EXISTS iceberg.staging.stg_ga4_sessions (
        session_id STRING, client_id STRING, user_id STRING, geo_country STRING,
        session_start TIMESTAMP, _staged_at TIMESTAMP) USING iceberg""")

    # Insert 2 sessions: older (US) and newer (Canada)
    sessions = [
        {"session_id": "old_session", "client_id": "geo_test_client",
         "user_id": "geo_user@example.com", "geo_country": "United States",
         "session_start": datetime(2026, 1, 1, 10, 0, 0), "_staged_at": datetime(2026, 1, 1, 11, 0, 0)},
        {"session_id": "new_session", "client_id": "geo_test_client",
         "user_id": "geo_user@example.com", "geo_country": "Canada",
         "session_start": datetime(2026, 2, 1, 10, 0, 0), "_staged_at": datetime(2026, 2, 1, 11, 0, 0)}
    ]

    sessions_df = spark.createDataFrame(pd.DataFrame(sessions))
    sessions_df.writeTo("iceberg.staging.stg_ga4_sessions").using("iceberg").append()

    # Insert entity
    entity_data = [{"unified_id": "geo_unified_id", "entity_type": "customer",
                   "source": "ga4_sessions", "source_id": "geo_test_client",
                   "match_type": "exact_email", "match_confidence": 1.0,
                   "match_reason": "email match", "linked_to_unified_id": None,
                   "matched_at": datetime.now(), "matched_by": "test", "_staged_at": datetime.now()}]

    entity_df = spark.createDataFrame(pd.DataFrame(entity_data))
    entity_df.writeTo("iceberg.semantic.entity_index").using("iceberg").append()

    # Run rebuild_blocking_index
    from jobs.spark.entity_backfill import rebuild_blocking_index
    rebuild_blocking_index(spark, dry_run=False)

    blocking_df = spark.sql("""SELECT * FROM iceberg.semantic.blocking_index
        WHERE key_value = 'geo_user@example.com' AND blocking_key_type = 'email'""")

    row_count = blocking_df.count()
    assert row_count == 1, f"Expected 1 row, got {row_count}"

    # Cleanup
    spark.sql("DROP TABLE IF EXISTS iceberg.semantic.entity_index")
    spark.sql("DROP TABLE IF EXISTS iceberg.semantic.blocking_index")
    spark.sql("DROP TABLE IF EXISTS iceberg.staging.stg_ga4_sessions")
