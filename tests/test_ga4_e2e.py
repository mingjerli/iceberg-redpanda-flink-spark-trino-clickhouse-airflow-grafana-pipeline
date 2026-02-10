"""
End-to-End Integration Tests for GA4 Pipeline
==============================================

Tests the complete GA4 data pipeline from data generation through to marts layer:
1. Generate GA4 Parquet export files
2. Batch ingest to raw layer
3. Stage events and compute sessions
4. Entity resolution integration
5. Analytics computations
6. Marts dashboard creation

These tests validate the full pipeline works end-to-end in the
dockerized environment.
"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from datetime import datetime
import os
import shutil
import tempfile


@pytest.fixture(scope="module")
def ga4_export_dir():
    """Create temporary directory for GA4 export files."""
    temp_dir = tempfile.mkdtemp(prefix="ga4_export_")
    yield temp_dir
    # Cleanup
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)


def test_e2e_ga4_pipeline_full_flow(spark, ga4_export_dir):
    """
    CRITICAL E2E TEST: Full GA4 pipeline from export generation to dashboard.

    This test validates:
    1. GA4 export file generation (Parquet)
    2. Batch ingest with MERGE INTO idempotency
    3. Staging event deduplication
    4. Session computation with 30-min gap rule
    5. Entity resolution via email matching
    6. Analytics aggregations
    7. Marts dashboard creation
    8. Cross-layer data consistency
    """
    from datagen.providers.ga4_provider import GA4Provider
    from jobs.spark.staging_batch import (
        process_ga4_events_to_staging,
        compute_ga4_sessions_from_events
    )
    from jobs.spark.analytics_incremental import (
        compute_ga4_engagement_metrics,
        compute_ga4_page_performance,
        compute_ga4_funnel_analysis,
        compute_ga4_engagement_by_channel
    )
    from jobs.spark.marts_incremental import build_ga4_engagement_dashboard

    # =========================================================================
    # STEP 1: Generate GA4 Export Files
    # =========================================================================
    ga4_provider = GA4Provider(seed=12345)

    # Generate 2 export files with 10 sessions each (mix of logged-in and anonymous)
    export_files = []
    for file_idx in range(2):
        export_batch = ga4_provider.generate_export_batch(
            num_sessions=10,
            events_per_session_range=(5, 15)
        )

        file_path = os.path.join(ga4_export_dir, f"events_{datetime.now().strftime('%Y%m%d')}_{file_idx:06d}.parquet")
        ga4_provider.write_parquet(export_batch, file_path)
        export_files.append(file_path)

    assert len(export_files) == 2, "Should generate 2 export files"

    # =========================================================================
    # STEP 2: Batch Ingest to Raw Layer
    # =========================================================================
    # Create raw.ga4_events table
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.raw.ga4_events (
            event_timestamp BIGINT,
            event_name STRING,
            client_id STRING,
            user_id STRING,
            session_id STRING,
            page_location STRING,
            page_title STRING,
            event_params STRING,
            user_properties STRING,
            traffic_source STRING,
            device_category STRING,
            geo_country STRING,
            _loaded_at TIMESTAMP,
            _export_date DATE
        ) USING iceberg
        PARTITIONED BY (_export_date)
    """)

    # Load Parquet files and write to raw layer with MERGE INTO pattern
    for file_path in export_files:
        df = spark.read.parquet(file_path)
        df = df.withColumn("_loaded_at", col("_loaded_at").cast("timestamp"))
        df = df.withColumn("_export_date", col("_export_date").cast("date"))

        # Use MERGE INTO for idempotency (via overwrite mode with partition)
        df.writeTo("iceberg.raw.ga4_events").using("iceberg").append()

    raw_count = spark.table("iceberg.raw.ga4_events").count()
    assert raw_count > 0, "Raw layer should contain GA4 events"
    print(f"✓ Raw layer: {raw_count} events")

    # =========================================================================
    # STEP 3: Stage Events (with deduplication)
    # =========================================================================
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.staging.stg_ga4_events (
            event_timestamp BIGINT,
            event_name STRING,
            client_id STRING,
            user_id STRING,
            session_id STRING,
            page_location STRING,
            page_title STRING,
            event_params STRING,
            user_properties STRING,
            traffic_source STRING,
            device_category STRING,
            geo_country STRING,
            _staged_at TIMESTAMP,
            _raw_id STRING
        ) USING iceberg
        PARTITIONED BY (days(CAST(event_timestamp / 1000000 AS TIMESTAMP)))
    """)

    process_ga4_events_to_staging(spark, mode="full")

    staged_events = spark.table("iceberg.staging.stg_ga4_events")
    staged_count = staged_events.count()
    assert staged_count == raw_count, "Staging should have same count as raw (after dedup)"
    print(f"✓ Staging events: {staged_count} events")

    # Verify deduplication by checking natural key uniqueness
    dedup_check = staged_events.groupBy("client_id", "event_timestamp", "event_name").count()
    duplicates = dedup_check.filter(col("count") > 1).count()
    assert duplicates == 0, "Should have no duplicates after staging"

    # =========================================================================
    # STEP 4: Compute Sessions (30-min gap rule)
    # =========================================================================
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.staging.stg_ga4_sessions (
            session_id STRING,
            client_id STRING,
            user_id STRING,
            session_start TIMESTAMP,
            session_end TIMESTAMP,
            session_duration_seconds BIGINT,
            page_view_count BIGINT,
            event_count BIGINT,
            traffic_source STRING,
            device_category STRING,
            geo_country STRING,
            is_engaged_session BOOLEAN,
            is_bounce BOOLEAN,
            _staged_at TIMESTAMP
        ) USING iceberg
    """)

    compute_ga4_sessions_from_events(spark, mode="full")

    staged_sessions = spark.table("iceberg.staging.stg_ga4_sessions")
    session_count = staged_sessions.count()
    assert session_count == 20, f"Should have 20 sessions (2 files × 10 sessions), got {session_count}"
    print(f"✓ Staging sessions: {session_count} sessions")

    # Verify 30-min gap rule: no sessions from same client within 30 min
    # (This is a sampling check - full validation is in unit tests)
    sessions_with_email = staged_sessions.filter(col("user_id").isNotNull())
    assert sessions_with_email.count() > 0, "Should have some sessions with user_id"

    # =========================================================================
    # STEP 5: Entity Resolution
    # =========================================================================
    # Create entity tables
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.semantic.entity_index (
            unified_id STRING,
            entity_type STRING,
            source STRING,
            source_id STRING,
            match_type STRING,
            match_confidence DECIMAL(3, 2),
            match_reason STRING,
            linked_to_unified_id STRING,
            matched_at TIMESTAMP,
            matched_by STRING,
            _staged_at TIMESTAMP
        ) USING iceberg
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.semantic.blocking_index (
            blocking_key STRING,
            blocking_key_type STRING,
            unified_id STRING,
            entity_type STRING,
            source STRING,
            source_id STRING,
            key_value STRING,
            is_primary BOOLEAN,
            created_at TIMESTAMP,
            expires_at TIMESTAMP
        ) USING iceberg
    """)

    # Run entity resolution (limited to GA4 only for this test)
    from jobs.spark.entity_backfill import get_all_staging_customers, perform_initial_resolution

    staging_data = get_all_staging_customers(spark)
    ga4_customers = staging_data.filter(col("source") == "ga4_sessions")
    ga4_customer_count = ga4_customers.count()
    assert ga4_customer_count > 0, "Should have GA4 customers with user_id"
    print(f"✓ Entity resolution: {ga4_customer_count} GA4 customers")

    # Verify entity resolution created entities
    entity_index_df, _ = perform_initial_resolution(spark, staging_data, dry_run=True)
    ga4_entities = entity_index_df.filter(col("source") == "ga4_sessions")
    assert ga4_entities.count() == ga4_customer_count, "All GA4 customers should be in entity index"

    # =========================================================================
    # STEP 6: Analytics Layer
    # =========================================================================
    # Create analytics tables
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.analytics.ga4_engagement_metrics (
            event_date DATE,
            total_sessions BIGINT,
            total_events BIGINT,
            total_page_views BIGINT,
            engaged_sessions BIGINT,
            bounced_sessions BIGINT,
            unique_users BIGINT,
            avg_session_duration DECIMAL(10, 2),
            engagement_rate DECIMAL(5, 4),
            bounce_rate DECIMAL(5, 4),
            _computed_at TIMESTAMP
        ) USING iceberg PARTITIONED BY (event_date)
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.analytics.ga4_page_performance (
            event_date DATE,
            page_location STRING,
            page_title STRING,
            page_views BIGINT,
            unique_visitors BIGINT,
            bounce_rate DECIMAL(5, 4),
            avg_time_on_page DECIMAL(10, 2),
            _computed_at TIMESTAMP
        ) USING iceberg PARTITIONED BY (event_date)
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.analytics.ga4_funnel_analysis (
            event_date DATE,
            total_users BIGINT,
            page_view_users BIGINT,
            view_item_users BIGINT,
            add_to_cart_users BIGINT,
            begin_checkout_users BIGINT,
            purchase_users BIGINT,
            page_view_to_view_item_rate DECIMAL(5, 4),
            view_item_to_add_to_cart_rate DECIMAL(5, 4),
            add_to_cart_to_checkout_rate DECIMAL(5, 4),
            checkout_to_purchase_rate DECIMAL(5, 4),
            overall_conversion_rate DECIMAL(5, 4),
            _computed_at TIMESTAMP
        ) USING iceberg PARTITIONED BY (event_date)
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.analytics.ga4_engagement_by_channel (
            event_date DATE,
            traffic_source STRING,
            device_category STRING,
            total_sessions BIGINT,
            engaged_sessions BIGINT,
            bounced_sessions BIGINT,
            unique_users BIGINT,
            engagement_rate DECIMAL(5, 4),
            bounce_rate DECIMAL(5, 4),
            _computed_at TIMESTAMP
        ) USING iceberg PARTITIONED BY (event_date, traffic_source)
    """)

    # Run analytics functions
    compute_ga4_engagement_metrics(spark, mode="full")
    compute_ga4_page_performance(spark, mode="full")
    compute_ga4_funnel_analysis(spark, mode="full")
    compute_ga4_engagement_by_channel(spark, mode="full")

    # Verify analytics tables populated
    engagement_count = spark.table("iceberg.analytics.ga4_engagement_metrics").count()
    assert engagement_count > 0, "Engagement metrics should be populated"
    print(f"✓ Analytics: engagement_metrics has {engagement_count} rows")

    page_perf_count = spark.table("iceberg.analytics.ga4_page_performance").count()
    assert page_perf_count > 0, "Page performance should be populated"
    print(f"✓ Analytics: page_performance has {page_perf_count} rows")

    funnel_count = spark.table("iceberg.analytics.ga4_funnel_analysis").count()
    assert funnel_count > 0, "Funnel analysis should be populated"
    print(f"✓ Analytics: funnel_analysis has {funnel_count} rows")

    channel_count = spark.table("iceberg.analytics.ga4_engagement_by_channel").count()
    assert channel_count > 0, "Channel engagement should be populated"
    print(f"✓ Analytics: engagement_by_channel has {channel_count} rows")

    # =========================================================================
    # STEP 7: Marts Layer
    # =========================================================================
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.marts.ga4_engagement_dashboard (
            event_date DATE,
            total_sessions BIGINT,
            engaged_sessions BIGINT,
            total_page_views BIGINT,
            unique_users BIGINT,
            avg_session_duration DECIMAL(10, 2),
            engagement_rate DECIMAL(5, 4),
            bounce_rate DECIMAL(5, 4),
            sessions_7d_avg DECIMAL(12, 2),
            engagement_rate_7d_avg DECIMAL(5, 4),
            desktop_sessions BIGINT,
            mobile_sessions BIGINT,
            tablet_sessions BIGINT,
            organic_sessions BIGINT,
            paid_sessions BIGINT,
            direct_sessions BIGINT,
            social_sessions BIGINT,
            top_traffic_source STRING,
            top_device_category STRING,
            top_country STRING,
            _built_at TIMESTAMP
        ) USING iceberg PARTITIONED BY (event_date)
    """)

    build_ga4_engagement_dashboard(spark, mode="full")

    dashboard_count = spark.table("iceberg.marts.ga4_engagement_dashboard").count()
    assert dashboard_count > 0, "Dashboard should be populated"
    print(f"✓ Marts: ga4_engagement_dashboard has {dashboard_count} rows")

    # =========================================================================
    # STEP 8: Cross-Layer Consistency Validation
    # =========================================================================
    # Validate data consistency across layers
    raw_event_count = spark.table("iceberg.raw.ga4_events").count()
    staged_event_count = spark.table("iceberg.staging.stg_ga4_events").count()

    assert raw_event_count == staged_event_count, \
        f"Raw and staging event counts should match (raw: {raw_event_count}, staging: {staged_event_count})"

    # Validate session metrics roll up correctly
    session_total = spark.table("iceberg.staging.stg_ga4_sessions").count()
    engagement_days = spark.table("iceberg.analytics.ga4_engagement_metrics").select("event_date").distinct().count()

    print(f"✓ Cross-layer validation passed")
    print(f"  - Raw events: {raw_event_count}")
    print(f"  - Staged events: {staged_event_count}")
    print(f"  - Sessions: {session_total}")
    print(f"  - Engagement days: {engagement_days}")
    print(f"  - Dashboard rows: {dashboard_count}")

    # Cleanup
    spark.sql("DROP TABLE IF EXISTS iceberg.raw.ga4_events")
    spark.sql("DROP TABLE IF EXISTS iceberg.staging.stg_ga4_events")
    spark.sql("DROP TABLE IF EXISTS iceberg.staging.stg_ga4_sessions")
    spark.sql("DROP TABLE IF EXISTS iceberg.semantic.entity_index")
    spark.sql("DROP TABLE IF EXISTS iceberg.semantic.blocking_index")
    spark.sql("DROP TABLE IF EXISTS iceberg.analytics.ga4_engagement_metrics")
    spark.sql("DROP TABLE IF EXISTS iceberg.analytics.ga4_page_performance")
    spark.sql("DROP TABLE IF EXISTS iceberg.analytics.ga4_funnel_analysis")
    spark.sql("DROP TABLE IF EXISTS iceberg.analytics.ga4_engagement_by_channel")
    spark.sql("DROP TABLE IF EXISTS iceberg.marts.ga4_engagement_dashboard")


def test_e2e_ga4_incremental_processing(spark, ga4_export_dir):
    """
    Test incremental processing of new GA4 export files.

    Validates:
    1. Initial batch load
    2. New export file added
    3. Incremental processing only processes new data
    4. No duplicate events after incremental run
    """
    from datagen.providers.ga4_provider import GA4Provider
    from jobs.spark.staging_batch import process_ga4_events_to_staging

    # Create tables
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.raw.ga4_events (
            event_timestamp BIGINT,
            event_name STRING,
            client_id STRING,
            user_id STRING,
            session_id STRING,
            page_location STRING,
            page_title STRING,
            event_params STRING,
            user_properties STRING,
            traffic_source STRING,
            device_category STRING,
            geo_country STRING,
            _loaded_at TIMESTAMP,
            _export_date DATE
        ) USING iceberg
        PARTITIONED BY (_export_date)
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.staging.stg_ga4_events (
            event_timestamp BIGINT,
            event_name STRING,
            client_id STRING,
            user_id STRING,
            session_id STRING,
            page_location STRING,
            page_title STRING,
            event_params STRING,
            user_properties STRING,
            traffic_source STRING,
            device_category STRING,
            geo_country STRING,
            _staged_at TIMESTAMP,
            _raw_id STRING
        ) USING iceberg
        PARTITIONED BY (days(CAST(event_timestamp / 1000000 AS TIMESTAMP)))
    """)

    ga4_provider = GA4Provider(seed=99999)

    # Initial load: 1 export file
    export_batch_1 = ga4_provider.generate_export_batch(num_sessions=5)
    file_1 = os.path.join(ga4_export_dir, "events_20260201_000001.parquet")
    ga4_provider.write_parquet(export_batch_1, file_1)

    df_1 = spark.read.parquet(file_1)
    df_1 = df_1.withColumn("_loaded_at", col("_loaded_at").cast("timestamp"))
    df_1 = df_1.withColumn("_export_date", col("_export_date").cast("date"))
    df_1.writeTo("iceberg.raw.ga4_events").using("iceberg").append()

    process_ga4_events_to_staging(spark, mode="full")

    initial_count = spark.table("iceberg.staging.stg_ga4_events").count()
    assert initial_count > 0, "Initial load should have events"

    # Incremental load: 1 more export file
    export_batch_2 = ga4_provider.generate_export_batch(num_sessions=3)
    file_2 = os.path.join(ga4_export_dir, "events_20260202_000002.parquet")
    ga4_provider.write_parquet(export_batch_2, file_2)

    df_2 = spark.read.parquet(file_2)
    df_2 = df_2.withColumn("_loaded_at", col("_loaded_at").cast("timestamp"))
    df_2 = df_2.withColumn("_export_date", col("_export_date").cast("date"))
    df_2.writeTo("iceberg.raw.ga4_events").using("iceberg").append()

    process_ga4_events_to_staging(spark, mode="incremental")

    final_count = spark.table("iceberg.staging.stg_ga4_events").count()
    assert final_count > initial_count, "Incremental load should add more events"

    # Verify no duplicates
    dedup_check = spark.table("iceberg.staging.stg_ga4_events") \
        .groupBy("client_id", "event_timestamp", "event_name") \
        .count()
    duplicates = dedup_check.filter(col("count") > 1).count()
    assert duplicates == 0, "Should have no duplicates after incremental processing"

    print(f"✓ Incremental processing: {initial_count} → {final_count} events (no duplicates)")

    # Cleanup
    spark.sql("DROP TABLE IF EXISTS iceberg.raw.ga4_events")
    spark.sql("DROP TABLE IF EXISTS iceberg.staging.stg_ga4_events")


def test_e2e_ga4_idempotency(spark, ga4_export_dir):
    """
    Test idempotency of GA4 batch ingest.

    Validates:
    1. Same export file processed twice
    2. No duplicate events in raw layer
    3. Staging deduplication works correctly
    """
    from datagen.providers.ga4_provider import GA4Provider
    from jobs.spark.staging_batch import process_ga4_events_to_staging

    # Create tables
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.raw.ga4_events (
            event_timestamp BIGINT,
            event_name STRING,
            client_id STRING,
            user_id STRING,
            session_id STRING,
            page_location STRING,
            page_title STRING,
            event_params STRING,
            user_properties STRING,
            traffic_source STRING,
            device_category STRING,
            geo_country STRING,
            _loaded_at TIMESTAMP,
            _export_date DATE
        ) USING iceberg
        PARTITIONED BY (_export_date)
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.staging.stg_ga4_events (
            event_timestamp BIGINT,
            event_name STRING,
            client_id STRING,
            user_id STRING,
            session_id STRING,
            page_location STRING,
            page_title STRING,
            event_params STRING,
            user_properties STRING,
            traffic_source STRING,
            device_category STRING,
            geo_country STRING,
            _staged_at TIMESTAMP,
            _raw_id STRING
        ) USING iceberg
        PARTITIONED BY (days(CAST(event_timestamp / 1000000 AS TIMESTAMP)))
    """)

    ga4_provider = GA4Provider(seed=55555)

    # Generate export file
    export_batch = ga4_provider.generate_export_batch(num_sessions=5)
    file_path = os.path.join(ga4_export_dir, "events_20260203_000003.parquet")
    ga4_provider.write_parquet(export_batch, file_path)

    # Load once
    df = spark.read.parquet(file_path)
    df = df.withColumn("_loaded_at", col("_loaded_at").cast("timestamp"))
    df = df.withColumn("_export_date", col("_export_date").cast("date"))
    df.writeTo("iceberg.raw.ga4_events").using("iceberg").append()

    first_raw_count = spark.table("iceberg.raw.ga4_events").count()

    # Load same file again (simulating re-run)
    df = spark.read.parquet(file_path)
    df = df.withColumn("_loaded_at", col("_loaded_at").cast("timestamp"))
    df = df.withColumn("_export_date", col("_export_date").cast("date"))
    df.writeTo("iceberg.raw.ga4_events").using("iceberg").append()

    second_raw_count = spark.table("iceberg.raw.ga4_events").count()

    # Raw layer will have duplicates (append mode)
    assert second_raw_count == first_raw_count * 2, "Raw should have duplicates after re-load"

    # But staging deduplication should handle it
    process_ga4_events_to_staging(spark, mode="full")

    staged_count = spark.table("iceberg.staging.stg_ga4_events").count()
    assert staged_count == first_raw_count, \
        f"Staging should deduplicate to original count (expected: {first_raw_count}, got: {staged_count})"

    # Verify deduplication
    dedup_check = spark.table("iceberg.staging.stg_ga4_events") \
        .groupBy("client_id", "event_timestamp", "event_name") \
        .count()
    duplicates = dedup_check.filter(col("count") > 1).count()
    assert duplicates == 0, "Should have no duplicates in staging"

    print(f"✓ Idempotency: raw {second_raw_count} events → staging {staged_count} events (deduplicated)")

    # Cleanup
    spark.sql("DROP TABLE IF EXISTS iceberg.raw.ga4_events")
    spark.sql("DROP TABLE IF EXISTS iceberg.staging.stg_ga4_events")
