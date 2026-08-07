"""
End-to-End Integration Tests for GA4 Pipeline
==============================================

Tests the complete GA4 data pipeline from data generation through to marts layer:
1. Generate GA4 Parquet export files
2. Batch ingest to raw layer (MERGE INTO, idempotent)
3. Stage events (deduplicated) and compute sessions
4. Entity resolution integration
5. Analytics computations
6. Marts dashboard creation

Each test drives the same production entry points the Airflow DAG calls, so a
signature drift between a job and its caller fails here rather than in a
scheduled run.
"""
import os
import shutil
import tempfile

import pytest
from pyspark.sql.functions import col

from tests.pipeline_tables import write_ga4_export_parquet

# The pool of known customers GA4Provider draws user_ids from, so a share of the
# generated sessions are logged-in and therefore resolvable to an entity.
SHARED_CUSTOMERS = [
    {"email": "customer1@example.com"},
    {"email": "customer2@example.com"},
    {"email": "customer3@example.com"},
]

RAW_GA4_EVENTS = "iceberg.raw.ga4_events"
STG_GA4_EVENTS = "iceberg.staging.stg_ga4_events"
STG_GA4_SESSIONS = "iceberg.staging.stg_ga4_sessions"


@pytest.fixture
def ga4_export_dir():
    """Temporary directory for GA4 export files, removed after each test."""
    temp_dir = tempfile.mkdtemp(prefix="ga4_export_")
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)


def _generate_export(spark, provider, export_dir, name, num_users):
    """Generate one GA4 export file and return (path, event count)."""
    events = provider.generate_export_batch(
        num_users=num_users,
        events_per_user_range=(5, 15),
        shared_customers=SHARED_CUSTOMERS,
    )
    path = write_ga4_export_parquet(spark, events, os.path.join(export_dir, name))
    return path, len(events)


def test_e2e_ga4_pipeline_full_flow(spark, pipeline_tables, ga4_export_dir):
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
    from jobs.spark.ga4_batch_ingest import create_raw_table_if_not_exists, ingest_ga4_export
    from jobs.spark.staging_batch import stage_ga4_events, compute_ga4_sessions
    from jobs.spark.analytics_incremental import (
        compute_ga4_engagement_metrics,
        compute_ga4_page_performance,
        compute_ga4_funnel_analysis,
        compute_ga4_engagement_by_channel,
    )
    from jobs.spark.marts_incremental import build_ga4_engagement_dashboard

    # =========================================================================
    # STEP 1: Generate GA4 Export Files
    # =========================================================================
    provider = GA4Provider(seed=12345)

    export_files = [
        _generate_export(spark, provider, ga4_export_dir, f"events_{idx:06d}.parquet", num_users=10)
        for idx in range(2)
    ]
    assert len(export_files) == 2, "Should generate 2 export files"

    # =========================================================================
    # STEP 2: Batch Ingest to Raw Layer
    # =========================================================================
    create_raw_table_if_not_exists(spark)
    for path, _ in export_files:
        ingest_ga4_export(spark, path)

    raw_count = spark.table(RAW_GA4_EVENTS).count()
    assert raw_count > 0, "Raw layer should contain GA4 events"

    # Ingest is keyed on _raw_id, so every raw row is a distinct natural key.
    distinct_raw_ids = spark.table(RAW_GA4_EVENTS).select("_raw_id").distinct().count()
    assert distinct_raw_ids == raw_count, "Raw layer should hold no duplicate _raw_id"

    # =========================================================================
    # STEP 3: Stage Events (with deduplication)
    # =========================================================================
    stage_ga4_events(spark, mode="full")

    staged_events = spark.table(STG_GA4_EVENTS)
    staged_count = staged_events.count()
    assert staged_count == raw_count, "Staging should match raw once deduplicated"

    duplicates = staged_events.groupBy("client_id", "event_timestamp", "event_name") \
                              .count().filter(col("count") > 1).count()
    assert duplicates == 0, "Should have no duplicates after staging"

    # =========================================================================
    # STEP 4: Compute Sessions (30-min gap rule)
    # =========================================================================
    compute_ga4_sessions(spark, mode="full")

    staged_sessions = spark.table(STG_GA4_SESSIONS)
    session_count = staged_sessions.count()
    assert session_count > 0, "Should compute at least one session"
    assert session_count <= staged_count, "Sessions cannot outnumber the events they group"

    # Sessions are unique per (client_id, session_id).
    duplicate_sessions = staged_sessions.groupBy("client_id", "session_id") \
                                        .count().filter(col("count") > 1).count()
    assert duplicate_sessions == 0, "Each session should appear once"

    assert staged_sessions.filter(col("user_id").isNotNull()).count() > 0, \
        "Should have some sessions with user_id"

    # =========================================================================
    # STEP 5: Entity Resolution
    # =========================================================================
    from jobs.spark.entity_backfill import get_all_staging_customers, perform_initial_resolution

    staging_data = get_all_staging_customers(spark)
    ga4_customer_count = staging_data.filter(col("source") == "ga4_sessions").count()
    assert ga4_customer_count > 0, "Should have GA4 customers with user_id"

    entity_index_df, _ = perform_initial_resolution(spark, staging_data, dry_run=True)
    ga4_entities = entity_index_df.filter(col("source") == "ga4_sessions")
    assert ga4_entities.count() == ga4_customer_count, \
        "All GA4 customers should be in entity index"

    # =========================================================================
    # STEP 6: Analytics Layer
    # =========================================================================
    compute_ga4_engagement_metrics(spark, mode="full")
    compute_ga4_page_performance(spark, mode="full")
    compute_ga4_funnel_analysis(spark, mode="full")
    compute_ga4_engagement_by_channel(spark, mode="full")

    for table in (
        "iceberg.analytics.ga4_engagement_metrics",
        "iceberg.analytics.ga4_page_performance",
        "iceberg.analytics.ga4_funnel_analysis",
        "iceberg.analytics.ga4_engagement_by_channel",
    ):
        assert spark.table(table).count() > 0, f"{table} should be populated"

    # =========================================================================
    # STEP 7: Marts Layer
    # =========================================================================
    build_ga4_engagement_dashboard(spark, mode="full")

    dashboard = spark.table("iceberg.marts.ga4_engagement_dashboard")
    assert dashboard.count() > 0, "Dashboard should be populated"

    # =========================================================================
    # STEP 8: Cross-Layer Consistency Validation
    # =========================================================================
    # One dashboard row per day of engagement metrics.
    engagement_days = spark.table("iceberg.analytics.ga4_engagement_metrics") \
                           .select("metric_date").distinct().count()
    assert dashboard.count() == engagement_days, \
        "Dashboard should cover exactly the days present in engagement metrics"

    # Sessions in staging must account for every session referenced by events.
    event_sessions = staged_events.select("client_id", "session_id").distinct().count()
    assert session_count == event_sessions, \
        "Every event session should produce exactly one session row"


def test_e2e_ga4_incremental_processing(spark, pipeline_tables, ga4_export_dir):
    """
    Test incremental processing of new GA4 export files.

    Validates:
    1. Initial batch load
    2. New export file added
    3. Incremental processing only processes new data
    4. No duplicate events after incremental run
    """
    from datagen.providers.ga4_provider import GA4Provider
    from jobs.spark.ga4_batch_ingest import create_raw_table_if_not_exists, ingest_ga4_export
    from jobs.spark.staging_batch import stage_ga4_events

    provider = GA4Provider(seed=99999)
    create_raw_table_if_not_exists(spark)

    # Initial load
    first_path, _ = _generate_export(spark, provider, ga4_export_dir, "events_000001.parquet", num_users=5)
    ingest_ga4_export(spark, first_path)
    stage_ga4_events(spark, mode="full")

    initial_count = spark.table(STG_GA4_EVENTS).count()
    assert initial_count > 0, "Initial load should have events"

    # Incremental load: a second, distinct export
    second_path, _ = _generate_export(spark, provider, ga4_export_dir, "events_000002.parquet", num_users=3)
    ingest_ga4_export(spark, second_path)
    stage_ga4_events(spark, mode="incremental")

    final_count = spark.table(STG_GA4_EVENTS).count()
    assert final_count > initial_count, "Incremental load should add more events"

    duplicates = spark.table(STG_GA4_EVENTS) \
        .groupBy("client_id", "event_timestamp", "event_name") \
        .count().filter(col("count") > 1).count()
    assert duplicates == 0, "Should have no duplicates after incremental processing"


def test_e2e_ga4_idempotency(spark, pipeline_tables, ga4_export_dir):
    """
    Test idempotency of GA4 batch ingest.

    Validates:
    1. Same export file processed twice
    2. No duplicate events in raw layer (MERGE INTO on _raw_id)
    3. Staging deduplication works correctly
    """
    from datagen.providers.ga4_provider import GA4Provider
    from jobs.spark.ga4_batch_ingest import create_raw_table_if_not_exists, ingest_ga4_export
    from jobs.spark.staging_batch import stage_ga4_events

    provider = GA4Provider(seed=55555)
    create_raw_table_if_not_exists(spark)

    path, _ = _generate_export(spark, provider, ga4_export_dir, "events_000003.parquet", num_users=5)

    ingest_ga4_export(spark, path)
    first_raw_count = spark.table(RAW_GA4_EVENTS).count()
    assert first_raw_count > 0, "First ingest should load events"

    # Re-ingest the same file: MERGE INTO on _raw_id must update, not insert.
    ingest_ga4_export(spark, path)
    second_raw_count = spark.table(RAW_GA4_EVENTS).count()

    assert second_raw_count == first_raw_count, \
        f"Re-ingest should be idempotent (expected {first_raw_count}, got {second_raw_count})"

    stage_ga4_events(spark, mode="full")

    staged_count = spark.table(STG_GA4_EVENTS).count()
    assert staged_count == first_raw_count, \
        f"Staging should match raw count (expected {first_raw_count}, got {staged_count})"

    duplicates = spark.table(STG_GA4_EVENTS) \
        .groupBy("client_id", "event_timestamp", "event_name") \
        .count().filter(col("count") > 1).count()
    assert duplicates == 0, "Should have no duplicates in staging"
