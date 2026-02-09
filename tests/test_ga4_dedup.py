"""
Tests for GA4 event deduplication logic (TDD).

Critical Issue #2: Deduplication must work in both incremental and full modes.
"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from datetime import datetime
import time


def test_dedup_full_mode_with_duplicates(spark, sample_events_with_duplicates):
    """
    CRITICAL TEST: Verify that full mode deduplicates correctly.

    Scenario: raw.ga4_events contains duplicates (from Airflow retry).
    Running stage_ga4_events in full mode should deduplicate.
    """
    # Create raw table
    spark.sql("""
        CREATE TABLE IF NOT EXISTS raw.ga4_events (
            _raw_id STRING, client_id STRING, user_id STRING, event_name STRING,
            event_timestamp BIGINT, event_date STRING, event_params STRING,
            user_properties STRING, traffic_source STRING, device STRING, geo STRING,
            page_location STRING, page_title STRING, page_referrer STRING,
            engagement_time_ms BIGINT, is_conversion BOOLEAN, currency STRING,
            value DOUBLE, session_id STRING, ga_session_number INT,
            _loaded_at TIMESTAMP, _source_file STRING
        ) USING iceberg
    """)

    # Insert duplicate events (3 events, but 2 are duplicates)
    # Expected: After dedup, should have 2 unique events
    events = sample_events_with_duplicates

    # Convert to DataFrame
    import pandas as pd
    df = spark.createDataFrame(pd.DataFrame(events))

    # Add required columns
    df = df.withColumn("_raw_id", col("client_id")) \
           .withColumn("event_date", "20260209") \
           .withColumn("user_properties", "[]") \
           .withColumn("ga_session_number", 1) \
           .withColumn("_source_file", "test.parquet")

    # Write to raw table
    df.writeTo("raw.ga4_events").using("iceberg").append()

    # Verify raw table has 3 rows (including duplicates)
    raw_count = spark.table("raw.ga4_events").count()
    assert raw_count == 3, f"Raw table should have 3 rows, got {raw_count}"

    # Now run staging in FULL mode
    from jobs.spark.staging_batch import stage_ga4_events
    staged_count = stage_ga4_events(spark, mode="full")

    # CRITICAL ASSERTION: Staging should have 2 unique events (duplicates removed)
    staging_df = spark.table("iceberg.staging.stg_ga4_events")
    actual_count = staging_df.count()

    assert actual_count == 2, \
        f"Expected 2 unique events after dedup, got {actual_count}. " \
        f"Deduplication failed in full mode!"

    # Cleanup
    spark.sql("DROP TABLE IF EXISTS raw.ga4_events")
    spark.sql("DROP TABLE IF EXISTS iceberg.staging.stg_ga4_events")


def test_dedup_incremental_mode(spark, sample_events_with_duplicates):
    """
    Test that incremental mode also deduplicates correctly.

    This is a regression test to ensure incremental mode still works.
    """
    # Create raw table
    spark.sql("""
        CREATE TABLE IF NOT EXISTS raw.ga4_events (
            _raw_id STRING, client_id STRING, user_id STRING, event_name STRING,
            event_timestamp BIGINT, event_date STRING, event_params STRING,
            user_properties STRING, traffic_source STRING, device STRING, geo STRING,
            page_location STRING, page_title STRING, page_referrer STRING,
            engagement_time_ms BIGINT, is_conversion BOOLEAN, currency STRING,
            value DOUBLE, session_id STRING, ga_session_number INT,
            _loaded_at TIMESTAMP, _source_file STRING
        ) USING iceberg
    """)

    events = sample_events_with_duplicates

    # Convert to DataFrame
    import pandas as pd
    df = spark.createDataFrame(pd.DataFrame(events))

    # Add required columns
    df = df.withColumn("_raw_id", col("client_id")) \
           .withColumn("event_date", "20260209") \
           .withColumn("user_properties", "[]") \
           .withColumn("ga_session_number", 1) \
           .withColumn("_source_file", "test.parquet")

    # Write to raw table
    df.writeTo("raw.ga4_events").using("iceberg").append()

    # Run staging in INCREMENTAL mode
    from jobs.spark.staging_batch import stage_ga4_events
    staged_count = stage_ga4_events(spark, mode="incremental")

    # Should have 2 unique events
    staging_df = spark.table("iceberg.staging.stg_ga4_events")
    actual_count = staging_df.count()

    assert actual_count == 2, \
        f"Expected 2 unique events after dedup, got {actual_count}"

    # Cleanup
    spark.sql("DROP TABLE IF EXISTS raw.ga4_events")
    spark.sql("DROP TABLE IF EXISTS iceberg.staging.stg_ga4_events")


def test_dedup_natural_key_uniqueness(spark):
    """
    Test that dedup natural key (client_id, event_timestamp, event_name) is correct.

    Different combinations should NOT be deduped:
    - Same client + timestamp, different event_name → 2 events
    - Same client + event_name, different timestamp → 2 events
    """
    import time
    import json

    base_time = int(time.time() * 1_000_000)

    events = [
        # Event 1: base event
        {
            "client_id": "1234567890.1234567890",
            "event_timestamp": base_time,
            "event_name": "page_view",
            "_loaded_at": datetime.now()
        },
        # Event 2: same client+timestamp, different event_name (should NOT dedup)
        {
            "client_id": "1234567890.1234567890",
            "event_timestamp": base_time,
            "event_name": "click",
            "_loaded_at": datetime.now()
        },
        # Event 3: same client+event_name, different timestamp (should NOT dedup)
        {
            "client_id": "1234567890.1234567890",
            "event_timestamp": base_time + 1000,
            "event_name": "page_view",
            "_loaded_at": datetime.now()
        },
    ]

    # Add default fields
    for event in events:
        event.update({
            "_raw_id": f"{event['client_id']}_{event['event_timestamp']}_{event['event_name']}",
            "user_id": None,
            "event_date": "20260209",
            "event_params": "[]",
            "user_properties": "[]",
            "traffic_source": json.dumps({"source": "direct", "medium": "(none)", "name": ""}),
            "device": json.dumps({"category": "desktop", "operating_system": "Windows", "web_info": {"browser": "Chrome"}}),
            "geo": json.dumps({"country": "US", "region": "CA", "city": "SF"}),
            "page_location": "https://example.com/",
            "page_title": "Home",
            "page_referrer": "",
            "engagement_time_ms": 1000,
            "is_conversion": False,
            "currency": "USD",
            "value": 0.0,
            "session_id": "1000000",
            "ga_session_number": 1,
            "_source_file": "test.parquet"
        })

    # Create tables and insert
    spark.sql("""
        CREATE TABLE IF NOT EXISTS raw.ga4_events (
            _raw_id STRING, client_id STRING, user_id STRING, event_name STRING,
            event_timestamp BIGINT, event_date STRING, event_params STRING,
            user_properties STRING, traffic_source STRING, device STRING, geo STRING,
            page_location STRING, page_title STRING, page_referrer STRING,
            engagement_time_ms BIGINT, is_conversion BOOLEAN, currency STRING,
            value DOUBLE, session_id STRING, ga_session_number INT,
            _loaded_at TIMESTAMP, _source_file STRING
        ) USING iceberg
    """)

    import pandas as pd
    df = spark.createDataFrame(pd.DataFrame(events))
    df.writeTo("raw.ga4_events").using("iceberg").append()

    # Run staging
    from jobs.spark.staging_batch import stage_ga4_events
    stage_ga4_events(spark, mode="full")

    # Should have 3 events (no dedup because natural keys are different)
    staging_df = spark.table("iceberg.staging.stg_ga4_events")
    actual_count = staging_df.count()

    assert actual_count == 3, \
        f"Expected 3 events (different natural keys), got {actual_count}"

    # Cleanup
    spark.sql("DROP TABLE IF EXISTS raw.ga4_events")
    spark.sql("DROP TABLE IF EXISTS iceberg.staging.stg_ga4_events")
