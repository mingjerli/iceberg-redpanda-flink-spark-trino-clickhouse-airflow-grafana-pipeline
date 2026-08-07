"""
Tests for GA4 event deduplication logic (TDD).

Critical Issue #2: Deduplication must work in both incremental and full modes.
"""
import json
import time
from datetime import datetime

from tests.pipeline_tables import insert_rows

RAW_GA4_EVENTS = "iceberg.raw.ga4_events"
STG_GA4_EVENTS = "iceberg.staging.stg_ga4_events"


def _with_ingest_columns(events):
    """Add the columns ga4_batch_ingest.py stamps on, without mutating inputs."""
    return [
        {
            **event,
            "_raw_id": f"{event['client_id']}_{event['event_timestamp']}_{event['event_name']}",
            "user_properties": "[]",
            "_source_file": "test.parquet",
        }
        for event in events
    ]


def test_dedup_full_mode_with_duplicates(spark, pipeline_tables, sample_events_with_duplicates):
    """
    CRITICAL TEST: Verify that full mode deduplicates correctly.

    Scenario: raw.ga4_events contains duplicates (from Airflow retry).
    Running stage_ga4_events in full mode should deduplicate.
    """
    insert_rows(spark, RAW_GA4_EVENTS, _with_ingest_columns(sample_events_with_duplicates))

    # Verify raw table has 3 rows (including duplicates)
    raw_count = spark.table(RAW_GA4_EVENTS).count()
    assert raw_count == 3, f"Raw table should have 3 rows, got {raw_count}"

    from jobs.spark.staging_batch import stage_ga4_events
    stage_ga4_events(spark, mode="full")

    # CRITICAL ASSERTION: Staging should have 2 unique events (duplicates removed)
    actual_count = spark.table(STG_GA4_EVENTS).count()

    assert actual_count == 2, \
        f"Expected 2 unique events after dedup, got {actual_count}. " \
        f"Deduplication failed in full mode!"


def test_dedup_incremental_mode(spark, pipeline_tables, sample_events_with_duplicates):
    """
    Test that incremental mode also deduplicates correctly.

    This is a regression test to ensure incremental mode still works.
    """
    insert_rows(spark, RAW_GA4_EVENTS, _with_ingest_columns(sample_events_with_duplicates))

    from jobs.spark.staging_batch import stage_ga4_events
    stage_ga4_events(spark, mode="incremental")

    actual_count = spark.table(STG_GA4_EVENTS).count()

    assert actual_count == 2, \
        f"Expected 2 unique events after dedup, got {actual_count}"


def test_dedup_natural_key_uniqueness(spark, pipeline_tables):
    """
    Test that dedup natural key (client_id, event_timestamp, event_name) is correct.

    Different combinations should NOT be deduped:
    - Same client + timestamp, different event_name → 2 events
    - Same client + event_name, different timestamp → 2 events
    """
    base_time = int(time.time() * 1_000_000)

    defaults = {
        "user_id": None,
        "event_date": "20260209",
        "event_params": "[]",
        "traffic_source": json.dumps({"source": "direct", "medium": "(none)", "name": ""}),
        "device": json.dumps({
            "category": "desktop",
            "operating_system": "Windows",
            "web_info": {"browser": "Chrome"},
        }),
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
        "_loaded_at": datetime.now(),
    }

    events = [
        # Event 1: base event
        {**defaults, "client_id": "1234567890.1234567890",
         "event_timestamp": base_time, "event_name": "page_view"},
        # Event 2: same client+timestamp, different event_name (should NOT dedup)
        {**defaults, "client_id": "1234567890.1234567890",
         "event_timestamp": base_time, "event_name": "click"},
        # Event 3: same client+event_name, different timestamp (should NOT dedup)
        {**defaults, "client_id": "1234567890.1234567890",
         "event_timestamp": base_time + 1000, "event_name": "page_view"},
    ]

    insert_rows(spark, RAW_GA4_EVENTS, _with_ingest_columns(events))

    from jobs.spark.staging_batch import stage_ga4_events
    stage_ga4_events(spark, mode="full")

    # Should have 3 events (no dedup because natural keys are different)
    actual_count = spark.table(STG_GA4_EVENTS).count()

    assert actual_count == 3, \
        f"Expected 3 events (different natural keys), got {actual_count}"
