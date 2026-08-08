"""
Shared pytest fixtures for GA4 integration tests.

Provides:
- SparkSession for local testing (no Iceberg catalog)
- Sample GA4 event data generators
- Mock data fixtures
- Helper assertion functions
"""
import pytest
import os
import tempfile
import shutil
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
import random

from tests.pipeline_tables import (
    JOB_MANAGED_TABLES,
    RAW_TABLE_DDL,
    SEMANTIC_TABLE_DDL,
    STAGING_TABLE_DDL,
    create_tables,
    drop_qualified_tables,
    drop_tables,
)


@pytest.fixture(scope="session")
def spark():
    """
    Local SparkSession for unit testing.

    Uses local[*] master with a temp-dir Iceberg warehouse.

    The catalog is registered as `iceberg` (not `local`) because both the
    production jobs and the tests address tables as `iceberg.staging.*`. It is
    backed by a hadoop-type catalog so the suite needs no REST catalog, MinIO,
    or Postgres running; the `type` override also shadows any REST catalog
    config baked into the container's spark-defaults.conf.
    """
    warehouse_dir = tempfile.mkdtemp(prefix="spark_warehouse_")

    spark = SparkSession.builder \
        .appName("GA4 Tests") \
        .master("local[*]") \
        .config("spark.sql.warehouse.dir", warehouse_dir) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "hadoop") \
        .config("spark.sql.catalog.iceberg.warehouse", warehouse_dir) \
        .config("spark.sql.defaultCatalog", "iceberg") \
        .getOrCreate()

    # Namespaces the pipeline layers write into. Created up front so individual
    # tests do not each repeat the namespace bootstrap.
    for namespace in ("raw", "staging", "semantic", "core", "analytics", "marts"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS iceberg.{namespace}")

    yield spark

    spark.stop()
    shutil.rmtree(warehouse_dir, ignore_errors=True)


@pytest.fixture
def pipeline_tables(spark):
    """
    Empty raw / staging / semantic tables for one test, torn down afterwards.

    The production entity-resolution functions union across all five sources, so
    every source table must exist even when a test only populates one of them.
    Tables the jobs create themselves are dropped rather than created, so each
    test sees the job's own DDL instead of a stale one.
    """
    create_tables(spark, "raw", RAW_TABLE_DDL)
    create_tables(spark, "staging", STAGING_TABLE_DDL)
    create_tables(spark, "semantic", SEMANTIC_TABLE_DDL)
    drop_qualified_tables(spark, JOB_MANAGED_TABLES)

    yield

    drop_qualified_tables(spark, JOB_MANAGED_TABLES)
    drop_tables(spark, "semantic", SEMANTIC_TABLE_DDL)
    drop_tables(spark, "staging", STAGING_TABLE_DDL)
    drop_tables(spark, "raw", RAW_TABLE_DDL)


@pytest.fixture
def sample_client_id():
    """Generate a sample GA4 client_id."""
    random_digits = random.randint(1000000000, 9999999999)
    timestamp = int(datetime.now().timestamp())
    return f"{random_digits}.{timestamp}"


@pytest.fixture
def sample_event(sample_client_id):
    """Generate a single sample GA4 event."""
    import time
    import json

    event_timestamp = int(time.time() * 1_000_000)
    event_date = datetime.fromtimestamp(event_timestamp / 1_000_000).strftime("%Y%m%d")

    return {
        "client_id": sample_client_id,
        "user_id": "test@example.com",
        "event_name": "page_view",
        "event_timestamp": event_timestamp,
        "event_date": event_date,
        "event_params": json.dumps([
            {"key": "page_location", "value": {"string_value": "https://example.com/"}},
            {"key": "engagement_time_msec", "value": {"int_value": 5000}}
        ]),
        "traffic_source": json.dumps({"source": "google", "medium": "organic", "name": ""}),
        "device": json.dumps({"category": "desktop", "operating_system": "Windows", "web_info": {"browser": "Chrome"}}),
        "geo": json.dumps({"country": "United States", "region": "California", "city": "San Francisco"}),
        "page_location": "https://example.com/",
        "page_title": "Home",
        "page_referrer": "",
        "engagement_time_ms": 5000,
        "is_conversion": False,
        "currency": "USD",
        "value": 0.0,
        "session_id": "1000000",
        "ga_session_number": 1
    }


@pytest.fixture
def sample_events_with_duplicates(sample_client_id):
    """Generate events with intentional duplicates for dedup testing."""
    import time
    import json

    base_time = int(time.time() * 1_000_000) - 3600 * 1_000_000  # 1 hour ago
    event_date = datetime.fromtimestamp(base_time / 1_000_000).strftime("%Y%m%d")

    events = []

    # Event 1 - unique
    events.append({
        "client_id": sample_client_id,
        "user_id": "test@example.com",
        "event_name": "page_view",
        "event_timestamp": base_time,
        "event_date": event_date,
        "event_params": "[]",
        "traffic_source": json.dumps({"source": "direct", "medium": "(none)", "name": ""}),
        "device": json.dumps({"category": "desktop", "operating_system": "Windows", "web_info": {"browser": "Chrome"}}),
        "geo": json.dumps({"country": "US", "region": "CA", "city": "SF"}),
        "page_location": "https://example.com/page1",
        "page_title": "Page 1",
        "page_referrer": "",
        "engagement_time_ms": 1000,
        "is_conversion": False,
        "currency": "USD",
        "value": 0.0,
        "session_id": "1000001",
        "ga_session_number": 1,
        "_loaded_at": datetime.now() - timedelta(hours=2)
    })

    # Event 2 - duplicate of event 1 (same client_id, event_timestamp, event_name)
    # but different _loaded_at (newer)
    events.append({
        "client_id": sample_client_id,
        "user_id": "test@example.com",
        "event_name": "page_view",
        "event_timestamp": base_time,  # Same timestamp
        "event_date": event_date,
        "event_params": "[]",
        "traffic_source": json.dumps({"source": "direct", "medium": "(none)", "name": ""}),
        "device": json.dumps({"category": "desktop", "operating_system": "Windows", "web_info": {"browser": "Chrome"}}),
        "geo": json.dumps({"country": "US", "region": "CA", "city": "SF"}),
        "page_location": "https://example.com/page1",
        "page_title": "Page 1",
        "page_referrer": "",
        "engagement_time_ms": 1000,
        "is_conversion": False,
        "currency": "USD",
        "value": 0.0,
        "session_id": "1000001",
        "ga_session_number": 1,
        "_loaded_at": datetime.now() - timedelta(hours=1)  # Newer - should be kept
    })

    # Event 3 - unique (different timestamp)
    events.append({
        "client_id": sample_client_id,
        "user_id": "test@example.com",
        "event_name": "page_view",
        "event_timestamp": base_time + 60_000_000,  # Different timestamp
        "event_date": event_date,
        "event_params": "[]",
        "traffic_source": json.dumps({"source": "direct", "medium": "(none)", "name": ""}),
        "device": json.dumps({"category": "desktop", "operating_system": "Windows", "web_info": {"browser": "Chrome"}}),
        "geo": json.dumps({"country": "US", "region": "CA", "city": "SF"}),
        "page_location": "https://example.com/page2",
        "page_title": "Page 2",
        "page_referrer": "",
        "engagement_time_ms": 2000,
        "is_conversion": False,
        "currency": "USD",
        "value": 0.0,
        "session_id": "1000001",
        "ga_session_number": 1,
        "_loaded_at": datetime.now()
    })

    return events


@pytest.fixture
def sample_session_events(sample_client_id):
    """Generate events for a coherent session (30-min gap testing)."""
    import time
    import json

    base_time = int(time.time() * 1_000_000) - 3600 * 1_000_000
    event_date = datetime.fromtimestamp(base_time / 1_000_000).strftime("%Y%m%d")
    session_id = "2000000"

    events = []

    # Event 1: session_start
    events.append({
        "client_id": sample_client_id,
        "user_id": "test@example.com",
        "event_name": "session_start",
        "event_timestamp": base_time,
        "event_date": event_date,
        "event_params": "[]",
        "traffic_source": json.dumps({"source": "google", "medium": "organic", "name": ""}),
        "device": json.dumps({"category": "desktop", "operating_system": "macOS", "web_info": {"browser": "Safari"}}),
        "geo": json.dumps({"country": "US", "region": "NY", "city": "NYC"}),
        "page_location": "https://example.com/",
        "page_title": "Home",
        "page_referrer": "https://google.com",
        "engagement_time_ms": 0,
        "is_conversion": False,
        "currency": "USD",
        "value": 0.0,
        "session_id": session_id,
        "ga_session_number": 1
    })

    # Event 2: page_view (10 seconds later)
    events.append({
        "client_id": sample_client_id,
        "user_id": "test@example.com",
        "event_name": "page_view",
        "event_timestamp": base_time + 10_000_000,
        "event_date": event_date,
        "event_params": "[]",
        "traffic_source": json.dumps({"source": "google", "medium": "organic", "name": ""}),
        "device": json.dumps({"category": "desktop", "operating_system": "macOS", "web_info": {"browser": "Safari"}}),
        "geo": json.dumps({"country": "US", "region": "NY", "city": "NYC"}),
        "page_location": "https://example.com/",
        "page_title": "Home",
        "page_referrer": "https://google.com",
        "engagement_time_ms": 10000,
        "is_conversion": False,
        "currency": "USD",
        "value": 0.0,
        "session_id": session_id,
        "ga_session_number": 1
    })

    # Event 3: page_view (5 minutes later - still same session)
    events.append({
        "client_id": sample_client_id,
        "user_id": "test@example.com",
        "event_name": "page_view",
        "event_timestamp": base_time + 310_000_000,  # 5 min 10 sec
        "event_date": event_date,
        "event_params": "[]",
        "traffic_source": json.dumps({"source": "google", "medium": "organic", "name": ""}),
        "device": json.dumps({"category": "desktop", "operating_system": "macOS", "web_info": {"browser": "Safari"}}),
        "geo": json.dumps({"country": "US", "region": "NY", "city": "NYC"}),
        "page_location": "https://example.com/products",
        "page_title": "Products",
        "page_referrer": "https://example.com/",
        "engagement_time_ms": 15000,
        "is_conversion": False,
        "currency": "USD",
        "value": 0.0,
        "session_id": session_id,
        "ga_session_number": 1
    })

    # Event 4: page_view (31 minutes later - NEW session due to 30-min gap)
    new_session_time = base_time + 2170_000_000  # 36 min 10 sec from start
    events.append({
        "client_id": sample_client_id,
        "user_id": "test@example.com",
        "event_name": "page_view",
        "event_timestamp": new_session_time,
        "event_date": event_date,
        "event_params": "[]",
        "traffic_source": json.dumps({"source": "direct", "medium": "(none)", "name": ""}),
        "device": json.dumps({"category": "desktop", "operating_system": "macOS", "web_info": {"browser": "Safari"}}),
        "geo": json.dumps({"country": "US", "region": "NY", "city": "NYC"}),
        "page_location": "https://example.com/checkout",
        "page_title": "Checkout",
        "page_referrer": "",
        "engagement_time_ms": 5000,
        "is_conversion": True,
        "currency": "USD",
        "value": 99.99,
        "session_id": "2000001",  # Different session_id
        "ga_session_number": 2
    })

    return events


@pytest.fixture
def sample_funnel_events(sample_client_id):
    """Generate events for funnel analysis testing."""
    import time
    import json

    base_time = int(time.time() * 1_000_000) - 3600 * 1_000_000
    event_date = datetime.fromtimestamp(base_time / 1_000_000).strftime("%Y%m%d")
    session_id = "3000000"

    events = []

    # Funnel: page_view → view_item → add_to_cart → begin_checkout → purchase

    funnel_steps = [
        ("page_view", "https://example.com/", 0),
        ("view_item", "https://example.com/product/123", 10_000_000),
        ("add_to_cart", "https://example.com/product/123", 20_000_000),
        ("begin_checkout", "https://example.com/checkout", 30_000_000),
        ("purchase", "https://example.com/checkout", 40_000_000),
    ]

    for idx, (event_name, page_location, time_offset) in enumerate(funnel_steps):
        events.append({
            "client_id": sample_client_id,
            "user_id": "test@example.com",
            "event_name": event_name,
            "event_timestamp": base_time + time_offset,
            "event_date": event_date,
            "event_params": "[]",
            "traffic_source": json.dumps({"source": "google", "medium": "cpc", "name": "spring_sale"}),
            "device": json.dumps({"category": "mobile", "operating_system": "iOS", "web_info": {"browser": "Safari"}}),
            "geo": json.dumps({"country": "US", "region": "CA", "city": "LA"}),
            "page_location": page_location,
            "page_title": f"Page {idx + 1}",
            "page_referrer": "",
            "engagement_time_ms": 5000,
            "is_conversion": (event_name == "purchase"),
            "currency": "USD",
            "value": 99.99 if event_name == "purchase" else 0.0,
            "session_id": session_id,
            "ga_session_number": 1
        })

    return events


def assert_schema_valid(df, expected_columns):
    """Helper to assert DataFrame has expected columns."""
    actual_columns = set(df.columns)
    expected_columns = set(expected_columns)

    missing = expected_columns - actual_columns
    extra = actual_columns - expected_columns

    assert not missing, f"Missing columns: {missing}"
    assert not extra, f"Extra columns: {extra}"


def assert_row_count(df, expected_count, message=""):
    """Helper to assert DataFrame row count."""
    actual = df.count()
    assert actual == expected_count, f"{message} Expected {expected_count} rows, got {actual}"
