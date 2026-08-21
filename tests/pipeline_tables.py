"""
Iceberg DDL and row-insert helpers for standing up pipeline tables in tests.

Tests exercise production functions (`get_all_staging_customers`,
`rebuild_blocking_index`, ...) that read from *every* source table, not just the
one under test. Each test therefore needs the full set to exist, even if empty.

Declaring that set once here keeps the column lists consistent with the
production jobs. Previously each test inlined its own narrow `CREATE TABLE IF
NOT EXISTS`, so whichever test ran first won and later tests wrote against a
schema they did not expect.

Tables are created without partitioning: the partition transforms used in
production (`months(session_start)`) add nothing to a local hadoop-catalog
warehouse holding a handful of rows.
"""
from decimal import Decimal

from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# The 19 columns GA4Provider emits, i.e. a BigQuery Export file before the
# ingest job stamps on _raw_id / _loaded_at / _source_file. Declared explicitly
# because a batch where every user is anonymous gives `user_id` no non-null
# value, and type inference would write it as a null-typed Parquet column.
GA4_EXPORT_SCHEMA = StructType([
    StructField("client_id", StringType()),
    StructField("user_id", StringType()),
    StructField("event_name", StringType()),
    StructField("event_timestamp", LongType()),   # microseconds since epoch
    StructField("event_date", StringType()),      # YYYYMMDD, GA4 export format
    StructField("event_params", StringType()),    # JSON array
    StructField("user_properties", StringType()),  # JSON array
    StructField("traffic_source", StringType()),  # JSON object
    StructField("device", StringType()),          # JSON object
    StructField("geo", StringType()),             # JSON object
    StructField("page_location", StringType()),
    StructField("page_title", StringType()),
    StructField("page_referrer", StringType()),
    StructField("engagement_time_ms", LongType()),
    StructField("is_conversion", BooleanType()),
    StructField("currency", StringType()),
    StructField("value", DoubleType()),
    StructField("session_id", StringType()),
    StructField("ga_session_number", IntegerType()),
])

# Columns required by jobs/spark/entity_backfill.py::get_all_staging_customers
# and ::rebuild_blocking_index.
STAGING_TABLE_DDL = {
    "stg_shopify_customers": """
        customer_id STRING, email_token STRING, first_name_token STRING,
        last_name_token STRING, full_name_token STRING, phone_token STRING,
        last_name_prefix_token STRING, address_line1_token STRING,
        city STRING, province STRING, zip STRING, country STRING,
        created_at TIMESTAMP, _staged_at TIMESTAMP
    """,
    "stg_hubspot_contacts": """
        contact_id STRING, email_token STRING, first_name_token STRING,
        last_name_token STRING, full_name_token STRING, phone_token STRING,
        mobile_phone_token STRING, last_name_prefix_token STRING,
        address_token STRING, city STRING, state STRING, zip STRING,
        country STRING, created_at TIMESTAMP, _staged_at TIMESTAMP
    """,
    "stg_stripe_customers": """
        customer_id STRING, email_token STRING, first_name_token STRING,
        last_name_token STRING, full_name_token STRING, phone_token STRING,
        last_name_prefix_token STRING, address_line1_token STRING,
        city STRING, state STRING, postal_code STRING, country STRING,
        created_at TIMESTAMP, _staged_at TIMESTAMP
    """,
    "stg_mailchimp_subscribers": """
        subscriber_id_token STRING, email_normalized_token STRING,
        first_name_token STRING, last_name_token STRING, full_name_token STRING,
        last_name_prefix_token STRING, phone_normalized_token STRING,
        signup_timestamp TIMESTAMP, _staged_at TIMESTAMP
    """,
    # Mirrors the schema created by staging_batch.py::compute_ga4_sessions.
    "stg_ga4_sessions": """
        session_id STRING, client_id STRING, user_id_token STRING, session_start TIMESTAMP,
        session_end TIMESTAMP, session_duration_sec INT, event_count INT,
        page_view_count INT, is_engaged_session BOOLEAN, traffic_source STRING,
        traffic_medium STRING, traffic_campaign STRING, channel_group STRING,
        landing_page STRING, exit_page STRING, device_category STRING,
        device_os STRING, geo_country STRING, geo_region STRING,
        total_engagement_ms BIGINT, conversions INT, total_value DECIMAL(18, 2),
        session_date DATE, is_bounce BOOLEAN, _loaded_at TIMESTAMP, _staged_at TIMESTAMP
    """,
}

SEMANTIC_TABLE_DDL = {
    "entity_index": """
        unified_id STRING, entity_type STRING, source STRING, source_id STRING,
        match_type STRING, match_confidence DECIMAL(3, 2), match_reason STRING,
        linked_to_unified_id STRING, matched_at TIMESTAMP, matched_by STRING,
        _staged_at TIMESTAMP
    """,
    "blocking_index": """
        blocking_key STRING, blocking_key_type STRING, unified_id STRING,
        entity_type STRING, source STRING, source_id STRING, key_value STRING,
        is_primary BOOLEAN, created_at TIMESTAMP, expires_at TIMESTAMP
    """,
}

# Mirrors the schema written by jobs/spark/ga4_batch_ingest.py.
RAW_TABLE_DDL = {
    "ga4_events": """
        _raw_id STRING, client_id STRING, user_id STRING, event_name STRING,
        event_timestamp BIGINT, event_date STRING, event_params STRING,
        user_properties STRING, traffic_source STRING, device STRING, geo STRING,
        page_location STRING, page_title STRING, page_referrer STRING,
        engagement_time_ms BIGINT, is_conversion BOOLEAN, currency STRING,
        value DOUBLE, session_id STRING, ga_session_number INT,
        _loaded_at TIMESTAMP, _source_file STRING
    """,
}

# Tables the production jobs create themselves via CREATE TABLE IF NOT EXISTS.
# Tests must not pre-create them, only guarantee they start absent.
JOB_MANAGED_TABLES = (
    "staging.stg_ga4_events",
    "analytics._watermarks",
)


def _qualified(namespace, name):
    return f"iceberg.{namespace}.{name}"


def create_tables(spark, namespace, ddl_by_name):
    """Create each table fresh, dropping any leftover from an earlier test."""
    for name, columns in ddl_by_name.items():
        table = _qualified(namespace, name)
        spark.sql(f"DROP TABLE IF EXISTS {table}")
        spark.sql(f"CREATE TABLE {table} ({columns}) USING iceberg")


def drop_tables(spark, namespace, names):
    """Drop tables, ignoring any that do not exist."""
    for name in names:
        spark.sql(f"DROP TABLE IF EXISTS {_qualified(namespace, name)}")


def drop_qualified_tables(spark, qualified_names):
    """Drop `namespace.table` entries, ignoring any that do not exist."""
    for name in qualified_names:
        spark.sql(f"DROP TABLE IF EXISTS iceberg.{name}")


def _coerce(value, field_type):
    """Convert Python values pandas would mistype for the target Spark type."""
    if value is None:
        return None
    if isinstance(field_type, DecimalType) and not isinstance(value, Decimal):
        return Decimal(str(value))
    return value


def insert_rows(spark, table, rows):
    """
    Append `rows` (list of dicts) to `table`, filling absent columns with NULL.

    Builds the DataFrame from the table's own schema rather than inferring from
    the dicts. Inference fails outright on all-NULL columns
    (CANNOT_DETERMINE_TYPE) and silently mistypes decimals, both of which the
    GA4 fixtures hit.
    """
    schema: StructType = spark.table(table).schema

    ordered = [
        tuple(_coerce(row.get(field.name), field.dataType) for field in schema.fields)
        for row in rows
    ]

    spark.createDataFrame(ordered, schema=schema) \
        .writeTo(table).using("iceberg").append()


def write_ga4_export_parquet(spark, events, path):
    """
    Write GA4Provider events to a Parquet file shaped like a BigQuery Export.

    Mirrors what datagen/generator.py drops on the volume that
    jobs/spark/ga4_batch_ingest.py reads.
    """
    rows = [
        tuple(event.get(field.name) for field in GA4_EXPORT_SCHEMA.fields)
        for event in events
    ]

    spark.createDataFrame(rows, schema=GA4_EXPORT_SCHEMA) \
        .coalesce(1).write.mode("overwrite").parquet(path)

    return path
