"""
Integration test for jobs/spark/marts_incremental.py::build_customer_360.

IMPORTANT 6 in the PII masking fix wave: the Mailchimp and GA4 joins in
build_customer_360 (:456, :502, using renamed *_token join keys) each live
inside a `try/except Exception: logger.warning(...)` block. If either key is
wrong -- a plausible risk right after the PII rename, since the join columns
changed from `email` to `email_token` -- the function silently produces a
mart with has_mailchimp/has_ga4 always False and every Mailchimp/GA4 column
NULL, and nothing in the suite previously called build_customer_360 at all.

This does not restructure the except blocks (out of scope for this fix wave;
see the task brief). It seeds real staging tables with a consistent
email_token join key across analytics.customer_metrics,
staging.stg_mailchimp_subscribers, and staging.stg_ga4_sessions, runs the
real production function, and asserts the joins actually connected: a
regression that reintroduces a bad join key fails here instead of shipping
silently.
"""
from __future__ import annotations

from datetime import datetime

from pyspark.sql.functions import lit

from pii.tokenize import normalize, token_expr
from tests.pipeline_tables import insert_rows

PEPPER = "test-pepper-do-not-use-in-production"
STAGED_AT = datetime(2026, 8, 21, 12, 0, 0)
CUSTOMER_EMAIL = "customer360@example.com"

# Full production DDL for analytics.customer_metrics
# (jobs/spark/analytics_incremental.py::compute_customer_metrics), copied here
# rather than read off a live warehouse: build_customer_360 reads this table
# directly and its "no core.customers" branch (marts_incremental.py:340-395)
# selects every one of these columns by name.
CUSTOMER_METRICS_DDL = """
    customer_id STRING,
    email_token STRING,
    full_name_token STRING,
    customer_tier STRING,
    lifecycle_stage STRING,
    customer_segment STRING,
    total_spent DECIMAL(18, 2),
    total_orders BIGINT,
    avg_order_value DECIMAL(18, 2),
    first_order_value DECIMAL(18, 2),
    last_order_value DECIMAL(18, 2),
    estimated_ltv DECIMAL(18, 2),
    first_order_date DATE,
    last_order_date DATE,
    days_since_first_order INT,
    days_since_last_order INT,
    order_frequency_days DECIMAL(10, 2),
    page_views BIGINT,
    sessions BIGINT,
    engagement_score DECIMAL(5, 2),
    rfm_recency_score INT,
    rfm_frequency_score INT,
    rfm_monetary_score INT,
    rfm_segment STRING,
    accepts_marketing BOOLEAN,
    acquisition_source STRING,
    source_count INT,
    has_shopify BOOLEAN,
    has_hubspot BOOLEAN,
    first_order_cohort STRING,
    signup_cohort STRING,
    customer_created_at TIMESTAMP,
    customer_updated_at TIMESTAMP,
    _computed_at TIMESTAMP,
    _version INT
"""

# Full production DDL for staging.stg_mailchimp_subscribers
# (jobs/spark/staging_batch.py::stage_mailchimp_subscribers), post CRITICAL-1
# (merge_fields dropped). Deliberately NOT the narrower entry in
# tests/pipeline_tables.py's STAGING_TABLE_DDL, which is missing status/
# avg_open_rate/avg_click_rate/has_sms -- columns build_customer_360's
# Mailchimp join actually selects (marts_incremental.py:443-451).
MAILCHIMP_SUBSCRIBERS_DDL = """
    _raw_id STRING,
    subscriber_id_token STRING,
    email_address_token STRING,
    email_normalized_token STRING,
    email_type STRING,
    status STRING,
    first_name_token STRING,
    last_name_token STRING,
    full_name_token STRING,
    last_name_prefix_token STRING,
    phone_token STRING,
    phone_normalized_token STRING,
    stats STRING,
    avg_open_rate DECIMAL(5, 4),
    avg_click_rate DECIMAL(5, 4),
    list_id STRING,
    tags STRING,
    ip_signup STRING,
    signup_timestamp TIMESTAMP,
    ip_opt STRING,
    timestamp_opt TIMESTAMP,
    last_changed TIMESTAMP,
    language STRING,
    vip BOOLEAN,
    source STRING,
    sms_status STRING,
    has_sms BOOLEAN,
    is_active BOOLEAN,
    days_since_signup INT,
    _webhook_received_at TIMESTAMP,
    _webhook_event_type STRING,
    _loaded_at TIMESTAMP,
    _staged_at TIMESTAMP
"""

# Full production DDL for staging.stg_mailchimp_events
# (jobs/spark/staging_batch.py::stage_mailchimp_events). Left empty in this
# test -- build_customer_360 only aggregates it, so an empty table just needs
# to exist for the LEFT JOIN, and has_mailchimp comes from the subscriber row.
MAILCHIMP_EVENTS_DDL = """
    _raw_id STRING,
    event_id STRING,
    campaign_id STRING,
    email_id_token STRING,
    email_address_token STRING,
    email_normalized_token STRING,
    action STRING,
    event_timestamp TIMESTAMP,
    event_date DATE,
    url STRING,
    ip STRING,
    user_agent STRING,
    location STRING,
    location_country STRING,
    location_region STRING,
    bounce_type STRING,
    list_id STRING,
    is_sms_event BOOLEAN,
    is_positive_engagement BOOLEAN,
    is_negative_event BOOLEAN,
    _webhook_received_at TIMESTAMP,
    _webhook_event_type STRING,
    _loaded_at TIMESTAMP,
    _staged_at TIMESTAMP
"""

# Mirrors jobs/spark/staging_batch.py::compute_ga4_sessions -- identical to
# tests/pipeline_tables.py's STAGING_TABLE_DDL entry, restated here so this
# file does not depend on the `pipeline_tables` fixture (see below).
GA4_SESSIONS_DDL = """
    session_id STRING, client_id STRING, user_id_token STRING, session_start TIMESTAMP,
    session_end TIMESTAMP, session_duration_sec INT, event_count INT,
    page_view_count INT, is_engaged_session BOOLEAN, traffic_source STRING,
    traffic_medium STRING, traffic_campaign STRING, channel_group STRING,
    landing_page STRING, exit_page STRING, device_category STRING,
    device_os STRING, geo_country STRING, geo_region STRING,
    total_engagement_ms BIGINT, conversions INT, total_value DECIMAL(18, 2),
    session_date DATE, is_bounce BOOLEAN, _loaded_at TIMESTAMP, _staged_at TIMESTAMP
"""


def token_for(spark, value, pii_class):
    df = spark.range(1).withColumn("v", lit(value))
    return df.select(
        token_expr(normalize("v", pii_class), pii_class, PEPPER).alias("t")
    ).collect()[0]["t"]


def _create(spark, table, ddl):
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    spark.sql(f"CREATE TABLE {table} ({ddl}) USING iceberg")


def customer_360_fixture(spark):
    """
    Seed one customer with a matching email_token in customer_metrics, a
    Mailchimp subscriber, and a GA4 session -- deliberately NOT using the
    `pipeline_tables` fixture, whose STAGING_TABLE_DDL for
    stg_mailchimp_subscribers is narrower than build_customer_360 needs (see
    MAILCHIMP_SUBSCRIBERS_DDL above) and would make the Mailchimp join except
    out for the wrong reason -- a missing column, not a join-key bug -- which
    would defeat the point of this test.
    """
    email_token = token_for(spark, CUSTOMER_EMAIL, "email")

    # build_customer_360 only takes the "join core.customers" branch when
    # this table exists; absent, it falls back to metrics-only (simpler,
    # sufficient for this test) -- but be defensive against another test in
    # the session having left one behind.
    spark.sql("DROP TABLE IF EXISTS iceberg.core.customers")
    spark.sql("DROP TABLE IF EXISTS iceberg.staging.stg_stripe_charges")

    _create(spark, "iceberg.analytics.customer_metrics", CUSTOMER_METRICS_DDL)
    insert_rows(spark, "iceberg.analytics.customer_metrics", [{
        "customer_id": "cust-360-1",
        "email_token": email_token,
        "full_name_token": token_for(spark, "Ada Lovelace", "name"),
        "customer_tier": "gold",
        "lifecycle_stage": "customer",
        "customer_segment": "loyal",
        "rfm_segment": "Champions",
        "total_spent": 100.0,
        "total_orders": 2,
        "avg_order_value": 50.0,
        "source_count": 1,
        "has_shopify": True,
        "has_hubspot": False,
        "rfm_recency_score": 5,
        "rfm_frequency_score": 5,
        "rfm_monetary_score": 5,
        "accepts_marketing": True,
        "customer_created_at": STAGED_AT,
        "customer_updated_at": STAGED_AT,
    }])

    _create(spark, "iceberg.staging.stg_mailchimp_subscribers", MAILCHIMP_SUBSCRIBERS_DDL)
    insert_rows(spark, "iceberg.staging.stg_mailchimp_subscribers", [{
        "subscriber_id_token": token_for(spark, "leak-check-subscriber-360", "mailchimp_id"),
        "email_normalized_token": email_token,
        "status": "subscribed",
        "avg_open_rate": 0.5,
        "avg_click_rate": 0.1,
        "has_sms": False,
        "_staged_at": STAGED_AT,
    }])

    _create(spark, "iceberg.staging.stg_mailchimp_events", MAILCHIMP_EVENTS_DDL)

    _create(spark, "iceberg.staging.stg_ga4_sessions", GA4_SESSIONS_DDL)
    insert_rows(spark, "iceberg.staging.stg_ga4_sessions", [{
        "session_id": "s1",
        "client_id": "c1",
        "user_id_token": email_token,
        "session_start": STAGED_AT,
        "page_view_count": 3,
        "is_engaged_session": True,
        "is_bounce": False,
    }])


def teardown_customer_360_fixture(spark):
    for table in (
        "iceberg.marts.customer_360",
        "iceberg.analytics.customer_metrics",
        "iceberg.staging.stg_mailchimp_subscribers",
        "iceberg.staging.stg_mailchimp_events",
        "iceberg.staging.stg_ga4_sessions",
    ):
        spark.sql(f"DROP TABLE IF EXISTS {table}")


def test_build_customer_360_joins_mailchimp_and_ga4(spark):
    from jobs.spark.marts_incremental import build_customer_360

    customer_360_fixture(spark)
    try:
        record_count = build_customer_360(spark, mode="full")
        assert record_count == 1

        row = spark.table("iceberg.marts.customer_360") \
            .where("customer_id = 'cust-360-1'").collect()[0]

        assert row["has_mailchimp"] is True, (
            "Mailchimp join produced no match -- the email_token join key at "
            "marts_incremental.py:456-461 is broken, and its except block "
            "silently swallowed the failure"
        )
        assert row["mailchimp_subscriber_id_token"] is not None
        assert row["mailchimp_status"] == "subscribed"

        assert row["has_ga4"] is True, (
            "GA4 join produced no match -- the user_id_token join key at "
            "marts_incremental.py:502-507 is broken, and its except block "
            "silently swallowed the failure"
        )
        assert row["ga4_total_sessions"] == 1
    finally:
        teardown_customer_360_fixture(spark)
