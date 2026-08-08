"""
Airflow DAG: Iceberg Data Pipeline
==================================

Batch data pipeline for processing Shopify, Stripe, HubSpot, Mailchimp, and GA4 data
through staging, semantic, core, analytics, and marts layers using Apache Iceberg.

Layer flow:
    raw (Flink streaming or batch) -> staging -> semantic -> core -> analytics -> marts

Each layer has specific Spark jobs:
- ga4_batch_ingest.py: GA4 Parquet -> Raw (batch ingestion)
- staging_batch.py: Raw -> Staging transforms
- entity_backfill.py: Entity resolution
- core_views.py: Unified business objects
- analytics_incremental.py: Metrics and aggregations
- marts_incremental.py: Dashboard-ready tables
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator


# =============================================================================
# Configuration
# =============================================================================

SPARK_JOBS_PATH = "/opt/spark/jobs"
SPARK_CONTAINER = "iceberg-spark-master"
SPARK_MASTER = os.environ.get("SPARK_MASTER", "spark://spark-master:7077")

# Get credentials from environment variables (set in docker-compose.yml)
MINIO_ROOT_USER = os.environ.get("MINIO_ROOT_USER", "admin")
MINIO_ROOT_PASSWORD = os.environ.get("MINIO_ROOT_PASSWORD", "admin123")

# GA4 arrives as Parquet on the volume mounted into the Spark containers,
# standing in for a BigQuery Export. Default matches infrastructure/.env.example.
GA4_EXPORT_PATH = os.environ.get("GA4_EXPORT_PATH", "/opt/spark/data/ga4/events.parquet")

SPARK_SUBMIT = (
    f"docker exec {SPARK_CONTAINER} /opt/spark/bin/spark-submit "
    f"--master {SPARK_MASTER} "
    "--deploy-mode client "
    "--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions "
    "--conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog "
    "--conf spark.sql.catalog.iceberg.type=rest "
    "--conf spark.sql.catalog.iceberg.uri=http://iceberg-rest:8181 "
    "--conf spark.sql.catalog.iceberg.warehouse=s3a://warehouse/ "
    "--conf spark.sql.catalog.iceberg.io-impl=org.apache.iceberg.aws.s3.S3FileIO "
    "--conf spark.sql.catalog.iceberg.s3.endpoint=http://minio:9000 "
    "--conf spark.sql.catalog.iceberg.s3.path-style-access=true "
    "--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 "
    f"--conf spark.hadoop.fs.s3a.access.key={MINIO_ROOT_USER} "
    f"--conf spark.hadoop.fs.s3a.secret.key={MINIO_ROOT_PASSWORD} "
    "--conf spark.hadoop.fs.s3a.path.style.access=true "
    "--conf spark.executor.memory=2g "
    "--conf spark.driver.memory=2g"
)

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}


# =============================================================================
# DAG Definition
# =============================================================================

with DAG(
    dag_id="iceberg_pipeline",
    description="Iceberg data pipeline: staging -> semantic -> core -> analytics -> marts (Shopify, Stripe, HubSpot, Mailchimp, GA4)",
    default_args=default_args,
    schedule="0 */4 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=4,
    tags=["iceberg", "incremental", "batch"],
    doc_md=__doc__,
    is_paused_upon_creation=False,
) as dag:

    # Markers
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    # -------------------------------------------------------------------------
    # Staging Layer: raw -> staging
    # These can run in parallel (no dependencies between them)
    # -------------------------------------------------------------------------
    stg_shopify_orders = BashOperator(
        task_id="stg_shopify_orders",
        bash_command=f"{SPARK_SUBMIT} {SPARK_JOBS_PATH}/staging_batch.py --table shopify_orders --mode incremental",
    )
    stg_shopify_customers = BashOperator(
        task_id="stg_shopify_customers",
        bash_command=f"{SPARK_SUBMIT} {SPARK_JOBS_PATH}/staging_batch.py --table shopify_customers --mode incremental",
    )
    stg_stripe_charges = BashOperator(
        task_id="stg_stripe_charges",
        bash_command=f"{SPARK_SUBMIT} {SPARK_JOBS_PATH}/staging_batch.py --table stripe_charges --mode incremental",
    )
    stg_stripe_customers = BashOperator(
        task_id="stg_stripe_customers",
        bash_command=f"{SPARK_SUBMIT} {SPARK_JOBS_PATH}/staging_batch.py --table stripe_customers --mode incremental",
    )
    stg_hubspot_contacts = BashOperator(
        task_id="stg_hubspot_contacts",
        bash_command=f"{SPARK_SUBMIT} {SPARK_JOBS_PATH}/staging_batch.py --table hubspot_contacts --mode incremental",
    )
    stg_mailchimp_campaigns = BashOperator(
        task_id="stg_mailchimp_campaigns",
        bash_command=f"{SPARK_SUBMIT} {SPARK_JOBS_PATH}/staging_batch.py --table mailchimp_campaigns --mode incremental",
    )
    stg_mailchimp_events = BashOperator(
        task_id="stg_mailchimp_events",
        bash_command=f"{SPARK_SUBMIT} {SPARK_JOBS_PATH}/staging_batch.py --table mailchimp_events --mode incremental",
    )
    stg_mailchimp_subscribers = BashOperator(
        task_id="stg_mailchimp_subscribers",
        bash_command=f"{SPARK_SUBMIT} {SPARK_JOBS_PATH}/staging_batch.py --table mailchimp_subscribers --mode incremental",
    )

    # -------------------------------------------------------------------------
    # GA4 Batch Ingestion: Parquet -> Raw
    # -------------------------------------------------------------------------
    ga4_batch_ingest = BashOperator(
        task_id="ga4_batch_ingest",
        # --input is required; MERGE INTO on _raw_id makes re-runs idempotent,
        # so append is the correct mode for a scheduled task.
        bash_command=(
            f"{SPARK_SUBMIT} {SPARK_JOBS_PATH}/ga4_batch_ingest.py "
            f"--input {GA4_EXPORT_PATH} --mode append"
        ),
    )

    # -------------------------------------------------------------------------
    # GA4 Staging: Raw -> Staging
    # -------------------------------------------------------------------------
    # --table takes the STAGING_FUNCTIONS key, which carries no stg_ prefix
    # (`ga4_events`, not `stg_ga4_events`). The prefixed form is the Iceberg
    # table name; passing it here is rejected by argparse before Spark starts.
    stg_ga4_events = BashOperator(
        task_id="stg_ga4_events",
        bash_command=f"{SPARK_SUBMIT} {SPARK_JOBS_PATH}/staging_batch.py --table ga4_events --mode incremental",
    )
    stg_ga4_sessions = BashOperator(
        task_id="stg_ga4_sessions",
        bash_command=f"{SPARK_SUBMIT} {SPARK_JOBS_PATH}/staging_batch.py --table ga4_sessions --mode incremental",
    )

    # -------------------------------------------------------------------------
    # Semantic Layer: entity resolution
    # Depends on customer data from staging
    # -------------------------------------------------------------------------
    entity_index = BashOperator(
        task_id="entity_index",
        bash_command=f"{SPARK_SUBMIT} {SPARK_JOBS_PATH}/entity_backfill.py --mode initial",
    )
    blocking_index = BashOperator(
        task_id="blocking_index",
        bash_command=f"{SPARK_SUBMIT} {SPARK_JOBS_PATH}/entity_resolution_fuzzy.py",
    )

    # -------------------------------------------------------------------------
    # Core Layer: unified business objects
    # Depends on semantic + staging
    # -------------------------------------------------------------------------
    core_customers = BashOperator(
        task_id="core_customers",
        bash_command=f"{SPARK_SUBMIT} {SPARK_JOBS_PATH}/core_views.py --view customers",
    )
    core_orders = BashOperator(
        task_id="core_orders",
        bash_command=f"{SPARK_SUBMIT} {SPARK_JOBS_PATH}/core_views.py --view orders",
    )

    # -------------------------------------------------------------------------
    # Analytics Layer: aggregations and metrics
    # Depends on core
    # -------------------------------------------------------------------------
    customer_metrics = BashOperator(
        task_id="customer_metrics",
        bash_command=f"{SPARK_SUBMIT} {SPARK_JOBS_PATH}/analytics_incremental.py --table customer_metrics --mode incremental",
    )
    order_summary = BashOperator(
        task_id="order_summary",
        bash_command=f"{SPARK_SUBMIT} {SPARK_JOBS_PATH}/analytics_incremental.py --table order_summary --mode incremental",
    )
    payment_metrics = BashOperator(
        task_id="payment_metrics",
        bash_command=f"{SPARK_SUBMIT} {SPARK_JOBS_PATH}/analytics_incremental.py --table payment_metrics --mode incremental",
    )
    campaign_metrics = BashOperator(
        task_id="campaign_metrics",
        bash_command=f"{SPARK_SUBMIT} {SPARK_JOBS_PATH}/analytics_incremental.py --table campaign_metrics --mode incremental",
    )

    # GA4 Analytics
    ga4_engagement_metrics = BashOperator(
        task_id="ga4_engagement_metrics",
        bash_command=f"{SPARK_SUBMIT} {SPARK_JOBS_PATH}/analytics_incremental.py --table ga4_engagement_metrics --mode incremental",
    )
    ga4_engagement_by_channel = BashOperator(
        task_id="ga4_engagement_by_channel",
        bash_command=f"{SPARK_SUBMIT} {SPARK_JOBS_PATH}/analytics_incremental.py --table ga4_engagement_by_channel --mode incremental",
    )
    ga4_page_performance = BashOperator(
        task_id="ga4_page_performance",
        bash_command=f"{SPARK_SUBMIT} {SPARK_JOBS_PATH}/analytics_incremental.py --table ga4_page_performance --mode incremental",
    )
    ga4_funnel_analysis = BashOperator(
        task_id="ga4_funnel_analysis",
        bash_command=f"{SPARK_SUBMIT} {SPARK_JOBS_PATH}/analytics_incremental.py --table ga4_funnel_analysis --mode incremental",
    )

    # -------------------------------------------------------------------------
    # Marts Layer: dashboard-ready tables
    # Depends on analytics
    # -------------------------------------------------------------------------
    customer_360 = BashOperator(
        task_id="customer_360",
        bash_command=f"{SPARK_SUBMIT} {SPARK_JOBS_PATH}/marts_incremental.py --table customer_360 --mode incremental",
    )
    sales_dashboard = BashOperator(
        task_id="sales_dashboard",
        bash_command=f"{SPARK_SUBMIT} {SPARK_JOBS_PATH}/marts_incremental.py --table sales_dashboard_daily --mode incremental",
    )
    campaign_dashboard = BashOperator(
        task_id="campaign_dashboard",
        bash_command=f"{SPARK_SUBMIT} {SPARK_JOBS_PATH}/marts_incremental.py --table campaign_dashboard --mode incremental",
    )
    ga4_engagement_dashboard = BashOperator(
        task_id="ga4_engagement_dashboard",
        bash_command=f"{SPARK_SUBMIT} {SPARK_JOBS_PATH}/marts_incremental.py --table ga4_engagement_dashboard --mode incremental",
    )

    # -------------------------------------------------------------------------
    # Dependencies
    # -------------------------------------------------------------------------

    # Staging: parallel from start (webhook sources)
    start >> [stg_shopify_orders, stg_shopify_customers, stg_stripe_charges, stg_stripe_customers, stg_hubspot_contacts,
              stg_mailchimp_campaigns, stg_mailchimp_events, stg_mailchimp_subscribers]

    # GA4: batch ingestion -> staging events -> staging sessions
    start >> ga4_batch_ingest >> stg_ga4_events >> stg_ga4_sessions

    # Semantic: needs customer data from all sources (including GA4)
    [stg_shopify_customers, stg_stripe_customers, stg_hubspot_contacts, stg_mailchimp_subscribers, stg_ga4_sessions] >> entity_index
    entity_index >> blocking_index

    # Core: needs semantic + staging (Mailchimp does not feed into core)
    [entity_index, stg_shopify_customers, stg_stripe_customers, stg_hubspot_contacts] >> core_customers
    [stg_shopify_orders, core_customers] >> core_orders

    # Analytics: needs core + Mailchimp staging + GA4 staging
    [core_customers, core_orders] >> customer_metrics
    core_orders >> order_summary
    stg_stripe_charges >> payment_metrics
    [stg_mailchimp_campaigns, stg_mailchimp_events] >> campaign_metrics

    # GA4 Analytics: needs GA4 staging (parallel execution)
    [stg_ga4_events, stg_ga4_sessions] >> ga4_engagement_metrics
    [stg_ga4_events, stg_ga4_sessions] >> ga4_engagement_by_channel
    [stg_ga4_events, stg_ga4_sessions] >> ga4_page_performance
    [stg_ga4_events, stg_ga4_sessions] >> ga4_funnel_analysis

    # Marts: needs analytics
    customer_metrics >> customer_360
    [order_summary, payment_metrics] >> sales_dashboard
    campaign_metrics >> campaign_dashboard
    [ga4_engagement_metrics, ga4_engagement_by_channel, ga4_page_performance, ga4_funnel_analysis] >> ga4_engagement_dashboard

    # End
    [customer_360, sales_dashboard, campaign_dashboard, ga4_engagement_dashboard] >> end
