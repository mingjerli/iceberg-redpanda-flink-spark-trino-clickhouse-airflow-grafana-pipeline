"""
Airflow DAG: Iceberg Data Pipeline
==================================

Batch data pipeline for processing Shopify, Stripe, and HubSpot data through
staging, semantic, core, analytics, and marts layers using Apache Iceberg.

Layer flow:
    raw (Flink streaming) -> staging -> semantic -> core -> analytics -> marts

Each layer has specific Spark jobs:
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
    "--conf spark.hadoop.fs.s3a.access.key=admin "
    "--conf spark.hadoop.fs.s3a.secret.key=admin123 "
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
    description="Iceberg data pipeline: staging -> semantic -> core -> analytics -> marts",
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
    stg_hubspot_contacts = BashOperator(
        task_id="stg_hubspot_contacts",
        bash_command=f"{SPARK_SUBMIT} {SPARK_JOBS_PATH}/staging_batch.py --table hubspot_contacts --mode incremental",
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

    # -------------------------------------------------------------------------
    # Dependencies
    # -------------------------------------------------------------------------

    # Staging: parallel from start
    start >> [stg_shopify_orders, stg_shopify_customers, stg_stripe_charges, stg_hubspot_contacts]

    # Semantic: needs customer data
    [stg_shopify_customers, stg_hubspot_contacts] >> entity_index
    entity_index >> blocking_index

    # Core: needs semantic + staging
    [entity_index, stg_shopify_customers, stg_hubspot_contacts] >> core_customers
    [stg_shopify_orders, core_customers] >> core_orders

    # Analytics: needs core
    [core_customers, core_orders] >> customer_metrics
    core_orders >> order_summary
    stg_stripe_charges >> payment_metrics

    # Marts: needs analytics
    customer_metrics >> customer_360
    [order_summary, payment_metrics] >> sales_dashboard

    # End
    [customer_360, sales_dashboard] >> end
