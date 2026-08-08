"""
Spark Batch Job: Analytics Layer Transforms
============================================

This script performs incremental analytics transforms from core/staging to analytics layer.
Supports both full refresh and incremental processing modes.

Usage:
    # Full refresh all analytics tables
    spark-submit analytics_incremental.py --mode full

    # Incremental processing all tables
    spark-submit analytics_incremental.py --mode incremental

    # Process specific table
    spark-submit analytics_incremental.py --table customer_metrics --mode incremental

    # Full refresh specific table
    spark-submit analytics_incremental.py --table order_summary --mode full
"""

import argparse
import logging
import os
from datetime import datetime, timedelta
from typing import Optional

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    avg,
    cast,
    coalesce,
    col,
    count,
    countDistinct,
    current_date,
    current_timestamp,
    datediff,
    dayofweek,
    first,
    greatest,
    hour,
    least,
    lit,
    max as spark_max,
    min as spark_min,
    quarter,
    round as spark_round,
    row_number,
    sum as spark_sum,
    weekofyear,
    when,
    year,
)
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_spark_session() -> SparkSession:
    """Create Spark session with Iceberg configuration."""
    return SparkSession.builder \
        .appName("AnalyticsIncrementalTransforms") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "rest") \
        .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181") \
        .config("spark.sql.catalog.iceberg.warehouse", "s3a://warehouse/") \
        .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_ROOT_USER", "admin")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_ROOT_PASSWORD", "admin123")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()


def get_watermark(spark: SparkSession, table_name: str) -> Optional[datetime]:
    """Get the last watermark for incremental processing."""
    try:
        result = spark.sql(f"""
            SELECT MAX(_computed_at) as last_computed
            FROM iceberg.analytics.{table_name}
        """).collect()
        if result and result[0].last_computed:
            return result[0].last_computed
    except Exception as e:
        logger.warning(f"Could not get watermark for {table_name}: {e}")
    return None


def update_watermark(spark: SparkSession, source_table: str, records_processed: int):
    """Update the watermark in the metadata table."""
    try:
        spark.sql(f"""
            INSERT INTO iceberg.metadata.incremental_watermarks
            (source_table, pipeline_name, last_sync_timestamp, records_processed, updated_at)
            VALUES (
                'analytics.{source_table}',
                'analytics_incremental',
                current_timestamp(),
                {records_processed},
                current_timestamp()
            )
        """)
        logger.info(f"Updated watermark for analytics.{source_table}")
    except Exception as e:
        logger.warning(f"Could not update watermark: {e}")


def compute_customer_metrics(spark: SparkSession, mode: str = "incremental"):
    """Compute analytics.customer_metrics from core.customers and core.orders."""
    logger.info(f"Processing customer_metrics in {mode} mode")

    # Create analytics table if not exists
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.analytics.customer_metrics (
            customer_id STRING,
            email STRING,
            full_name STRING,
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
        )
        USING iceberg
        PARTITIONED BY (customer_segment)
    """)

    # Get watermark for incremental
    watermark = None
    if mode == "incremental":
        watermark = get_watermark(spark, "customer_metrics")
        if watermark:
            logger.info(f"Incremental filter: updated_at > {watermark}")

    # Read core customers
    customers_df = spark.table("iceberg.core.customers") if spark.catalog.tableExists("iceberg.core.customers") else None

    if customers_df is None:
        logger.warning("core.customers table does not exist. Attempting to create view from staging...")
        # Try to use staging data directly if core view doesn't exist
        try:
            customers_df = spark.table("iceberg.staging.stg_shopify_customers")
            customers_df = customers_df.select(
                col("customer_id").cast("string").alias("customer_id"),
                col("email"),
                col("full_name"),
                col("customer_tier"),
                lit("customer").alias("lifecycle_stage"),
                col("total_spent"),
                col("orders_count").alias("orders_count"),
                col("avg_order_value"),
                col("accepts_marketing"),
                col("is_active"),
                coalesce(col("page_views"), lit(0)).alias("page_views"),
                coalesce(col("sessions"), lit(0)).alias("sessions"),
                lit(None).cast("string").alias("analytics_source"),
                col("created_at"),
                col("updated_at"),
                lit(1).alias("source_count"),
                lit(True).alias("has_shopify"),
                lit(False).alias("has_hubspot"),
                lit(False).alias("is_engaged")
            )
        except Exception as e:
            logger.error(f"Could not read staging data: {e}")
            return 0

    # Apply watermark filter
    if watermark and mode == "incremental":
        customers_df = customers_df.filter(col("updated_at") > watermark)

    record_count = customers_df.count()
    if record_count == 0:
        logger.info("No new records to process")
        return 0

    logger.info(f"Processing {record_count} customer records")

    # Get order aggregates
    try:
        orders_df = spark.table("iceberg.core.orders") if spark.catalog.tableExists("iceberg.core.orders") else None
        if orders_df is None:
            orders_df = spark.table("iceberg.staging.stg_shopify_orders")
            orders_df = orders_df.select(
                col("customer_id").cast("string").alias("customer_id"),
                col("order_id"),
                col("order_status"),
                col("total_price"),
                col("created_at").alias("order_date"),
                col("is_test")
            )

        # Aggregate order stats per customer
        order_stats = orders_df.filter(
            (col("order_status") != "cancelled") &
            (col("is_test") == False)
        ).groupBy("customer_id").agg(
            count("*").alias("total_orders"),
            spark_min(col("order_date").cast("date")).alias("first_order_date"),
            spark_max(col("order_date").cast("date")).alias("last_order_date"),
            first(col("total_price")).alias("first_order_value"),
            spark_max(col("total_price")).alias("last_order_value")
        )
    except Exception as e:
        logger.warning(f"Could not aggregate orders: {e}")
        order_stats = None

    # Join customers with order stats
    if order_stats is not None:
        metrics_df = customers_df.join(order_stats, "customer_id", "left")
    else:
        metrics_df = customers_df.withColumn("total_orders", lit(0).cast("bigint")) \
            .withColumn("first_order_date", lit(None).cast("date")) \
            .withColumn("last_order_date", lit(None).cast("date")) \
            .withColumn("first_order_value", lit(None).cast("decimal(18,2)")) \
            .withColumn("last_order_value", lit(None).cast("decimal(18,2)"))

    # Compute derived metrics
    metrics_df = metrics_df.select(
        col("customer_id"),
        col("email"),
        col("full_name"),
        coalesce(col("customer_tier"), lit("new")).alias("customer_tier"),
        coalesce(col("lifecycle_stage"), lit("prospect")).alias("lifecycle_stage"),
        # Derived segment
        when(
            (coalesce(col("total_spent"), lit(0)) >= 1000) &
            (coalesce(col("page_views"), lit(0)) > 10), "high_value_engaged"
        ).when(
            coalesce(col("total_spent"), lit(0)) >= 1000, "high_value"
        ).when(
            coalesce(col("page_views"), lit(0)) > 10, "engaged"
        ).when(
            coalesce(col("total_orders"), lit(0)) > 0, "active"
        ).otherwise("inactive").alias("customer_segment"),

        # Financial
        coalesce(col("total_spent"), lit(0)).cast("decimal(18,2)").alias("total_spent"),
        coalesce(col("total_orders"), lit(0)).cast("bigint").alias("total_orders"),
        coalesce(col("avg_order_value"), lit(0)).cast("decimal(18,2)").alias("avg_order_value"),
        col("first_order_value").cast("decimal(18,2)"),
        col("last_order_value").cast("decimal(18,2)"),
        # Simple LTV estimate
        (coalesce(col("avg_order_value"), lit(0)) * 4).cast("decimal(18,2)").alias("estimated_ltv"),

        # Order dates
        col("first_order_date"),
        col("last_order_date"),
        datediff(current_date(), col("first_order_date")).alias("days_since_first_order"),
        datediff(current_date(), col("last_order_date")).alias("days_since_last_order"),
        when(col("total_orders") > 1,
             datediff(col("last_order_date"), col("first_order_date")) / (col("total_orders") - 1)
        ).otherwise(lit(None)).cast("decimal(10,2)").alias("order_frequency_days"),

        # Engagement
        coalesce(col("page_views"), lit(0)).cast("bigint").alias("page_views"),
        coalesce(col("sessions"), lit(0)).cast("bigint").alias("sessions"),
        least(lit(100), coalesce(col("page_views"), lit(0)) * 2 + coalesce(col("sessions"), lit(0)) * 10).cast("decimal(5,2)").alias("engagement_score"),

        # RFM Scores
        when(datediff(current_date(), col("last_order_date")) <= 30, 5)
        .when(datediff(current_date(), col("last_order_date")) <= 60, 4)
        .when(datediff(current_date(), col("last_order_date")) <= 90, 3)
        .when(datediff(current_date(), col("last_order_date")) <= 180, 2)
        .otherwise(1).alias("rfm_recency_score"),

        when(coalesce(col("total_orders"), lit(0)) >= 10, 5)
        .when(coalesce(col("total_orders"), lit(0)) >= 5, 4)
        .when(coalesce(col("total_orders"), lit(0)) >= 3, 3)
        .when(coalesce(col("total_orders"), lit(0)) >= 2, 2)
        .otherwise(1).alias("rfm_frequency_score"),

        when(coalesce(col("total_spent"), lit(0)) >= 1000, 5)
        .when(coalesce(col("total_spent"), lit(0)) >= 500, 4)
        .when(coalesce(col("total_spent"), lit(0)) >= 200, 3)
        .when(coalesce(col("total_spent"), lit(0)) >= 50, 2)
        .otherwise(1).alias("rfm_monetary_score"),

        # RFM Segment
        when(
            (datediff(current_date(), col("last_order_date")) <= 30) &
            (coalesce(col("total_orders"), lit(0)) >= 5) &
            (coalesce(col("total_spent"), lit(0)) >= 500), "Champions"
        ).when(
            (datediff(current_date(), col("last_order_date")) <= 60) &
            (coalesce(col("total_orders"), lit(0)) >= 3), "Loyal"
        ).when(
            (datediff(current_date(), col("last_order_date")) <= 30) &
            (coalesce(col("total_orders"), lit(0)) == 1), "New"
        ).when(
            (datediff(current_date(), col("last_order_date")) > 60) &
            (datediff(current_date(), col("last_order_date")) <= 180), "At Risk"
        ).when(
            datediff(current_date(), col("last_order_date")) > 180, "Lost"
        ).when(
            coalesce(col("total_orders"), lit(0)) == 0, "Prospects"
        ).otherwise("Potential").alias("rfm_segment"),

        # Marketing
        coalesce(col("accepts_marketing"), lit(False)).alias("accepts_marketing"),
        col("analytics_source").alias("acquisition_source"),

        # Source attribution
        col("source_count"),
        col("has_shopify"),
        col("has_hubspot"),

        # Cohort
        when(col("first_order_date").isNotNull(),
             col("first_order_date").cast("string").substr(1, 7)
        ).alias("first_order_cohort"),
        col("created_at").cast("string").substr(1, 7).alias("signup_cohort"),

        # Timestamps
        col("created_at").alias("customer_created_at"),
        col("updated_at").alias("customer_updated_at"),
        current_timestamp().alias("_computed_at"),
        lit(1).alias("_version")
    )

    # Write to analytics table using MERGE
    metrics_df.createOrReplaceTempView("new_metrics")

    if mode == "full":
        # Full refresh - overwrite
        metrics_df.write \
            .format("iceberg") \
            .mode("overwrite") \
            .saveAsTable("iceberg.analytics.customer_metrics")
    else:
        # Incremental - use MERGE
        spark.sql("""
            MERGE INTO iceberg.analytics.customer_metrics AS target
            USING new_metrics AS source
            ON target.customer_id = source.customer_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

    logger.info(f"Successfully computed customer_metrics for {record_count} customers")
    update_watermark(spark, "customer_metrics", record_count)
    return record_count


def compute_order_summary(spark: SparkSession, mode: str = "incremental"):
    """Compute analytics.order_summary from core.orders."""
    logger.info(f"Processing order_summary in {mode} mode")

    # Create analytics table if not exists
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.analytics.order_summary (
            order_date DATE,
            order_hour INT,
            shipping_country STRING,
            shipping_country_code STRING,
            shipping_state STRING,
            channel STRING,
            source STRING,
            total_orders BIGINT,
            completed_orders BIGINT,
            cancelled_orders BIGINT,
            refunded_orders BIGINT,
            pending_orders BIGINT,
            unique_customers BIGINT,
            new_customers BIGINT,
            returning_customers BIGINT,
            gross_revenue DECIMAL(18, 2),
            net_revenue DECIMAL(18, 2),
            total_discounts DECIMAL(18, 2),
            total_shipping DECIMAL(18, 2),
            total_tax DECIMAL(18, 2),
            avg_order_value DECIMAL(18, 2),
            avg_discount_per_order DECIMAL(18, 2),
            avg_items_per_order DECIMAL(10, 2),
            orders_with_discount BIGINT,
            discount_rate DECIMAL(5, 4),
            _computed_at TIMESTAMP,
            _partition_key STRING
        )
        USING iceberg
        PARTITIONED BY (order_date)
    """)

    # Get watermark for incremental
    watermark = None
    if mode == "incremental":
        watermark = get_watermark(spark, "order_summary")
        if watermark:
            logger.info(f"Incremental filter: staged_at > {watermark}")

    # Read orders
    try:
        orders_df = spark.table("iceberg.core.orders") if spark.catalog.tableExists("iceberg.core.orders") else None
        if orders_df is None:
            orders_df = spark.table("iceberg.staging.stg_shopify_orders")
            orders_df = orders_df.withColumn("customer_id", col("customer_id").cast("string")) \
                .withColumn("source", lit("shopify")) \
                .withColumn("channel", coalesce(col("source_name"), lit("web"))) \
                .withColumn("order_date", col("created_at")) \
                .withColumn("discount_amount", col("total_discounts")) \
                .withColumn("shipping_amount", col("total_shipping")) \
                .withColumn("tax_amount", col("total_tax"))
    except Exception as e:
        logger.error(f"Could not read orders: {e}")
        return 0

    # Apply watermark filter (core.orders uses staged_at)
    if watermark and mode == "incremental":
        orders_df = orders_df.filter(col("staged_at") > watermark)

    # Filter test orders
    orders_df = orders_df.filter(col("is_test") == False)

    record_count = orders_df.count()
    if record_count == 0:
        logger.info("No new records to process")
        return 0

    logger.info(f"Processing {record_count} order records")

    # Aggregate by dimensions
    summary_df = orders_df.groupBy(
        col("order_date").cast("date").alias("order_date"),
        hour(col("created_at")).alias("order_hour"),
        coalesce(col("shipping_country"), lit("Unknown")).alias("shipping_country"),
        coalesce(col("shipping_country_code"), lit("XX")).alias("shipping_country_code"),
        coalesce(col("shipping_state"), lit("Unknown")).alias("shipping_state"),
        coalesce(col("channel"), lit("Unknown")).alias("channel"),
        col("source")
    ).agg(
        count("*").alias("total_orders"),
        spark_sum(when(col("order_status") == "completed", 1).otherwise(0)).alias("completed_orders"),
        spark_sum(when(col("order_status") == "cancelled", 1).otherwise(0)).alias("cancelled_orders"),
        spark_sum(when(col("order_status") == "refunded", 1).otherwise(0)).alias("refunded_orders"),
        spark_sum(when(col("order_status").isin("pending_payment", "processing", "open"), 1).otherwise(0)).alias("pending_orders"),
        countDistinct("customer_id").alias("unique_customers"),
        lit(0).cast("bigint").alias("new_customers"),  # Would need window function
        lit(0).cast("bigint").alias("returning_customers"),  # Would need window function
        spark_sum(col("total_price")).alias("gross_revenue"),
        spark_sum(when(~col("order_status").isin("cancelled", "refunded"), col("total_price")).otherwise(0)).alias("net_revenue"),
        spark_sum(coalesce(col("discount_amount"), lit(0))).alias("total_discounts"),
        spark_sum(coalesce(col("shipping_amount"), lit(0))).alias("total_shipping"),
        spark_sum(coalesce(col("tax_amount"), lit(0))).alias("total_tax"),
        spark_round(avg(col("total_price")), 2).alias("avg_order_value"),
        spark_round(avg(coalesce(col("discount_amount"), lit(0))), 2).alias("avg_discount_per_order"),
        spark_round(avg(coalesce(col("line_item_count"), lit(1))), 2).alias("avg_items_per_order"),
        spark_sum(when(col("has_discount") == True, 1).otherwise(0)).alias("orders_with_discount")
    ).withColumn(
        "discount_rate",
        spark_round(col("orders_with_discount").cast("decimal(10,4)") / col("total_orders"), 4)
    ).withColumn(
        "_computed_at", current_timestamp()
    ).withColumn(
        "_partition_key",
        col("order_date").cast("string")
    )

    # Write to analytics table
    if mode == "full":
        summary_df.write \
            .format("iceberg") \
            .mode("overwrite") \
            .saveAsTable("iceberg.analytics.order_summary")
    else:
        # Incremental - overwrite partitions
        summary_df.write \
            .format("iceberg") \
            .mode("overwrite") \
            .option("overwrite-mode", "dynamic") \
            .saveAsTable("iceberg.analytics.order_summary")

    logger.info(f"Successfully computed order_summary for {record_count} orders")
    update_watermark(spark, "order_summary", record_count)
    return record_count


def compute_payment_metrics(spark: SparkSession, mode: str = "incremental"):
    """Compute analytics.payment_metrics from staging.stg_stripe_charges."""
    logger.info(f"Processing payment_metrics in {mode} mode")

    # Create analytics table if not exists
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.analytics.payment_metrics (
            payment_date DATE,
            card_brand STRING,
            card_funding STRING,
            billing_country STRING,
            total_charges BIGINT,
            successful_charges BIGINT,
            failed_charges BIGINT,
            disputed_charges BIGINT,
            full_refunds BIGINT,
            partial_refunds BIGINT,
            gross_volume DECIMAL(18, 2),
            successful_volume DECIMAL(18, 2),
            refunded_volume DECIMAL(18, 2),
            net_volume DECIMAL(18, 2),
            fee_volume DECIMAL(18, 2),
            avg_charge_amount DECIMAL(18, 2),
            avg_refund_amount DECIMAL(18, 2),
            success_rate DECIMAL(5, 4),
            refund_rate DECIMAL(5, 4),
            dispute_rate DECIMAL(5, 4),
            avg_risk_score DECIMAL(5, 2),
            high_risk_charges BIGINT,
            unique_customers BIGINT,
            unique_payment_methods BIGINT,
            _computed_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (payment_date)
    """)

    # Get watermark for incremental
    watermark = None
    if mode == "incremental":
        watermark = get_watermark(spark, "payment_metrics")
        if watermark:
            logger.info(f"Incremental filter: staged_at > {watermark}")

    # Read charges
    try:
        charges_df = spark.table("iceberg.staging.stg_stripe_charges")
    except Exception as e:
        logger.warning(f"Could not read stripe charges: {e}")
        return 0

    # Apply watermark filter
    if watermark and mode == "incremental":
        charges_df = charges_df.filter(col("_staged_at") > watermark)

    # Filter test charges
    charges_df = charges_df.filter(col("is_live") == True)

    record_count = charges_df.count()
    if record_count == 0:
        logger.info("No new records to process")
        return 0

    logger.info(f"Processing {record_count} charge records")

    # Aggregate by dimensions
    metrics_df = charges_df.groupBy(
        col("created_at").cast("date").alias("payment_date"),
        coalesce(col("card_brand"), lit("Unknown")).alias("card_brand"),
        coalesce(col("card_funding"), lit("Unknown")).alias("card_funding"),
        coalesce(col("billing_country"), lit("Unknown")).alias("billing_country")
    ).agg(
        count("*").alias("total_charges"),
        spark_sum(when(col("is_successful") == True, 1).otherwise(0)).alias("successful_charges"),
        spark_sum(when(col("status") == "failed", 1).otherwise(0)).alias("failed_charges"),
        spark_sum(when(col("disputed") == True, 1).otherwise(0)).alias("disputed_charges"),
        spark_sum(when(col("is_fully_refunded") == True, 1).otherwise(0)).alias("full_refunds"),
        spark_sum(when((col("is_refunded") == True) & (col("is_fully_refunded") == False), 1).otherwise(0)).alias("partial_refunds"),
        spark_sum(col("amount")).alias("gross_volume"),
        spark_sum(when(col("is_successful") == True, col("amount_captured")).otherwise(0)).alias("successful_volume"),
        spark_sum(col("amount_refunded")).alias("refunded_volume"),
        spark_sum(when(col("is_successful") == True, col("net_amount")).otherwise(0)).alias("net_volume"),
        spark_sum(coalesce(col("application_fee_amount"), lit(0))).alias("fee_volume"),
        spark_round(avg(col("amount")), 2).alias("avg_charge_amount"),
        spark_round(avg(when(col("amount_refunded") > 0, col("amount_refunded"))), 2).alias("avg_refund_amount"),
        countDistinct("customer_id").alias("unique_customers"),
        countDistinct("payment_method_id").alias("unique_payment_methods"),
        avg(col("risk_score").cast("decimal(5,2)")).alias("avg_risk_score"),
        spark_sum(when(col("risk_level").isin("elevated", "highest"), 1).otherwise(0)).alias("high_risk_charges")
    ).withColumn(
        "success_rate",
        spark_round(col("successful_charges").cast("decimal(10,4)") / col("total_charges"), 4)
    ).withColumn(
        "refund_rate",
        spark_round(
            (col("full_refunds") + col("partial_refunds")).cast("decimal(10,4)") /
            when(col("successful_charges") > 0, col("successful_charges")).otherwise(1),
            4
        )
    ).withColumn(
        "dispute_rate",
        spark_round(
            col("disputed_charges").cast("decimal(10,4)") /
            when(col("successful_charges") > 0, col("successful_charges")).otherwise(1),
            4
        )
    ).withColumn(
        "_computed_at", current_timestamp()
    )

    # Write to analytics table
    if mode == "full":
        metrics_df.write \
            .format("iceberg") \
            .mode("overwrite") \
            .saveAsTable("iceberg.analytics.payment_metrics")
    else:
        metrics_df.write \
            .format("iceberg") \
            .mode("overwrite") \
            .option("overwrite-mode", "dynamic") \
            .saveAsTable("iceberg.analytics.payment_metrics")

    logger.info(f"Successfully computed payment_metrics for {record_count} charges")
    update_watermark(spark, "payment_metrics", record_count)
    return record_count


def compute_campaign_metrics(spark: SparkSession, mode: str = "incremental"):
    """Compute analytics.campaign_metrics from staging Mailchimp campaigns and events."""
    logger.info(f"Processing campaign_metrics in {mode} mode")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.analytics.campaign_metrics (
            campaign_id STRING,
            campaign_type STRING,
            subject_line STRING,
            send_time TIMESTAMP,
            list_id STRING,
            is_sms BOOLEAN,
            is_automated BOOLEAN,
            total_sent INT,
            total_delivered INT,
            total_opens BIGINT,
            unique_opens INT,
            total_clicks BIGINT,
            unique_clicks INT,
            total_bounces INT,
            hard_bounces BIGINT,
            soft_bounces BIGINT,
            total_unsubscribes INT,
            sms_sent BIGINT,
            sms_clicks BIGINT,
            delivery_rate DECIMAL(5, 4),
            open_rate DECIMAL(5, 4),
            click_rate DECIMAL(5, 4),
            click_to_open_rate DECIMAL(5, 4),
            bounce_rate DECIMAL(5, 4),
            unsubscribe_rate DECIMAL(5, 4),
            sms_click_rate DECIMAL(5, 4),
            engagement_score DECIMAL(7, 2),
            performance_tier STRING,
            _computed_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (months(send_time))
    """)

    watermark = None
    if mode == "incremental":
        watermark = get_watermark(spark, "campaign_metrics")
        if watermark:
            logger.info(f"Incremental filter: _staged_at > {watermark}")

    try:
        campaigns_df = spark.table("iceberg.staging.stg_mailchimp_campaigns")
    except Exception as e:
        logger.warning(f"Could not read stg_mailchimp_campaigns: {e}")
        return 0

    if watermark and mode == "incremental":
        campaigns_df = campaigns_df.filter(col("_staged_at") > watermark)

    record_count = campaigns_df.count()
    if record_count == 0:
        logger.info("No new records to process")
        return 0

    logger.info(f"Processing {record_count} campaign records")

    # Aggregate events per campaign
    try:
        events_df = spark.table("iceberg.staging.stg_mailchimp_events")
        event_agg = events_df.groupBy("campaign_id").agg(
            count("*").alias("total_events"),
            spark_sum(when(col("action") == "open", 1).otherwise(0)).alias("event_opens"),
            spark_sum(when(col("action") == "click", 1).otherwise(0)).alias("event_clicks"),
            spark_sum(when(col("action") == "bounce", 1).otherwise(0)).alias("event_bounces"),
            spark_sum(when(col("bounce_type") == "hard", 1).otherwise(0)).alias("hard_bounces"),
            spark_sum(when(col("bounce_type") == "soft", 1).otherwise(0)).alias("soft_bounces"),
            spark_sum(when(col("action") == "unsub", 1).otherwise(0)).alias("event_unsubs"),
            spark_sum(when(col("action") == "sms_sent", 1).otherwise(0)).alias("sms_sent"),
            spark_sum(when(col("action") == "sms_click", 1).otherwise(0)).alias("sms_clicks")
        )
    except Exception:
        event_agg = None
        logger.warning("Could not read stg_mailchimp_events for aggregation")

    # Join campaigns with event aggregates
    if event_agg is not None:
        joined_df = campaigns_df.join(event_agg, "campaign_id", "left")
    else:
        joined_df = campaigns_df \
            .withColumn("event_opens", lit(0).cast("bigint")) \
            .withColumn("event_clicks", lit(0).cast("bigint")) \
            .withColumn("event_bounces", lit(0).cast("bigint")) \
            .withColumn("hard_bounces", lit(0).cast("bigint")) \
            .withColumn("soft_bounces", lit(0).cast("bigint")) \
            .withColumn("event_unsubs", lit(0).cast("bigint")) \
            .withColumn("sms_sent", lit(0).cast("bigint")) \
            .withColumn("sms_clicks", lit(0).cast("bigint"))

    # Compute metrics
    total_sent_col = coalesce(col("emails_sent"), lit(0))
    total_bounces_col = coalesce(col("bounces"), lit(0))
    total_delivered_col = total_sent_col - total_bounces_col
    unique_opens_col = coalesce(col("unique_opens"), lit(0))
    unique_clicks_col = coalesce(col("unique_clicks"), lit(0))

    # Rate calculations with division-by-zero protection
    delivery_rate = when(
        total_sent_col > 0,
        spark_round(total_delivered_col.cast("decimal(10,4)") / total_sent_col, 4)
    ).otherwise(lit(None).cast("decimal(5,4)"))

    open_rate = when(
        total_sent_col > 0,
        spark_round(unique_opens_col.cast("decimal(10,4)") / total_sent_col, 4)
    ).otherwise(lit(None).cast("decimal(5,4)"))

    click_rate = when(
        total_sent_col > 0,
        spark_round(unique_clicks_col.cast("decimal(10,4)") / total_sent_col, 4)
    ).otherwise(lit(None).cast("decimal(5,4)"))

    cto_rate = when(
        unique_opens_col > 0,
        spark_round(unique_clicks_col.cast("decimal(10,4)") / unique_opens_col, 4)
    ).otherwise(lit(None).cast("decimal(5,4)"))

    bounce_rate = when(
        total_sent_col > 0,
        spark_round(total_bounces_col.cast("decimal(10,4)") / total_sent_col, 4)
    ).otherwise(lit(None).cast("decimal(5,4)"))

    unsub_rate = when(
        total_sent_col > 0,
        spark_round(coalesce(col("unsubscribes"), lit(0)).cast("decimal(10,4)") / total_sent_col, 4)
    ).otherwise(lit(None).cast("decimal(5,4)"))

    sms_click_rate = when(
        coalesce(col("sms_sent"), lit(0)) > 0,
        spark_round(coalesce(col("sms_clicks"), lit(0)).cast("decimal(10,4)") / coalesce(col("sms_sent"), lit(1)), 4)
    ).otherwise(lit(None).cast("decimal(5,4)"))

    # Engagement score: rate-based, range approximately -75 to +100
    engagement_score = spark_round(
        (coalesce(open_rate, lit(0)) * 25 +
         coalesce(click_rate, lit(0)) * 50 +
         coalesce(cto_rate, lit(0)) * 25 -
         coalesce(bounce_rate, lit(0)) * 25 -
         coalesce(unsub_rate, lit(0)) * 50).cast("decimal(7,2)"),
        2
    )

    # Performance tier from engagement score
    performance_tier = when(engagement_score > 60, lit("excellent")) \
        .when(engagement_score > 40, lit("good")) \
        .when(engagement_score > 20, lit("average")) \
        .otherwise(lit("poor"))

    metrics_df = joined_df.select(
        col("campaign_id"),
        col("campaign_type"),
        col("subject_line"),
        col("send_time"),
        col("list_id"),
        col("is_sms"),
        col("is_automated"),
        total_sent_col.alias("total_sent"),
        total_delivered_col.alias("total_delivered"),
        coalesce(col("event_opens"), col("opens"), lit(0)).alias("total_opens"),
        unique_opens_col.alias("unique_opens"),
        coalesce(col("event_clicks"), col("clicks"), lit(0)).alias("total_clicks"),
        unique_clicks_col.alias("unique_clicks"),
        total_bounces_col.alias("total_bounces"),
        coalesce(col("hard_bounces"), lit(0)).alias("hard_bounces"),
        coalesce(col("soft_bounces"), lit(0)).alias("soft_bounces"),
        coalesce(col("unsubscribes"), lit(0)).alias("total_unsubscribes"),
        coalesce(col("sms_sent"), lit(0)).alias("sms_sent"),
        coalesce(col("sms_clicks"), lit(0)).alias("sms_clicks"),
        delivery_rate.alias("delivery_rate"),
        open_rate.alias("open_rate"),
        click_rate.alias("click_rate"),
        cto_rate.alias("click_to_open_rate"),
        bounce_rate.alias("bounce_rate"),
        unsub_rate.alias("unsubscribe_rate"),
        sms_click_rate.alias("sms_click_rate"),
        engagement_score.alias("engagement_score"),
        performance_tier.alias("performance_tier"),
        current_timestamp().alias("_computed_at")
    )

    metrics_df.createOrReplaceTempView("new_campaign_metrics")

    if mode == "full":
        metrics_df.write \
            .format("iceberg") \
            .mode("overwrite") \
            .saveAsTable("iceberg.analytics.campaign_metrics")
    else:
        spark.sql("""
            MERGE INTO iceberg.analytics.campaign_metrics AS target
            USING new_campaign_metrics AS source
            ON target.campaign_id = source.campaign_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

    logger.info(f"Successfully computed campaign_metrics for {record_count} campaigns")
    update_watermark(spark, "campaign_metrics", record_count)
    return record_count


def compute_ga4_engagement_metrics(spark: SparkSession, mode: str = "incremental"):
    """
    Compute analytics.ga4_engagement_metrics from staging.stg_ga4_sessions.

    Daily aggregation of engagement metrics for web analytics dashboard.
    """
    logger.info(f"Processing ga4_engagement_metrics in {mode} mode")

    # Create analytics table
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.analytics.ga4_engagement_metrics (
            metric_date DATE,
            total_sessions BIGINT,
            total_users BIGINT,
            total_page_views BIGINT,
            engaged_sessions BIGINT,
            bounced_sessions BIGINT,
            avg_session_duration_sec DECIMAL(10, 2),
            avg_engagement_time_ms BIGINT,
            total_conversions BIGINT,
            total_conversion_value DECIMAL(18, 2),
            engagement_rate DECIMAL(5, 4),
            bounce_rate DECIMAL(5, 4),
            conversion_rate DECIMAL(5, 4),
            avg_events_per_session DECIMAL(10, 2),
            _computed_at TIMESTAMP,
            _version INT
        )
        USING iceberg
        PARTITIONED BY (months(metric_date))
    """)

    # Read sessions
    sessions_df = spark.table("iceberg.staging.stg_ga4_sessions")

    # Compute daily metrics
    metrics = sessions_df.groupBy(col("session_date").alias("metric_date")).agg(
        count("*").alias("total_sessions"),
        countDistinct("client_id").alias("total_users"),
        spark_sum("page_view_count").alias("total_page_views"),
        spark_sum(when(col("is_engaged_session"), 1).otherwise(0)).alias("engaged_sessions"),
        spark_sum(when(col("is_bounce"), 1).otherwise(0)).alias("bounced_sessions"),
        avg("session_duration_sec").cast(DecimalType(10, 2)).alias("avg_session_duration_sec"),
        avg("total_engagement_ms").cast(LongType()).alias("avg_engagement_time_ms"),
        spark_sum("conversions").alias("total_conversions"),
        spark_sum("total_value").alias("total_conversion_value"),
        avg("event_count").cast(DecimalType(10, 2)).alias("avg_events_per_session")
    ).withColumn(
        "engagement_rate",
        (col("engaged_sessions").cast("double") / col("total_sessions")).cast(DecimalType(5, 4))
    ).withColumn(
        "bounce_rate",
        (col("bounced_sessions").cast("double") / col("total_sessions")).cast(DecimalType(5, 4))
    ).withColumn(
        "conversion_rate",
        (col("total_conversions").cast("double") / col("total_sessions")).cast(DecimalType(5, 4))
    ).withColumn(
        "_computed_at", current_timestamp()
    ).withColumn(
        "_version", lit(1)
    )

    # Write
    # Full recomputation regardless of mode: the read above is unfiltered,
    # so this must replace rather than append. Appending a complete
    # recomputation on top of the previous one duplicated the whole table on
    # every scheduled run -- and two rows per metric_date then broke the marts MERGE.
    metrics.writeTo("iceberg.analytics.ga4_engagement_metrics").using("iceberg").createOrReplace()

    record_count = metrics.count()
    logger.info(f"✅ Computed {record_count} engagement metric rows")
    update_watermark(spark, "ga4_engagement_metrics", record_count)
    return record_count


def compute_ga4_page_performance(spark: SparkSession, mode: str = "incremental"):
    """
    Compute analytics.ga4_page_performance from staging.stg_ga4_events + stg_ga4_sessions.

    CRITICAL GAP: Page-level metrics with bounce rate, engagement, and traffic attribution.
    """
    logger.info(f"Processing ga4_page_performance in {mode} mode")

    # Create analytics table
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.analytics.ga4_page_performance (
            metric_date DATE,
            page_location STRING,
            page_title STRING,
            page_views BIGINT,
            unique_visitors BIGINT,
            unique_sessions BIGINT,
            avg_engagement_time_ms BIGINT,
            entrances BIGINT,
            exits BIGINT,
            bounces BIGINT,
            bounce_rate DECIMAL(5, 4),
            avg_time_on_page_sec DECIMAL(10, 2),
            traffic_source STRING,
            traffic_medium STRING,
            device_category STRING,
            geo_country STRING,
            _computed_at TIMESTAMP,
            _version INT
        )
        USING iceberg
        PARTITIONED BY (months(metric_date))
    """)

    # Read events and sessions
    events_df = spark.table("iceberg.staging.stg_ga4_events").filter(col("event_name") == "page_view")
    sessions_df = spark.table("iceberg.staging.stg_ga4_sessions")

    # Compute page performance metrics
    page_metrics = events_df.join(
        sessions_df.select("session_id", "is_bounce", "landing_page", "exit_page"),
        on="session_id",
        how="left"
    ).groupBy(
        col("event_date").alias("metric_date"),
        col("page_location"),
        col("page_title"),
        col("traffic_source"),
        col("traffic_medium"),
        col("device_category"),
        col("geo_country")
    ).agg(
        count("*").alias("page_views"),
        countDistinct("client_id").alias("unique_visitors"),
        countDistinct("session_id").alias("unique_sessions"),
        avg("engagement_time_ms").cast(LongType()).alias("avg_engagement_time_ms"),
        spark_sum(when(col("landing_page") == col("page_location"), 1).otherwise(0)).alias("entrances"),
        spark_sum(when(col("exit_page") == col("page_location"), 1).otherwise(0)).alias("exits"),
        spark_sum(when((col("landing_page") == col("page_location")) & col("is_bounce"), 1).otherwise(0)).alias("bounces")
    ).withColumn(
        "bounce_rate",
        when(col("entrances") > 0, (col("bounces").cast("double") / col("entrances"))).otherwise(lit(0)).cast(DecimalType(5, 4))
    ).withColumn(
        "avg_time_on_page_sec",
        (col("avg_engagement_time_ms") / 1000.0).cast(DecimalType(10, 2))
    ).withColumn(
        "_computed_at", current_timestamp()
    ).withColumn(
        "_version", lit(1)
    )

    # Write
    # Full recomputation regardless of mode: the read above is unfiltered,
    # so this must replace rather than append. Appending a complete
    # recomputation on top of the previous one duplicated the whole table on
    # every scheduled run -- one full copy of page stats per run.
    page_metrics.writeTo("iceberg.analytics.ga4_page_performance").using("iceberg").createOrReplace()

    record_count = page_metrics.count()
    logger.info(f"✅ Computed {record_count} page performance rows")
    update_watermark(spark, "ga4_page_performance", record_count)
    return record_count


def compute_ga4_funnel_analysis(spark: SparkSession, mode: str = "incremental"):
    """
    Compute analytics.ga4_funnel_analysis from staging.stg_ga4_events.

    CRITICAL GAP: Step-by-step conversion funnel with dropoff rates.
    Funnel steps: page_view → view_item → add_to_cart → begin_checkout → purchase
    """
    logger.info(f"Processing ga4_funnel_analysis in {mode} mode")

    # Create analytics table
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.analytics.ga4_funnel_analysis (
            metric_date DATE,
            funnel_name STRING,
            step_number INT,
            step_name STRING,
            step_users BIGINT,
            step_sessions BIGINT,
            step_completion_rate DECIMAL(5, 4),
            dropoff_users BIGINT,
            dropoff_rate DECIMAL(5, 4),
            total_conversions BIGINT,
            total_conversion_value DECIMAL(18, 2),
            _computed_at TIMESTAMP,
            _version INT
        )
        USING iceberg
        PARTITIONED BY (funnel_name, months(metric_date))
    """)

    # Read events
    events_df = spark.table("iceberg.staging.stg_ga4_events")

    # Define e-commerce funnel steps
    funnel_steps = [
        (1, "page_view", "Landing"),
        (2, "view_item", "Product View"),
        (3, "add_to_cart", "Add to Cart"),
        (4, "begin_checkout", "Begin Checkout"),
        (5, "purchase", "Purchase")
    ]

    # Compute funnel by session (session-level aggregation)
    funnel_events = events_df.filter(
        col("event_name").isin([step[1] for step in funnel_steps])
    ).groupBy("session_id", "event_date").agg(
        spark_max(when(col("event_name") == "page_view", lit(1)).otherwise(lit(0))).alias("has_page_view"),
        spark_max(when(col("event_name") == "view_item", lit(1)).otherwise(lit(0))).alias("has_view_item"),
        spark_max(when(col("event_name") == "add_to_cart", lit(1)).otherwise(lit(0))).alias("has_add_to_cart"),
        spark_max(when(col("event_name") == "begin_checkout", lit(1)).otherwise(lit(0))).alias("has_begin_checkout"),
        spark_max(when(col("event_name") == "purchase", lit(1)).otherwise(lit(0))).alias("has_purchase"),
        spark_sum(when(col("is_conversion"), col("event_value")).otherwise(lit(0))).alias("conversion_value"),
        first("client_id").alias("client_id")
    )

    # Aggregate by date
    funnel_daily = funnel_events.groupBy("event_date").agg(
        spark_sum("has_page_view").alias("step1_users"),
        spark_sum("has_view_item").alias("step2_users"),
        spark_sum("has_add_to_cart").alias("step3_users"),
        spark_sum("has_begin_checkout").alias("step4_users"),
        spark_sum("has_purchase").alias("step5_users"),
        count("session_id").alias("total_sessions"),
        spark_sum("has_purchase").alias("total_conversions"),
        spark_sum("conversion_value").alias("total_conversion_value")
    )

    # Create funnel rows for each step
    from pyspark.sql.functions import array, explode, struct

    funnel_results = []
    for step_num, event_name, step_name in funnel_steps:
        step_df = funnel_daily.select(
            col("event_date").alias("metric_date"),
            lit("ecommerce").alias("funnel_name"),
            lit(step_num).alias("step_number"),
            lit(step_name).alias("step_name"),
            col(f"step{step_num}_users").alias("step_users"),
            col("total_sessions").alias("step_sessions"),
            (col(f"step{step_num}_users").cast("double") / col("step1_users")).cast(DecimalType(5, 4)).alias("step_completion_rate"),
            (col(f"step{step_num}_users") - coalesce(col(f"step{step_num+1}_users") if step_num < 5 else lit(0), lit(0))).alias("dropoff_users"),
            when(col(f"step{step_num}_users") > 0,
                 ((col(f"step{step_num}_users") - coalesce(col(f"step{step_num+1}_users") if step_num < 5 else lit(0), lit(0))).cast("double") / col(f"step{step_num}_users"))
            ).otherwise(lit(0)).cast(DecimalType(5, 4)).alias("dropoff_rate"),
            col("total_conversions"),
            col("total_conversion_value"),
            current_timestamp().alias("_computed_at"),
            lit(1).alias("_version")
        )
        funnel_results.append(step_df)

    # Union all funnel steps
    final_funnel = funnel_results[0]
    for df in funnel_results[1:]:
        final_funnel = final_funnel.union(df)

    # Write
    # Full recomputation regardless of mode: the read above is unfiltered,
    # so this must replace rather than append. Appending a complete
    # recomputation on top of the previous one duplicated the whole table on
    # every scheduled run -- one full copy of every funnel step per run.
    final_funnel.writeTo("iceberg.analytics.ga4_funnel_analysis").using("iceberg").createOrReplace()

    record_count = final_funnel.count()
    logger.info(f"✅ Computed {record_count} funnel analysis rows")
    update_watermark(spark, "ga4_funnel_analysis", record_count)
    return record_count


def compute_ga4_engagement_by_channel(spark: SparkSession, mode: str = "incremental"):
    """
    Compute analytics.ga4_engagement_by_channel from staging.stg_ga4_sessions.

    Channel-level engagement metrics for attribution analysis.
    """
    logger.info(f"Processing ga4_engagement_by_channel in {mode} mode")

    # Create analytics table
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.analytics.ga4_engagement_by_channel (
            metric_date DATE,
            channel_group STRING,
            traffic_source STRING,
            traffic_medium STRING,
            total_sessions BIGINT,
            engaged_sessions BIGINT,
            bounced_sessions BIGINT,
            avg_session_duration_sec DECIMAL(10, 2),
            total_conversions BIGINT,
            total_conversion_value DECIMAL(18, 2),
            engagement_rate DECIMAL(5, 4),
            bounce_rate DECIMAL(5, 4),
            conversion_rate DECIMAL(5, 4),
            _computed_at TIMESTAMP,
            _version INT
        )
        USING iceberg
        PARTITIONED BY (channel_group, months(metric_date))
    """)

    # Read sessions
    sessions_df = spark.table("iceberg.staging.stg_ga4_sessions")

    # Compute by channel
    channel_metrics = sessions_df.groupBy(
        col("session_date").alias("metric_date"),
        col("channel_group"),
        col("traffic_source"),
        col("traffic_medium")
    ).agg(
        count("*").alias("total_sessions"),
        spark_sum(when(col("is_engaged_session"), 1).otherwise(0)).alias("engaged_sessions"),
        spark_sum(when(col("is_bounce"), 1).otherwise(0)).alias("bounced_sessions"),
        avg("session_duration_sec").cast(DecimalType(10, 2)).alias("avg_session_duration_sec"),
        spark_sum("conversions").alias("total_conversions"),
        spark_sum("total_value").alias("total_conversion_value")
    ).withColumn(
        "engagement_rate",
        (col("engaged_sessions").cast("double") / col("total_sessions")).cast(DecimalType(5, 4))
    ).withColumn(
        "bounce_rate",
        (col("bounced_sessions").cast("double") / col("total_sessions")).cast(DecimalType(5, 4))
    ).withColumn(
        "conversion_rate",
        (col("total_conversions").cast("double") / col("total_sessions")).cast(DecimalType(5, 4))
    ).withColumn(
        "_computed_at", current_timestamp()
    ).withColumn(
        "_version", lit(1)
    )

    # Write
    # Full recomputation regardless of mode: the read above is unfiltered,
    # so this must replace rather than append. Appending a complete
    # recomputation on top of the previous one duplicated the whole table on
    # every scheduled run -- one full copy of every channel row per run.
    channel_metrics.writeTo("iceberg.analytics.ga4_engagement_by_channel").using("iceberg").createOrReplace()

    record_count = channel_metrics.count()
    logger.info(f"✅ Computed {record_count} channel metric rows")
    update_watermark(spark, "ga4_engagement_by_channel", record_count)
    return record_count


# Mapping of table names to compute functions
ANALYTICS_FUNCTIONS = {
    "customer_metrics": compute_customer_metrics,
    "order_summary": compute_order_summary,
    "payment_metrics": compute_payment_metrics,
    "campaign_metrics": compute_campaign_metrics,
    "ga4_engagement_metrics": compute_ga4_engagement_metrics,
    "ga4_engagement_by_channel": compute_ga4_engagement_by_channel,
    "ga4_page_performance": compute_ga4_page_performance,
    "ga4_funnel_analysis": compute_ga4_funnel_analysis,
}


def main():
    parser = argparse.ArgumentParser(description="Analytics Layer Incremental Transforms")
    parser.add_argument(
        "--mode",
        choices=["full", "incremental"],
        default="incremental",
        help="Processing mode: full (all data) or incremental (new data only)"
    )
    parser.add_argument(
        "--table",
        choices=list(ANALYTICS_FUNCTIONS.keys()) + ["all"],
        default="all",
        help="Table to process (default: all)"
    )
    args = parser.parse_args()

    logger.info(f"Starting analytics batch job - mode: {args.mode}, table: {args.table}")

    spark = create_spark_session()

    # Ensure analytics database exists
    spark.sql("CREATE DATABASE IF NOT EXISTS iceberg.analytics")

    try:
        total_records = 0

        if args.table == "all":
            # Process all tables in order
            for table_name, compute_func in ANALYTICS_FUNCTIONS.items():
                try:
                    records = compute_func(spark, args.mode)
                    total_records += records
                except Exception as e:
                    logger.error(f"Error processing {table_name}: {e}")
                    raise
        else:
            # Process specific table
            compute_func = ANALYTICS_FUNCTIONS[args.table]
            total_records = compute_func(spark, args.mode)

        logger.info(f"Analytics batch job completed. Total records processed: {total_records}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
