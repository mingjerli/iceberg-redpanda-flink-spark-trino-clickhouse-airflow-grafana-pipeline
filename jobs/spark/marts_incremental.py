"""
Spark Batch Job: Marts Layer Transforms
=======================================

This script performs incremental marts transforms from analytics/core to marts layer.
Supports both full refresh and incremental processing modes.

Usage:
    # Full refresh all marts tables
    spark-submit marts_incremental.py --mode full

    # Incremental processing all tables
    spark-submit marts_incremental.py --mode incremental

    # Process specific table
    spark-submit marts_incremental.py --table customer_360 --mode incremental

    # Full refresh specific table
    spark-submit marts_incremental.py --table sales_dashboard_daily --mode full
"""

import argparse
import logging
import os
from datetime import datetime, timedelta
from typing import Optional

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    avg,
    coalesce,
    col,
    concat,
    count,
    countDistinct,
    current_date,
    current_timestamp,
    date_format,
    date_trunc,
    dayofweek,
    first,
    greatest,
    hour,
    lag,
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
        .appName("MartsIncrementalTransforms") \
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
            FROM iceberg.marts.{table_name}
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
                'marts.{source_table}',
                'marts_incremental',
                current_timestamp(),
                {records_processed},
                current_timestamp()
            )
        """)
        logger.info(f"Updated watermark for marts.{source_table}")
    except Exception as e:
        logger.warning(f"Could not update watermark: {e}")


def build_customer_360(spark: SparkSession, mode: str = "incremental"):
    """Build marts.customer_360 denormalized customer view."""
    logger.info(f"Processing customer_360 in {mode} mode")

    # Create marts table if not exists
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.marts.customer_360 (
            customer_id STRING,
            email STRING,
            first_name STRING,
            last_name STRING,
            full_name STRING,
            phone STRING,
            address_line1 STRING,
            address_line2 STRING,
            city STRING,
            state STRING,
            zip STRING,
            country STRING,
            country_code STRING,
            company STRING,
            job_title STRING,
            customer_tier STRING,
            lifecycle_stage STRING,
            customer_segment STRING,
            rfm_segment STRING,
            currency STRING,
            total_spent DECIMAL(18, 2),
            total_orders BIGINT,
            avg_order_value DECIMAL(18, 2),
            estimated_ltv DECIMAL(18, 2),
            first_order_date DATE,
            last_order_date DATE,
            first_order_value DECIMAL(18, 2),
            last_order_value DECIMAL(18, 2),
            days_since_last_order INT,
            order_frequency_days DECIMAL(10, 2),
            total_charges BIGINT,
            successful_charges BIGINT,
            failed_charges BIGINT,
            total_refunds BIGINT,
            refund_rate DECIMAL(5, 4),
            preferred_card_brand STRING,
            page_views BIGINT,
            sessions BIGINT,
            engagement_score DECIMAL(5, 2),
            is_engaged BOOLEAN,
            rfm_recency_score INT,
            rfm_frequency_score INT,
            rfm_monetary_score INT,
            rfm_total_score INT,
            accepts_marketing BOOLEAN,
            acquisition_source STRING,
            buyer_accepts_marketing BOOLEAN,
            source_count INT,
            has_shopify BOOLEAN,
            has_hubspot BOOLEAN,
            has_stripe BOOLEAN,
            is_active BOOLEAN,
            is_customer BOOLEAN,
            is_at_risk BOOLEAN,
            is_high_value BOOLEAN,
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
        watermark = get_watermark(spark, "customer_360")
        if watermark:
            logger.info(f"Incremental filter: _computed_at > {watermark}")

    # Read customer metrics
    try:
        metrics_df = spark.table("iceberg.analytics.customer_metrics")
    except Exception as e:
        logger.error(f"Could not read customer_metrics: {e}")
        return 0

    # Apply watermark filter
    if watermark and mode == "incremental":
        metrics_df = metrics_df.filter(col("_computed_at") > watermark)

    record_count = metrics_df.count()
    if record_count == 0:
        logger.info("No new records to process")
        return 0

    logger.info(f"Processing {record_count} customer records")

    # Try to read core.customers for additional attributes
    try:
        core_customers = spark.table("iceberg.core.customers") if spark.catalog.tableExists("iceberg.core.customers") else None
    except Exception:
        core_customers = None

    # Try to read Stripe charges for payment data
    try:
        stripe_df = spark.table("iceberg.staging.stg_stripe_charges")
        # Aggregate payment data per customer
        stripe_agg = stripe_df.filter(col("is_live") == True).groupBy("customer_id").agg(
            count("*").alias("total_charges"),
            spark_sum(when(col("is_successful") == True, 1).otherwise(0)).alias("successful_charges"),
            spark_sum(when(col("status") == "failed", 1).otherwise(0)).alias("failed_charges"),
            spark_sum(when(col("is_refunded") == True, 1).otherwise(0)).alias("total_refunds"),
            first(col("card_brand")).alias("preferred_card_brand")
        ).withColumn(
            "refund_rate",
            spark_round(
                col("total_refunds").cast("decimal(10,4)") /
                when(col("successful_charges") > 0, col("successful_charges")).otherwise(1),
                4
            )
        )
    except Exception:
        stripe_agg = None
        logger.warning("Could not read Stripe data")

    # Build the denormalized view
    if core_customers is not None:
        # Join with core customers
        mart_df = metrics_df.alias("m").join(
            core_customers.alias("c"),
            col("m.customer_id") == col("c.customer_id"),
            "inner"
        )
        # Add core customer columns
        mart_df = mart_df.select(
            col("m.customer_id"),
            col("c.email"),
            col("c.first_name"),
            col("c.last_name"),
            col("c.full_name"),
            col("c.phone"),
            col("c.address_line1"),
            col("c.address_line2"),
            col("c.city"),
            col("c.state"),
            col("c.zip"),
            col("c.country"),
            col("c.country_code"),
            col("c.company"),
            col("c.job_title"),
            col("m.customer_tier"),
            col("m.lifecycle_stage"),
            col("m.customer_segment"),
            col("m.rfm_segment"),
            col("c.currency"),
            col("m.total_spent"),
            col("m.total_orders"),
            col("m.avg_order_value"),
            col("m.estimated_ltv"),
            col("m.first_order_date"),
            col("m.last_order_date"),
            col("m.first_order_value"),
            col("m.last_order_value"),
            col("m.days_since_last_order"),
            col("m.order_frequency_days"),
            col("m.page_views"),
            col("m.sessions"),
            col("m.engagement_score"),
            col("c.is_engaged"),
            col("m.rfm_recency_score"),
            col("m.rfm_frequency_score"),
            col("m.rfm_monetary_score"),
            (col("m.rfm_recency_score") + col("m.rfm_frequency_score") + col("m.rfm_monetary_score")).alias("rfm_total_score"),
            col("m.accepts_marketing"),
            col("m.acquisition_source"),
            col("c.accepts_marketing").alias("buyer_accepts_marketing"),
            col("m.source_count"),
            col("m.has_shopify"),
            col("m.has_hubspot"),
            col("c.is_active"),
            col("c.is_customer"),
            col("m.rfm_segment").isin("At Risk", "Lost").alias("is_at_risk"),
            col("m.customer_tier").isin("vip", "gold").alias("is_high_value"),
            col("m.first_order_cohort"),
            col("m.signup_cohort"),
            col("m.customer_created_at"),
            col("m.customer_updated_at"),
            current_timestamp().alias("_computed_at"),
            lit(1).alias("_version")
        )
    else:
        # Build from metrics only
        mart_df = metrics_df.select(
            col("customer_id"),
            col("email"),
            lit(None).cast("string").alias("first_name"),
            lit(None).cast("string").alias("last_name"),
            col("full_name"),
            lit(None).cast("string").alias("phone"),
            lit(None).cast("string").alias("address_line1"),
            lit(None).cast("string").alias("address_line2"),
            lit(None).cast("string").alias("city"),
            lit(None).cast("string").alias("state"),
            lit(None).cast("string").alias("zip"),
            lit(None).cast("string").alias("country"),
            lit(None).cast("string").alias("country_code"),
            lit(None).cast("string").alias("company"),
            lit(None).cast("string").alias("job_title"),
            col("customer_tier"),
            col("lifecycle_stage"),
            col("customer_segment"),
            col("rfm_segment"),
            lit(None).cast("string").alias("currency"),
            col("total_spent"),
            col("total_orders"),
            col("avg_order_value"),
            col("estimated_ltv"),
            col("first_order_date"),
            col("last_order_date"),
            col("first_order_value"),
            col("last_order_value"),
            col("days_since_last_order"),
            col("order_frequency_days"),
            col("page_views"),
            col("sessions"),
            col("engagement_score"),
            (col("engagement_score") > 50).alias("is_engaged"),
            col("rfm_recency_score"),
            col("rfm_frequency_score"),
            col("rfm_monetary_score"),
            (col("rfm_recency_score") + col("rfm_frequency_score") + col("rfm_monetary_score")).alias("rfm_total_score"),
            col("accepts_marketing"),
            col("acquisition_source"),
            col("accepts_marketing").alias("buyer_accepts_marketing"),
            col("source_count"),
            col("has_shopify"),
            col("has_hubspot"),
            lit(True).alias("is_active"),
            (col("total_orders") > 0).alias("is_customer"),
            col("rfm_segment").isin("At Risk", "Lost").alias("is_at_risk"),
            col("customer_tier").isin("vip", "gold").alias("is_high_value"),
            col("first_order_cohort"),
            col("signup_cohort"),
            col("customer_created_at"),
            col("customer_updated_at"),
            current_timestamp().alias("_computed_at"),
            lit(1).alias("_version")
        )

    # Add Stripe data if available
    if stripe_agg is not None:
        mart_df = mart_df.join(stripe_agg, mart_df.customer_id == stripe_agg.customer_id, "left") \
            .drop(stripe_agg.customer_id)
        mart_df = mart_df.withColumn("has_stripe", col("total_charges") > 0)
    else:
        mart_df = mart_df.withColumn("total_charges", lit(0).cast("bigint")) \
            .withColumn("successful_charges", lit(0).cast("bigint")) \
            .withColumn("failed_charges", lit(0).cast("bigint")) \
            .withColumn("total_refunds", lit(0).cast("bigint")) \
            .withColumn("refund_rate", lit(None).cast("decimal(5,4)")) \
            .withColumn("preferred_card_brand", lit(None).cast("string")) \
            .withColumn("has_stripe", lit(False))

    # Write to marts table
    mart_df.createOrReplaceTempView("new_customer_360")

    if mode == "full":
        mart_df.write \
            .format("iceberg") \
            .mode("overwrite") \
            .saveAsTable("iceberg.marts.customer_360")
    else:
        spark.sql("""
            MERGE INTO iceberg.marts.customer_360 AS target
            USING new_customer_360 AS source
            ON target.customer_id = source.customer_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

    logger.info(f"Successfully built customer_360 for {record_count} customers")
    update_watermark(spark, "customer_360", record_count)
    return record_count


def build_sales_dashboard_daily(spark: SparkSession, mode: str = "incremental"):
    """Build marts.sales_dashboard_daily from analytics tables."""
    logger.info(f"Processing sales_dashboard_daily in {mode} mode")

    # Create marts table if not exists
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.marts.sales_dashboard_daily (
            date_key DATE,
            day_of_week INT,
            day_name STRING,
            week_of_year INT,
            month_key STRING,
            quarter_key STRING,
            year_key INT,
            is_weekend BOOLEAN,
            gross_revenue DECIMAL(18, 2),
            net_revenue DECIMAL(18, 2),
            total_discounts DECIMAL(18, 2),
            total_shipping DECIMAL(18, 2),
            total_tax DECIMAL(18, 2),
            total_orders BIGINT,
            completed_orders BIGINT,
            cancelled_orders BIGINT,
            refunded_orders BIGINT,
            avg_order_value DECIMAL(18, 2),
            total_customers BIGINT,
            new_customers BIGINT,
            returning_customers BIGINT,
            new_customer_pct DECIMAL(5, 4),
            web_orders BIGINT,
            mobile_orders BIGINT,
            pos_orders BIGINT,
            api_orders BIGINT,
            payment_volume DECIMAL(18, 2),
            successful_payments BIGINT,
            failed_payments BIGINT,
            payment_success_rate DECIMAL(5, 4),
            top_country STRING,
            top_country_revenue DECIMAL(18, 2),
            revenue_7d_avg DECIMAL(18, 2),
            revenue_30d_avg DECIMAL(18, 2),
            orders_7d_avg DECIMAL(10, 2),
            _computed_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (month_key)
    """)

    # Get watermark for incremental
    watermark = None
    if mode == "incremental":
        watermark = get_watermark(spark, "sales_dashboard_daily")
        if watermark:
            logger.info(f"Incremental filter: _computed_at > {watermark}")

    # Read order summary
    try:
        order_summary_df = spark.table("iceberg.analytics.order_summary")
    except Exception as e:
        logger.error(f"Could not read order_summary: {e}")
        return 0

    # Apply watermark filter
    if watermark and mode == "incremental":
        order_summary_df = order_summary_df.filter(col("_computed_at") > watermark)

    record_count = order_summary_df.count()
    if record_count == 0:
        logger.info("No new records to process")
        return 0

    logger.info(f"Processing {record_count} order summary records")

    # Try to read payment metrics
    try:
        payment_metrics_df = spark.table("iceberg.analytics.payment_metrics")
        # Aggregate payment by date
        payment_agg = payment_metrics_df.groupBy("payment_date").agg(
            spark_sum("successful_volume").alias("payment_volume"),
            spark_sum("successful_charges").alias("successful_payments"),
            spark_sum("failed_charges").alias("failed_payments")
        ).withColumn(
            "payment_success_rate",
            spark_round(
                col("successful_payments").cast("decimal(10,4)") /
                (col("successful_payments") + col("failed_payments")),
                4
            )
        )
    except Exception:
        payment_agg = None
        logger.warning("Could not read payment metrics")

    # Aggregate order summary by day
    daily_df = order_summary_df.groupBy("order_date").agg(
        spark_sum("gross_revenue").alias("gross_revenue"),
        spark_sum("net_revenue").alias("net_revenue"),
        spark_sum("total_discounts").alias("total_discounts"),
        spark_sum("total_shipping").alias("total_shipping"),
        spark_sum("total_tax").alias("total_tax"),
        spark_sum("total_orders").alias("total_orders"),
        spark_sum("completed_orders").alias("completed_orders"),
        spark_sum("cancelled_orders").alias("cancelled_orders"),
        spark_sum("refunded_orders").alias("refunded_orders"),
        spark_sum("unique_customers").alias("total_customers"),
        spark_sum("new_customers").alias("new_customers"),
        spark_sum("returning_customers").alias("returning_customers"),
        spark_sum(when(col("channel") == "web", col("total_orders")).otherwise(0)).alias("web_orders"),
        spark_sum(when(col("channel") == "mobile", col("total_orders")).otherwise(0)).alias("mobile_orders"),
        spark_sum(when(col("channel") == "pos", col("total_orders")).otherwise(0)).alias("pos_orders"),
        spark_sum(when(col("channel") == "api", col("total_orders")).otherwise(0)).alias("api_orders")
    ).withColumn(
        "avg_order_value",
        spark_round(col("gross_revenue") / when(col("total_orders") > 0, col("total_orders")).otherwise(1), 2)
    ).withColumn(
        "new_customer_pct",
        spark_round(col("new_customers").cast("decimal(10,4)") /
                    when(col("total_customers") > 0, col("total_customers")).otherwise(1), 4)
    )

    # Add date dimensions
    daily_df = daily_df.withColumn("date_key", col("order_date")) \
        .withColumn("day_of_week", dayofweek(col("order_date"))) \
        .withColumn("day_name", date_format(col("order_date"), "EEEE")) \
        .withColumn("week_of_year", weekofyear(col("order_date"))) \
        .withColumn("month_key", date_format(col("order_date"), "yyyy-MM")) \
        .withColumn("quarter_key", concat(year(col("order_date")).cast("string"), lit("-Q"), quarter(col("order_date")).cast("string"))) \
        .withColumn("year_key", year(col("order_date"))) \
        .withColumn("is_weekend", dayofweek(col("order_date")).isin(1, 7))

    # Get top country per day
    top_country_df = order_summary_df.groupBy("order_date", "shipping_country") \
        .agg(spark_sum("gross_revenue").alias("country_revenue")) \
        .withColumn("rn", row_number().over(
            Window.partitionBy("order_date").orderBy(col("country_revenue").desc())
        )).filter(col("rn") == 1) \
        .select(
            col("order_date"),
            col("shipping_country").alias("top_country"),
            col("country_revenue").alias("top_country_revenue")
        )

    daily_df = daily_df.join(top_country_df, "order_date", "left")

    # Join payment data
    if payment_agg is not None:
        daily_df = daily_df.join(
            payment_agg,
            daily_df.order_date == payment_agg.payment_date,
            "left"
        ).drop("payment_date")
    else:
        daily_df = daily_df.withColumn("payment_volume", lit(0).cast("decimal(18,2)")) \
            .withColumn("successful_payments", lit(0).cast("bigint")) \
            .withColumn("failed_payments", lit(0).cast("bigint")) \
            .withColumn("payment_success_rate", lit(None).cast("decimal(5,4)"))

    # Add rolling averages
    window_7d = Window.orderBy("order_date").rowsBetween(-6, 0)
    window_30d = Window.orderBy("order_date").rowsBetween(-29, 0)

    daily_df = daily_df.withColumn(
        "revenue_7d_avg",
        spark_round(avg("gross_revenue").over(window_7d), 2)
    ).withColumn(
        "revenue_30d_avg",
        spark_round(avg("gross_revenue").over(window_30d), 2)
    ).withColumn(
        "orders_7d_avg",
        spark_round(avg("total_orders").over(window_7d), 2)
    )

    # Add metadata and drop temp columns
    daily_df = daily_df.withColumn("_computed_at", current_timestamp()) \
        .drop("order_date")

    # Write to marts table
    if mode == "full":
        daily_df.write \
            .format("iceberg") \
            .mode("overwrite") \
            .saveAsTable("iceberg.marts.sales_dashboard_daily")
    else:
        daily_df.write \
            .format("iceberg") \
            .mode("overwrite") \
            .option("overwrite-mode", "dynamic") \
            .saveAsTable("iceberg.marts.sales_dashboard_daily")

    logger.info(f"Successfully built sales_dashboard_daily")
    update_watermark(spark, "sales_dashboard_daily", record_count)
    return record_count


def build_executive_summary(spark: SparkSession, mode: str = "incremental"):
    """Build marts.executive_summary snapshot table."""
    logger.info(f"Processing executive_summary in {mode} mode")

    # Create marts table if not exists
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.marts.executive_summary (
            period_type STRING,
            period_start DATE,
            period_end DATE,
            gross_revenue DECIMAL(18, 2),
            net_revenue DECIMAL(18, 2),
            gross_revenue_prior DECIMAL(18, 2),
            revenue_growth_pct DECIMAL(7, 4),
            total_orders BIGINT,
            total_orders_prior BIGINT,
            order_growth_pct DECIMAL(7, 4),
            avg_order_value DECIMAL(18, 2),
            total_customers BIGINT,
            new_customers BIGINT,
            customer_acquisition_cost DECIMAL(18, 2),
            cancelled_order_pct DECIMAL(5, 4),
            refund_pct DECIMAL(5, 4),
            payment_success_rate DECIMAL(5, 4),
            _computed_at TIMESTAMP
        )
        USING iceberg
    """)

    # Read sales dashboard
    try:
        dashboard_df = spark.table("iceberg.marts.sales_dashboard_daily")
    except Exception as e:
        logger.error(f"Could not read sales_dashboard_daily: {e}")
        return 0

    # Build period summaries
    periods = []

    # Today
    today_df = dashboard_df.filter(col("date_key") == current_date())
    if today_df.count() > 0:
        today_row = today_df.select(
            lit("today").alias("period_type"),
            current_date().alias("period_start"),
            current_date().alias("period_end"),
            col("gross_revenue"),
            col("net_revenue"),
            lit(None).cast("decimal(18,2)").alias("gross_revenue_prior"),
            lit(None).cast("decimal(7,4)").alias("revenue_growth_pct"),
            col("total_orders"),
            lit(None).cast("bigint").alias("total_orders_prior"),
            lit(None).cast("decimal(7,4)").alias("order_growth_pct"),
            col("avg_order_value"),
            col("total_customers"),
            col("new_customers"),
            lit(None).cast("decimal(18,2)").alias("customer_acquisition_cost"),
            spark_round(col("cancelled_orders").cast("decimal(10,4)") /
                        when(col("total_orders") > 0, col("total_orders")).otherwise(1), 4).alias("cancelled_order_pct"),
            spark_round(col("refunded_orders").cast("decimal(10,4)") /
                        when(col("total_orders") > 0, col("total_orders")).otherwise(1), 4).alias("refund_pct"),
            col("payment_success_rate"),
            current_timestamp().alias("_computed_at")
        )
        periods.append(today_row)

    # Month to date
    mtd_df = dashboard_df.filter(col("date_key") >= date_trunc("month", current_date()))
    if mtd_df.count() > 0:
        mtd_row = mtd_df.agg(
            lit("mtd").alias("period_type"),
            spark_min("date_key").alias("period_start"),
            current_date().alias("period_end"),
            spark_sum("gross_revenue").alias("gross_revenue"),
            spark_sum("net_revenue").alias("net_revenue"),
            lit(None).cast("decimal(18,2)").alias("gross_revenue_prior"),
            lit(None).cast("decimal(7,4)").alias("revenue_growth_pct"),
            spark_sum("total_orders").alias("total_orders"),
            lit(None).cast("bigint").alias("total_orders_prior"),
            lit(None).cast("decimal(7,4)").alias("order_growth_pct"),
            spark_round(spark_sum("gross_revenue") / spark_sum("total_orders"), 2).alias("avg_order_value"),
            spark_sum("total_customers").alias("total_customers"),
            spark_sum("new_customers").alias("new_customers"),
            lit(None).cast("decimal(18,2)").alias("customer_acquisition_cost"),
            spark_round(spark_sum("cancelled_orders").cast("decimal(10,4)") / spark_sum("total_orders"), 4).alias("cancelled_order_pct"),
            spark_round(spark_sum("refunded_orders").cast("decimal(10,4)") / spark_sum("total_orders"), 4).alias("refund_pct"),
            avg("payment_success_rate").alias("payment_success_rate"),
            current_timestamp().alias("_computed_at")
        )
        periods.append(mtd_row)

    # Year to date
    ytd_df = dashboard_df.filter(col("date_key") >= date_trunc("year", current_date()))
    if ytd_df.count() > 0:
        ytd_row = ytd_df.agg(
            lit("ytd").alias("period_type"),
            spark_min("date_key").alias("period_start"),
            current_date().alias("period_end"),
            spark_sum("gross_revenue").alias("gross_revenue"),
            spark_sum("net_revenue").alias("net_revenue"),
            lit(None).cast("decimal(18,2)").alias("gross_revenue_prior"),
            lit(None).cast("decimal(7,4)").alias("revenue_growth_pct"),
            spark_sum("total_orders").alias("total_orders"),
            lit(None).cast("bigint").alias("total_orders_prior"),
            lit(None).cast("decimal(7,4)").alias("order_growth_pct"),
            spark_round(spark_sum("gross_revenue") / spark_sum("total_orders"), 2).alias("avg_order_value"),
            spark_sum("total_customers").alias("total_customers"),
            spark_sum("new_customers").alias("new_customers"),
            lit(None).cast("decimal(18,2)").alias("customer_acquisition_cost"),
            spark_round(spark_sum("cancelled_orders").cast("decimal(10,4)") / spark_sum("total_orders"), 4).alias("cancelled_order_pct"),
            spark_round(spark_sum("refunded_orders").cast("decimal(10,4)") / spark_sum("total_orders"), 4).alias("refund_pct"),
            avg("payment_success_rate").alias("payment_success_rate"),
            current_timestamp().alias("_computed_at")
        )
        periods.append(ytd_row)

    if not periods:
        logger.info("No data for executive summary")
        return 0

    # Union all periods
    result_df = periods[0]
    for df in periods[1:]:
        result_df = result_df.union(df)

    # Always overwrite executive summary (it's a snapshot)
    result_df.write \
        .format("iceberg") \
        .mode("overwrite") \
        .saveAsTable("iceberg.marts.executive_summary")

    logger.info("Successfully built executive_summary")
    update_watermark(spark, "executive_summary", len(periods))
    return len(periods)


# Mapping of table names to build functions
MARTS_FUNCTIONS = {
    "customer_360": build_customer_360,
    "sales_dashboard_daily": build_sales_dashboard_daily,
    "executive_summary": build_executive_summary,
}


def main():
    parser = argparse.ArgumentParser(description="Marts Layer Incremental Transforms")
    parser.add_argument(
        "--mode",
        choices=["full", "incremental"],
        default="incremental",
        help="Processing mode: full (all data) or incremental (new data only)"
    )
    parser.add_argument(
        "--table",
        choices=list(MARTS_FUNCTIONS.keys()) + ["all"],
        default="all",
        help="Table to process (default: all)"
    )
    args = parser.parse_args()

    logger.info(f"Starting marts batch job - mode: {args.mode}, table: {args.table}")

    spark = create_spark_session()

    # Ensure marts database exists
    spark.sql("CREATE DATABASE IF NOT EXISTS iceberg.marts")

    try:
        total_records = 0

        if args.table == "all":
            # Process all tables in dependency order
            for table_name, build_func in MARTS_FUNCTIONS.items():
                try:
                    records = build_func(spark, args.mode)
                    total_records += records
                except Exception as e:
                    logger.error(f"Error processing {table_name}: {e}")
                    raise
        else:
            # Process specific table
            build_func = MARTS_FUNCTIONS[args.table]
            total_records = build_func(spark, args.mode)

        logger.info(f"Marts batch job completed. Total records processed: {total_records}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
