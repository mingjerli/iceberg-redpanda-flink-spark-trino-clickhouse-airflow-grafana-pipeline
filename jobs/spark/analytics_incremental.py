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
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "admin123") \
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
            logger.info(f"Incremental filter: _staged_at > {watermark}")

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

    # Apply watermark filter
    if watermark and mode == "incremental":
        orders_df = orders_df.filter(col("_staged_at") > watermark)

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
        coalesce(col("shipping_province"), lit("Unknown")).alias("shipping_state"),
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
            logger.info(f"Incremental filter: _staged_at > {watermark}")

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


# Mapping of table names to compute functions
ANALYTICS_FUNCTIONS = {
    "customer_metrics": compute_customer_metrics,
    "order_summary": compute_order_summary,
    "payment_metrics": compute_payment_metrics,
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
