"""
Spark Batch Job: Staging Layer Transforms
==========================================

This script performs batch staging transforms from raw to staging layer.
Supports both full refresh and incremental processing modes.

Usage:
    # Full refresh all tables
    spark-submit staging_batch.py --mode full

    # Incremental processing all tables
    spark-submit staging_batch.py --mode incremental

    # Process specific table
    spark-submit staging_batch.py --table shopify_orders --mode incremental

    # Full refresh specific table
    spark-submit staging_batch.py --table stripe_charges --mode full
"""

import argparse
import logging
import os
from datetime import datetime, timedelta
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql import Window
# count/first/max/min/sum shadow Python builtins. That is intentional and safe
# here: this module uses them only as Spark column functions, never as builtins.
from pyspark.sql.functions import (
    col,
    coalesce,
    concat,
    count,
    current_date,
    current_timestamp,
    datediff,
    expr,
    first,
    get_json_object,
    hour,
    lag,
    length,
    lit,
    lower,
    max,
    min,
    regexp_replace,
    round as spark_round,
    row_number,
    size,
    split,
    sum,
    to_date,
    trim,
    udf,
    unix_timestamp,
    upper,
    when,
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
        .appName("StagingBatchTransforms") \
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
            SELECT MAX(_staged_at) as last_staged
            FROM iceberg.staging.{table_name}
        """).collect()
        if result and result[0].last_staged:
            return result[0].last_staged
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
                'staging.{source_table}',
                'staging_batch',
                current_timestamp(),
                {records_processed},
                current_timestamp()
            )
        """)
        logger.info(f"Updated watermark for staging.{source_table}")
    except Exception as e:
        logger.warning(f"Could not update watermark: {e}")


def stage_shopify_orders(spark: SparkSession, mode: str = "incremental"):
    """Transform raw.shopify_orders to staging.stg_shopify_orders."""
    logger.info(f"Processing shopify_orders in {mode} mode")

    # Create staging table if not exists
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.staging.stg_shopify_orders (
            order_id BIGINT,
            order_number BIGINT,
            customer_id BIGINT,
            customer_email STRING,
            customer_phone STRING,
            order_name STRING,
            currency STRING,
            subtotal_price DECIMAL(18, 2),
            total_discounts DECIMAL(18, 2),
            total_shipping DECIMAL(18, 2),
            total_tax DECIMAL(18, 2),
            total_price DECIMAL(18, 2),
            total_outstanding DECIMAL(18, 2),
            current_subtotal_price DECIMAL(18, 2),
            current_total_price DECIMAL(18, 2),
            current_total_tax DECIMAL(18, 2),
            financial_status STRING,
            fulfillment_status STRING,
            order_status STRING,
            line_item_count INT,
            discount_count INT,
            has_discount BOOLEAN,
            is_cancelled BOOLEAN,
            is_test BOOLEAN,
            taxes_included BOOLEAN,
            tax_exempt BOOLEAN,
            buyer_accepts_marketing BOOLEAN,
            shipping_country STRING,
            shipping_country_code STRING,
            shipping_province STRING,
            shipping_city STRING,
            billing_country STRING,
            billing_country_code STRING,
            source_name STRING,
            referring_site STRING,
            landing_site STRING,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            processed_at TIMESTAMP,
            cancelled_at TIMESTAMP,
            closed_at TIMESTAMP,
            _raw_id BIGINT,
            _webhook_topic STRING,
            _loaded_at TIMESTAMP,
            _staged_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (months(created_at))
    """)

    # Read source data
    raw_df = spark.table("iceberg.raw.shopify_orders")

    # Apply watermark filter for incremental mode
    if mode == "incremental":
        watermark = get_watermark(spark, "stg_shopify_orders")
        if watermark:
            raw_df = raw_df.filter(col("_loaded_at") > watermark)
            logger.info(f"Incremental filter: _loaded_at > {watermark}")
        else:
            logger.info("No watermark found, processing all data")

    # Count records before transform
    record_count = raw_df.count()
    if record_count == 0:
        logger.info("No new records to process")
        return 0

    logger.info(f"Processing {record_count} records")

    # Transform
    staged_df = raw_df.select(
        col("id").alias("order_id"),
        col("order_number"),
        col("customer_id"),
        lower(trim(col("email"))).alias("customer_email"),
        col("phone").alias("customer_phone"),
        col("name").alias("order_name"),
        upper(col("currency")).alias("currency"),
        col("subtotal_price"),
        col("total_discounts"),
        coalesce(
            get_json_object(col("total_shipping_price_set"), "$.shop_money.amount").cast("decimal(18,2)"),
            lit(0).cast("decimal(18,2)")
        ).alias("total_shipping"),
        col("total_tax"),
        col("total_price"),
        col("total_outstanding"),
        col("current_subtotal_price"),
        col("current_total_price"),
        col("current_total_tax"),
        col("financial_status"),
        col("fulfillment_status"),
        when(col("cancelled_at").isNotNull(), "cancelled")
        .when(col("financial_status").isin("refunded", "partially_refunded"), "refunded")
        .when((col("financial_status") == "paid") & (col("fulfillment_status") == "fulfilled"), "completed")
        .when((col("financial_status") == "paid") & (col("fulfillment_status").isNull() | (col("fulfillment_status") == "partial")), "processing")
        .when(col("financial_status").isin("pending", "authorized"), "pending_payment")
        .otherwise("open")
        .alias("order_status"),
        # Handle line_items as JSON string - count array elements by counting separators
        # JSON arrays stored as strings: count occurrences of "},{" + 1, or 0 if null/empty
        when(col("line_items").isNull() | (col("line_items") == "[]") | (col("line_items") == ""), lit(0))
        .otherwise(size(split(col("line_items"), r"\},\s*\{")) )
        .alias("line_item_count"),
        # Handle discount_codes as JSON string similarly
        when(col("discount_codes").isNull() | (col("discount_codes") == "[]") | (col("discount_codes") == ""), lit(0))
        .otherwise(size(split(col("discount_codes"), r"\},\s*\{")))
        .alias("discount_count"),
        (col("total_discounts") > 0).alias("has_discount"),
        col("cancelled_at").isNotNull().alias("is_cancelled"),
        coalesce(col("test"), lit(False)).alias("is_test"),
        coalesce(col("taxes_included"), lit(False)).alias("taxes_included"),
        coalesce(col("tax_exempt"), lit(False)).alias("tax_exempt"),
        coalesce(col("buyer_accepts_marketing"), lit(False)).alias("buyer_accepts_marketing"),
        get_json_object(col("shipping_address"), "$.country").alias("shipping_country"),
        get_json_object(col("shipping_address"), "$.country_code").alias("shipping_country_code"),
        get_json_object(col("shipping_address"), "$.province").alias("shipping_province"),
        get_json_object(col("shipping_address"), "$.city").alias("shipping_city"),
        get_json_object(col("billing_address"), "$.country").alias("billing_country"),
        get_json_object(col("billing_address"), "$.country_code").alias("billing_country_code"),
        col("source_name"),
        col("referring_site"),
        col("landing_site"),
        col("created_at"),
        col("updated_at"),
        col("processed_at"),
        col("cancelled_at"),
        col("closed_at"),
        col("id").alias("_raw_id"),
        col("_webhook_topic"),
        col("_loaded_at"),
        current_timestamp().alias("_staged_at")
    )

    # Write to staging
    staged_df.write \
        .format("iceberg") \
        .mode("append") \
        .saveAsTable("iceberg.staging.stg_shopify_orders")

    logger.info(f"Successfully staged {record_count} shopify_orders records")
    update_watermark(spark, "stg_shopify_orders", record_count)
    return record_count


def stage_shopify_customers(spark: SparkSession, mode: str = "incremental"):
    """Transform raw.shopify_customers to staging.stg_shopify_customers."""
    logger.info(f"Processing shopify_customers in {mode} mode")

    # Create staging table if not exists
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.staging.stg_shopify_customers (
            customer_id BIGINT,
            email STRING,
            first_name STRING,
            last_name STRING,
            full_name STRING,
            phone STRING,
            address_line1 STRING,
            address_line2 STRING,
            city STRING,
            province STRING,
            province_code STRING,
            country STRING,
            country_code STRING,
            zip STRING,
            currency STRING,
            total_spent DECIMAL(18, 2),
            orders_count BIGINT,
            avg_order_value DECIMAL(18, 2),
            customer_tier STRING,
            accepts_marketing BOOLEAN,
            marketing_opt_in_level STRING,
            accepts_marketing_updated_at TIMESTAMP,
            state STRING,
            is_active BOOLEAN,
            verified_email BOOLEAN,
            tax_exempt BOOLEAN,
            note STRING,
            tags STRING,
            address_count INT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            _raw_id BIGINT,
            _webhook_topic STRING,
            _loaded_at TIMESTAMP,
            _staged_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (months(created_at))
    """)

    # Read source data
    raw_df = spark.table("iceberg.raw.shopify_customers")

    # Apply watermark filter for incremental mode
    if mode == "incremental":
        watermark = get_watermark(spark, "stg_shopify_customers")
        if watermark:
            raw_df = raw_df.filter(col("_loaded_at") > watermark)
            logger.info(f"Incremental filter: _loaded_at > {watermark}")

    record_count = raw_df.count()
    if record_count == 0:
        logger.info("No new records to process")
        return 0

    logger.info(f"Processing {record_count} records")

    # Transform
    staged_df = raw_df.select(
        col("id").alias("customer_id"),
        lower(trim(col("email"))).alias("email"),
        trim(col("first_name")).alias("first_name"),
        trim(col("last_name")).alias("last_name"),
        trim(concat(
            coalesce(col("first_name"), lit("")),
            when(col("first_name").isNotNull() & col("last_name").isNotNull(), lit(" ")).otherwise(lit("")),
            coalesce(col("last_name"), lit(""))
        )).alias("full_name"),
        trim(col("phone")).alias("phone"),
        get_json_object(col("default_address"), "$.address1").alias("address_line1"),
        get_json_object(col("default_address"), "$.address2").alias("address_line2"),
        get_json_object(col("default_address"), "$.city").alias("city"),
        get_json_object(col("default_address"), "$.province").alias("province"),
        get_json_object(col("default_address"), "$.province_code").alias("province_code"),
        get_json_object(col("default_address"), "$.country").alias("country"),
        get_json_object(col("default_address"), "$.country_code").alias("country_code"),
        get_json_object(col("default_address"), "$.zip").alias("zip"),
        upper(col("currency")).alias("currency"),
        col("total_spent"),
        col("orders_count"),
        when(col("orders_count") > 0, spark_round(col("total_spent") / col("orders_count"), 2))
        .otherwise(lit(0).cast("decimal(18,2)"))
        .alias("avg_order_value"),
        when(col("total_spent") >= 1000, "vip")
        .when(col("total_spent") >= 500, "gold")
        .when(col("total_spent") >= 100, "silver")
        .when(col("total_spent") > 0, "bronze")
        .otherwise("new")
        .alias("customer_tier"),
        coalesce(col("accepts_marketing"), lit(False)).alias("accepts_marketing"),
        col("marketing_opt_in_level"),
        col("accepts_marketing_updated_at"),
        col("state"),
        (col("state") == "enabled").alias("is_active"),
        coalesce(col("verified_email"), lit(False)).alias("verified_email"),
        coalesce(col("tax_exempt"), lit(False)).alias("tax_exempt"),
        col("note"),
        col("tags"),
        # Handle addresses as JSON string - count array elements
        when(col("addresses").isNull() | (col("addresses") == "[]") | (col("addresses") == ""), lit(0))
        .when(col("addresses").isNotNull(), size(split(col("addresses"), r"\},\s*\{")))
        .otherwise(when(col("default_address").isNotNull(), lit(1)).otherwise(lit(0)))
        .alias("address_count"),
        col("created_at"),
        col("updated_at"),
        col("id").alias("_raw_id"),
        col("_webhook_topic"),
        col("_loaded_at"),
        current_timestamp().alias("_staged_at")
    )

    # Write to staging
    staged_df.write \
        .format("iceberg") \
        .mode("append") \
        .saveAsTable("iceberg.staging.stg_shopify_customers")

    logger.info(f"Successfully staged {record_count} shopify_customers records")
    update_watermark(spark, "stg_shopify_customers", record_count)
    return record_count


def stage_stripe_charges(spark: SparkSession, mode: str = "incremental"):
    """Transform raw.stripe_charges to staging.stg_stripe_charges."""
    logger.info(f"Processing stripe_charges in {mode} mode")

    # Create staging table if not exists
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.staging.stg_stripe_charges (
            charge_id STRING,
            customer_id STRING,
            currency STRING,
            amount DECIMAL(18, 2),
            amount_captured DECIMAL(18, 2),
            amount_refunded DECIMAL(18, 2),
            net_amount DECIMAL(18, 2),
            application_fee_amount DECIMAL(18, 2),
            payment_method_id STRING,
            payment_method_type STRING,
            card_brand STRING,
            card_last4 STRING,
            card_exp_month INT,
            card_exp_year INT,
            card_funding STRING,
            card_country STRING,
            billing_name STRING,
            billing_email STRING,
            billing_phone STRING,
            billing_city STRING,
            billing_country STRING,
            billing_postal_code STRING,
            status STRING,
            charge_status STRING,
            captured BOOLEAN,
            paid BOOLEAN,
            refunded BOOLEAN,
            disputed BOOLEAN,
            is_successful BOOLEAN,
            is_refunded BOOLEAN,
            is_fully_refunded BOOLEAN,
            is_live BOOLEAN,
            failure_code STRING,
            failure_message STRING,
            risk_level STRING,
            risk_score INT,
            seller_message STRING,
            network_status STRING,
            payment_intent_id STRING,
            invoice_id STRING,
            balance_transaction_id STRING,
            statement_descriptor STRING,
            description STRING,
            receipt_url STRING,
            created_at TIMESTAMP,
            _raw_id STRING,
            _webhook_event_id STRING,
            _loaded_at TIMESTAMP,
            _staged_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (months(created_at))
    """)

    # Read source data
    raw_df = spark.table("iceberg.raw.stripe_charges")

    # Apply watermark filter for incremental mode
    if mode == "incremental":
        watermark = get_watermark(spark, "stg_stripe_charges")
        if watermark:
            raw_df = raw_df.filter(col("_loaded_at") > watermark)
            logger.info(f"Incremental filter: _loaded_at > {watermark}")

    record_count = raw_df.count()
    if record_count == 0:
        logger.info("No new records to process")
        return 0

    logger.info(f"Processing {record_count} records")

    # Transform
    staged_df = raw_df.select(
        col("id").alias("charge_id"),
        col("customer").alias("customer_id"),
        upper(col("currency")).alias("currency"),
        (col("amount").cast("decimal(18,2)") / 100).alias("amount"),
        (col("amount_captured").cast("decimal(18,2)") / 100).alias("amount_captured"),
        (col("amount_refunded").cast("decimal(18,2)") / 100).alias("amount_refunded"),
        ((col("amount") - col("amount_refunded")).cast("decimal(18,2)") / 100).alias("net_amount"),
        (coalesce(col("application_fee_amount"), lit(0)).cast("decimal(18,2)") / 100).alias("application_fee_amount"),
        col("payment_method").alias("payment_method_id"),
        get_json_object(col("payment_method_details"), "$.type").alias("payment_method_type"),
        get_json_object(col("payment_method_details"), "$.card.brand").alias("card_brand"),
        get_json_object(col("payment_method_details"), "$.card.last4").alias("card_last4"),
        get_json_object(col("payment_method_details"), "$.card.exp_month").cast("int").alias("card_exp_month"),
        get_json_object(col("payment_method_details"), "$.card.exp_year").cast("int").alias("card_exp_year"),
        get_json_object(col("payment_method_details"), "$.card.funding").alias("card_funding"),
        get_json_object(col("payment_method_details"), "$.card.country").alias("card_country"),
        get_json_object(col("billing_details"), "$.name").alias("billing_name"),
        lower(trim(get_json_object(col("billing_details"), "$.email"))).alias("billing_email"),
        get_json_object(col("billing_details"), "$.phone").alias("billing_phone"),
        get_json_object(col("billing_details"), "$.address.city").alias("billing_city"),
        get_json_object(col("billing_details"), "$.address.country").alias("billing_country"),
        get_json_object(col("billing_details"), "$.address.postal_code").alias("billing_postal_code"),
        col("status"),
        when(col("disputed") == True, "disputed")
        .when(col("refunded") == True, "fully_refunded")
        .when(col("amount_refunded") > 0, "partially_refunded")
        .when(col("status") == "succeeded", "succeeded")
        .when(col("status") == "failed", "failed")
        .otherwise(col("status"))
        .alias("charge_status"),
        coalesce(col("captured"), lit(False)).alias("captured"),
        coalesce(col("paid"), lit(False)).alias("paid"),
        coalesce(col("refunded"), lit(False)).alias("refunded"),
        coalesce(col("disputed"), lit(False)).alias("disputed"),
        ((col("status") == "succeeded") & coalesce(col("paid"), lit(False))).alias("is_successful"),
        (col("amount_refunded") > 0).alias("is_refunded"),
        (col("amount_refunded") == col("amount")).alias("is_fully_refunded"),
        coalesce(col("livemode"), lit(False)).alias("is_live"),
        col("failure_code"),
        col("failure_message"),
        get_json_object(col("outcome"), "$.risk_level").alias("risk_level"),
        get_json_object(col("outcome"), "$.risk_score").cast("int").alias("risk_score"),
        get_json_object(col("outcome"), "$.seller_message").alias("seller_message"),
        get_json_object(col("outcome"), "$.network_status").alias("network_status"),
        col("payment_intent").alias("payment_intent_id"),
        col("invoice").alias("invoice_id"),
        col("balance_transaction").alias("balance_transaction_id"),
        coalesce(col("calculated_statement_descriptor"), col("statement_descriptor")).alias("statement_descriptor"),
        col("description"),
        col("receipt_url"),
        col("created").alias("created_at"),
        col("id").alias("_raw_id"),
        col("_webhook_event_id"),
        col("_loaded_at"),
        current_timestamp().alias("_staged_at")
    )

    # Write to staging
    staged_df.write \
        .format("iceberg") \
        .mode("append") \
        .saveAsTable("iceberg.staging.stg_stripe_charges")

    logger.info(f"Successfully staged {record_count} stripe_charges records")
    update_watermark(spark, "stg_stripe_charges", record_count)
    return record_count


def stage_stripe_customers(spark: SparkSession, mode: str = "incremental"):
    """Transform raw.stripe_customers to staging.stg_stripe_customers."""
    logger.info(f"Processing stripe_customers in {mode} mode")

    # Create staging table if not exists
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.staging.stg_stripe_customers (
            customer_id STRING,
            email STRING,
            name STRING,
            first_name STRING,
            last_name STRING,
            full_name STRING,
            phone STRING,
            address_line1 STRING,
            address_line2 STRING,
            city STRING,
            state STRING,
            postal_code STRING,
            country STRING,
            shipping_name STRING,
            shipping_address_line1 STRING,
            shipping_city STRING,
            shipping_state STRING,
            shipping_postal_code STRING,
            shipping_country STRING,
            balance DECIMAL(18, 2),
            currency STRING,
            delinquent BOOLEAN,
            tax_exempt STRING,
            invoice_prefix STRING,
            description STRING,
            is_live BOOLEAN,
            created_at TIMESTAMP,
            _raw_id STRING,
            _webhook_event_id STRING,
            _loaded_at TIMESTAMP,
            _staged_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (months(created_at))
    """)

    # Read source data
    raw_df = spark.table("iceberg.raw.stripe_customers")

    # Apply watermark filter for incremental mode
    if mode == "incremental":
        watermark = get_watermark(spark, "stg_stripe_customers")
        if watermark:
            raw_df = raw_df.filter(col("_loaded_at") > watermark)
            logger.info(f"Incremental filter: _loaded_at > {watermark}")

    record_count = raw_df.count()
    if record_count == 0:
        logger.info("No new records to process")
        return 0

    logger.info(f"Processing {record_count} records")

    # Transform
    staged_df = raw_df.select(
        col("id").alias("customer_id"),
        lower(trim(col("email"))).alias("email"),
        trim(col("name")).alias("name"),
        # Extract first/last name from full name
        when(col("name").contains(" "),
             trim(split(col("name"), " ").getItem(0)))
        .otherwise(col("name"))
        .alias("first_name"),
        when(col("name").contains(" "),
             trim(expr("substring(name, instr(name, ' ') + 1)")))
        .otherwise(lit(None))
        .alias("last_name"),
        trim(col("name")).alias("full_name"),
        trim(col("phone")).alias("phone"),
        # Address fields from JSON
        get_json_object(col("address"), "$.line1").alias("address_line1"),
        get_json_object(col("address"), "$.line2").alias("address_line2"),
        get_json_object(col("address"), "$.city").alias("city"),
        get_json_object(col("address"), "$.state").alias("state"),
        get_json_object(col("address"), "$.postal_code").alias("postal_code"),
        get_json_object(col("address"), "$.country").alias("country"),
        # Shipping address
        get_json_object(col("shipping"), "$.name").alias("shipping_name"),
        get_json_object(col("shipping"), "$.address.line1").alias("shipping_address_line1"),
        get_json_object(col("shipping"), "$.address.city").alias("shipping_city"),
        get_json_object(col("shipping"), "$.address.state").alias("shipping_state"),
        get_json_object(col("shipping"), "$.address.postal_code").alias("shipping_postal_code"),
        get_json_object(col("shipping"), "$.address.country").alias("shipping_country"),
        # Financial
        (col("balance") / 100).cast("decimal(18,2)").alias("balance"),
        upper(col("currency")).alias("currency"),
        coalesce(col("delinquent"), lit(False)).alias("delinquent"),
        col("tax_exempt"),
        col("invoice_prefix"),
        col("description"),
        coalesce(col("livemode"), lit(False)).alias("is_live"),
        col("created").alias("created_at"),
        col("id").alias("_raw_id"),
        col("_webhook_event_id"),
        col("_loaded_at"),
        current_timestamp().alias("_staged_at")
    )

    # Write to staging
    staged_df.write \
        .format("iceberg") \
        .mode("append") \
        .saveAsTable("iceberg.staging.stg_stripe_customers")

    logger.info(f"Successfully staged {record_count} stripe_customers records")
    update_watermark(spark, "stg_stripe_customers", record_count)
    return record_count


def stage_hubspot_contacts(spark: SparkSession, mode: str = "incremental"):
    """Transform raw.hubspot_contacts to staging.stg_hubspot_contacts."""
    logger.info(f"Processing hubspot_contacts in {mode} mode")

    # Create staging table if not exists
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.staging.stg_hubspot_contacts (
            contact_id STRING,
            email STRING,
            first_name STRING,
            last_name STRING,
            full_name STRING,
            phone STRING,
            mobile_phone STRING,
            address STRING,
            city STRING,
            state STRING,
            zip STRING,
            country STRING,
            company STRING,
            job_title STRING,
            associated_company_id STRING,
            lifecycle_stage STRING,
            lifecycle_stage_normalized STRING,
            lead_status STRING,
            analytics_source STRING,
            first_page_seen STRING,
            page_views BIGINT,
            sessions BIGINT,
            avg_pages_per_session DECIMAL(10, 2),
            email_optout BOOLEAN,
            is_marketable BOOLEAN,
            has_company BOOLEAN,
            has_phone BOOLEAN,
            is_customer BOOLEAN,
            is_engaged BOOLEAN,
            website STRING,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            _raw_id STRING,
            _webhook_subscription_type STRING,
            _loaded_at TIMESTAMP,
            _staged_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (months(created_at))
    """)

    # Read source data
    raw_df = spark.table("iceberg.raw.hubspot_contacts")

    # Apply watermark filter for incremental mode
    if mode == "incremental":
        watermark = get_watermark(spark, "stg_hubspot_contacts")
        if watermark:
            raw_df = raw_df.filter(col("_loaded_at") > watermark)
            logger.info(f"Incremental filter: _loaded_at > {watermark}")

    record_count = raw_df.count()
    if record_count == 0:
        logger.info("No new records to process")
        return 0

    logger.info(f"Processing {record_count} records")

    # Transform - extract from properties JSON if flattened fields are empty
    # HubSpot data may come with nested properties or flattened fields
    props = col("properties")

    # Helper to get value from either direct column or properties JSON
    def prop_or_col(field_name, json_path=None):
        """Get value from direct column or fallback to properties JSON."""
        if json_path is None:
            json_path = f"$.{field_name}"
        return coalesce(col(field_name), get_json_object(props, json_path))

    # Extract contact_id with multiple fallbacks
    contact_id_col = coalesce(
        col("id"),
        col("hs_object_id"),
        get_json_object(props, "$.hs_object_id")
    )

    # Extract core fields with fallback to properties
    email_col = lower(trim(coalesce(col("email"), get_json_object(props, "$.email"))))
    firstname_col = trim(coalesce(col("firstname"), get_json_object(props, "$.firstname")))
    lastname_col = trim(coalesce(col("lastname"), get_json_object(props, "$.lastname")))
    phone_col = trim(coalesce(col("phone"), get_json_object(props, "$.phone")))
    mobilephone_col = trim(coalesce(col("mobilephone"), get_json_object(props, "$.mobilephone")))
    company_col = trim(coalesce(col("company"), get_json_object(props, "$.company")))
    jobtitle_col = trim(coalesce(col("jobtitle"), get_json_object(props, "$.jobtitle")))
    lifecycle_col = lower(coalesce(col("lifecyclestage"), get_json_object(props, "$.lifecyclestage")))

    # Analytics fields
    page_views_col = coalesce(
        col("hs_analytics_num_page_views"),
        get_json_object(props, "$.hs_analytics_num_page_views").cast("bigint"),
        lit(0)
    )
    visits_col = coalesce(
        col("hs_analytics_num_visits"),
        get_json_object(props, "$.hs_analytics_num_visits").cast("bigint"),
        lit(0)
    )

    # Email optout - handle boolean column vs string from JSON
    # Cast boolean to string for coalesce compatibility, then compare
    email_optout_raw = coalesce(
        col("hs_email_optout").cast("string"),
        get_json_object(props, "$.hs_email_optout")
    )
    email_optout_col = when(email_optout_raw == "true", lit(True)).otherwise(lit(False))

    staged_df = raw_df.select(
        contact_id_col.alias("contact_id"),
        email_col.alias("email"),
        firstname_col.alias("first_name"),
        lastname_col.alias("last_name"),
        trim(concat(
            coalesce(firstname_col, lit("")),
            when(firstname_col.isNotNull() & lastname_col.isNotNull(), lit(" ")).otherwise(lit("")),
            coalesce(lastname_col, lit(""))
        )).alias("full_name"),
        phone_col.alias("phone"),
        mobilephone_col.alias("mobile_phone"),
        trim(coalesce(col("address"), get_json_object(props, "$.address"))).alias("address"),
        trim(coalesce(col("city"), get_json_object(props, "$.city"))).alias("city"),
        trim(coalesce(col("state"), get_json_object(props, "$.state"))).alias("state"),
        trim(coalesce(col("zip"), get_json_object(props, "$.zip"))).alias("zip"),
        trim(coalesce(col("country"), get_json_object(props, "$.country"))).alias("country"),
        company_col.alias("company"),
        jobtitle_col.alias("job_title"),
        coalesce(col("associatedcompanyid"), get_json_object(props, "$.associatedcompanyid")).alias("associated_company_id"),
        lifecycle_col.alias("lifecycle_stage"),
        when(lifecycle_col.isin("subscriber", "lead"), "prospect")
        .when(lifecycle_col.isin("marketingqualifiedlead", "mql", "salesqualifiedlead", "sql", "opportunity"), "qualified")
        .when(lifecycle_col.isin("customer", "evangelist"), "customer")
        .otherwise("prospect")
        .alias("lifecycle_stage_normalized"),
        coalesce(col("hs_lead_status"), get_json_object(props, "$.hs_lead_status")).alias("lead_status"),
        coalesce(col("hs_analytics_source"), get_json_object(props, "$.hs_analytics_source")).alias("analytics_source"),
        coalesce(col("hs_analytics_first_url"), get_json_object(props, "$.hs_analytics_first_url")).alias("first_page_seen"),
        page_views_col.alias("page_views"),
        visits_col.alias("sessions"),
        when(visits_col > 0,
             spark_round(page_views_col.cast("decimal(10,2)") / visits_col, 2))
        .otherwise(lit(0).cast("decimal(10,2)"))
        .alias("avg_pages_per_session"),
        email_optout_col.alias("email_optout"),
        (email_col.isNotNull() & (email_optout_col == False)).alias("is_marketable"),
        (company_col.isNotNull() | coalesce(col("associatedcompanyid"), get_json_object(props, "$.associatedcompanyid")).isNotNull()).alias("has_company"),
        (phone_col.isNotNull() | mobilephone_col.isNotNull()).alias("has_phone"),
        (lifecycle_col == "customer").alias("is_customer"),
        ((page_views_col > 5) | (visits_col > 2)).alias("is_engaged"),
        coalesce(col("website"), get_json_object(props, "$.website")).alias("website"),
        # Handle timestamp columns - createdate may be TIMESTAMP or STRING
        coalesce(
            col("createdate").cast("timestamp"),
            get_json_object(props, "$.createdate").cast("timestamp")
        ).alias("created_at"),
        coalesce(
            col("lastmodifieddate").cast("timestamp"),
            get_json_object(props, "$.lastmodifieddate").cast("timestamp")
        ).alias("updated_at"),
        contact_id_col.alias("_raw_id"),
        col("_webhook_subscription_type"),
        col("_loaded_at"),
        current_timestamp().alias("_staged_at")
    ).filter(
        # Filter out records without a valid contact_id
        col("contact_id").isNotNull()
    )

    # Write to staging
    staged_df.write \
        .format("iceberg") \
        .mode("append") \
        .saveAsTable("iceberg.staging.stg_hubspot_contacts")

    logger.info(f"Successfully staged {record_count} hubspot_contacts records")
    update_watermark(spark, "stg_hubspot_contacts", record_count)
    return record_count


def stage_mailchimp_campaigns(spark: SparkSession, mode: str = "incremental"):
    """Transform raw.mailchimp_campaigns to staging.stg_mailchimp_campaigns."""
    logger.info(f"Processing mailchimp_campaigns in {mode} mode")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.staging.stg_mailchimp_campaigns (
            _raw_id STRING,
            campaign_id STRING,
            campaign_type STRING,
            status STRING,
            list_id STRING,
            subject_line STRING,
            preview_text STRING,
            from_name STRING,
            from_email STRING,
            reply_to STRING,
            send_time TIMESTAMP,
            content_type STRING,
            emails_sent INT,
            opens INT,
            unique_opens INT,
            clicks INT,
            unique_clicks INT,
            unsubscribes INT,
            bounces INT,
            open_rate DECIMAL(5, 4),
            click_rate DECIMAL(5, 4),
            click_to_open_rate DECIMAL(5, 4),
            is_sms BOOLEAN,
            is_automated BOOLEAN,
            settings STRING,
            tracking STRING,
            _webhook_received_at TIMESTAMP,
            _webhook_event_type STRING,
            _loaded_at TIMESTAMP,
            _staged_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (months(send_time))
    """)

    raw_df = spark.table("iceberg.raw.mailchimp_campaigns")

    if mode == "incremental":
        watermark = get_watermark(spark, "stg_mailchimp_campaigns")
        if watermark:
            raw_df = raw_df.filter(col("_loaded_at") > watermark)
            logger.info(f"Incremental filter: _loaded_at > {watermark}")

    record_count = raw_df.count()
    if record_count == 0:
        logger.info("No new records to process")
        return 0

    logger.info(f"Processing {record_count} records")

    staged_df = raw_df.select(
        col("campaign_id").alias("_raw_id"),
        col("campaign_id"),
        col("campaign_type"),
        col("status"),
        col("list_id"),
        col("subject_line"),
        col("preview_text"),
        col("from_name"),
        lower(trim(col("from_email"))).alias("from_email"),
        lower(trim(col("reply_to"))).alias("reply_to"),
        col("send_time"),
        col("content_type"),
        col("emails_sent"),
        col("opens"),
        col("unique_opens"),
        col("clicks"),
        col("unique_clicks"),
        col("unsubscribes"),
        col("bounces"),
        col("open_rate"),
        col("click_rate"),
        # click_to_open_rate = unique_clicks / NULLIF(unique_opens, 0)
        when(col("unique_opens") > 0,
             spark_round((col("unique_clicks").cast("decimal(10,4)") / col("unique_opens")), 4))
        .otherwise(lit(None).cast("decimal(5,4)"))
        .alias("click_to_open_rate"),
        (col("campaign_type") == "sms").alias("is_sms"),
        (col("campaign_type") == "automation").alias("is_automated"),
        col("settings"),
        col("tracking"),
        col("_webhook_received_at"),
        col("_webhook_event_type"),
        col("_loaded_at"),
        current_timestamp().alias("_staged_at"),
    )

    staged_df.write \
        .format("iceberg") \
        .mode("append") \
        .saveAsTable("iceberg.staging.stg_mailchimp_campaigns")

    logger.info(f"Successfully staged {record_count} mailchimp_campaigns records")
    update_watermark(spark, "stg_mailchimp_campaigns", record_count)
    return record_count


def stage_mailchimp_events(spark: SparkSession, mode: str = "incremental"):
    """Transform raw.mailchimp_events to staging.stg_mailchimp_events."""
    logger.info(f"Processing mailchimp_events in {mode} mode")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.staging.stg_mailchimp_events (
            _raw_id STRING,
            event_id STRING,
            campaign_id STRING,
            email_id STRING,
            email_address STRING,
            email_normalized STRING,
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
        )
        USING iceberg
        PARTITIONED BY (months(event_timestamp))
    """)

    raw_df = spark.table("iceberg.raw.mailchimp_events")

    if mode == "incremental":
        watermark = get_watermark(spark, "stg_mailchimp_events")
        if watermark:
            raw_df = raw_df.filter(col("_loaded_at") > watermark)
            logger.info(f"Incremental filter: _loaded_at > {watermark}")

    record_count = raw_df.count()
    if record_count == 0:
        logger.info("No new records to process")
        return 0

    logger.info(f"Processing {record_count} records")

    staged_df = raw_df.select(
        col("event_id").alias("_raw_id"),
        col("event_id"),
        col("campaign_id"),
        col("email_id"),
        col("email_address"),
        lower(trim(col("email_address"))).alias("email_normalized"),
        col("action"),
        col("event_timestamp"),
        to_date(col("event_timestamp")).alias("event_date"),
        col("url"),
        col("ip"),
        col("user_agent"),
        col("location"),
        get_json_object(col("location"), "$.country_code").alias("location_country"),
        get_json_object(col("location"), "$.region").alias("location_region"),
        col("bounce_type"),
        col("list_id"),
        col("action").isin("sms_sent", "sms_click").alias("is_sms_event"),
        col("action").isin("open", "click", "sms_click").alias("is_positive_engagement"),
        col("action").isin("bounce", "unsub", "abuse").alias("is_negative_event"),
        col("_webhook_received_at"),
        col("_webhook_event_type"),
        col("_loaded_at"),
        current_timestamp().alias("_staged_at"),
    )

    staged_df.write \
        .format("iceberg") \
        .mode("append") \
        .saveAsTable("iceberg.staging.stg_mailchimp_events")

    logger.info(f"Successfully staged {record_count} mailchimp_events records")
    update_watermark(spark, "stg_mailchimp_events", record_count)
    return record_count


def stage_mailchimp_subscribers(spark: SparkSession, mode: str = "incremental"):
    """Transform raw.mailchimp_subscribers to staging.stg_mailchimp_subscribers."""
    logger.info(f"Processing mailchimp_subscribers in {mode} mode")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.staging.stg_mailchimp_subscribers (
            _raw_id STRING,
            subscriber_id STRING,
            email_address STRING,
            email_normalized STRING,
            email_type STRING,
            status STRING,
            first_name STRING,
            last_name STRING,
            full_name STRING,
            phone STRING,
            phone_normalized STRING,
            merge_fields STRING,
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
        )
        USING iceberg
        PARTITIONED BY (months(signup_timestamp))
    """)

    raw_df = spark.table("iceberg.raw.mailchimp_subscribers")

    if mode == "incremental":
        watermark = get_watermark(spark, "stg_mailchimp_subscribers")
        if watermark:
            raw_df = raw_df.filter(col("_loaded_at") > watermark)
            logger.info(f"Incremental filter: _loaded_at > {watermark}")

    record_count = raw_df.count()
    if record_count == 0:
        logger.info("No new records to process")
        return 0

    logger.info(f"Processing {record_count} records")

    # Dedup: keep latest record per subscriber_id within the batch.
    # Mailchimp webhooks may fire multiple events for the same subscriber
    # (e.g., profile update + tag change) in a single batch window.
    window = Window.partitionBy("subscriber_id").orderBy(col("_loaded_at").desc())
    deduped_df = (
        raw_df
        .withColumn("_rn", row_number().over(window))
        .where(col("_rn") == 1)
        .drop("_rn")
    )

    # Extract fields from merge_fields JSON
    first_name_col = trim(get_json_object(col("merge_fields"), "$.FNAME"))
    last_name_col = trim(get_json_object(col("merge_fields"), "$.LNAME"))
    phone_col = trim(get_json_object(col("merge_fields"), "$.PHONE"))

    staged_df = deduped_df.select(
        col("subscriber_id").alias("_raw_id"),
        col("subscriber_id"),
        col("email_address"),
        lower(trim(col("email_address"))).alias("email_normalized"),
        col("email_type"),
        col("status"),
        first_name_col.alias("first_name"),
        last_name_col.alias("last_name"),
        trim(concat(
            coalesce(first_name_col, lit("")),
            when(first_name_col.isNotNull() & last_name_col.isNotNull(), lit(" ")).otherwise(lit("")),
            coalesce(last_name_col, lit(""))
        )).alias("full_name"),
        coalesce(col("phone"), phone_col).alias("phone"),
        # phone_normalized: digits only for entity resolution matching
        regexp_replace(
            coalesce(col("phone"), phone_col, lit("")),
            "[^0-9+]", ""
        ).alias("phone_normalized"),
        col("merge_fields"),
        col("stats"),
        get_json_object(col("stats"), "$.avg_open_rate").cast("decimal(5,4)").alias("avg_open_rate"),
        get_json_object(col("stats"), "$.avg_click_rate").cast("decimal(5,4)").alias("avg_click_rate"),
        col("list_id"),
        col("tags"),
        col("ip_signup"),
        col("timestamp_signup").alias("signup_timestamp"),
        col("ip_opt"),
        col("timestamp_opt"),
        col("last_changed"),
        col("language"),
        col("vip"),
        col("source"),
        col("sms_status"),
        (col("phone").isNotNull() & (col("sms_status") == "subscribed")).alias("has_sms"),
        (col("status") == "subscribed").alias("is_active"),
        datediff(current_date(), col("timestamp_signup")).alias("days_since_signup"),
        col("_webhook_received_at"),
        col("_webhook_event_type"),
        col("_loaded_at"),
        current_timestamp().alias("_staged_at"),
    )

    staged_df.write \
        .format("iceberg") \
        .mode("append") \
        .saveAsTable("iceberg.staging.stg_mailchimp_subscribers")

    deduped_count = staged_df.count()
    logger.info(f"Successfully staged {deduped_count} mailchimp_subscribers records (from {record_count} raw)")
    update_watermark(spark, "stg_mailchimp_subscribers", deduped_count)
    return deduped_count


def stage_ga4_events(spark, mode="incremental"):
    """Stage GA4 events with deduplication (Staff Engineer RC-2)."""
    logger.info(f"Staging GA4 events (mode={mode})")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS staging.stg_ga4_events (
            client_id STRING, user_id STRING, session_id STRING, event_name STRING,
            event_timestamp TIMESTAMP, page_location STRING, page_title STRING,
            page_referrer STRING, engagement_time_ms BIGINT, traffic_source STRING,
            traffic_medium STRING, traffic_campaign STRING, device_category STRING,
            device_os STRING, device_browser STRING, geo_country STRING, geo_region STRING,
            geo_city STRING, is_conversion BOOLEAN, event_value DECIMAL(18, 2),
            currency STRING, event_date DATE, hour_of_day INT, is_ecommerce_event BOOLEAN,
            is_engagement_event BOOLEAN, _raw_id STRING, _loaded_at TIMESTAMP, _staged_at TIMESTAMP
        ) USING iceberg PARTITIONED BY (months(event_timestamp))
    """)

    raw_df = spark.table("iceberg.raw.ga4_events")

    if mode == "incremental":
        watermark = get_watermark(spark, "stg_ga4_events")
        if watermark:
            raw_df = raw_df.filter(col("_loaded_at") > watermark)
            logger.info(f"Incremental filter: _loaded_at > {watermark}")
        else:
            logger.info("No watermark found, processing all data")

    if raw_df.count() == 0:
        logger.info("No new GA4 events"); return 0

    # CRITICAL FIX #2: Deduplication works in BOTH incremental and full modes
    # Natural key: (client_id, event_timestamp, event_name)
    # Keeps latest record by _loaded_at DESC (handles late-arriving data)
    dedup_window = Window.partitionBy("client_id", "event_timestamp", "event_name").orderBy(col("_loaded_at").desc())
    staged_df = raw_df.withColumn("_dedup_rank", row_number().over(dedup_window)).filter(col("_dedup_rank") == 1).drop("_dedup_rank").select(
        col("client_id"), col("user_id"), col("session_id"), col("event_name"),
        expr("CAST(CAST(event_timestamp AS DOUBLE) / 1000000 AS TIMESTAMP)").alias("event_timestamp"),
        get_json_object("traffic_source", "$.source").alias("traffic_source"),
        get_json_object("traffic_source", "$.medium").alias("traffic_medium"),
        get_json_object("traffic_source", "$.name").alias("traffic_campaign"),
        get_json_object("device", "$.category").alias("device_category"),
        get_json_object("device", "$.operating_system").alias("device_os"),
        get_json_object("device", "$.web_info.browser").alias("device_browser"),
        get_json_object("geo", "$.country").alias("geo_country"),
        get_json_object("geo", "$.region").alias("geo_region"),
        get_json_object("geo", "$.city").alias("geo_city"),
        col("page_location"), col("page_title"), col("page_referrer"), col("engagement_time_ms"),
        col("is_conversion"), col("value").cast("decimal(18,2)").alias("event_value"), col("currency"),
        expr("CAST(CAST(CAST(event_timestamp AS DOUBLE) / 1000000 AS TIMESTAMP) AS DATE)").alias("event_date"),
        hour(expr("CAST(CAST(event_timestamp AS DOUBLE) / 1000000 AS TIMESTAMP)")).alias("hour_of_day"),
        when(col("event_name").isin("purchase", "add_to_cart", "begin_checkout", "view_item"), lit(True)).otherwise(lit(False)).alias("is_ecommerce_event"),
        when(col("event_name").isin("scroll", "click", "video_start", "file_download"), lit(True)).otherwise(lit(False)).alias("is_engagement_event"),
        col("_raw_id"), col("_loaded_at"), current_timestamp().alias("_staged_at")
    )

    if mode == "full":
        staged_df.writeTo("iceberg.staging.stg_ga4_events").using("iceberg").createOrReplace()
    else:
        staged_df.writeTo("iceberg.staging.stg_ga4_events").using("iceberg").append()

    record_count = staged_df.count()
    logger.info(f"✅ Staged {record_count} GA4 events")
    update_watermark(spark, "stg_ga4_events", record_count)
    return record_count


def compute_ga4_sessions(spark, mode="incremental"):
    """Compute GA4 sessions from events using 30-min gap (Staff Engineer RC-4 fix)."""
    logger.info(f"Computing GA4 sessions (mode={mode})")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS staging.stg_ga4_sessions (
            session_id STRING, client_id STRING, user_id STRING, session_start TIMESTAMP,
            session_end TIMESTAMP, session_duration_sec INT, event_count INT, page_view_count INT,
            is_engaged_session BOOLEAN, traffic_source STRING, traffic_medium STRING,
            traffic_campaign STRING, channel_group STRING, landing_page STRING, exit_page STRING,
            device_category STRING, device_os STRING, geo_country STRING, geo_region STRING,
            total_engagement_ms BIGINT, conversions INT, total_value DECIMAL(18, 2),
            session_date DATE, is_bounce BOOLEAN, _loaded_at TIMESTAMP, _staged_at TIMESTAMP
        ) USING iceberg PARTITIONED BY (months(session_start))
    """)

    events_df = spark.table("iceberg.staging.stg_ga4_events")
    if events_df.count() == 0: logger.info("No events"); return 0

    client_win = Window.partitionBy("client_id").orderBy("event_timestamp")
    events = events_df.withColumn("gap_sec", unix_timestamp("event_timestamp") - unix_timestamp(lag("event_timestamp").over(client_win))) \
        .withColumn("new_sess", when(col("gap_sec").isNull(), lit(1)).when(col("gap_sec") > 1800, lit(1)).otherwise(lit(0))) \
        .withColumn("sess_num", sum("new_sess").over(client_win))

    sess_win = Window.partitionBy("client_id", "sess_num").orderBy("event_timestamp")
    first_attrs = events.withColumn("rn", row_number().over(sess_win)).filter(col("rn") == 1).select(
        col("client_id"), col("sess_num"), col("traffic_source").alias("t_src"),
        col("traffic_medium").alias("t_med"), col("traffic_campaign").alias("t_camp"),
        col("device_category").alias("d_cat"), col("device_os").alias("d_os"),
        col("geo_country").alias("g_cty"), col("geo_region").alias("g_reg"),
        when(col("event_name") == "page_view", col("page_location")).alias("land")
    )

    last_attrs = events.withColumn("rn", row_number().over(sess_win.orderBy(col("event_timestamp").desc()))) \
        .filter((col("rn") == 1) & (col("event_name") == "page_view")).select(
        col("client_id"), col("sess_num"), col("page_location").alias("exit")
    )

    sessions = events.groupBy("client_id", "sess_num").agg(
        coalesce(first("session_id"), concat(col("client_id"), lit("_"), col("sess_num").cast("string"))).alias("session_id"),
        first("user_id").alias("user_id"), min("event_timestamp").alias("session_start"),
        max("event_timestamp").alias("session_end"),
        (unix_timestamp(max("event_timestamp")) - unix_timestamp(min("event_timestamp"))).cast("int").alias("session_duration_sec"),
        count("*").alias("event_count"),
        sum(when(col("event_name") == "page_view", lit(1)).otherwise(lit(0))).alias("page_view_count"),
        sum("engagement_time_ms").alias("total_engagement_ms"),
        sum(when(col("is_conversion"), lit(1)).otherwise(lit(0))).alias("conversions"),
        sum("event_value").alias("total_value"), to_date(min("event_timestamp")).alias("session_date"),
        max("_loaded_at").alias("_loaded_at")
    ).join(first_attrs, ["client_id", "sess_num"], "left").join(last_attrs, ["client_id", "sess_num"], "left")

    def ch_grp(src, med):
        if src == "(direct)" or (src == "(none)" and med == "(none)"): return "direct"
        elif med in ("organic", "organic search"): return "organic_search"
        elif med in ("cpc", "ppc", "paid search"): return "paid_search"
        elif med in ("social", "social-network", "social media"): return "social"
        elif med == "email": return "email"
        elif med == "referral": return "referral"
        elif med == "sms": return "sms"
        else: return "other"

    ch_udf = udf(ch_grp)
    sessions = sessions.withColumn("channel_group", ch_udf(col("t_src"), col("t_med"))) \
        .withColumn("traffic_source", col("t_src")).withColumn("traffic_medium", col("t_med")) \
        .withColumn("traffic_campaign", col("t_camp")).withColumn("device_category", col("d_cat")) \
        .withColumn("device_os", col("d_os")).withColumn("geo_country", col("g_cty")) \
        .withColumn("geo_region", col("g_reg")).withColumn("landing_page", col("land")) \
        .withColumn("exit_page", col("exit")) \
        .withColumn("is_engaged_session", (col("total_engagement_ms") > 10000) | (col("page_view_count") >= 2) | (col("conversions") > 0)) \
        .withColumn("is_bounce", (col("page_view_count") <= 1) & (~col("is_engaged_session"))) \
        .withColumn("_staged_at", current_timestamp()) \
        .drop("t_src", "t_med", "t_camp", "d_cat", "d_os", "g_cty", "g_reg", "land", "exit", "sess_num")

    if mode == "full":
        sessions.writeTo("iceberg.staging.stg_ga4_sessions").using("iceberg").createOrReplace()
    else:
        sessions.writeTo("iceberg.staging.stg_ga4_sessions").using("iceberg").append()

    record_count = sessions.count()
    logger.info(f"✅ Computed {record_count} sessions")
    update_watermark(spark, "stg_ga4_sessions", record_count)
    return record_count


# Mapping of table names to staging functions
STAGING_FUNCTIONS = {
    "shopify_orders": stage_shopify_orders,
    "shopify_customers": stage_shopify_customers,
    "stripe_charges": stage_stripe_charges,
    "stripe_customers": stage_stripe_customers,
    "hubspot_contacts": stage_hubspot_contacts,
    "mailchimp_campaigns": stage_mailchimp_campaigns,
    "mailchimp_events": stage_mailchimp_events,
    "mailchimp_subscribers": stage_mailchimp_subscribers,
    "ga4_events": stage_ga4_events,
    "ga4_sessions": compute_ga4_sessions,
}


def main():
    parser = argparse.ArgumentParser(description="Staging Layer Batch Transforms")
    parser.add_argument(
        "--mode",
        choices=["full", "incremental"],
        default="incremental",
        help="Processing mode: full (all data) or incremental (new data only)"
    )
    parser.add_argument(
        "--table",
        choices=list(STAGING_FUNCTIONS.keys()) + ["all"],
        default="all",
        help="Table to process (default: all)"
    )
    args = parser.parse_args()

    logger.info(f"Starting staging batch job - mode: {args.mode}, table: {args.table}")

    spark = create_spark_session()

    # Ensure staging database exists
    spark.sql("CREATE DATABASE IF NOT EXISTS iceberg.staging")

    try:
        total_records = 0

        if args.table == "all":
            # Process all tables
            for table_name, staging_func in STAGING_FUNCTIONS.items():
                try:
                    records = staging_func(spark, args.mode)
                    total_records += records
                except Exception as e:
                    logger.error(f"Error processing {table_name}: {e}")
                    raise
        else:
            # Process specific table
            staging_func = STAGING_FUNCTIONS[args.table]
            total_records = staging_func(spark, args.mode)

        logger.info(f"Staging batch job completed. Total records processed: {total_records}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
