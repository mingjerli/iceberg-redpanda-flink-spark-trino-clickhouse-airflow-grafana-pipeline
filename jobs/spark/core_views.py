"""
Spark Job: Core Views Creation
==============================

This script creates/refreshes the core layer views.
Core views join semantic.entity_index with staging tables to provide
unified business entities.

Note: Views are created in Spark's default catalog (not Iceberg) because
the Iceberg REST catalog doesn't support views. The views query Iceberg
tables using fully qualified names (iceberg.schema.table).

Usage:
    spark-submit core_views.py                    # Create all core views
    spark-submit core_views.py --view customers   # Create only customers view
    spark-submit core_views.py --view orders      # Create only orders view
"""

import argparse
import logging
import os
from typing import Optional

from pyspark.sql import SparkSession

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_spark_session() -> SparkSession:
    """Create Spark session with Iceberg configuration."""
    return SparkSession.builder \
        .appName("CoreViewsCreation") \
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


def ensure_database_exists(spark: SparkSession):
    """Create core database in Iceberg catalog if it doesn't exist."""
    logger.info("Ensuring core database exists in Iceberg catalog...")
    spark.sql("CREATE DATABASE IF NOT EXISTS iceberg.core COMMENT 'Core business entities'")
    logger.info("Core database ready")


def create_customers_view(spark: SparkSession):
    """
    Create the core.customers view as a materialized Iceberg table.

    Since Iceberg REST catalog doesn't support views, we create a table
    that gets refreshed when this job runs.
    """
    logger.info("Creating iceberg.core.customers table...")

    # Create the table (or replace if exists)
    spark.sql("DROP TABLE IF EXISTS iceberg.core.customers")

    customers_sql = """
    CREATE TABLE iceberg.core.customers
    USING iceberg
    AS
    WITH entity_index_pivoted AS (
        SELECT
            unified_id,
            MAX(CASE WHEN source = 'shopify_customers' THEN source_id END) AS shopify_id,
            MAX(CASE WHEN source = 'hubspot_contacts' THEN source_id END) AS hubspot_id,
            MAX(match_confidence) AS max_confidence,
            MAX(matched_at) AS last_matched_at,
            COUNT(DISTINCT source) AS source_count
        FROM iceberg.semantic.entity_index
        WHERE entity_type = 'customer'
          AND linked_to_unified_id IS NULL
        GROUP BY unified_id
    )
    SELECT
        eip.unified_id AS customer_id,
        COALESCE(hc.email, sc.email) AS email,
        COALESCE(hc.first_name, sc.first_name) AS first_name,
        COALESCE(hc.last_name, sc.last_name) AS last_name,
        COALESCE(hc.full_name, sc.full_name) AS full_name,
        COALESCE(hc.phone, hc.mobile_phone, sc.phone) AS phone,
        COALESCE(hc.address, sc.address_line1) AS address_line1,
        sc.address_line2 AS address_line2,
        COALESCE(hc.city, sc.city) AS city,
        COALESCE(hc.state, sc.province) AS state,
        COALESCE(hc.zip, sc.zip) AS zip,
        COALESCE(hc.country, sc.country) AS country,
        sc.country_code AS country_code,
        hc.company AS company,
        hc.job_title AS job_title,
        hc.lifecycle_stage_normalized AS lifecycle_stage,
        sc.customer_tier AS customer_tier,
        sc.currency AS currency,
        COALESCE(sc.total_spent, CAST(0 AS DECIMAL(18, 2))) AS total_spent,
        COALESCE(sc.orders_count, CAST(0 AS BIGINT)) AS orders_count,
        COALESCE(sc.avg_order_value, CAST(0 AS DECIMAL(18, 2))) AS avg_order_value,
        COALESCE(sc.accepts_marketing, FALSE) AND COALESCE(hc.is_marketable, TRUE) AS accepts_marketing,
        hc.page_views AS page_views,
        hc.sessions AS sessions,
        hc.analytics_source AS analytics_source,
        COALESCE(hc.is_customer, sc.customer_tier IN ('vip', 'gold', 'silver', 'bronze')) AS is_customer,
        COALESCE(sc.is_active, TRUE) AS is_active,
        hc.is_engaged AS is_engaged,
        LEAST(
            COALESCE(hc.created_at, sc.created_at),
            COALESCE(sc.created_at, hc.created_at)
        ) AS created_at,
        GREATEST(
            COALESCE(hc.updated_at, sc.updated_at),
            COALESCE(sc.updated_at, hc.updated_at)
        ) AS updated_at,
        eip.shopify_id IS NOT NULL AS has_shopify,
        eip.hubspot_id IS NOT NULL AS has_hubspot,
        eip.source_count AS source_count,
        CAST(sc.customer_id AS BIGINT) AS shopify_customer_id,
        hc.contact_id AS hubspot_contact_id,
        eip.max_confidence AS match_confidence,
        eip.last_matched_at AS matched_at
    FROM entity_index_pivoted eip
    LEFT JOIN iceberg.staging.stg_shopify_customers sc
        ON eip.shopify_id = CAST(sc.customer_id AS STRING)
    LEFT JOIN iceberg.staging.stg_hubspot_contacts hc
        ON eip.hubspot_id = hc.contact_id
    """

    spark.sql(customers_sql)

    # Verify
    count = spark.sql("SELECT COUNT(*) as cnt FROM iceberg.core.customers").collect()[0].cnt
    logger.info(f"iceberg.core.customers table created with {count} rows")
    return count


def create_orders_view(spark: SparkSession):
    """
    Create the core.orders view as a materialized Iceberg table.
    """
    logger.info("Creating iceberg.core.orders table...")

    # Create the table (or replace if exists)
    spark.sql("DROP TABLE IF EXISTS iceberg.core.orders")

    orders_sql = """
    CREATE TABLE iceberg.core.orders
    USING iceberg
    AS
    SELECT
        CONCAT('shopify_', CAST(so.order_id AS STRING)) AS order_id,
        so.order_number AS order_number,
        so.order_name AS order_name,
        ei.unified_id AS customer_id,
        so.customer_email AS customer_email,
        so.customer_phone AS customer_phone,
        'shopify' AS source,
        so.order_id AS source_order_id,
        CAST(so.customer_id AS STRING) AS source_customer_id,
        so.order_status AS order_status,
        so.financial_status AS financial_status,
        so.fulfillment_status AS fulfillment_status,
        so.currency AS currency,
        so.subtotal_price AS subtotal_price,
        so.total_discounts AS discount_amount,
        so.total_shipping AS shipping_amount,
        so.total_tax AS tax_amount,
        so.total_price AS total_price,
        so.total_outstanding AS total_outstanding,
        so.current_subtotal_price AS current_subtotal,
        so.current_total_price AS current_total,
        so.line_item_count AS line_item_count,
        so.discount_count AS discount_count,
        so.has_discount AS has_discount,
        so.is_cancelled AS is_cancelled,
        so.is_test AS is_test,
        so.taxes_included AS taxes_included,
        so.tax_exempt AS tax_exempt,
        so.buyer_accepts_marketing AS buyer_accepts_marketing,
        so.shipping_country AS shipping_country,
        so.shipping_country_code AS shipping_country_code,
        so.shipping_province AS shipping_state,
        so.shipping_city AS shipping_city,
        so.billing_country AS billing_country,
        so.billing_country_code AS billing_country_code,
        so.source_name AS channel,
        so.referring_site AS referring_site,
        so.landing_site AS landing_site,
        so.created_at AS order_date,
        so.created_at AS created_at,
        so.updated_at AS updated_at,
        so.processed_at AS processed_at,
        so.cancelled_at AS cancelled_at,
        so.closed_at AS closed_at,
        so._staged_at AS staged_at
    FROM iceberg.staging.stg_shopify_orders so
    LEFT JOIN iceberg.semantic.entity_index ei
        ON ei.entity_type = 'customer'
        AND ei.source = 'shopify_customers'
        AND ei.source_id = CAST(so.customer_id AS STRING)
        AND ei.linked_to_unified_id IS NULL
    WHERE so.is_test = FALSE
    """

    spark.sql(orders_sql)

    # Verify
    count = spark.sql("SELECT COUNT(*) as cnt FROM iceberg.core.orders").collect()[0].cnt
    logger.info(f"iceberg.core.orders table created with {count} rows")
    return count


def main():
    parser = argparse.ArgumentParser(description="Create core layer tables")
    parser.add_argument(
        "--view",
        choices=["customers", "orders", "all"],
        default="all",
        help="Which table to create (default: all)"
    )
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Core Tables Creation")
    logger.info("=" * 60)

    spark = create_spark_session()

    try:
        ensure_database_exists(spark)

        if args.view in ("all", "customers"):
            create_customers_view(spark)

        if args.view in ("all", "orders"):
            create_orders_view(spark)

        logger.info("=" * 60)
        logger.info("Core tables created successfully")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Error creating core tables: {e}")
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
