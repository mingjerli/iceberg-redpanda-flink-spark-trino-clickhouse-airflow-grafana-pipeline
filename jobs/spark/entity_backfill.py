"""
Spark Batch Job: Entity Resolution Backfill
=============================================

This script performs initial population and backfill of entity resolution tables.
Use this when:
1. Setting up entity resolution for the first time
2. Re-processing historical data after schema changes
3. Rebuilding blocking indexes

The job processes staging data in chronological order to maintain consistency
with how real-time resolution would have processed the data.

Usage:
    # Initial setup - process all staging data
    spark-submit entity_backfill.py --mode initial

    # Rebuild blocking index only
    spark-submit entity_backfill.py --mode rebuild-index

    # Backfill specific date range
    spark-submit entity_backfill.py --mode range --start-date 2024-01-01 --end-date 2024-01-31

    # Dry run
    spark-submit entity_backfill.py --mode initial --dry-run
"""

import argparse
import logging
import os
from datetime import datetime
from typing import Optional
import uuid

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    concat,
    concat_ws,
    current_timestamp,
    expr,
    lit,
    row_number,
    when,
)
from pyspark.sql.window import Window

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_spark_session() -> SparkSession:
    """Create Spark session with Iceberg configuration."""
    return SparkSession.builder \
        .appName("EntityResolutionBackfill") \
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


def ensure_tables_exist(spark: SparkSession):
    """Create semantic tables if they don't exist."""
    logger.info("Ensuring semantic tables exist...")

    # Create semantic database
    spark.sql("CREATE DATABASE IF NOT EXISTS iceberg.semantic")

    # Create entity_index table
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.semantic.entity_index (
            unified_id STRING NOT NULL,
            entity_type STRING NOT NULL,
            source STRING NOT NULL,
            source_id STRING NOT NULL,
            match_type STRING,
            match_confidence DECIMAL(3, 2),
            match_reason STRING,
            linked_to_unified_id STRING,
            matched_at TIMESTAMP NOT NULL,
            matched_by STRING,
            _staged_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (entity_type)
    """)

    # Create blocking_index table
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.semantic.blocking_index (
            blocking_key STRING NOT NULL,
            blocking_key_type STRING NOT NULL,
            unified_id STRING NOT NULL,
            entity_type STRING NOT NULL,
            source STRING NOT NULL,
            source_id STRING NOT NULL,
            key_value STRING,
            is_primary BOOLEAN,
            created_at TIMESTAMP NOT NULL,
            expires_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (blocking_key_type, entity_type)
    """)

    # Create resolution stats table
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.semantic.entity_resolution_stats (
            batch_id STRING NOT NULL,
            batch_type STRING NOT NULL,
            records_processed BIGINT,
            new_entities_created BIGINT,
            existing_entities_linked BIGINT,
            exact_email_matches BIGINT,
            exact_phone_matches BIGINT,
            fuzzy_name_matches BIGINT,
            deterministic_rule_matches BIGINT,
            ml_score_matches BIGINT,
            no_match_new_entity BIGINT,
            avg_match_confidence DECIMAL(3, 2),
            low_confidence_matches BIGINT,
            started_at TIMESTAMP NOT NULL,
            completed_at TIMESTAMP,
            duration_seconds BIGINT,
            source_breakdown STRING
        )
        USING iceberg
        PARTITIONED BY (months(started_at))
    """)

    logger.info("Semantic tables created/verified")


def get_all_staging_customers(
    spark: SparkSession,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> DataFrame:
    """
    Get all customer records from staging tables for backfill.
    """
    logger.info("Loading staging customer data...")

    date_filter = ""
    if start_date:
        date_filter += f" AND _staged_at >= '{start_date}'"
    if end_date:
        date_filter += f" AND _staged_at <= '{end_date}'"

    # Shopify customers
    shopify = spark.sql(f"""
        SELECT
            'shopify_customers' AS source,
            CAST(customer_id AS STRING) AS source_id,
            email_token,
            first_name_token,
            last_name_token,
            full_name_token,
            phone_token,
            last_name_prefix_token,
            address_line1_token AS address_token,
            city,
            province AS state,
            zip,
            country,
            created_at,
            _staged_at
        FROM iceberg.staging.stg_shopify_customers
        WHERE 1=1 {date_filter}
    """)

    # HubSpot contacts
    hubspot = spark.sql(f"""
        SELECT
            'hubspot_contacts' AS source,
            contact_id AS source_id,
            email_token,
            first_name_token,
            last_name_token,
            full_name_token,
            COALESCE(phone_token, mobile_phone_token) AS phone_token,
            last_name_prefix_token,
            address_token,
            city,
            state,
            zip,
            country,
            created_at,
            _staged_at
        FROM iceberg.staging.stg_hubspot_contacts
        WHERE 1=1 {date_filter}
    """)

    # Stripe customers
    stripe = spark.sql(f"""
        SELECT
            'stripe_customers' AS source,
            customer_id AS source_id,
            email_token,
            first_name_token,
            last_name_token,
            full_name_token,
            phone_token,
            last_name_prefix_token,
            address_line1_token AS address_token,
            city,
            state,
            postal_code AS zip,
            country,
            created_at,
            _staged_at
        FROM iceberg.staging.stg_stripe_customers
        WHERE 1=1 {date_filter}
    """)

    # Mailchimp subscribers
    mailchimp = spark.sql(f"""
        SELECT
            'mailchimp_subscribers' AS source,
            CAST(subscriber_id_token AS STRING) AS source_id,
            email_normalized_token AS email_token,
            first_name_token,
            last_name_token,
            full_name_token,
            phone_normalized_token AS phone_token,
            last_name_prefix_token,
            CAST(NULL AS STRING) AS address_token,
            CAST(NULL AS STRING) AS city,
            CAST(NULL AS STRING) AS state,
            CAST(NULL AS STRING) AS zip,
            CAST(NULL AS STRING) AS country,
            signup_timestamp AS created_at,
            _staged_at
        FROM iceberg.staging.stg_mailchimp_subscribers
        WHERE 1=1 {date_filter}
    """)

    # GA4 sessions (only logged-in users with user_id)
    # Note: user_id in GA4 is set to email for entity resolution demo, so
    # user_id_token carries class email like every other source's email_token.
    ga4 = spark.sql(f"""
        SELECT
            'ga4_sessions' AS source,
            CAST(client_id AS STRING) AS source_id,
            user_id_token AS email_token,
            CAST(NULL AS STRING) AS first_name_token,
            CAST(NULL AS STRING) AS last_name_token,
            CAST(NULL AS STRING) AS full_name_token,
            CAST(NULL AS STRING) AS phone_token,
            CAST(NULL AS STRING) AS last_name_prefix_token,
            CAST(NULL AS STRING) AS address_token,
            CAST(NULL AS STRING) AS city,
            CAST(NULL AS STRING) AS state,
            CAST(NULL AS STRING) AS zip,
            geo_country AS country,
            session_start AS created_at,
            _staged_at
        FROM iceberg.staging.stg_ga4_sessions
        WHERE user_id_token IS NOT NULL
          AND user_id_token != ''
          {date_filter}
    """)

    all_customers = shopify.union(hubspot).union(stripe).union(mailchimp).union(ga4).filter(
        # Filter out records without a valid source_id
        col("source_id").isNotNull()
    )
    count = all_customers.count()
    logger.info(f"Loaded {count} staging customer records")
    return all_customers


def perform_initial_resolution(spark: SparkSession, staging_data: DataFrame, dry_run: bool) -> tuple:
    """
    Perform initial entity resolution on all staging data.
    Uses exact matching on email first, then groups remaining.

    Returns:
        Tuple of (entity_index_df, blocking_index_df)
    """
    logger.info("Performing initial entity resolution...")

    # Values arrive pre-normalized: staging tokenizes email/phone as its last
    # step, so exact-token equality reproduces the previous lower(trim(email))
    # match exactly. Keeping the internal names normalized_email/normalized_phone
    # means the windowing below needs no further change.
    prepared = staging_data.withColumnRenamed("email_token", "normalized_email") \
                           .withColumnRenamed("phone_token", "normalized_phone")

    # Group by normalized email to find matching records
    # Records with the same email get the same unified_id
    window_email = Window.partitionBy("normalized_email").orderBy("_staged_at")

    with_groups = prepared.withColumn(
        "email_group_rank",
        row_number().over(window_email)
    ).withColumn(
        # Generate unified_id based on first record in email group
        "unified_id_base",
        when(
            col("email_group_rank") == 1,
            concat(lit("U-"), expr("uuid()"))
        )
    )

    # Fill in the unified_id for all records in the same email group
    window_fill = Window.partitionBy("normalized_email").orderBy("_staged_at").rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )

    with_unified_id = with_groups.withColumn(
        "unified_id",
        expr("last(unified_id_base, true) OVER (PARTITION BY normalized_email ORDER BY _staged_at)")
    ).withColumn(
        # For records without email, each gets its own unified_id
        "unified_id",
        when(
            (col("normalized_email").isNull()) | (col("normalized_email") == ""),
            concat(lit("U-"), expr("uuid()"))
        ).otherwise(col("unified_id"))
    )

    # Determine match type
    entity_index_df = with_unified_id.withColumn(
        "match_type",
        when(
            col("email_group_rank") > 1,
            lit("exact_email")
        ).otherwise(lit("new_entity"))
    ).withColumn(
        "match_confidence",
        when(
            col("match_type") == "exact_email",
            lit(1.0).cast("decimal(3,2)")
        ).otherwise(lit(1.0).cast("decimal(3,2)"))
    ).withColumn(
        "match_reason",
        when(
            col("match_type") == "exact_email",
            concat(lit("Matched via email token: "), col("normalized_email"))
        ).otherwise(lit("New entity created during backfill"))
    ).select(
        col("unified_id"),
        lit("customer").alias("entity_type"),
        col("source"),
        col("source_id"),
        col("match_type"),
        col("match_confidence"),
        col("match_reason"),
        lit(None).cast("string").alias("linked_to_unified_id"),
        current_timestamp().alias("matched_at"),
        lit("spark_backfill").alias("matched_by"),
        col("_staged_at")
    )

    # Build blocking index entries
    # Email blocking keys
    email_blocking = with_unified_id.filter(
        (col("normalized_email").isNotNull()) & (col("normalized_email") != "")
    ).select(
        concat(lit("email:"), col("normalized_email")).alias("blocking_key"),
        lit("email").alias("blocking_key_type"),
        col("unified_id"),
        lit("customer").alias("entity_type"),
        col("source"),
        col("source_id"),
        col("normalized_email").alias("key_value"),
        lit(True).alias("is_primary"),
        current_timestamp().alias("created_at"),
        lit(None).cast("timestamp").alias("expires_at")
    )

    # Phone blocking keys
    phone_blocking = with_unified_id.filter(
        (col("normalized_phone").isNotNull()) &
        (col("normalized_phone") != "")
    ).select(
        concat(lit("phone:"), col("normalized_phone")).alias("blocking_key"),
        lit("phone").alias("blocking_key_type"),
        col("unified_id"),
        lit("customer").alias("entity_type"),
        col("source"),
        col("source_id"),
        col("normalized_phone").alias("key_value"),
        lit(False).alias("is_primary"),
        current_timestamp().alias("created_at"),
        lit(None).cast("timestamp").alias("expires_at")
    )

    # Name+zip blocking keys (for fuzzy matching)
    name_zip_blocking = with_unified_id.filter(
        (col("last_name_prefix_token").isNotNull()) &
        (col("zip").isNotNull())
    ).select(
        concat(
            lit("name_zip:"),
            col("last_name_prefix_token"),
            lit("_"),
            col("zip")
        ).alias("blocking_key"),
        lit("name_zip").alias("blocking_key_type"),
        col("unified_id"),
        lit("customer").alias("entity_type"),
        col("source"),
        col("source_id"),
        concat_ws("_", col("last_name_prefix_token"), col("zip")).alias("key_value"),
        lit(False).alias("is_primary"),
        current_timestamp().alias("created_at"),
        lit(None).cast("timestamp").alias("expires_at")
    )

    blocking_index_df = email_blocking.union(phone_blocking).union(name_zip_blocking)

    # Log statistics
    total_records = entity_index_df.count()
    unique_entities = entity_index_df.select("unified_id").distinct().count()
    exact_matches = entity_index_df.filter(col("match_type") == "exact_email").count()

    logger.info(f"Entity resolution statistics:")
    logger.info(f"  Total records: {total_records}")
    logger.info(f"  Unique entities: {unique_entities}")
    logger.info(f"  Exact email matches: {exact_matches}")
    logger.info(f"  New entities: {total_records - exact_matches}")
    logger.info(f"  Blocking index entries: {blocking_index_df.count()}")

    return entity_index_df, blocking_index_df


def write_results(
    spark: SparkSession,
    entity_index_df: DataFrame,
    blocking_index_df: DataFrame,
    dry_run: bool
):
    """Write results to Iceberg tables."""
    if dry_run:
        logger.info("DRY RUN - Would write entity_index:")
        entity_index_df.show(20, truncate=False)
        logger.info("DRY RUN - Would write blocking_index:")
        blocking_index_df.show(20, truncate=False)
        return

    logger.info("Writing entity_index...")
    entity_index_df.writeTo("iceberg.semantic.entity_index").append()
    logger.info(f"Wrote {entity_index_df.count()} entity_index records")

    logger.info("Writing blocking_index...")
    blocking_index_df.writeTo("iceberg.semantic.blocking_index").append()
    logger.info(f"Wrote {blocking_index_df.count()} blocking_index records")


def rebuild_blocking_index(spark: SparkSession, dry_run: bool):
    """
    Rebuild blocking index from existing entity_index.
    Useful after schema changes or index corruption.
    """
    logger.info("Rebuilding blocking index from entity_index...")

    # Clear existing blocking index
    if not dry_run:
        logger.info("Truncating existing blocking_index...")
        spark.sql("DELETE FROM iceberg.semantic.blocking_index WHERE 1=1")

    # Get all entities with their staging attributes
    entities = spark.sql("""
        SELECT
            ei.unified_id,
            ei.source,
            ei.source_id,
            COALESCE(hc.email_token, sc.email_token, stc.email_token,
                     mc.email_normalized_token, ga4_sub.user_id_token) AS normalized_email,
            COALESCE(hc.phone_token, hc.mobile_phone_token, sc.phone_token,
                     stc.phone_token, mc.phone_normalized_token) AS normalized_phone,
            COALESCE(hc.last_name_prefix_token, sc.last_name_prefix_token,
                     stc.last_name_prefix_token, mc.last_name_prefix_token) AS last_name_prefix_token,
            COALESCE(hc.zip, sc.zip, stc.postal_code) AS zip
        FROM iceberg.semantic.entity_index ei
        LEFT JOIN iceberg.staging.stg_shopify_customers sc
            ON ei.source = 'shopify_customers'
            AND ei.source_id = CAST(sc.customer_id AS STRING)
        LEFT JOIN iceberg.staging.stg_hubspot_contacts hc
            ON ei.source = 'hubspot_contacts'
            AND ei.source_id = hc.contact_id
        LEFT JOIN iceberg.staging.stg_stripe_customers stc
            ON ei.source = 'stripe_customers'
            AND ei.source_id = stc.customer_id
        LEFT JOIN iceberg.staging.stg_mailchimp_subscribers mc
            ON ei.source = 'mailchimp_subscribers'
            AND ei.source_id = mc.subscriber_id_token
        LEFT JOIN (
            SELECT
                client_id,
                user_id_token,
                geo_country,
                session_start,
                ROW_NUMBER() OVER (
                    PARTITION BY client_id
                    ORDER BY session_start DESC
                ) AS rn
            FROM iceberg.staging.stg_ga4_sessions
            WHERE user_id_token IS NOT NULL AND user_id_token != ''
        ) ga4_sub ON ei.source = 'ga4_sessions' AND ei.source_id = ga4_sub.client_id AND ga4_sub.rn = 1
        WHERE ei.entity_type = 'customer'
          AND ei.linked_to_unified_id IS NULL
    """)

    # Build email blocking keys
    email_keys = entities.filter(
        (col("normalized_email").isNotNull()) & (col("normalized_email") != "")
    ).select(
        concat(lit("email:"), col("normalized_email")).alias("blocking_key"),
        lit("email").alias("blocking_key_type"),
        col("unified_id"),
        lit("customer").alias("entity_type"),
        col("source"),
        col("source_id"),
        col("normalized_email").alias("key_value"),
        lit(True).alias("is_primary"),
        current_timestamp().alias("created_at"),
        lit(None).cast("timestamp").alias("expires_at")
    )

    # Build phone blocking keys
    phone_keys = entities.filter(
        (col("normalized_phone").isNotNull()) &
        (col("normalized_phone") != "")
    ).select(
        concat(lit("phone:"), col("normalized_phone")).alias("blocking_key"),
        lit("phone").alias("blocking_key_type"),
        col("unified_id"),
        lit("customer").alias("entity_type"),
        col("source"),
        col("source_id"),
        col("normalized_phone").alias("key_value"),
        lit(False).alias("is_primary"),
        current_timestamp().alias("created_at"),
        lit(None).cast("timestamp").alias("expires_at")
    )

    # Build name_zip blocking keys
    name_zip_keys = entities.filter(
        (col("last_name_prefix_token").isNotNull()) & (col("zip").isNotNull())
    ).select(
        concat(
            lit("name_zip:"),
            col("last_name_prefix_token"),
            lit("_"),
            col("zip")
        ).alias("blocking_key"),
        lit("name_zip").alias("blocking_key_type"),
        col("unified_id"),
        lit("customer").alias("entity_type"),
        col("source"),
        col("source_id"),
        concat_ws("_", col("last_name_prefix_token"), col("zip")).alias("key_value"),
        lit(False).alias("is_primary"),
        current_timestamp().alias("created_at"),
        lit(None).cast("timestamp").alias("expires_at")
    )

    blocking_index_df = email_keys.union(phone_keys).union(name_zip_keys)

    if dry_run:
        logger.info("DRY RUN - Would rebuild blocking_index:")
        blocking_index_df.show(20, truncate=False)
        logger.info(f"DRY RUN - Would write {blocking_index_df.count()} blocking keys")
        return

    blocking_index_df.writeTo("iceberg.semantic.blocking_index").append()
    logger.info(f"Rebuilt blocking_index with {blocking_index_df.count()} entries")


def update_backfill_stats(spark: SparkSession, mode: str, records: int, entities: int, start_time: datetime):
    """Record backfill statistics."""
    try:
        end_time = datetime.now()
        duration = int((end_time - start_time).total_seconds())

        spark.sql(f"""
            INSERT INTO iceberg.semantic.entity_resolution_stats
            (batch_id, batch_type, records_processed, new_entities_created,
             existing_entities_linked, started_at, completed_at, duration_seconds)
            VALUES (
                'backfill-{end_time.strftime("%Y%m%d%H%M%S")}',
                'backfill_{mode}',
                {records},
                {entities},
                {records - entities},
                timestamp '{start_time.strftime("%Y-%m-%d %H:%M:%S")}',
                timestamp '{end_time.strftime("%Y-%m-%d %H:%M:%S")}',
                {duration}
            )
        """)
    except Exception as e:
        logger.warning(f"Could not update backfill stats: {e}")


def main():
    parser = argparse.ArgumentParser(description='Entity Resolution Backfill')
    parser.add_argument(
        '--mode',
        type=str,
        default='initial',
        choices=['initial', 'rebuild-index', 'range'],
        help='Backfill mode: initial (full), rebuild-index, or range'
    )
    parser.add_argument(
        '--start-date',
        type=str,
        help='Start date for range mode (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--end-date',
        type=str,
        help='End date for range mode (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be done without writing'
    )
    args = parser.parse_args()

    start_time = datetime.now()
    logger.info(f"Starting entity backfill (mode={args.mode})")

    spark = create_spark_session()

    try:
        # Ensure tables exist
        ensure_tables_exist(spark)

        if args.mode == 'rebuild-index':
            rebuild_blocking_index(spark, args.dry_run)
            return

        # Get staging data
        staging_data = get_all_staging_customers(
            spark,
            start_date=args.start_date,
            end_date=args.end_date
        )

        if staging_data.count() == 0:
            logger.info("No staging data to process")
            return

        # Perform resolution
        entity_index_df, blocking_index_df = perform_initial_resolution(
            spark, staging_data, args.dry_run
        )

        # Write results
        write_results(spark, entity_index_df, blocking_index_df, args.dry_run)

        # Update stats
        if not args.dry_run:
            update_backfill_stats(
                spark,
                args.mode,
                staging_data.count(),
                entity_index_df.select("unified_id").distinct().count(),
                start_time
            )

        logger.info("Backfill complete!")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
