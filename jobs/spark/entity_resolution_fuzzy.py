"""
Spark Batch Job: Fuzzy Entity Resolution
=========================================

This script performs batch fuzzy entity resolution for records that couldn't
be matched via exact matching in Flink. It uses name similarity and address
matching to find potential matches.

Matching Strategy (Tiered):
1. Jaro-Winkler similarity on full name (> 0.85 threshold)
2. Levenshtein distance on address (normalized)
3. Combined score weighting

Usage:
    # Process all pending matches
    spark-submit entity_resolution_fuzzy.py

    # Process with custom confidence threshold
    spark-submit entity_resolution_fuzzy.py --threshold 0.80

    # Dry run (no writes, just show candidates)
    spark-submit entity_resolution_fuzzy.py --dry-run

    # Process specific entity type
    spark-submit entity_resolution_fuzzy.py --entity-type customer
"""

import argparse
import logging
from datetime import datetime
from typing import Optional, Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    concat,
    concat_ws,
    current_timestamp,
    expr,
    levenshtein,
    length,
    lit,
    lower,
    regexp_replace,
    soundex,
    trim,
    udf,
    when,
)
from pyspark.sql.types import DoubleType, StringType

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Default match threshold
DEFAULT_MATCH_THRESHOLD = 0.85


def create_spark_session() -> SparkSession:
    """Create Spark session with Iceberg configuration."""
    return SparkSession.builder \
        .appName("EntityResolutionFuzzy") \
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


def jaro_winkler_similarity(s1: str, s2: str) -> float:
    """
    Calculate Jaro-Winkler similarity between two strings.
    Returns a value between 0 (no match) and 1 (exact match).
    """
    if s1 is None or s2 is None:
        return 0.0
    if s1 == s2:
        return 1.0

    s1, s2 = s1.lower().strip(), s2.lower().strip()
    if len(s1) == 0 or len(s2) == 0:
        return 0.0

    # Jaro similarity calculation
    match_distance = max(len(s1), len(s2)) // 2 - 1
    if match_distance < 0:
        match_distance = 0

    s1_matches = [False] * len(s1)
    s2_matches = [False] * len(s2)

    matches = 0
    transpositions = 0

    for i, c1 in enumerate(s1):
        start = max(0, i - match_distance)
        end = min(len(s2), i + match_distance + 1)

        for j in range(start, end):
            if s2_matches[j] or c1 != s2[j]:
                continue
            s1_matches[i] = True
            s2_matches[j] = True
            matches += 1
            break

    if matches == 0:
        return 0.0

    k = 0
    for i, c1 in enumerate(s1):
        if not s1_matches[i]:
            continue
        while not s2_matches[k]:
            k += 1
        if c1 != s2[k]:
            transpositions += 1
        k += 1

    jaro = (matches / len(s1) + matches / len(s2) +
            (matches - transpositions / 2) / matches) / 3

    # Winkler modification (boost for common prefix)
    prefix_length = 0
    for i in range(min(4, min(len(s1), len(s2)))):
        if s1[i] == s2[i]:
            prefix_length += 1
        else:
            break

    return jaro + prefix_length * 0.1 * (1 - jaro)


# Register UDF for Jaro-Winkler similarity
jaro_winkler_udf = udf(jaro_winkler_similarity, DoubleType())


def normalize_address(address: str) -> str:
    """Normalize address string for comparison."""
    if address is None:
        return ""
    # Lowercase, remove punctuation, normalize whitespace
    address = address.lower().strip()
    # Common abbreviation normalization
    replacements = {
        'street': 'st',
        'avenue': 'ave',
        'boulevard': 'blvd',
        'drive': 'dr',
        'road': 'rd',
        'lane': 'ln',
        'court': 'ct',
        'apartment': 'apt',
        'suite': 'ste',
        'north': 'n',
        'south': 's',
        'east': 'e',
        'west': 'w',
    }
    for full, abbr in replacements.items():
        address = address.replace(full, abbr)
    # Remove common punctuation
    for char in ['.', ',', '#', '-']:
        address = address.replace(char, ' ')
    # Normalize whitespace
    address = ' '.join(address.split())
    return address


normalize_address_udf = udf(normalize_address, StringType())


def get_unresolved_entities(spark: SparkSession, entity_type: str) -> DataFrame:
    """
    Get staging records that don't have entity_index entries yet.
    These are records that couldn't be matched via exact matching.
    """
    logger.info(f"Finding unresolved {entity_type} entities...")

    # Get Shopify customers without entity mappings
    shopify_unresolved = spark.sql("""
        SELECT
            'shopify_customers' AS source,
            CAST(sc.customer_id AS STRING) AS source_id,
            sc.email,
            sc.first_name,
            sc.last_name,
            sc.full_name,
            sc.phone,
            sc.address_line1 AS address,
            sc.city,
            sc.province AS state,
            sc.zip,
            sc.country,
            sc._staged_at
        FROM iceberg.staging.stg_shopify_customers sc
        LEFT JOIN iceberg.semantic.entity_index ei
            ON ei.source = 'shopify_customers'
            AND ei.source_id = CAST(sc.customer_id AS STRING)
        WHERE ei.unified_id IS NULL
    """)

    # Get HubSpot contacts without entity mappings
    hubspot_unresolved = spark.sql("""
        SELECT
            'hubspot_contacts' AS source,
            hc.contact_id AS source_id,
            hc.email,
            hc.first_name,
            hc.last_name,
            hc.full_name,
            COALESCE(hc.phone, hc.mobile_phone) AS phone,
            hc.address,
            hc.city,
            hc.state,
            hc.zip,
            hc.country,
            hc._staged_at
        FROM iceberg.staging.stg_hubspot_contacts hc
        LEFT JOIN iceberg.semantic.entity_index ei
            ON ei.source = 'hubspot_contacts'
            AND ei.source_id = hc.contact_id
        WHERE ei.unified_id IS NULL
    """)

    unresolved = shopify_unresolved.union(hubspot_unresolved)
    count = unresolved.count()
    logger.info(f"Found {count} unresolved {entity_type} entities")
    return unresolved


def get_existing_entities(spark: SparkSession, entity_type: str) -> DataFrame:
    """
    Get existing unified entities with their key attributes for matching.
    """
    logger.info(f"Loading existing {entity_type} entities for matching...")

    # Get all existing entities with their attributes from all sources
    existing = spark.sql("""
        SELECT DISTINCT
            ei.unified_id,
            COALESCE(hc.full_name, sc.full_name) AS full_name,
            COALESCE(hc.first_name, sc.first_name) AS first_name,
            COALESCE(hc.last_name, sc.last_name) AS last_name,
            COALESCE(hc.address, sc.address_line1) AS address,
            COALESCE(hc.city, sc.city) AS city,
            COALESCE(hc.state, sc.province) AS state,
            COALESCE(hc.zip, sc.zip) AS zip
        FROM iceberg.semantic.entity_index ei
        LEFT JOIN iceberg.staging.stg_shopify_customers sc
            ON ei.source = 'shopify_customers'
            AND ei.source_id = CAST(sc.customer_id AS STRING)
        LEFT JOIN iceberg.staging.stg_hubspot_contacts hc
            ON ei.source = 'hubspot_contacts'
            AND ei.source_id = hc.contact_id
        WHERE ei.entity_type = 'customer'
          AND ei.linked_to_unified_id IS NULL
    """)

    count = existing.count()
    logger.info(f"Loaded {count} existing {entity_type} entities")
    return existing


def find_fuzzy_matches(
    spark: SparkSession,
    unresolved: DataFrame,
    existing: DataFrame,
    threshold: float
) -> DataFrame:
    """
    Find potential matches using fuzzy string matching.
    Uses Jaro-Winkler similarity on names and normalized address comparison.
    """
    logger.info(f"Finding fuzzy matches with threshold {threshold}...")

    # Add normalized fields for matching
    unresolved_prep = unresolved \
        .withColumn("norm_full_name", lower(trim(col("full_name")))) \
        .withColumn("norm_address", normalize_address_udf(col("address"))) \
        .withColumn("name_soundex", soundex(col("last_name")))

    existing_prep = existing \
        .withColumn("exist_norm_full_name", lower(trim(col("full_name")))) \
        .withColumn("exist_norm_address", normalize_address_udf(col("address"))) \
        .withColumn("exist_name_soundex", soundex(col("last_name")))

    # Blocking: Only compare records with same soundex or same zip
    # This dramatically reduces the comparison space
    candidates = unresolved_prep.alias("u").join(
        existing_prep.alias("e"),
        (col("u.name_soundex") == col("e.exist_name_soundex")) |
        ((col("u.zip").isNotNull()) & (col("u.zip") == col("e.zip"))),
        "inner"
    )

    # Calculate similarity scores
    scored = candidates \
        .withColumn(
            "name_similarity",
            jaro_winkler_udf(col("u.norm_full_name"), col("e.exist_norm_full_name"))
        ) \
        .withColumn(
            "address_levenshtein",
            levenshtein(col("u.norm_address"), col("e.exist_norm_address"))
        ) \
        .withColumn(
            "address_max_len",
            when(
                length(col("u.norm_address")) > length(col("e.exist_norm_address")),
                length(col("u.norm_address"))
            ).otherwise(length(col("e.exist_norm_address")))
        ) \
        .withColumn(
            "address_similarity",
            when(
                col("address_max_len") > 0,
                1 - (col("address_levenshtein") / col("address_max_len"))
            ).otherwise(lit(0.0))
        ) \
        .withColumn(
            "overall_score",
            # Weight: 70% name, 30% address
            col("name_similarity") * 0.7 + col("address_similarity") * 0.3
        )

    # Filter to matches above threshold
    matches = scored.filter(col("overall_score") >= threshold) \
        .select(
            col("u.source"),
            col("u.source_id"),
            col("e.unified_id").alias("matched_unified_id"),
            col("name_similarity"),
            col("address_similarity"),
            col("overall_score").alias("match_confidence"),
            concat_ws(
                "; ",
                concat(lit("Name similarity: "), col("name_similarity")),
                concat(lit("Address similarity: "), col("address_similarity")),
                concat(lit("Overall score: "), col("overall_score"))
            ).alias("match_reason")
        )

    # Keep only the best match per unresolved record
    from pyspark.sql.window import Window
    window = Window.partitionBy("source", "source_id").orderBy(col("match_confidence").desc())

    best_matches = matches \
        .withColumn("rank", expr("row_number() OVER (PARTITION BY source, source_id ORDER BY match_confidence DESC)")) \
        .filter(col("rank") == 1) \
        .drop("rank")

    match_count = best_matches.count()
    logger.info(f"Found {match_count} fuzzy matches above threshold {threshold}")
    return best_matches


def create_new_entities(
    spark: SparkSession,
    unresolved: DataFrame,
    matched_ids: DataFrame
) -> DataFrame:
    """
    Create new unified entities for records that couldn't be fuzzy matched.
    """
    logger.info("Creating new entities for unmatched records...")

    # Find unresolved records that didn't get matched
    unmatched = unresolved.alias("u").join(
        matched_ids.select("source", "source_id").alias("m"),
        (col("u.source") == col("m.source")) & (col("u.source_id") == col("m.source_id")),
        "left_anti"
    )

    # Generate new unified IDs
    new_entities = unmatched.select(
        concat(
            lit("U-"),
            expr("unix_timestamp()"),
            lit("-new-"),
            col("source_id")
        ).alias("unified_id"),
        lit("customer").alias("entity_type"),
        col("source"),
        col("source_id"),
        lit("new_entity").alias("match_type"),
        lit(1.0).cast("decimal(3,2)").alias("match_confidence"),
        lit("New entity created - no fuzzy match found").alias("match_reason"),
        lit(None).cast("string").alias("linked_to_unified_id"),
        current_timestamp().alias("matched_at"),
        lit("spark_fuzzy_batch").alias("matched_by"),
        col("_staged_at")
    )

    count = new_entities.count()
    logger.info(f"Creating {count} new entities")
    return new_entities


def write_entity_index(spark: SparkSession, matches: DataFrame, new_entities: DataFrame, dry_run: bool):
    """
    Write resolved entities to the entity_index table.
    """
    if dry_run:
        logger.info("DRY RUN - Would write the following matches:")
        matches.show(20, truncate=False)
        logger.info("DRY RUN - Would create the following new entities:")
        new_entities.show(20, truncate=False)
        return

    # Write fuzzy matches to entity_index
    if matches.count() > 0:
        logger.info("Writing fuzzy matches to entity_index...")
        match_records = matches.select(
            col("matched_unified_id").alias("unified_id"),
            lit("customer").alias("entity_type"),
            col("source"),
            col("source_id"),
            lit("fuzzy_name_address").alias("match_type"),
            col("match_confidence").cast("decimal(3,2)"),
            col("match_reason"),
            lit(None).cast("string").alias("linked_to_unified_id"),
            current_timestamp().alias("matched_at"),
            lit("spark_fuzzy_batch").alias("matched_by"),
            lit(None).cast("timestamp").alias("_staged_at")
        )
        match_records.writeTo("iceberg.semantic.entity_index").append()
        logger.info(f"Wrote {matches.count()} fuzzy match records")

    # Write new entities to entity_index
    if new_entities.count() > 0:
        logger.info("Writing new entities to entity_index...")
        new_entities.writeTo("iceberg.semantic.entity_index").append()
        logger.info(f"Wrote {new_entities.count()} new entity records")


def write_blocking_index(spark: SparkSession, new_entities: DataFrame, dry_run: bool):
    """
    Add blocking keys for new entities.
    """
    if dry_run:
        logger.info("DRY RUN - Would add blocking keys for new entities")
        return

    if new_entities.count() == 0:
        return

    logger.info("Adding blocking keys for new entities...")

    # Get the staging data for new entities to extract blocking keys
    # This requires joining back to staging tables
    # For simplicity, we'll add this in a separate step or use the backfill job
    logger.info("Blocking keys for new entities will be added by the backfill job")


def update_resolution_stats(
    spark: SparkSession,
    total_processed: int,
    fuzzy_matches: int,
    new_entities: int,
    avg_confidence: float,
    start_time: datetime
):
    """
    Record resolution statistics for monitoring.
    """
    try:
        end_time = datetime.now()
        duration = int((end_time - start_time).total_seconds())

        spark.sql(f"""
            INSERT INTO iceberg.semantic.entity_resolution_stats
            (batch_id, batch_type, records_processed, new_entities_created,
             existing_entities_linked, fuzzy_name_matches, avg_match_confidence,
             started_at, completed_at, duration_seconds)
            VALUES (
                'fuzzy-{end_time.strftime("%Y%m%d%H%M%S")}',
                'fuzzy_match',
                {total_processed},
                {new_entities},
                {fuzzy_matches},
                {fuzzy_matches},
                {avg_confidence:.2f},
                timestamp '{start_time.strftime("%Y-%m-%d %H:%M:%S")}',
                timestamp '{end_time.strftime("%Y-%m-%d %H:%M:%S")}',
                {duration}
            )
        """)
        logger.info("Updated resolution statistics")
    except Exception as e:
        logger.warning(f"Could not update resolution stats: {e}")


def main():
    parser = argparse.ArgumentParser(description='Fuzzy Entity Resolution Batch Job')
    parser.add_argument(
        '--threshold',
        type=float,
        default=DEFAULT_MATCH_THRESHOLD,
        help=f'Match confidence threshold (default: {DEFAULT_MATCH_THRESHOLD})'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show matches without writing to database'
    )
    parser.add_argument(
        '--entity-type',
        type=str,
        default='customer',
        choices=['customer'],
        help='Entity type to process (default: customer)'
    )
    args = parser.parse_args()

    start_time = datetime.now()
    logger.info(f"Starting fuzzy entity resolution (threshold={args.threshold})")

    spark = create_spark_session()

    try:
        # Get unresolved entities (no exact match)
        unresolved = get_unresolved_entities(spark, args.entity_type)

        if unresolved.count() == 0:
            logger.info("No unresolved entities to process")
            return

        # Get existing entities for comparison
        existing = get_existing_entities(spark, args.entity_type)

        if existing.count() == 0:
            logger.info("No existing entities - all unresolved will become new entities")
            new_entities = create_new_entities(spark, unresolved, spark.createDataFrame([], unresolved.schema))
            write_entity_index(spark, spark.createDataFrame([], new_entities.schema), new_entities, args.dry_run)
            return

        # Find fuzzy matches
        matches = find_fuzzy_matches(spark, unresolved, existing, args.threshold)

        # Create new entities for unmatched records
        new_entities = create_new_entities(spark, unresolved, matches)

        # Write results
        write_entity_index(spark, matches, new_entities, args.dry_run)
        write_blocking_index(spark, new_entities, args.dry_run)

        # Calculate average confidence for stats
        avg_confidence = matches.selectExpr("avg(match_confidence)").collect()[0][0] or 0.0

        # Update stats
        if not args.dry_run:
            update_resolution_stats(
                spark,
                total_processed=unresolved.count(),
                fuzzy_matches=matches.count(),
                new_entities=new_entities.count(),
                avg_confidence=float(avg_confidence),
                start_time=start_time
            )

        logger.info(f"Fuzzy resolution complete: {matches.count()} matched, {new_entities.count()} new entities")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
