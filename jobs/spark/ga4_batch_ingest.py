"""
GA4 Batch Ingestion: Parquet → raw.ga4_events (Iceberg)

Implements MERGE INTO pattern for idempotent re-execution (Staff Engineer RC-3).
Simulates ingesting a GA4 BigQuery Export. In production, this would be:
  - A BigQuery-to-Iceberg connector
  - An Airflow task that queries BigQuery and writes to Iceberg
  - A scheduled Cloud Function that exports and loads data

For the demo, we read Parquet files from the mounted volume.
"""
import argparse
import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, lit, sha2

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def create_raw_table_if_not_exists(spark):
    """Create raw.ga4_events table with proper schema (Staff Engineer RC-1: includes _raw_id)."""
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.raw.ga4_events (
            _raw_id            STRING    COMMENT 'Unique event identifier (hash of client_id|event_timestamp|event_name)',
            client_id          STRING,
            user_id            STRING,
            event_name         STRING,
            event_timestamp    BIGINT    COMMENT 'Microseconds since epoch (GA4 native)',
            event_date         STRING    COMMENT 'YYYYMMDD (GA4 BigQuery Export format)',
            event_params       STRING    COMMENT 'JSON array of {key, value} pairs',
            user_properties    STRING    COMMENT 'JSON array',
            traffic_source     STRING    COMMENT 'JSON: {source, medium, campaign}',
            device             STRING    COMMENT 'JSON: {category, os, browser, screen_resolution}',
            geo                STRING    COMMENT 'JSON: {country, region, city}',
            page_location      STRING,
            page_title         STRING,
            page_referrer      STRING,
            engagement_time_ms BIGINT,
            is_conversion      BOOLEAN,
            currency           STRING,
            value              DOUBLE,
            session_id         STRING,
            ga_session_number  INT,
            -- Ingestion metadata
            _loaded_at         TIMESTAMP COMMENT 'When loaded into Iceberg',
            _source_file       STRING    COMMENT 'Source Parquet file path'
        ) USING iceberg
        PARTITIONED BY (event_date)
        TBLPROPERTIES (
            'format-version' = '2',
            'write.upsert.enabled' = 'true',
            'write.parquet.compression-codec' = 'zstd'
        )
    """)
    logger.info("Table raw.ga4_events created or already exists")


def ingest_ga4_export(spark, input_path, mode="append"):
    """
    Read GA4 export Parquet and write to raw Iceberg with idempotency.

    Uses MERGE INTO to prevent duplicates on re-execution (Staff Engineer RC-3).
    Natural key: (_raw_id) = hash(client_id, event_timestamp, event_name)
    """
    logger.info(f"Reading Parquet from {input_path}")
    df = spark.read.parquet(input_path)

    # Generate _raw_id as deterministic hash of natural key
    df = df.withColumn(
        "_raw_id",
        sha2(concat_ws("|", col("client_id"), col("event_timestamp").cast("string"), col("event_name")), 256)
    )

    # Add ingestion metadata.
    #
    # These must be deterministic expressions: Spark rejects a MERGE whose
    # source plan contains current_timestamp() or input_file_name() with
    # INVALID_NON_DETERMINISTIC_EXPRESSIONS, because the source is scanned more
    # than once and every scan has to agree. Stamping one Python-side timestamp
    # and the known input path keeps the source stable across scans.
    loaded_at = datetime.now()
    df = df.withColumn("_loaded_at", lit(loaded_at).cast("timestamp")) \
           .withColumn("_source_file", lit(input_path))

    # Create temp view for MERGE
    df.createOrReplaceTempView("ga4_staging")

    # MERGE INTO for idempotent upsert
    logger.info("Executing MERGE INTO for idempotent ingestion")
    merge_result = spark.sql("""
        MERGE INTO iceberg.raw.ga4_events AS target
        USING ga4_staging AS source
        ON target._raw_id = source._raw_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    row_count = df.count()
    logger.info(f"Ingested {row_count} events from {input_path}")
    return row_count


def main():
    parser = argparse.ArgumentParser(description="GA4 Batch Ingestion")
    parser.add_argument("--input", required=True, help="Path to GA4 Parquet export file")
    parser.add_argument("--mode", default="append", choices=["append", "overwrite"], help="Write mode")
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("GA4 Batch Ingest") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "rest") \
        .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181") \
        .getOrCreate()

    try:
        create_raw_table_if_not_exists(spark)
        row_count = ingest_ga4_export(spark, args.input, args.mode)
        logger.info(f"✅ GA4 batch ingestion completed successfully ({row_count} rows)")
    except Exception as e:
        logger.error(f"❌ GA4 batch ingestion failed: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
