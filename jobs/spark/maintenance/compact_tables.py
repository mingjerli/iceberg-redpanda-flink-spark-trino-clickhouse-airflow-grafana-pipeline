"""
Spark Maintenance Job: Table Compaction
========================================

Compacts small files in Iceberg tables to improve query performance.
Iceberg accumulates small files from streaming ingestion and incremental updates.
This job rewrites data files to consolidate them into larger files.

Usage:
    # Compact all tables
    spark-submit compact_tables.py

    # Compact specific namespace
    spark-submit compact_tables.py --namespace staging

    # Compact specific table
    spark-submit compact_tables.py --table iceberg.staging.stg_shopify_orders

    # Dry run (show what would be compacted)
    spark-submit compact_tables.py --dry-run

Configuration:
    - TARGET_FILE_SIZE_BYTES: Target size for compacted files (default: 128MB)
    - MIN_FILES_TO_COMPACT: Minimum files before compaction triggers (default: 5)
"""

import argparse
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
TARGET_FILE_SIZE_BYTES = 128 * 1024 * 1024  # 128 MB
MIN_FILES_TO_COMPACT = 5


@dataclass
class TableStats:
    """Statistics for a table before/after compaction."""
    table_name: str
    file_count_before: int
    file_count_after: int
    data_size_bytes: int
    compaction_time_ms: float
    status: str
    error: Optional[str] = None


def create_spark_session() -> SparkSession:
    """Create Spark session with Iceberg configuration."""
    return SparkSession.builder \
        .appName("IcebergTableCompaction") \
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


def get_all_tables(spark: SparkSession, namespaces: list[str]) -> list[str]:
    """Get all tables in the specified namespaces."""
    tables = []
    for ns in namespaces:
        try:
            result = spark.sql(f"SHOW TABLES IN iceberg.{ns}").collect()
            for row in result:
                tables.append(f"iceberg.{ns}.{row.tableName}")
        except Exception as e:
            logger.warning(f"Failed to list tables in {ns}: {e}")
    return tables


def get_table_file_stats(spark: SparkSession, table_name: str) -> tuple[int, int]:
    """Get file count and total size for a table."""
    try:
        result = spark.sql(f"""
            SELECT
                COUNT(*) as file_count,
                COALESCE(SUM(file_size_in_bytes), 0) as total_size
            FROM {table_name}.files
        """).collect()
        if result:
            return result[0].file_count, result[0].total_size
    except Exception as e:
        logger.debug(f"Could not get file stats for {table_name}: {e}")
    return 0, 0


def compact_table(spark: SparkSession, table_name: str, dry_run: bool = False) -> TableStats:
    """Compact a single table."""
    logger.info(f"Processing table: {table_name}")

    # Get stats before compaction
    file_count_before, data_size = get_table_file_stats(spark, table_name)
    logger.info(f"  Files before: {file_count_before}, Size: {data_size / (1024*1024):.2f} MB")

    # Check if compaction is needed
    if file_count_before < MIN_FILES_TO_COMPACT:
        logger.info(f"  Skipping: Only {file_count_before} files (minimum: {MIN_FILES_TO_COMPACT})")
        return TableStats(
            table_name=table_name,
            file_count_before=file_count_before,
            file_count_after=file_count_before,
            data_size_bytes=data_size,
            compaction_time_ms=0,
            status="skipped",
        )

    if dry_run:
        logger.info(f"  [DRY RUN] Would compact {file_count_before} files")
        return TableStats(
            table_name=table_name,
            file_count_before=file_count_before,
            file_count_after=file_count_before,
            data_size_bytes=data_size,
            compaction_time_ms=0,
            status="dry_run",
        )

    # Run compaction
    start_time = datetime.now()
    try:
        # Use Iceberg's rewrite_data_files procedure
        spark.sql(f"""
            CALL iceberg.system.rewrite_data_files(
                table => '{table_name}',
                options => map(
                    'target-file-size-bytes', '{TARGET_FILE_SIZE_BYTES}',
                    'min-input-files', '{MIN_FILES_TO_COMPACT}'
                )
            )
        """)

        elapsed_ms = (datetime.now() - start_time).total_seconds() * 1000
        file_count_after, _ = get_table_file_stats(spark, table_name)

        logger.info(f"  Compacted: {file_count_before} -> {file_count_after} files in {elapsed_ms:.0f}ms")

        return TableStats(
            table_name=table_name,
            file_count_before=file_count_before,
            file_count_after=file_count_after,
            data_size_bytes=data_size,
            compaction_time_ms=elapsed_ms,
            status="success",
        )

    except Exception as e:
        elapsed_ms = (datetime.now() - start_time).total_seconds() * 1000
        logger.error(f"  Failed to compact: {e}")
        return TableStats(
            table_name=table_name,
            file_count_before=file_count_before,
            file_count_after=file_count_before,
            data_size_bytes=data_size,
            compaction_time_ms=elapsed_ms,
            status="failed",
            error=str(e),
        )


def print_summary(results: list[TableStats]):
    """Print compaction summary."""
    print("\n" + "=" * 80)
    print("COMPACTION SUMMARY")
    print("=" * 80)

    success = [r for r in results if r.status == "success"]
    skipped = [r for r in results if r.status == "skipped"]
    failed = [r for r in results if r.status == "failed"]
    dry_run = [r for r in results if r.status == "dry_run"]

    print(f"Total tables: {len(results)}")
    print(f"  Compacted: {len(success)}")
    print(f"  Skipped: {len(skipped)}")
    print(f"  Failed: {len(failed)}")
    if dry_run:
        print(f"  Dry run: {len(dry_run)}")
    print()

    if success:
        total_files_before = sum(r.file_count_before for r in success)
        total_files_after = sum(r.file_count_after for r in success)
        total_time = sum(r.compaction_time_ms for r in success)
        print(f"Files reduced: {total_files_before} -> {total_files_after}")
        print(f"Total time: {total_time / 1000:.1f}s")

    if failed:
        print("\nFailed tables:")
        for r in failed:
            print(f"  {r.table_name}: {r.error}")


def main():
    parser = argparse.ArgumentParser(description="Compact Iceberg tables")
    parser.add_argument(
        "--namespace",
        type=str,
        help="Specific namespace to compact (default: all)",
    )
    parser.add_argument(
        "--table",
        type=str,
        help="Specific table to compact (e.g., iceberg.staging.stg_shopify_orders)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be compacted without executing",
    )
    args = parser.parse_args()

    logger.info("Starting Iceberg table compaction")
    spark = create_spark_session()

    try:
        if args.table:
            # Compact specific table
            tables = [args.table]
        elif args.namespace:
            # Compact all tables in namespace
            tables = get_all_tables(spark, [args.namespace])
        else:
            # Compact all tables
            namespaces = ["raw", "staging", "semantic", "analytics", "marts"]
            tables = get_all_tables(spark, namespaces)

        logger.info(f"Found {len(tables)} tables to process")

        results = []
        for table in tables:
            result = compact_table(spark, table, args.dry_run)
            results.append(result)

        print_summary(results)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
