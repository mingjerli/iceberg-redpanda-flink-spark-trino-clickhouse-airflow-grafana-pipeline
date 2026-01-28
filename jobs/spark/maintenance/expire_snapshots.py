"""
Spark Maintenance Job: Snapshot Expiration
============================================

Expires old snapshots from Iceberg tables to reclaim storage space.
Iceberg creates a new snapshot for each commit, which accumulates over time.
This job removes snapshots older than the retention period.

Usage:
    # Expire snapshots older than 7 days (default)
    spark-submit expire_snapshots.py

    # Custom retention period (3 days)
    spark-submit expire_snapshots.py --retention-days 3

    # Specific namespace
    spark-submit expire_snapshots.py --namespace staging

    # Specific table
    spark-submit expire_snapshots.py --table iceberg.staging.stg_shopify_orders

    # Dry run
    spark-submit expire_snapshots.py --dry-run

Configuration:
    - DEFAULT_RETENTION_DAYS: Default snapshot retention period (default: 7 days)
    - RETAIN_LAST_N: Always keep at least this many snapshots (default: 3)
"""

import argparse
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

from pyspark.sql import SparkSession

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
DEFAULT_RETENTION_DAYS = 7
RETAIN_LAST_N = 3


@dataclass
class ExpirationStats:
    """Statistics for snapshot expiration."""
    table_name: str
    snapshots_before: int
    snapshots_after: int
    snapshots_expired: int
    files_deleted: int
    bytes_freed: int
    expiration_time_ms: float
    status: str
    error: Optional[str] = None


def create_spark_session() -> SparkSession:
    """Create Spark session with Iceberg configuration."""
    return SparkSession.builder \
        .appName("IcebergSnapshotExpiration") \
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


def get_snapshot_stats(spark: SparkSession, table_name: str) -> tuple[int, list]:
    """Get snapshot count and list for a table."""
    try:
        result = spark.sql(f"""
            SELECT
                snapshot_id,
                committed_at,
                operation
            FROM {table_name}.snapshots
            ORDER BY committed_at DESC
        """).collect()
        return len(result), result
    except Exception as e:
        logger.debug(f"Could not get snapshot stats for {table_name}: {e}")
    return 0, []


def expire_snapshots(
    spark: SparkSession,
    table_name: str,
    retention_days: int,
    dry_run: bool = False,
) -> ExpirationStats:
    """Expire old snapshots for a single table."""
    logger.info(f"Processing table: {table_name}")

    # Get stats before expiration
    snapshot_count_before, snapshots = get_snapshot_stats(spark, table_name)
    logger.info(f"  Snapshots before: {snapshot_count_before}")

    if snapshot_count_before <= RETAIN_LAST_N:
        logger.info(f"  Skipping: Only {snapshot_count_before} snapshots (minimum to retain: {RETAIN_LAST_N})")
        return ExpirationStats(
            table_name=table_name,
            snapshots_before=snapshot_count_before,
            snapshots_after=snapshot_count_before,
            snapshots_expired=0,
            files_deleted=0,
            bytes_freed=0,
            expiration_time_ms=0,
            status="skipped",
        )

    # Calculate expiration timestamp
    expiration_timestamp = datetime.now() - timedelta(days=retention_days)
    expiration_ts_ms = int(expiration_timestamp.timestamp() * 1000)

    # Count snapshots that would be expired
    snapshots_to_expire = [
        s for s in snapshots[RETAIN_LAST_N:]  # Keep at least RETAIN_LAST_N
        if s.committed_at and s.committed_at.timestamp() * 1000 < expiration_ts_ms
    ]

    if not snapshots_to_expire:
        logger.info(f"  No snapshots older than {retention_days} days")
        return ExpirationStats(
            table_name=table_name,
            snapshots_before=snapshot_count_before,
            snapshots_after=snapshot_count_before,
            snapshots_expired=0,
            files_deleted=0,
            bytes_freed=0,
            expiration_time_ms=0,
            status="skipped",
        )

    logger.info(f"  Found {len(snapshots_to_expire)} snapshots older than {retention_days} days")

    if dry_run:
        logger.info(f"  [DRY RUN] Would expire {len(snapshots_to_expire)} snapshots")
        return ExpirationStats(
            table_name=table_name,
            snapshots_before=snapshot_count_before,
            snapshots_after=snapshot_count_before - len(snapshots_to_expire),
            snapshots_expired=len(snapshots_to_expire),
            files_deleted=0,
            bytes_freed=0,
            expiration_time_ms=0,
            status="dry_run",
        )

    # Run expiration
    start_time = datetime.now()
    try:
        # Use Iceberg's expire_snapshots procedure
        result = spark.sql(f"""
            CALL iceberg.system.expire_snapshots(
                table => '{table_name}',
                older_than => TIMESTAMP '{expiration_timestamp.strftime('%Y-%m-%d %H:%M:%S')}',
                retain_last => {RETAIN_LAST_N}
            )
        """).collect()

        elapsed_ms = (datetime.now() - start_time).total_seconds() * 1000
        snapshot_count_after, _ = get_snapshot_stats(spark, table_name)

        # Parse result for deleted files count
        files_deleted = 0
        bytes_freed = 0
        if result:
            row = result[0]
            files_deleted = getattr(row, 'deleted_data_files_count', 0) or 0
            bytes_freed = getattr(row, 'deleted_manifest_files_count', 0) or 0

        snapshots_expired = snapshot_count_before - snapshot_count_after

        logger.info(f"  Expired: {snapshots_expired} snapshots, {files_deleted} files deleted in {elapsed_ms:.0f}ms")

        return ExpirationStats(
            table_name=table_name,
            snapshots_before=snapshot_count_before,
            snapshots_after=snapshot_count_after,
            snapshots_expired=snapshots_expired,
            files_deleted=files_deleted,
            bytes_freed=bytes_freed,
            expiration_time_ms=elapsed_ms,
            status="success",
        )

    except Exception as e:
        elapsed_ms = (datetime.now() - start_time).total_seconds() * 1000
        logger.error(f"  Failed to expire snapshots: {e}")
        return ExpirationStats(
            table_name=table_name,
            snapshots_before=snapshot_count_before,
            snapshots_after=snapshot_count_before,
            snapshots_expired=0,
            files_deleted=0,
            bytes_freed=0,
            expiration_time_ms=elapsed_ms,
            status="failed",
            error=str(e),
        )


def remove_orphan_files(spark: SparkSession, table_name: str, dry_run: bool = False) -> int:
    """Remove orphan files that are no longer referenced by any snapshot."""
    logger.info(f"  Checking for orphan files...")

    try:
        if dry_run:
            # List orphan files without deleting
            result = spark.sql(f"""
                CALL iceberg.system.remove_orphan_files(
                    table => '{table_name}',
                    dry_run => true
                )
            """).collect()
            orphan_count = len(result)
            if orphan_count > 0:
                logger.info(f"  [DRY RUN] Found {orphan_count} orphan files")
            return orphan_count
        else:
            # Actually remove orphan files
            result = spark.sql(f"""
                CALL iceberg.system.remove_orphan_files(
                    table => '{table_name}'
                )
            """).collect()
            orphan_count = len(result)
            if orphan_count > 0:
                logger.info(f"  Removed {orphan_count} orphan files")
            return orphan_count
    except Exception as e:
        logger.warning(f"  Failed to check orphan files: {e}")
        return 0


def print_summary(results: list[ExpirationStats]):
    """Print expiration summary."""
    print("\n" + "=" * 80)
    print("SNAPSHOT EXPIRATION SUMMARY")
    print("=" * 80)

    success = [r for r in results if r.status == "success"]
    skipped = [r for r in results if r.status == "skipped"]
    failed = [r for r in results if r.status == "failed"]
    dry_run = [r for r in results if r.status == "dry_run"]

    print(f"Total tables: {len(results)}")
    print(f"  Processed: {len(success)}")
    print(f"  Skipped: {len(skipped)}")
    print(f"  Failed: {len(failed)}")
    if dry_run:
        print(f"  Dry run: {len(dry_run)}")
    print()

    if success:
        total_expired = sum(r.snapshots_expired for r in success)
        total_files = sum(r.files_deleted for r in success)
        total_time = sum(r.expiration_time_ms for r in success)
        print(f"Snapshots expired: {total_expired}")
        print(f"Files deleted: {total_files}")
        print(f"Total time: {total_time / 1000:.1f}s")

    if dry_run:
        total_would_expire = sum(r.snapshots_expired for r in dry_run)
        print(f"\n[DRY RUN] Would expire: {total_would_expire} snapshots")

    if failed:
        print("\nFailed tables:")
        for r in failed:
            print(f"  {r.table_name}: {r.error}")


def main():
    parser = argparse.ArgumentParser(description="Expire Iceberg snapshots")
    parser.add_argument(
        "--retention-days",
        type=int,
        default=DEFAULT_RETENTION_DAYS,
        help=f"Retention period in days (default: {DEFAULT_RETENTION_DAYS})",
    )
    parser.add_argument(
        "--namespace",
        type=str,
        help="Specific namespace to process (default: all)",
    )
    parser.add_argument(
        "--table",
        type=str,
        help="Specific table to process (e.g., iceberg.staging.stg_shopify_orders)",
    )
    parser.add_argument(
        "--remove-orphans",
        action="store_true",
        help="Also remove orphan files after expiration",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be expired without executing",
    )
    args = parser.parse_args()

    logger.info(f"Starting Iceberg snapshot expiration (retention: {args.retention_days} days)")
    spark = create_spark_session()

    try:
        if args.table:
            tables = [args.table]
        elif args.namespace:
            tables = get_all_tables(spark, [args.namespace])
        else:
            namespaces = ["raw", "staging", "semantic", "analytics", "marts"]
            tables = get_all_tables(spark, namespaces)

        logger.info(f"Found {len(tables)} tables to process")

        results = []
        for table in tables:
            result = expire_snapshots(spark, table, args.retention_days, args.dry_run)
            results.append(result)

            # Optionally remove orphan files
            if args.remove_orphans and result.status == "success":
                remove_orphan_files(spark, table, args.dry_run)

        print_summary(results)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
