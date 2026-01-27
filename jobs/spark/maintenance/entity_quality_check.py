"""
Spark Maintenance Job: Entity Resolution Quality Check
========================================================

Validates the quality of entity resolution mappings and generates reports.
Checks for duplicate mappings, orphaned records, and match quality metrics.

Usage:
    # Run full quality check
    spark-submit entity_quality_check.py

    # Quick check (counts only)
    spark-submit entity_quality_check.py --quick

    # Generate detailed report
    spark-submit entity_quality_check.py --output /tmp/entity_quality_report.json

Quality Checks:
    1. Duplicate unified_ids (same source record mapped multiple times)
    2. Orphaned records (staging records without entity mapping)
    3. Cross-source match quality (records matched across sources)
    4. Match coverage by source
    5. Entity cardinality distribution
"""

import argparse
import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, lit, when

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class QualityMetric:
    """A single quality metric."""
    name: str
    value: float
    threshold: Optional[float] = None
    status: str = "info"  # info, warning, error
    description: str = ""


@dataclass
class SourceStats:
    """Statistics for a single source."""
    source: str
    total_records: int
    mapped_records: int
    orphaned_records: int
    coverage_pct: float
    unique_unified_ids: int


@dataclass
class QualityReport:
    """Complete entity resolution quality report."""
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    total_entity_mappings: int = 0
    total_unified_ids: int = 0
    total_cross_source_matches: int = 0
    metrics: list = field(default_factory=list)
    source_stats: list = field(default_factory=list)
    issues: list = field(default_factory=list)
    status: str = "healthy"  # healthy, warning, critical


def create_spark_session() -> SparkSession:
    """Create Spark session with Iceberg configuration."""
    return SparkSession.builder \
        .appName("EntityQualityCheck") \
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


def check_entity_index_stats(spark: SparkSession) -> tuple[int, int]:
    """Get basic entity index statistics."""
    try:
        result = spark.sql("""
            SELECT
                COUNT(*) as total_mappings,
                COUNT(DISTINCT unified_id) as unique_unified_ids
            FROM iceberg.semantic.entity_index
            WHERE entity_type = 'customer'
        """).collect()
        if result:
            return result[0].total_mappings, result[0].unique_unified_ids
    except Exception as e:
        logger.warning(f"Could not get entity index stats: {e}")
    return 0, 0


def check_duplicate_mappings(spark: SparkSession) -> list[dict]:
    """Find duplicate mappings (same source_id mapped multiple times)."""
    try:
        result = spark.sql("""
            SELECT
                source,
                source_id,
                COUNT(*) as mapping_count,
                COLLECT_LIST(unified_id) as unified_ids
            FROM iceberg.semantic.entity_index
            WHERE entity_type = 'customer'
            GROUP BY source, source_id
            HAVING COUNT(*) > 1
            LIMIT 100
        """).collect()

        duplicates = []
        for row in result:
            duplicates.append({
                "source": row.source,
                "source_id": row.source_id,
                "mapping_count": row.mapping_count,
                "unified_ids": row.unified_ids[:5],  # Limit for readability
            })
        return duplicates
    except Exception as e:
        logger.warning(f"Could not check duplicate mappings: {e}")
    return []


def check_cross_source_matches(spark: SparkSession) -> tuple[int, list[dict]]:
    """Count and sample cross-source matches."""
    try:
        # Count unified_ids that appear in multiple sources
        result = spark.sql("""
            SELECT
                unified_id,
                COUNT(DISTINCT source) as source_count,
                COLLECT_SET(source) as sources
            FROM iceberg.semantic.entity_index
            WHERE entity_type = 'customer'
            GROUP BY unified_id
            HAVING COUNT(DISTINCT source) > 1
        """).collect()

        cross_matches = len(result)
        samples = []
        for row in result[:10]:  # Sample first 10
            samples.append({
                "unified_id": row.unified_id,
                "source_count": row.source_count,
                "sources": list(row.sources),
            })
        return cross_matches, samples
    except Exception as e:
        logger.warning(f"Could not check cross-source matches: {e}")
    return 0, []


def check_source_coverage(spark: SparkSession) -> list[SourceStats]:
    """Check entity coverage for each source."""
    sources = [
        ("shopify", "iceberg.staging.stg_shopify_customers", "customer_id"),
        ("hubspot", "iceberg.staging.stg_hubspot_contacts", "contact_id"),
    ]

    stats = []
    for source_name, table, id_col in sources:
        try:
            # Get total records in staging
            staging_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {table}").collect()[0].cnt

            # Get mapped records
            mapped = spark.sql(f"""
                SELECT COUNT(DISTINCT e.source_id) as cnt
                FROM iceberg.semantic.entity_index e
                WHERE e.source = '{source_name}'
                  AND e.entity_type = 'customer'
            """).collect()
            mapped_count = mapped[0].cnt if mapped else 0

            # Get unique unified_ids for this source
            unified = spark.sql(f"""
                SELECT COUNT(DISTINCT unified_id) as cnt
                FROM iceberg.semantic.entity_index
                WHERE source = '{source_name}'
                  AND entity_type = 'customer'
            """).collect()
            unified_count = unified[0].cnt if unified else 0

            orphaned = staging_count - mapped_count
            coverage = (mapped_count / staging_count * 100) if staging_count > 0 else 0

            stats.append(SourceStats(
                source=source_name,
                total_records=staging_count,
                mapped_records=mapped_count,
                orphaned_records=orphaned,
                coverage_pct=round(coverage, 2),
                unique_unified_ids=unified_count,
            ))

        except Exception as e:
            logger.warning(f"Could not check coverage for {source_name}: {e}")

    return stats


def check_cardinality_distribution(spark: SparkSession) -> dict:
    """Check distribution of records per unified_id."""
    try:
        result = spark.sql("""
            SELECT
                records_per_unified_id,
                COUNT(*) as unified_id_count
            FROM (
                SELECT
                    unified_id,
                    COUNT(*) as records_per_unified_id
                FROM iceberg.semantic.entity_index
                WHERE entity_type = 'customer'
                GROUP BY unified_id
            )
            GROUP BY records_per_unified_id
            ORDER BY records_per_unified_id
        """).collect()

        distribution = {}
        for row in result:
            key = str(row.records_per_unified_id)
            if row.records_per_unified_id > 3:
                key = "4+"
            distribution[key] = distribution.get(key, 0) + row.unified_id_count

        return distribution
    except Exception as e:
        logger.warning(f"Could not check cardinality distribution: {e}")
    return {}


def check_match_methods(spark: SparkSession) -> dict:
    """Analyze match methods used."""
    try:
        result = spark.sql("""
            SELECT
                COALESCE(match_method, 'unknown') as match_method,
                COUNT(*) as count
            FROM iceberg.semantic.entity_index
            WHERE entity_type = 'customer'
            GROUP BY match_method
            ORDER BY count DESC
        """).collect()

        methods = {}
        for row in result:
            methods[row.match_method] = row.count
        return methods
    except Exception as e:
        logger.warning(f"Could not check match methods: {e}")
    return {}


def run_quality_check(spark: SparkSession, quick: bool = False) -> QualityReport:
    """Run comprehensive quality check."""
    report = QualityReport()
    logger.info("Running entity resolution quality check...")

    # Basic stats
    logger.info("  Checking entity index stats...")
    total_mappings, unique_unified_ids = check_entity_index_stats(spark)
    report.total_entity_mappings = total_mappings
    report.total_unified_ids = unique_unified_ids

    report.metrics.append(QualityMetric(
        name="total_mappings",
        value=total_mappings,
        description="Total source-to-unified mappings",
    ))
    report.metrics.append(QualityMetric(
        name="unique_unified_ids",
        value=unique_unified_ids,
        description="Unique customer entities",
    ))

    # Cross-source matches
    logger.info("  Checking cross-source matches...")
    cross_matches, samples = check_cross_source_matches(spark)
    report.total_cross_source_matches = cross_matches

    match_rate = (cross_matches / unique_unified_ids * 100) if unique_unified_ids > 0 else 0
    report.metrics.append(QualityMetric(
        name="cross_source_match_rate",
        value=round(match_rate, 2),
        threshold=5.0,  # Expect at least 5% cross-source matches
        status="warning" if match_rate < 5 else "info",
        description="Percentage of entities matched across sources",
    ))

    if quick:
        logger.info("  Quick check complete")
        return report

    # Source coverage
    logger.info("  Checking source coverage...")
    source_stats = check_source_coverage(spark)
    report.source_stats = [asdict(s) for s in source_stats]

    for stat in source_stats:
        if stat.coverage_pct < 95:
            report.metrics.append(QualityMetric(
                name=f"{stat.source}_coverage",
                value=stat.coverage_pct,
                threshold=95.0,
                status="warning" if stat.coverage_pct < 95 else "info",
                description=f"Entity mapping coverage for {stat.source}",
            ))
            if stat.orphaned_records > 0:
                report.issues.append({
                    "type": "orphaned_records",
                    "source": stat.source,
                    "count": stat.orphaned_records,
                    "message": f"{stat.orphaned_records} records in {stat.source} have no entity mapping",
                })

    # Duplicate mappings
    logger.info("  Checking for duplicate mappings...")
    duplicates = check_duplicate_mappings(spark)
    if duplicates:
        report.issues.append({
            "type": "duplicate_mappings",
            "count": len(duplicates),
            "samples": duplicates[:5],
            "message": f"{len(duplicates)} source records have multiple unified_id mappings",
        })
        report.metrics.append(QualityMetric(
            name="duplicate_mappings",
            value=len(duplicates),
            threshold=0,
            status="error" if len(duplicates) > 0 else "info",
            description="Source records with multiple unified_id mappings",
        ))

    # Cardinality distribution
    logger.info("  Checking cardinality distribution...")
    distribution = check_cardinality_distribution(spark)
    report.metrics.append(QualityMetric(
        name="cardinality_distribution",
        value=distribution.get("1", 0),
        description="Entities with single source record",
    ))

    # Match methods
    logger.info("  Checking match methods...")
    methods = check_match_methods(spark)
    for method, count in methods.items():
        report.metrics.append(QualityMetric(
            name=f"match_method_{method}",
            value=count,
            description=f"Records matched via {method}",
        ))

    # Determine overall status
    has_errors = any(m.status == "error" for m in report.metrics)
    has_warnings = any(m.status == "warning" for m in report.metrics)

    if has_errors:
        report.status = "critical"
    elif has_warnings:
        report.status = "warning"
    else:
        report.status = "healthy"

    logger.info(f"  Quality check complete: {report.status}")
    return report


def print_report(report: QualityReport):
    """Print quality report to console."""
    print("\n" + "=" * 80)
    print("ENTITY RESOLUTION QUALITY REPORT")
    print("=" * 80)
    print(f"Timestamp: {report.timestamp}")
    print(f"Status: {report.status.upper()}")
    print()

    print("SUMMARY")
    print("-" * 40)
    print(f"Total entity mappings: {report.total_entity_mappings:,}")
    print(f"Unique unified IDs: {report.total_unified_ids:,}")
    print(f"Cross-source matches: {report.total_cross_source_matches:,}")
    print()

    if report.source_stats:
        print("SOURCE COVERAGE")
        print("-" * 40)
        print(f"{'Source':<15} {'Total':>10} {'Mapped':>10} {'Coverage':>10} {'Orphaned':>10}")
        for stat in report.source_stats:
            print(f"{stat['source']:<15} {stat['total_records']:>10,} {stat['mapped_records']:>10,} {stat['coverage_pct']:>9.1f}% {stat['orphaned_records']:>10,}")
        print()

    if report.issues:
        print("ISSUES")
        print("-" * 40)
        for issue in report.issues:
            status = "⚠️" if issue["type"] != "duplicate_mappings" else "❌"
            print(f"{status} {issue['message']}")
        print()

    print("METRICS")
    print("-" * 40)
    for metric in report.metrics:
        status_icon = "✓" if metric.status == "info" else ("⚠️" if metric.status == "warning" else "❌")
        threshold_str = f" (threshold: {metric.threshold})" if metric.threshold is not None else ""
        print(f"{status_icon} {metric.name}: {metric.value}{threshold_str}")


def main():
    parser = argparse.ArgumentParser(description="Entity resolution quality check")
    parser.add_argument(
        "--quick",
        action="store_true",
        help="Run quick check (counts only)",
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Output file for JSON report",
    )
    args = parser.parse_args()

    logger.info("Starting entity resolution quality check")
    spark = create_spark_session()

    try:
        report = run_quality_check(spark, args.quick)
        print_report(report)

        if args.output:
            output_data = {
                "timestamp": report.timestamp,
                "status": report.status,
                "total_entity_mappings": report.total_entity_mappings,
                "total_unified_ids": report.total_unified_ids,
                "total_cross_source_matches": report.total_cross_source_matches,
                "metrics": [asdict(m) for m in report.metrics],
                "source_stats": report.source_stats,
                "issues": report.issues,
            }
            with open(args.output, "w") as f:
                json.dump(output_data, f, indent=2)
            print(f"\nReport saved to: {args.output}")

        # Exit with appropriate code
        if report.status == "critical":
            exit(2)
        elif report.status == "warning":
            exit(1)
        else:
            exit(0)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
