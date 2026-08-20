"""
Spark Job: Export Iceberg Table Metrics
=======================================

Collects row, file, and snapshot counts for every pipeline table and pushes them
to the Pushgateway, where Prometheus scrapes them.

Runs at the end of each Airflow pipeline run. It is read-only and never fails
the DAG: metric collection problems are logged, not raised.

This file lives at the jobs root rather than inside metrics/ on purpose.
spark-submit puts the script's own directory on sys.path, so running it from
here makes /opt/spark/jobs the path root and `import metrics.X` resolve. A copy
inside metrics/ would put /opt/spark/jobs/metrics there instead, and the package
would not be importable.

Usage:
    spark-submit export_metrics.py
    spark-submit export_metrics.py --namespace raw --namespace staging
    spark-submit export_metrics.py --dry-run

Environment:
    PUSHGATEWAY_URL   Pushgateway base URL (default: http://pushgateway:9091)
"""
from __future__ import annotations

import argparse
import logging
import os
import sys

from pyspark.sql import SparkSession

from metrics.entity_metrics import collect_entity_metrics
from metrics.pushgateway import (
    DEFAULT_GATEWAY_URL,
    push_samples,
    render_exposition,
)
from metrics.table_metrics import (
    PIPELINE_NAMESPACES,
    collect_table_metrics,
    list_pipeline_tables,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_spark_session() -> SparkSession:
    """Create Spark session with Iceberg configuration."""
    return SparkSession.builder \
        .appName("IcebergTableMetricsExport") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "rest") \
        .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181") \
        .config("spark.sql.catalog.iceberg.warehouse", "s3a://warehouse/") \
        .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.iceberg.s3.endpoint", "http://minio:9000") \
        .config("spark.sql.catalog.iceberg.s3.path-style-access", "true") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_ROOT_USER", "admin")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_ROOT_PASSWORD", "admin123")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()


def main() -> int:
    parser = argparse.ArgumentParser(description="Export Iceberg table metrics")
    parser.add_argument(
        "--namespace",
        action="append",
        dest="namespaces",
        help="Namespace to scan; repeatable (default: all pipeline layers)",
    )
    parser.add_argument(
        "--gateway-url",
        default=DEFAULT_GATEWAY_URL,
        help="Pushgateway base URL (default: {})".format(DEFAULT_GATEWAY_URL),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the exposition body instead of pushing",
    )
    args = parser.parse_args()

    namespaces = args.namespaces or list(PIPELINE_NAMESPACES)
    logger.info("Exporting table metrics for namespaces: %s", ", ".join(namespaces))

    spark = create_spark_session()
    try:
        tables = list_pipeline_tables(spark, namespaces)
        samples = collect_table_metrics(spark, tables)
        samples.extend(collect_entity_metrics(spark))

        if args.dry_run:
            print(render_exposition(samples))
            return 0

        push_samples(samples, gateway_url=args.gateway_url)
    except Exception as exc:
        # Never fail the pipeline over metrics. The DAG task runs with
        # trigger_rule="all_done" precisely so a broken run still reports its
        # row counts; raising here would defeat that.
        logger.error("Metric export failed: %s", exc, exc_info=True)
        return 0
    finally:
        spark.stop()

    return 0


if __name__ == "__main__":
    sys.exit(main())
