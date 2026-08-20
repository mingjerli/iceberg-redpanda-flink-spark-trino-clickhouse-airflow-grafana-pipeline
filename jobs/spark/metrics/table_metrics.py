"""
Iceberg Table Metrics
=====================

Reads Iceberg metadata tables and turns them into MetricSample values for the
alerts in monitoring/alerts/iceberg_alerts.yaml.

Row counts come from SUM(record_count) on the .files metadata table rather than
COUNT(*) on the table itself, so cost stays flat as the table grows.

A table that cannot be read is logged and skipped: a metrics job must never be
the reason a pipeline run fails.
"""
from __future__ import annotations

import logging

from pyspark.sql import SparkSession

from metrics.registry import MetricSample

logger = logging.getLogger(__name__)

PIPELINE_NAMESPACES = (
    "raw", "staging", "semantic", "core", "analytics", "marts",
)


def split_table_identifier(qualified: str) -> tuple:
    """
    Split `catalog.namespace.table` into (namespace, table).

    The namespace doubles as the `layer` label, which is what the
    RawDataIngestionStopped and StagingDataStale alerts filter on.
    """
    parts = qualified.split(".")
    if len(parts) != 3:
        raise ValueError(
            "expected a fully qualified catalog.namespace.table name, "
            "got {!r}".format(qualified)
        )
    return parts[1], parts[2]


def list_pipeline_tables(spark: SparkSession, namespaces: list) -> list:
    """List every table in the given namespaces as a fully qualified name."""
    tables = []
    for namespace in namespaces:
        try:
            rows = spark.sql("SHOW TABLES IN iceberg.{}".format(namespace)).collect()
        except Exception as exc:
            logger.warning("Could not list tables in %s: %s", namespace, exc)
            continue
        tables.extend(
            "iceberg.{}.{}".format(namespace, row.tableName) for row in rows
        )
    return tables


def _collect_one(spark: SparkSession, qualified: str) -> list:
    """Metrics for a single table, or [] if its metadata cannot be read."""
    layer, table = split_table_identifier(qualified)
    labels = {"layer": layer, "table": table}

    try:
        files = spark.sql("""
            SELECT
                COUNT(*) AS file_count,
                COALESCE(SUM(record_count), 0) AS row_count
            FROM {}.files
        """.format(qualified)).collect()[0]

        snapshots = spark.sql("""
            SELECT COUNT(*) AS snapshot_count FROM {}.snapshots
        """.format(qualified)).collect()[0]
    except Exception as exc:
        logger.warning("Skipping %s: %s", qualified, exc)
        return []

    return [
        MetricSample("iceberg_table_row_count", labels, float(files.row_count)),
        MetricSample("iceberg_table_file_count", labels, float(files.file_count)),
        MetricSample(
            "iceberg_table_snapshot_count", labels, float(snapshots.snapshot_count)
        ),
    ]


def collect_table_metrics(spark: SparkSession, tables: list) -> list:
    """Collect row, file, and snapshot gauges for every readable table."""
    samples = []
    for qualified in tables:
        samples.extend(_collect_one(spark, qualified))
    logger.info("Collected %d samples across %d tables", len(samples), len(tables))
    return samples
