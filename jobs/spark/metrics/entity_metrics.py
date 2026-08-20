"""
Entity Resolution Metrics
=========================

Turns the semantic.entity_index into the gauges EntityCoverageLow and
DuplicateEntityMappings read.

Coverage is per source: the share of that source's rows in the index carrying a
non-null entity_id. A duplicate mapping is one (source_system, source_id) pair
resolved to more than one entity_id, which means resolution has split a single
identity -- downstream joins against it will fan out and inflate every
aggregate built on top.

A missing or unreadable index is logged and skipped: a metrics collector must
never be the reason a pipeline run fails.
"""
from __future__ import annotations

import logging

from pyspark.sql import SparkSession

from metrics.registry import MetricSample

logger = logging.getLogger(__name__)

ENTITY_INDEX = "iceberg.semantic.entity_index"


def collect_entity_metrics(spark: SparkSession) -> list:
    """Coverage per source plus the global duplicate-mapping count."""
    try:
        coverage_rows = spark.sql("""
            SELECT
                source_system,
                COUNT(*) AS total,
                SUM(CASE WHEN entity_id IS NOT NULL THEN 1 ELSE 0 END) AS resolved
            FROM {}
            GROUP BY source_system
        """.format(ENTITY_INDEX)).collect()

        duplicate_row = spark.sql("""
            SELECT COUNT(*) AS duplicates FROM (
                SELECT source_system, source_id
                FROM {}
                GROUP BY source_system, source_id
                HAVING COUNT(DISTINCT entity_id) > 1
            )
        """.format(ENTITY_INDEX)).collect()[0]
    except Exception as exc:
        logger.warning("Skipping entity metrics: %s", exc)
        return []

    samples = [
        MetricSample(
            "entity_resolution_coverage_percent",
            {"source": row.source_system},
            100.0 * row.resolved / row.total if row.total else 0.0,
        )
        for row in coverage_rows
    ]
    samples.append(
        MetricSample(
            "entity_resolution_duplicate_mappings", {}, float(duplicate_row.duplicates)
        )
    )

    logger.info(
        "Entity metrics: %d sources, %d duplicate mappings",
        len(coverage_rows), duplicate_row.duplicates,
    )
    return samples
