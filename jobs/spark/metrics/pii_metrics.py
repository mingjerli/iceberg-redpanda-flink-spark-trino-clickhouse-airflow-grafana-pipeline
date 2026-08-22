"""
PII Metrics
===========

Producers for the two gauges declared in metrics/registry.py but never wired
up: pipeline_pii_vault_entries and pipeline_pii_tokenization_null_rate. Design
doc Section 10 calls the null-rate gauge "the canary" for a broken normalizer.
Registering a metric without a producer reproduces, in miniature, the incident
that motivated tests/test_metrics_registry.py: 13 of 15 alerts once referenced
series nothing emitted.

The two gauges are collected differently, on purpose:

pipeline_pii_vault_entries is a scan of semantic.pii_vault's current state,
grouped by pii_class -- the same pattern as
metrics.entity_metrics.collect_entity_metrics for semantic.entity_index. It is
called from export_metrics.py at the end of each pipeline run, alongside the
other table-scan gauges.

pipeline_pii_tokenization_null_rate cannot use that pattern as cheaply: the
value that matters is "did tokenization just now start turning this column
NULL", not "what fraction of the table happens to be NULL as of some later
scan" (which would blend today's run in with every row ever written). That is
only visible from the DataFrame tokenize_frame() just produced, before it is
written. staging_batch.py calls collect_null_rate_samples() on that in-memory
frame immediately after each tokenize_frame() call.

A missing or unreadable vault is logged and skipped: a metrics collector must
never be the reason a pipeline run fails.

The __future__ import is load-bearing: the Spark image runs Python 3.8.
"""
from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, when

from metrics.registry import MetricSample
from pii.registry import derived_columns, pii_columns, token_column

logger = logging.getLogger(__name__)

VAULT_TABLE = "iceberg.semantic.pii_vault"


def collect_vault_metrics(spark: SparkSession) -> list:
    """Distinct vault tokens per pii_class, read from the vault's current state."""
    try:
        rows = spark.sql("""
            SELECT pii_class, COUNT(*) AS entries
            FROM {}
            GROUP BY pii_class
        """.format(VAULT_TABLE)).collect()
    except Exception as exc:
        logger.warning("Skipping PII vault metrics: %s", exc)
        return []

    samples = [
        MetricSample(
            "pipeline_pii_vault_entries", {"pii_class": row.pii_class}, float(row.entries)
        )
        for row in rows
    ]
    logger.info("PII vault metrics: %d classes", len(samples))
    return samples


def _token_plan(table: str) -> list:
    """[(label_column, token_column_name)] registered for a staging table.

    label_column is the pre-tokenization name (e.g. "email"), which is what
    an operator reading the metric wants to see -- not "email_token", which is
    what is actually being measured.
    """
    plan = [(new_column, token_column(new_column)) for new_column in derived_columns(table)]
    plan += [(column, token_column(column)) for column in pii_columns(table)]
    return plan


def collect_null_rate_samples(df: DataFrame, table: str) -> list:
    """
    Fraction of rows whose token column is NULL, per registered column.

    df must be the frame tokenize_frame(df, table, ...) returned -- it already
    carries the `_token` columns, not the plaintext ones tokenize_frame
    dropped. Columns tokenize_frame did not touch (a table with no registered
    PII) are silently skipped, and any failure returns no samples rather than
    raising: a metrics collector must never be the reason a staging job fails.
    """
    plan = [(label, tcol) for label, tcol in _token_plan(table) if tcol in df.columns]
    if not plan:
        return []

    try:
        agg_row = df.agg(
            count("*").alias("_total"),
            *[
                spark_sum(when(col(tcol).isNull(), 1).otherwise(0)).alias("_null_" + tcol)
                for _, tcol in plan
            ]
        ).collect()[0]
    except Exception as exc:
        logger.warning("Skipping null-rate metrics for %s: %s", table, exc)
        return []

    total = agg_row["_total"] or 0
    if total == 0:
        return []

    samples = [
        MetricSample(
            "pipeline_pii_tokenization_null_rate",
            {"table": table, "column": label},
            (agg_row["_null_" + tcol] or 0) / total,
        )
        for label, tcol in plan
    ]
    logger.info("PII null-rate metrics: %d columns on %s", len(samples), table)
    return samples
