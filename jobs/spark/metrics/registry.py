"""
Pipeline Metric Registry
========================

Single source of truth for every Prometheus metric this pipeline emits itself.

Alert rules in monitoring/alerts/iceberg_alerts.yaml may only reference a metric
that appears here, or one matching EXTERNAL_METRIC_PREFIXES (produced by a
third-party exporter we scrape). tests/test_metrics_registry.py enforces that.

Everything here is a gauge on purpose. Pushgateway replaces a group's samples on
each push rather than accumulating them, so a pushed counter cannot support
increase() or rate(). Failure tracking uses *_last_failure_timestamp gauges
compared against their *_last_success_timestamp counterpart.

The __future__ import is load-bearing: the Spark image runs Python 3.8, where an
evaluated `tuple[str, ...]` annotation raises TypeError at import time.
"""
from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class MetricDef:
    """Declaration of a metric this pipeline emits."""
    name: str
    kind: str
    labels: tuple
    help: str


@dataclass(frozen=True)
class MetricSample:
    """One observed value for a registered metric."""
    name: str
    labels: dict
    value: float


PIPELINE_METRICS = (
    MetricDef(
        name="iceberg_table_row_count",
        kind="gauge",
        labels=("layer", "table"),
        help="Rows in an Iceberg table, summed from the files metadata table",
    ),
    MetricDef(
        name="iceberg_table_file_count",
        kind="gauge",
        labels=("layer", "table"),
        help="Data files in an Iceberg table's current snapshot",
    ),
    MetricDef(
        name="iceberg_table_snapshot_count",
        kind="gauge",
        labels=("layer", "table"),
        help="Retained snapshots for an Iceberg table",
    ),
    MetricDef(
        name="entity_resolution_coverage_percent",
        kind="gauge",
        labels=("source",),
        help="Percent of a source's entity index rows that resolved to an entity_id",
    ),
    MetricDef(
        name="entity_resolution_duplicate_mappings",
        kind="gauge",
        labels=(),
        help="Source-system ids mapped to more than one entity_id",
    ),
)

# Metric families produced by third-party exporters we scrape directly.
# Do not widen this to silence a ratchet failure -- that recreates the bug the
# ratchet exists to catch. Add the producer instead.
EXTERNAL_METRIC_PREFIXES = frozenset({
    "up",
    "redpanda_",
    "minio_",
    "airflow_",
    "trino_",
    "clickhouse_",
    "probe_",
})

# Alert-referenced metrics that still have no producer. Shrinks task by task;
# Task 9 asserts it is empty.
KNOWN_GAPS = frozenset({
    "maintenance_job_failed_total",
})

# PromQL identifiers that are never metric names.
_PROMQL_KEYWORDS = frozenset({
    "increase", "rate", "irate", "delta", "idelta", "time", "sum", "avg",
    "min", "max", "count", "by", "without", "on", "ignoring", "group_left",
    "group_right", "offset", "bool", "and", "or", "unless", "absent",
    "clamp_max", "clamp_min", "histogram_quantile", "round", "ceil", "floor",
})

_LABEL_BLOCK = re.compile(r"\{[^}]*\}")
_RANGE_SELECTOR = re.compile(r"\[[^\]]*\]")
_GROUPING_CLAUSE = re.compile(
    r"\b(?:by|without|on|ignoring|group_left|group_right)\s*\([^)]*\)"
)
_STRING_LITERAL = re.compile(r'"[^"]*"')
_IDENTIFIER = re.compile(r"[a-zA-Z_][a-zA-Z0-9_]*")


def extract_metric_names(expr: str) -> set:
    """
    Pull metric names out of a PromQL expression.

    Everything that can hold a non-metric identifier is stripped before the
    identifier scan, in this order:

      {job="minio"}                 label matchers
      [1h]                          range selectors -- note `1h` still yields a
                                    bare `h` to an identifier scan, so these
                                    cannot be left in
      by (topic) / on (topic, ...)  grouping and join clauses, whose contents
                                    are label names, not metrics
      "iceberg_pipeline"            string literals

    Without the grouping-clause strip, `sum by (redpanda_group)` reports
    `redpanda_group` as a metric.
    """
    stripped = _LABEL_BLOCK.sub("", expr)
    stripped = _RANGE_SELECTOR.sub("", stripped)
    stripped = _GROUPING_CLAUSE.sub("", stripped)
    stripped = _STRING_LITERAL.sub("", stripped)
    return {
        token for token in _IDENTIFIER.findall(stripped)
        if token not in _PROMQL_KEYWORDS
    }


def metric_names() -> frozenset:
    """Names of every metric the pipeline emits."""
    return frozenset(m.name for m in PIPELINE_METRICS)


def is_external(name: str) -> bool:
    """True when a metric comes from a third-party exporter rather than us."""
    return any(name.startswith(prefix) for prefix in EXTERNAL_METRIC_PREFIXES)
