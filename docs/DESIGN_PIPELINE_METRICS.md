# Pipeline Metrics & Alerting Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `monitoring/alerts/iceberg_alerts.yaml` real — every alert expression must reference a metric something actually produces, and a regression test must make it impossible to add one that doesn't.

**Architecture:** The pipeline emits its own metrics from a single registry module (`jobs/spark/metrics/`) and pushes them to a Prometheus Pushgateway at the end of each Spark job, because Spark drivers are ephemeral and cannot be scraped. Third-party services (Trino, ClickHouse, Redpanda, MinIO) stay on native scrape. The Iceberg REST catalog has no metrics endpoint, so its liveness comes from a blackbox-exporter probe rather than a scrape job. A pytest ratchet asserts that the set of alert-referenced metrics with no producer exactly equals an explicit `KNOWN_GAPS` set, which each task shrinks and the final task empties.

**Tech Stack:** Prometheus 2.48, Pushgateway 1.9, blackbox-exporter 0.25, PySpark 3.5.3, Iceberg 1.5.0 metadata tables, Airflow 3.1.6, pytest.

**Spec:** This document. The audit that motivates it is in "Audit Findings" below.

## Global Constraints

- **Python: the Spark image is 3.8.10, the Airflow image is 3.12.** Every module
  under `jobs/spark/` must start with `from __future__ import annotations`.
  Without it, `list[str]` / `tuple[str, ...]` / `dict[str, str]` in an evaluated
  annotation position raises `TypeError: 'type' object is not subscriptable` on
  3.8 — at import time, so the job dies before it runs. Verified in the image.
  `jobs/spark/maintenance/compact_tables.py:78` already carries this latent bug;
  it has gone unnoticed only because the job is not in the DAG. Task 6 adds it,
  so Task 6 must fix it.
- **No new pip dependencies in the Spark image.** The push client uses `urllib.request` from the stdlib. Adding `prometheus_client` would require a custom Spark Dockerfile, which this plan does not introduce.
- **Import path — read this before writing any module.** `docker-compose.yml` mounts `../jobs/spark` at `/opt/spark/jobs`, so the `spark` directory level does not exist inside the container: `jobs.spark.metrics` is importable under pytest (`run_tests.sh` sets `PYTHONPATH=/work`) but **not** under `spark-submit`. Existing jobs dodge this by never importing each other. This plan needs a shared package, so it uses one import form that resolves identically in both contexts: `from metrics.X import Y`, with `<repo>/jobs/spark` added to `sys.path` in `tests/conftest.py` (Task 1) to mirror what `/opt/spark/jobs` gives the container. The entrypoint therefore lives at `jobs/spark/export_metrics.py`, **not** inside `metrics/` — a script inside the package would put `/opt/spark/jobs/metrics` on `sys.path[0]` instead of `/opt/spark/jobs`, and the package would not resolve. Scripts one level deeper (`maintenance/*.py`) prepend the jobs root explicitly; see Task 6.
- Tests run only via `./scripts/run_tests.sh` (Java 11 + Spark 3.5.3 live in the container). Never run pytest on the host.
- Tests must not require a running stack. Anything touching Iceberg uses the `spark` fixture from `tests/conftest.py`, which registers a hadoop-type catalog named `iceberg` in a temp dir.
- Metric and label names are fixed by the existing alert expressions. Where an expression cannot be satisfied, the plan rewrites the **alert**, and says so explicitly — it never silently renames a metric.
- Every metric the pipeline emits is a **gauge**. Counters do not survive the Pushgateway model: a push replaces the group's value rather than adding to it, so `increase()` over a pushed counter is meaningless. Where an alert wanted a counter, it is rewritten against a `_last_failure_timestamp` / `_last_success_timestamp` gauge.
- Follow existing file conventions: multi-line module docstring, `logging.basicConfig` + named logger, type hints on signatures, `argparse` CLI.
- Files stay under ~400 lines; split by responsibility.

---

## Audit Findings

Verified against the working tree at commit `ca0ed3c`. Of the 15 alerts in `monitoring/alerts/iceberg_alerts.yaml`, **2 can fire and 13 are inert.** Every row below was confirmed against the running stack; nothing here is inferred.

| # | Alert | Metric | Status | Why |
|---|-------|--------|--------|-----|
| 1 | PipelineFailure | `airflow_dag_run_failed_total{dag_id}` | DEAD | statsd-exporter runs with no mapping config, so no `dag_id` label is ever produced |
| 2 | PipelineDurationHigh | `airflow_dag_run_duration_seconds{dag_id,state}` | DEAD | same |
| 3 | PipelineStale | `airflow_dag_last_success_timestamp{dag_id}` | DEAD | Airflow emits no such statsd metric at all |
| 4 | RawDataIngestionStopped | `iceberg_table_row_count{layer}` | DEAD | no producer anywhere in repo |
| 5 | StagingDataStale | `iceberg_table_row_count{layer}` | DEAD | same |
| 6 | EntityCoverageLow | `entity_resolution_coverage_percent` | DEAD | no producer |
| 7 | DuplicateEntityMappings | `entity_resolution_duplicate_mappings` | DEAD | no producer |
| 8 | IcebergCatalogDown | `up{job="iceberg-rest"}` | DEAD | `prometheus.yml` has no `iceberg-rest` scrape job |
| 9 | MinIODown | `up{job="minio"}` | **WORKS** | minio is scraped |
| 10 | RedpandaDown | `up{job="redpanda"}` | **WORKS** | redpanda is scraped |
| 11 | KafkaConsumerLagHigh | `redpanda_kafka_consumer_group_lag` | DEAD | **Verified:** Redpanda v24.1.1 publishes no lag series under any name; lag must be computed (Task 8) |
| 12 | MinIOStorageAlmostFull | `minio_bucket_quota_bytes` | DEAD | **Verified:** scrape uses `/metrics/cluster`, which emits no per-bucket series; no quota is configured either |
| 13 | TableNeedsCompaction | `iceberg_table_file_count` | DEAD | no producer |
| 14 | TooManySnapshots | `iceberg_table_snapshot_count` | DEAD | no producer |
| 15 | CompactionJobFailed | `maintenance_job_failed_total` | DEAD | no producer; `compact_tables.py` also is not in the DAG |

Two decisions follow from this audit:

**Spark is not a scrape target.** `spark-submit` drivers exist only for the life of a task, so a scrape job pointing at a driver UI would be down more often than up. Batch Spark metrics go to the Pushgateway. This reverses the "add a Spark scrape target" suggestion made before the audit.

**Airflow statsd is not worth mapping.** Alerts 1–3 want series Airflow either does not emit (`last_success_timestamp`) or emits under names that need a hand-written statsd mapping to carry a `dag_id` label. Since the Pushgateway path exists anyway for alerts 4–7 and 13–15, the DAG emits its own `iceberg_pipeline_*` gauges and alerts 1–3 are rewritten against those. statsd-exporter stays running for generic Airflow internals; nothing alerts on it.

---

## File Structure

**Create:**

| File | Responsibility |
|------|----------------|
| `jobs/spark/metrics/__init__.py` | Package marker; re-exports `MetricSample`, `MetricDef` |
| `jobs/spark/metrics/registry.py` | Single source of truth: every metric the pipeline emits, plus `EXTERNAL_METRIC_PREFIXES` and the `KNOWN_GAPS` ratchet |
| `jobs/spark/metrics/pushgateway.py` | Text-exposition rendering + stdlib HTTP push |
| `jobs/spark/metrics/table_metrics.py` | Iceberg metadata → `MetricSample` list |
| `jobs/spark/metrics/entity_metrics.py` | Entity-resolution coverage and duplicate mappings |
| `jobs/spark/metrics/job_metrics.py` | Maintenance job outcome recording |
| `jobs/spark/export_metrics.py` | Spark job entrypoint (argparse CLI). Sits **outside** `metrics/` so `sys.path[0]` is `/opt/spark/jobs` under `spark-submit` — see the import-path constraint |
| `airflow/dags/callbacks.py` | DAG-level pipeline health push |
| `infrastructure/prometheus/blackbox.yml` | blackbox-exporter module config |
| `tests/test_metrics_registry.py` | Alert-coverage ratchet + PromQL extraction tests |
| `tests/test_table_metrics.py` | Table metric computation against a real temp Iceberg table |
| `tests/test_pushgateway.py` | Exposition-format rendering tests |
| `tests/test_entity_metrics.py` | Entity metric computation |

**Modify:**

| File | Change |
|------|--------|
| `tests/conftest.py` | Add `<repo>/jobs/spark` to `sys.path` so test imports match the container's |
| `infrastructure/docker-compose.yml` | Add `pushgateway` and `blackbox-exporter` services + `pushgateway-data` volume |
| `infrastructure/prometheus/prometheus.yml` | Add `pushgateway` (with `honor_labels`), `trino`, `clickhouse`, `minio-bucket`, `blackbox-iceberg-rest` jobs |
| `infrastructure/clickhouse/config.xml:81` | Add `<prometheus>` block before `</clickhouse>` |
| `infrastructure/.env.example` | Add `EXTERNAL_PUSHGATEWAY_PORT`, `PUSHGATEWAY_URL` |
| `jobs/spark/maintenance/compact_tables.py` | Emit maintenance gauges |
| `jobs/spark/maintenance/expire_snapshots.py` | Emit maintenance gauges |
| `airflow/dags/iceberg_pipeline.py` | Add `export_table_metrics` + `compact_tables` tasks, pipeline-health callbacks |
| `scripts/reset_and_run.sh` | Publish metrics at the end of a run |
| `monitoring/alerts/iceberg_alerts.yaml` | Rewrite alerts 1, 2, 3, 8, 11, 12, 15 |
| `docs/RUNBOOK.md` | Metrics section + runbook anchors the alerts link to |
| `CLAUDE.md` | Note the registry rule under Testing and Validation |

---

## Delivery: three PRs, in order

The nine tasks ship as three pull requests. They are **sequential, not parallel** —
all three touch `monitoring/alerts/iceberg_alerts.yaml`, and PRs 1 and 2 both touch
`docker-compose.yml` and `prometheus.yml`, so concurrent branches conflict on every
one of those files.

| PR | Branch | Tasks | Lands |
|----|--------|-------|-------|
| 1 | `fix/prometheus-scrape-targets-and-alerts` | 8 | Trino/ClickHouse/bucket scrape jobs, catalog blackbox probe, computed Redpanda lag. Alerts 8, 11, 12 start working |
| 2 | `feature/pipeline-metric-registry` | 1–3 | `jobs/spark/metrics/` package, ratchet test, Pushgateway + export job. Tests green; nothing scheduled yet |
| 3 | `feature/pipeline-metrics-airflow-integration` | 4–7, 9 | DAG wiring, entity/maintenance/health collectors, ratchet closed, docs. Alerts 1–7, 13–15 start working |

**PR 1 goes first because it is fully independent.** Task 8 touches no Python and
has no dependency on the registry or the Pushgateway; every endpoint fact behind
it is verified in Task 8 Step 1. It restores `IcebergCatalogDown`, which is the
alert that matters most when the catalog dies.

**PR 2 is pure addition.** New package, new tests, two new containers, nothing
wired into a schedule. Safe to merge on its own; `KNOWN_GAPS` is still non-empty
and the ratchet asserts exactly that, so the suite is green.

**PR 3 carries the only high-blast-radius change.** It is where
`airflow/dags/iceberg_pipeline.py` changes, and a DAG import error takes down the
scheduler for every DAG in the instance — not just this one. That is why it gets
its own review boundary instead of riding along with 700 lines of new library
code. Verify with `airflow dags list-import-errors` before merging (Task 4 Step 2,
Task 7 Step 5).

---

## Task 1: Metric registry and the alert-coverage ratchet

> **Status: DONE** -- PR 2, commit `6810747`.

Establishes the contract before anything emits. The ratchet starts loose (`KNOWN_GAPS` lists the metrics with no producer) and every later task removes entries from it.

**Files:**
- Create: `jobs/spark/metrics/__init__.py`
- Create: `jobs/spark/metrics/registry.py`
- Modify: `tests/conftest.py`
- Test: `tests/test_metrics_registry.py`

**Interfaces:**
- Consumes: nothing
- Produces:
  - `MetricSample(name: str, labels: dict[str, str], value: float)` — frozen dataclass
  - `MetricDef(name: str, kind: str, labels: tuple[str, ...], help: str)` — frozen dataclass
  - `PIPELINE_METRICS: tuple[MetricDef, ...]`
  - `EXTERNAL_METRIC_PREFIXES: frozenset[str]`
  - `KNOWN_GAPS: frozenset[str]`
  - `metric_names() -> frozenset[str]`
  - `is_external(name: str) -> bool`
  - `extract_metric_names(expr: str) -> set[str]`

- [x] **Step 1: Make the metrics package importable the same way the container sees it**

`spark-submit` puts the script's own directory on `sys.path`, so a job at
`/opt/spark/jobs/export_metrics.py` resolves `import metrics.registry` against
`/opt/spark/jobs`. Tests get `PYTHONPATH=/work` instead, where the same package
sits at `/work/jobs/spark/metrics`. Adding that directory makes both contexts
agree on one import form, so a path mistake fails in pytest rather than only
under a real `spark-submit`.

In `tests/conftest.py`, add below the existing stdlib imports and above the
`from tests.pipeline_tables import ...` line:

```python
import sys
from pathlib import Path

# docker-compose mounts ../jobs/spark at /opt/spark/jobs, so the container sees
# `metrics` as a top-level package. Mirror that here: tests and spark-submit
# then use the identical `from metrics.X import Y` form, and an import mistake
# fails in the suite instead of only in a scheduled run.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs" / "spark"))
```

This is additive — `/work` stays on the path, so the existing
`from jobs.spark.staging_batch import ...` imports in the GA4 tests keep working.

- [x] **Step 2: Write the failing test**

Create `tests/test_metrics_registry.py`:

```python
"""
Tests for the pipeline metric registry and the alert-coverage ratchet.

The ratchet is the point of this file: it fails whenever an alert rule
references a metric that nothing produces, unless that metric is explicitly
listed in KNOWN_GAPS. Tasks that add producers shrink KNOWN_GAPS; the final
task asserts it is empty.
"""
from pathlib import Path

import yaml

from metrics.registry import (
    KNOWN_GAPS,
    PIPELINE_METRICS,
    extract_metric_names,
    is_external,
    metric_names,
)

ALERTS_PATH = (
    Path(__file__).resolve().parents[1] / "monitoring" / "alerts" / "iceberg_alerts.yaml"
)


def alert_expressions() -> list[tuple[str, str]]:
    """Return (alert_name, expr) for every rule in the alert file."""
    doc = yaml.safe_load(ALERTS_PATH.read_text())
    return [
        (rule["alert"], rule["expr"])
        for group in doc["groups"]
        for rule in group["rules"]
    ]


def test_extract_metric_names_strips_labels_and_functions():
    expr = 'increase(iceberg_table_row_count{layer="raw"}[1h]) == 0'
    assert extract_metric_names(expr) == {"iceberg_table_row_count"}


def test_extract_metric_names_handles_multiline_division():
    expr = (
        '(iceberg_table_row_count{layer="raw"} - iceberg_table_row_count{layer="staging"})\n'
        '/ iceberg_table_row_count{layer="raw"} > 0.1'
    )
    assert extract_metric_names(expr) == {"iceberg_table_row_count"}


def test_extract_metric_names_drops_time_function():
    expr = 'time() - airflow_dag_last_success_timestamp{dag_id="iceberg_pipeline"} > 21600'
    assert extract_metric_names(expr) == {"airflow_dag_last_success_timestamp"}


def test_registry_has_no_duplicate_names():
    names = [m.name for m in PIPELINE_METRICS]
    assert len(names) == len(set(names))


def test_registry_metrics_are_all_gauges():
    """Counters do not survive the Pushgateway replace-on-push model."""
    non_gauges = [m.name for m in PIPELINE_METRICS if m.kind != "gauge"]
    assert non_gauges == []


def test_alert_file_parses():
    assert len(alert_expressions()) == 15


def test_every_alert_metric_has_a_producer():
    referenced: set[str] = set()
    for _, expr in alert_expressions():
        referenced |= extract_metric_names(expr)

    produced = metric_names()
    unresolved = {n for n in referenced if n not in produced and not is_external(n)}

    assert unresolved == set(KNOWN_GAPS), (
        f"Alert metrics with no producer changed.\n"
        f"  Newly unresolved: {sorted(unresolved - set(KNOWN_GAPS))}\n"
        f"  Fixed (remove from KNOWN_GAPS): {sorted(set(KNOWN_GAPS) - unresolved)}"
    )
```

- [x] **Step 3: Run test to verify it fails**

Run: `./scripts/run_tests.sh tests/test_metrics_registry.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'metrics'`

If it instead fails on `import yaml`, add `PyYAML>=6.0` to `requirements-dev.txt` and re-run.

- [x] **Step 4: Write minimal implementation**

Create `jobs/spark/metrics/__init__.py`:

```python
"""Pipeline metric emission: registry, rendering, and Pushgateway transport."""

from metrics.registry import MetricDef, MetricSample

__all__ = ["MetricDef", "MetricSample"]
```

Create `jobs/spark/metrics/registry.py`:

```python
"""
Pipeline Metric Registry
========================

Single source of truth for every Prometheus metric this pipeline emits itself.

Alert rules in monitoring/alerts/iceberg_alerts.yaml may only reference a metric
that appears here, or one matching EXTERNAL_METRIC_PREFIXES (produced by a
third-party exporter we scrape). tests/test_metrics_registry.py enforces that.

Everything here is a gauge on purpose. Pushgateway replaces a group's samples on
each push rather than accumulating them, so a pushed counter cannot support
increase() or rate(). Failure tracking uses *_last_failure_timestamp gauges.
"""

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class MetricDef:
    """Declaration of a metric this pipeline emits."""
    name: str
    kind: str
    labels: tuple[str, ...]
    help: str


@dataclass(frozen=True)
class MetricSample:
    """One observed value for a registered metric."""
    name: str
    labels: dict[str, str]
    value: float


PIPELINE_METRICS: tuple[MetricDef, ...] = (
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
)

# Metric families produced by third-party exporters we scrape directly.
EXTERNAL_METRIC_PREFIXES: frozenset[str] = frozenset({
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
KNOWN_GAPS: frozenset[str] = frozenset({
    "entity_resolution_coverage_percent",
    "entity_resolution_duplicate_mappings",
    "maintenance_job_failed_total",
})

# PromQL identifiers that are never metric names.
_PROMQL_KEYWORDS: frozenset[str] = frozenset({
    "increase", "rate", "irate", "delta", "idelta", "time", "sum", "avg",
    "min", "max", "count", "by", "without", "on", "ignoring", "group_left",
    "group_right", "offset", "bool", "and", "or", "unless", "absent",
    "clamp_max", "clamp_min", "histogram_quantile", "round", "ceil", "floor",
})

_LABEL_BLOCK = re.compile(r"\{[^}]*\}")
_STRING_LITERAL = re.compile(r'"[^"]*"')
_IDENTIFIER = re.compile(r"[a-zA-Z_][a-zA-Z0-9_]*")


def extract_metric_names(expr: str) -> set[str]:
    """
    Pull metric names out of a PromQL expression.

    Label matchers and string literals are stripped first so label names and
    label values cannot be mistaken for metrics. Duration literals like [1h]
    are ignored for free because they start with a digit.
    """
    stripped = _LABEL_BLOCK.sub("", expr)
    stripped = _STRING_LITERAL.sub("", stripped)
    return {
        token for token in _IDENTIFIER.findall(stripped)
        if token not in _PROMQL_KEYWORDS
    }


def metric_names() -> frozenset[str]:
    """Names of every metric the pipeline emits."""
    return frozenset(m.name for m in PIPELINE_METRICS)


def is_external(name: str) -> bool:
    """True when a metric comes from a third-party exporter rather than us."""
    return any(name.startswith(prefix) for prefix in EXTERNAL_METRIC_PREFIXES)
```

`KNOWN_GAPS` deliberately omits `iceberg_table_*` even though nothing emits them yet — Task 2 lands the producer and `PIPELINE_METRICS` already declares them, so the registry is self-consistent from the start. It also omits `airflow_dag_*`, which match an external prefix; Task 7 rewrites those alerts and the ratchet will pick up the replacement names.

- [x] **Step 5: Run test to verify it passes**

Run: `./scripts/run_tests.sh tests/test_metrics_registry.py -v`
Expected: 7 passed.

If `test_every_alert_metric_has_a_producer` fails, the assertion message names exactly which metrics to add to or remove from `KNOWN_GAPS`. Do not widen `EXTERNAL_METRIC_PREFIXES` to make it pass — that is the failure mode this test exists to prevent.

- [x] **Step 6: Commit**

```bash
git add jobs/spark/metrics/__init__.py jobs/spark/metrics/registry.py \
        tests/conftest.py tests/test_metrics_registry.py
git commit -m "feat: add pipeline metric registry with alert-coverage ratchet"
```

---

## Task 2: Iceberg table metrics from metadata

> **Status: DONE** -- PR 2, commit `4a5056b`.

**Files:**
- Create: `jobs/spark/metrics/table_metrics.py`
- Test: `tests/test_table_metrics.py`

**Interfaces:**
- Consumes: `MetricSample` from `metrics.registry`
- Produces:
  - `split_table_identifier(qualified: str) -> tuple[str, str]` — `"iceberg.raw.foo"` → `("raw", "foo")`
  - `collect_table_metrics(spark: SparkSession, tables: list[str]) -> list[MetricSample]`
  - `list_pipeline_tables(spark: SparkSession, namespaces: list[str]) -> list[str]`
  - `PIPELINE_NAMESPACES: tuple[str, ...]`

Row counts come from `SUM(record_count)` on the `.files` metadata table, not `COUNT(*)` on the table. Metadata-only, so cost stays flat as volume grows.

- [x] **Step 1: Write the failing test**

Create `tests/test_table_metrics.py`:

```python
"""
Tests for Iceberg table metric collection.

Uses the session `spark` fixture from conftest.py, which is backed by a
hadoop-type catalog in a temp dir, so no REST catalog or MinIO is needed.
"""
import pytest

from metrics.table_metrics import (
    collect_table_metrics,
    list_pipeline_tables,
    split_table_identifier,
)


@pytest.fixture
def metrics_table(spark):
    """A small raw-layer table with two snapshots."""
    spark.sql("DROP TABLE IF EXISTS iceberg.raw.metrics_probe")
    spark.sql("""
        CREATE TABLE iceberg.raw.metrics_probe (
            id BIGINT,
            payload STRING
        ) USING iceberg
        TBLPROPERTIES ('format-version' = '2')
    """)
    spark.sql("INSERT INTO iceberg.raw.metrics_probe VALUES (1, 'a'), (2, 'b')")
    spark.sql("INSERT INTO iceberg.raw.metrics_probe VALUES (3, 'c')")

    yield "iceberg.raw.metrics_probe"

    spark.sql("DROP TABLE IF EXISTS iceberg.raw.metrics_probe")


def sample_for(samples, name):
    matches = [s for s in samples if s.name == name]
    assert len(matches) == 1, f"expected exactly one {name}, got {len(matches)}"
    return matches[0]


def test_split_table_identifier_returns_layer_and_table():
    assert split_table_identifier("iceberg.raw.ga4_events") == ("raw", "ga4_events")


def test_split_table_identifier_rejects_unqualified_name():
    with pytest.raises(ValueError, match="fully qualified"):
        split_table_identifier("ga4_events")


def test_collect_row_count_matches_inserted_rows(spark, metrics_table):
    samples = collect_table_metrics(spark, [metrics_table])
    assert sample_for(samples, "iceberg_table_row_count").value == 3


def test_collect_snapshot_count_counts_both_inserts(spark, metrics_table):
    samples = collect_table_metrics(spark, [metrics_table])
    assert sample_for(samples, "iceberg_table_snapshot_count").value == 2


def test_collect_file_count_is_positive(spark, metrics_table):
    samples = collect_table_metrics(spark, [metrics_table])
    assert sample_for(samples, "iceberg_table_file_count").value >= 1


def test_collect_labels_carry_layer_and_table(spark, metrics_table):
    samples = collect_table_metrics(spark, [metrics_table])
    for sample in samples:
        assert sample.labels == {"layer": "raw", "table": "metrics_probe"}


def test_collect_skips_missing_table_without_raising(spark, metrics_table):
    samples = collect_table_metrics(spark, [metrics_table, "iceberg.raw.does_not_exist"])
    tables = {s.labels["table"] for s in samples}
    assert tables == {"metrics_probe"}


def test_list_pipeline_tables_finds_created_table(spark, metrics_table):
    tables = list_pipeline_tables(spark, ["raw"])
    assert "iceberg.raw.metrics_probe" in tables
```

- [x] **Step 2: Run test to verify it fails**

Run: `./scripts/run_tests.sh tests/test_table_metrics.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'metrics.table_metrics'`

- [x] **Step 3: Write minimal implementation**

Create `jobs/spark/metrics/table_metrics.py`:

```python
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

import logging

from pyspark.sql import SparkSession

from metrics.registry import MetricSample

logger = logging.getLogger(__name__)

PIPELINE_NAMESPACES: tuple[str, ...] = (
    "raw", "staging", "semantic", "core", "analytics", "marts",
)


def split_table_identifier(qualified: str) -> tuple[str, str]:
    """
    Split `catalog.namespace.table` into (namespace, table).

    The namespace doubles as the `layer` label, which is what alerts 4 and 5
    filter on.
    """
    parts = qualified.split(".")
    if len(parts) != 3:
        raise ValueError(
            f"expected a fully qualified catalog.namespace.table name, got {qualified!r}"
        )
    return parts[1], parts[2]


def list_pipeline_tables(spark: SparkSession, namespaces: list[str]) -> list[str]:
    """List every table in the given namespaces as a fully qualified name."""
    tables: list[str] = []
    for namespace in namespaces:
        try:
            rows = spark.sql(f"SHOW TABLES IN iceberg.{namespace}").collect()
        except Exception as exc:
            logger.warning("Could not list tables in %s: %s", namespace, exc)
            continue
        tables.extend(f"iceberg.{namespace}.{row.tableName}" for row in rows)
    return tables


def _collect_one(spark: SparkSession, qualified: str) -> list[MetricSample]:
    """Metrics for a single table, or [] if its metadata cannot be read."""
    layer, table = split_table_identifier(qualified)
    labels = {"layer": layer, "table": table}

    try:
        files = spark.sql(f"""
            SELECT
                COUNT(*) AS file_count,
                COALESCE(SUM(record_count), 0) AS row_count
            FROM {qualified}.files
        """).collect()[0]

        snapshots = spark.sql(f"""
            SELECT COUNT(*) AS snapshot_count FROM {qualified}.snapshots
        """).collect()[0]
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


def collect_table_metrics(
    spark: SparkSession, tables: list[str]
) -> list[MetricSample]:
    """Collect row, file, and snapshot gauges for every readable table."""
    samples: list[MetricSample] = []
    for qualified in tables:
        samples.extend(_collect_one(spark, qualified))
    logger.info("Collected %d samples across %d tables", len(samples), len(tables))
    return samples
```

- [x] **Step 4: Run test to verify it passes**

Run: `./scripts/run_tests.sh tests/test_table_metrics.py -v`
Expected: 8 passed.

- [x] **Step 5: Commit**

```bash
git add jobs/spark/metrics/table_metrics.py tests/test_table_metrics.py
git commit -m "feat: collect Iceberg row, file, and snapshot metrics from metadata"
```

---

## Task 3: Pushgateway transport and the export job

> **Status: DONE** -- PR 2, commit `8283c77`.

**Files:**
- Create: `jobs/spark/metrics/pushgateway.py`
- Create: `jobs/spark/export_metrics.py`
- Create: `tests/test_pushgateway.py`
- Modify: `infrastructure/docker-compose.yml`
- Modify: `infrastructure/prometheus/prometheus.yml`
- Modify: `infrastructure/.env.example`

**Interfaces:**
- Consumes: `MetricSample`, `PIPELINE_METRICS` from `registry`; `collect_table_metrics`, `list_pipeline_tables`, `PIPELINE_NAMESPACES` from `table_metrics`
- Produces:
  - `render_exposition(samples: list[MetricSample]) -> str`
  - `push_samples(samples: list[MetricSample], gateway_url: str = DEFAULT_GATEWAY_URL, job: str = "iceberg_pipeline", timeout: int = 10) -> None`
  - `DEFAULT_GATEWAY_URL: str`

Two details that bite:

1. The exposition body **must end with a newline**. Pushgateway returns 400 without it.
2. The scrape job needs `honor_labels: true`, or Prometheus overwrites the pushed `job` label with `pushgateway` and every alert grouping breaks.

- [x] **Step 1: Write the failing test**

Create `tests/test_pushgateway.py`:

```python
"""
Tests for Prometheus text-exposition rendering.

Transport itself is not tested here — push_samples is a thin urllib wrapper and
exercising it would need a live Pushgateway, which the suite deliberately avoids.
"""
import pytest

from metrics.pushgateway import render_exposition
from metrics.registry import MetricSample


def test_render_includes_help_and_type_lines():
    body = render_exposition([
        MetricSample("iceberg_table_row_count", {"layer": "raw", "table": "t"}, 5)
    ])
    assert "# HELP iceberg_table_row_count" in body
    assert "# TYPE iceberg_table_row_count gauge" in body


def test_render_emits_sorted_labels():
    body = render_exposition([
        MetricSample("iceberg_table_row_count", {"table": "t", "layer": "raw"}, 5)
    ])
    assert 'iceberg_table_row_count{layer="raw",table="t"} 5.0' in body


def test_render_ends_with_newline():
    """Pushgateway rejects a body with no trailing newline."""
    body = render_exposition([
        MetricSample("iceberg_table_row_count", {"layer": "raw", "table": "t"}, 5)
    ])
    assert body.endswith("\n")


def test_render_groups_repeated_metric_under_one_header():
    body = render_exposition([
        MetricSample("iceberg_table_row_count", {"layer": "raw", "table": "a"}, 1),
        MetricSample("iceberg_table_row_count", {"layer": "raw", "table": "b"}, 2),
    ])
    assert body.count("# TYPE iceberg_table_row_count gauge") == 1


def test_render_escapes_label_values():
    body = render_exposition([
        MetricSample("iceberg_table_row_count", {"layer": 'ra"w', "table": "t"}, 1)
    ])
    assert 'layer="ra\\"w"' in body


def test_render_rejects_unregistered_metric():
    with pytest.raises(ValueError, match="not in the registry"):
        render_exposition([MetricSample("made_up_metric", {}, 1)])


def test_render_empty_samples_returns_empty_string():
    assert render_exposition([]) == ""
```

- [x] **Step 2: Run test to verify it fails**

Run: `./scripts/run_tests.sh tests/test_pushgateway.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'metrics.pushgateway'`

- [x] **Step 3: Write minimal implementation**

Create `jobs/spark/metrics/pushgateway.py`:

```python
"""
Pushgateway Transport
=====================

Renders MetricSample values as Prometheus text exposition format and POSTs them
to a Pushgateway.

Uses urllib from the stdlib rather than prometheus_client, because the Spark
image is stock and this plan does not introduce a custom Dockerfile for it.

Spark drivers are ephemeral, so batch jobs push rather than being scraped.
"""

import logging
import os
import urllib.error
import urllib.request

from metrics.registry import PIPELINE_METRICS, MetricSample

logger = logging.getLogger(__name__)

DEFAULT_GATEWAY_URL = os.environ.get("PUSHGATEWAY_URL", "http://pushgateway:9091")

_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"
_DEFS = {m.name: m for m in PIPELINE_METRICS}


def _escape(value: str) -> str:
    """Escape a label value per the exposition format spec."""
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _format_labels(labels: dict[str, str]) -> str:
    if not labels:
        return ""
    pairs = ",".join(f'{k}="{_escape(v)}"' for k, v in sorted(labels.items()))
    return "{" + pairs + "}"


def render_exposition(samples: list[MetricSample]) -> str:
    """
    Render samples as Prometheus text exposition format.

    Samples are grouped by metric so each family emits one HELP/TYPE header.
    The result always ends with a newline; Pushgateway 400s without it.
    """
    if not samples:
        return ""

    unknown = {s.name for s in samples if s.name not in _DEFS}
    if unknown:
        raise ValueError(
            f"metrics not in the registry: {sorted(unknown)}. "
            f"Add them to PIPELINE_METRICS in registry.py first."
        )

    lines: list[str] = []
    for name in sorted({s.name for s in samples}):
        definition = _DEFS[name]
        lines.append(f"# HELP {name} {definition.help}")
        lines.append(f"# TYPE {name} {definition.kind}")
        for sample in [s for s in samples if s.name == name]:
            lines.append(f"{name}{_format_labels(sample.labels)} {float(sample.value)}")

    return "\n".join(lines) + "\n"


def push_samples(
    samples: list[MetricSample],
    gateway_url: str = DEFAULT_GATEWAY_URL,
    job: str = "iceberg_pipeline",
    timeout: int = 10,
) -> None:
    """
    POST samples to the Pushgateway under the given job group.

    Raises on transport failure so the caller can decide. Callers that must not
    fail their pipeline task should catch and log.
    """
    body = render_exposition(samples)
    if not body:
        logger.info("No samples to push")
        return

    url = f"{gateway_url.rstrip('/')}/metrics/job/{job}"
    request = urllib.request.Request(
        url,
        data=body.encode("utf-8"),
        method="POST",
        headers={"Content-Type": _CONTENT_TYPE},
    )

    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            logger.info(
                "Pushed %d samples to %s (HTTP %d)", len(samples), url, response.status
            )
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"Pushgateway rejected push to {url}: {exc.code} {detail}"
        ) from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Pushgateway unreachable at {url}: {exc.reason}") from exc
```

Create `jobs/spark/export_metrics.py`:

```python
"""
Spark Job: Export Iceberg Table Metrics
=======================================

Collects row, file, and snapshot counts for every pipeline table and pushes them
to the Pushgateway, where Prometheus scrapes them.

Runs at the end of each Airflow pipeline run. It is read-only and never fails
the DAG: metric collection problems are logged, not raised.

Usage:
    spark-submit export_metrics.py
    spark-submit export_metrics.py --namespace raw --namespace staging
    spark-submit export_metrics.py --dry-run

Environment:
    PUSHGATEWAY_URL   Pushgateway base URL (default: http://pushgateway:9091)
"""

import argparse
import logging
import os
import sys

from pyspark.sql import SparkSession

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
        help=f"Pushgateway base URL (default: {DEFAULT_GATEWAY_URL})",
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

        if args.dry_run:
            print(render_exposition(samples))
            return 0

        push_samples(samples, gateway_url=args.gateway_url)
    except Exception as exc:
        # Never fail the pipeline over metrics.
        logger.error("Metric export failed: %s", exc, exc_info=True)
        return 0
    finally:
        spark.stop()

    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [x] **Step 4: Run test to verify it passes**

Run: `./scripts/run_tests.sh tests/test_pushgateway.py -v`
Expected: 7 passed.

- [x] **Step 5: Add the Pushgateway service**

In `infrastructure/docker-compose.yml`, add after the `statsd-exporter` block (around line 788):

```yaml
  # ===========================================================================
  # Pushgateway - Batch Job Metrics
  # ===========================================================================
  # Spark drivers are ephemeral, so batch jobs push here instead of being
  # scraped. Prometheus scrapes this with honor_labels so pushed labels win.
  pushgateway:
    image: prom/pushgateway:v1.9.0
    container_name: iceberg-pushgateway
    hostname: pushgateway
    ports:
      - "${EXTERNAL_PUSHGATEWAY_PORT:-9091}:9091"
    command:
      - '--web.listen-address=:9091'
      - '--persistence.file=/data/pushgateway.store'
      - '--persistence.interval=5m'
    volumes:
      - pushgateway-data:/data
    healthcheck:
      test: ["CMD-SHELL", "wget -q --spider http://localhost:9091/-/healthy || exit 1"]
      interval: 15s
      timeout: 5s
      retries: 5
    networks:
      - iceberg-network
```

In the `volumes:` block at the bottom (near line 860), add:

```yaml
  pushgateway-data:
    name: iceberg-demo-pushgateway-data
```

- [x] **Step 6: Add the scrape job**

Append to `scrape_configs` in `infrastructure/prometheus/prometheus.yml`:

```yaml
  # Batch job metrics pushed by Spark (see jobs/spark/metrics/)
  # honor_labels is required: without it Prometheus overwrites the pushed
  # `job` label with "pushgateway" and every alert grouping breaks.
  - job_name: 'pushgateway'
    honor_labels: true
    static_configs:
      - targets: ['pushgateway:9091']
```

- [x] **Step 7: Add env vars**

In `infrastructure/.env.example`, near the other port definitions:

```bash
# Pushgateway (batch job metrics)
EXTERNAL_PUSHGATEWAY_PORT=9091
PUSHGATEWAY_URL=http://pushgateway:9091
```

- [x] **Step 8: Verify end to end against the running stack**

```bash
cd infrastructure && docker-compose up -d pushgateway prometheus
docker exec iceberg-spark-master /opt/spark/bin/spark-submit \
  /opt/spark/jobs/export_metrics.py --dry-run
```
Expected: exposition text listing `iceberg_table_row_count` for real tables.

```bash
docker exec iceberg-spark-master /opt/spark/bin/spark-submit \
  /opt/spark/jobs/export_metrics.py
curl -s localhost:9091/metrics | grep iceberg_table_row_count | head
```
Expected: pushed series present.

```bash
curl -s 'localhost:9090/api/v1/query?query=iceberg_table_row_count' | head -c 400
```
Expected: non-empty `result` array. If empty, wait one scrape interval (15s) and retry.

- [x] **Step 9: Commit**

```bash
git add jobs/spark/metrics/pushgateway.py jobs/spark/export_metrics.py \
        tests/test_pushgateway.py infrastructure/docker-compose.yml \
        infrastructure/prometheus/prometheus.yml infrastructure/.env.example
git commit -m "feat: push Iceberg table metrics to Pushgateway"
```

---

## Task 4: Wire the export job into Airflow

Alerts 4, 5, 13, and 14 go live at the end of this task.

**Files:**
- Modify: `airflow/dags/iceberg_pipeline.py`
- Modify: `scripts/reset_and_run.sh`

**Interfaces:**
- Consumes: `export_metrics.py` from Task 3
- Produces: an `export_table_metrics` Airflow task on the tail of the DAG

- [ ] **Step 1: Add the task to the DAG**

In `airflow/dags/iceberg_pipeline.py`, after the marts task definitions and before the dependency block:

```python
    # -------------------------------------------------------------------------
    # Observability: publish table metrics for Prometheus
    # trigger_rule="all_done" so metrics are still published when an upstream
    # layer fails — a failed run is exactly when the row counts matter most.
    # -------------------------------------------------------------------------
    export_table_metrics = BashOperator(
        task_id="export_table_metrics",
        bash_command=f"{SPARK_SUBMIT} {SPARK_JOBS_PATH}/export_metrics.py",
        trigger_rule="all_done",
    )
```

Then change the final dependency line from:

```python
    [customer_360, sales_dashboard, campaign_dashboard, ga4_engagement_dashboard] >> end
```

to:

```python
    [customer_360, sales_dashboard, campaign_dashboard, ga4_engagement_dashboard] >> export_table_metrics >> end
```

- [ ] **Step 2: Verify the DAG parses**

```bash
docker exec iceberg-airflow-scheduler airflow dags list-import-errors
docker exec iceberg-airflow-scheduler airflow tasks list iceberg_pipeline | grep export_table_metrics
```
Expected: no import errors; `export_table_metrics` listed.

- [ ] **Step 3: Add the step to reset_and_run.sh**

In `scripts/reset_and_run.sh`, after the marts section, using the existing helper so the exit code is not masked:

```bash
log_info "Publishing table metrics..."
run_spark_job "export_metrics.py" "Table metrics export"
```

- [ ] **Step 4: Run the pipeline and confirm the alerts have data**

```bash
./scripts/reset_and_run.sh --no-datagen
curl -s 'localhost:9090/api/v1/query?query=iceberg_table_row_count{layer="raw"}' | head -c 400
curl -s 'localhost:9090/api/v1/query?query=iceberg_table_file_count' | head -c 400
```
Expected: both non-empty.

- [ ] **Step 5: Confirm the alert rules evaluate**

```bash
curl -s localhost:9090/api/v1/rules | python3 -c "
import json,sys
for g in json.load(sys.stdin)['data']['groups']:
    for r in g['rules']:
        if r['name'] in {'RawDataIngestionStopped','StagingDataStale','TableNeedsCompaction','TooManySnapshots'}:
            print(f\"{r['name']:28} health={r['health']} state={r.get('state')}\")
"
```
Expected: each shows `health=ok`, not `unknown`. `state=inactive` is correct — it means the rule evaluates and the condition is false.

- [ ] **Step 6: Commit**

```bash
git add airflow/dags/iceberg_pipeline.py scripts/reset_and_run.sh
git commit -m "feat: publish table metrics at the end of each pipeline run"
```

---

## Task 5: Entity resolution metrics

Closes alerts 6 and 7 and removes two entries from `KNOWN_GAPS`.

**Files:**
- Create: `jobs/spark/metrics/entity_metrics.py`
- Modify: `jobs/spark/metrics/registry.py`
- Modify: `jobs/spark/export_metrics.py`
- Test: `tests/test_entity_metrics.py`

**Interfaces:**
- Consumes: `MetricSample`
- Produces: `collect_entity_metrics(spark: SparkSession) -> list[MetricSample]`

Coverage is the share of a source's rows in `semantic.entity_index` carrying a non-null `entity_id`. A duplicate mapping is one `(source_system, source_id)` pair resolved to more than one `entity_id`.

- [ ] **Step 1: Register the metrics and shrink the ratchet**

In `jobs/spark/metrics/registry.py`, add to `PIPELINE_METRICS`:

```python
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
```

And shrink `KNOWN_GAPS` to:

```python
KNOWN_GAPS: frozenset[str] = frozenset({
    "maintenance_job_failed_total",
})
```

- [ ] **Step 2: Write the failing test**

Create `tests/test_entity_metrics.py`:

```python
"""
Tests for entity-resolution metric collection.

Builds a small semantic.entity_index by hand rather than running the full
backfill, so the assertions are about the metric maths, not resolution quality.
"""
import pytest

from metrics.entity_metrics import collect_entity_metrics


@pytest.fixture
def entity_index(spark):
    spark.sql("DROP TABLE IF EXISTS iceberg.semantic.entity_index")
    spark.sql("""
        CREATE TABLE iceberg.semantic.entity_index (
            entity_id STRING,
            source_system STRING,
            source_id STRING
        ) USING iceberg
    """)
    spark.sql("""
        INSERT INTO iceberg.semantic.entity_index VALUES
            ('e1', 'shopify', 's1'),
            ('e2', 'shopify', 's2'),
            ('e3', 'stripe',  'p1'),
            ('e4', 'stripe',  'p1')
    """)
    yield
    spark.sql("DROP TABLE IF EXISTS iceberg.semantic.entity_index")


def sample_for(samples, name, **labels):
    matches = [
        s for s in samples
        if s.name == name and all(s.labels.get(k) == v for k, v in labels.items())
    ]
    assert len(matches) == 1, f"expected one {name}{labels}, got {len(matches)}"
    return matches[0]


def test_duplicate_mappings_counts_source_ids_with_two_entities(spark, entity_index):
    samples = collect_entity_metrics(spark)
    assert sample_for(samples, "entity_resolution_duplicate_mappings").value == 1


def test_coverage_is_emitted_per_source(spark, entity_index):
    samples = collect_entity_metrics(spark)
    sources = {
        s.labels["source"] for s in samples
        if s.name == "entity_resolution_coverage_percent"
    }
    assert sources == {"shopify", "stripe"}


def test_coverage_is_full_when_every_row_resolved(spark, entity_index):
    samples = collect_entity_metrics(spark)
    assert sample_for(
        samples, "entity_resolution_coverage_percent", source="shopify"
    ).value == 100.0


def test_coverage_drops_when_entity_id_is_null(spark, entity_index):
    spark.sql(
        "INSERT INTO iceberg.semantic.entity_index VALUES (NULL, 'shopify', 's3')"
    )
    samples = collect_entity_metrics(spark)
    coverage = sample_for(
        samples, "entity_resolution_coverage_percent", source="shopify"
    ).value
    assert coverage == pytest.approx(200.0 / 3.0)


def test_missing_entity_index_returns_empty(spark):
    spark.sql("DROP TABLE IF EXISTS iceberg.semantic.entity_index")
    assert collect_entity_metrics(spark) == []
```

- [ ] **Step 3: Run test to verify it fails**

Run: `./scripts/run_tests.sh tests/test_entity_metrics.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'metrics.entity_metrics'`

- [ ] **Step 4: Write minimal implementation**

Create `jobs/spark/metrics/entity_metrics.py`:

```python
"""
Entity Resolution Metrics
=========================

Turns the semantic.entity_index into the gauges alerts 6 and 7 read.

Coverage is per source: the share of that source's rows in the index that carry
a non-null entity_id. A duplicate mapping is one (source_system, source_id) pair
resolved to more than one entity_id, which means resolution has split an
identity and downstream joins will fan out.
"""

import logging

from pyspark.sql import SparkSession

from metrics.registry import MetricSample

logger = logging.getLogger(__name__)

ENTITY_INDEX = "iceberg.semantic.entity_index"


def collect_entity_metrics(spark: SparkSession) -> list[MetricSample]:
    """Coverage per source plus the global duplicate-mapping count."""
    try:
        coverage_rows = spark.sql(f"""
            SELECT
                source_system,
                COUNT(*) AS total,
                SUM(CASE WHEN entity_id IS NOT NULL THEN 1 ELSE 0 END) AS resolved
            FROM {ENTITY_INDEX}
            GROUP BY source_system
        """).collect()

        duplicate_row = spark.sql(f"""
            SELECT COUNT(*) AS duplicates FROM (
                SELECT source_system, source_id
                FROM {ENTITY_INDEX}
                GROUP BY source_system, source_id
                HAVING COUNT(DISTINCT entity_id) > 1
            )
        """).collect()[0]
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
    return samples
```

- [ ] **Step 5: Run test to verify it passes**

Run: `./scripts/run_tests.sh tests/test_entity_metrics.py tests/test_metrics_registry.py -v`
Expected: all passed, including the ratchet with its shrunk `KNOWN_GAPS`.

- [ ] **Step 6: Emit from the export job**

In `jobs/spark/export_metrics.py`, add the import:

```python
from metrics.entity_metrics import collect_entity_metrics
```

and inside `main()`, immediately after `samples = collect_table_metrics(spark, tables)`:

```python
        samples.extend(collect_entity_metrics(spark))
```

- [ ] **Step 7: Commit**

```bash
git add jobs/spark/metrics/entity_metrics.py jobs/spark/metrics/registry.py \
        jobs/spark/export_metrics.py tests/test_entity_metrics.py
git commit -m "feat: emit entity resolution coverage and duplicate mapping metrics"
```

---

## Task 6: Maintenance job metrics, and put compaction in the DAG

Alert 15 currently watches a job that never runs. This task schedules it, makes it observable, and moves the alert off a counter onto timestamp gauges.

**Files:**
- Create: `jobs/spark/metrics/job_metrics.py`
- Modify: `jobs/spark/metrics/registry.py`
- Modify: `jobs/spark/maintenance/compact_tables.py`
- Modify: `jobs/spark/maintenance/expire_snapshots.py`
- Modify: `airflow/dags/iceberg_pipeline.py`
- Modify: `monitoring/alerts/iceberg_alerts.yaml`

**Interfaces:**
- Produces: `record_job_outcome(job_name: str, succeeded: bool, duration_seconds: float, gateway_url: str = DEFAULT_GATEWAY_URL) -> None`

- [ ] **Step 1: Register the metrics and empty that gap**

In `registry.py`, add to `PIPELINE_METRICS`:

```python
    MetricDef(
        name="maintenance_job_last_success_timestamp",
        kind="gauge",
        labels=("maintenance_job",),
        help="Unix time of the last successful run of a maintenance job",
    ),
    MetricDef(
        name="maintenance_job_last_failure_timestamp",
        kind="gauge",
        labels=("maintenance_job",),
        help="Unix time of the last failed run of a maintenance job",
    ),
    MetricDef(
        name="maintenance_job_duration_seconds",
        kind="gauge",
        labels=("maintenance_job",),
        help="Wall-clock duration of the last maintenance job run",
    ),
```

and set:

```python
KNOWN_GAPS: frozenset[str] = frozenset()
```

The label is `maintenance_job`, not `job` — `job` is the Pushgateway grouping key and would be overwritten.

- [ ] **Step 2: Rewrite alert 15**

In `monitoring/alerts/iceberg_alerts.yaml`, replace the `CompactionJobFailed` rule with:

```yaml
      # Alert when compaction last failed more recently than it last succeeded.
      # Timestamp gauges rather than a counter: Pushgateway replaces a group's
      # samples on push, so increase() over a pushed counter is meaningless.
      - alert: CompactionJobFailed
        expr: |
          maintenance_job_last_failure_timestamp{maintenance_job="compact_tables"}
          > maintenance_job_last_success_timestamp{maintenance_job="compact_tables"}
        for: 0m
        labels:
          severity: warning
          team: data-engineering
        annotations:
          summary: "Compaction job failed"
          description: "The table compaction maintenance job has failed."
          runbook_url: "https://docs/runbook#compaction-failure"
```

- [ ] **Step 3: Write the outcome recorder**

Create `jobs/spark/metrics/job_metrics.py`:

```python
"""
Maintenance Job Outcome Metrics
===============================

Records whether a maintenance job succeeded, when, and how long it took.

Success and failure are separate timestamp gauges rather than a counter because
Pushgateway replaces a group's samples on each push, so increase() over a pushed
counter never reflects reality. Alerts compare the two timestamps instead.
"""

import logging
import time

from metrics.pushgateway import DEFAULT_GATEWAY_URL, push_samples
from metrics.registry import MetricSample

logger = logging.getLogger(__name__)


def record_job_outcome(
    job_name: str,
    succeeded: bool,
    duration_seconds: float,
    gateway_url: str = DEFAULT_GATEWAY_URL,
) -> None:
    """
    Push the outcome of one maintenance run.

    Never raises: a metrics failure must not turn a successful maintenance run
    into a failed one.
    """
    now = time.time()
    labels = {"maintenance_job": job_name}
    stamp = (
        "maintenance_job_last_success_timestamp" if succeeded
        else "maintenance_job_last_failure_timestamp"
    )
    samples = [
        MetricSample(stamp, labels, now),
        MetricSample("maintenance_job_duration_seconds", labels, duration_seconds),
    ]

    try:
        push_samples(samples, gateway_url=gateway_url, job=f"maintenance_{job_name}")
    except Exception as exc:
        logger.warning("Could not record outcome for %s: %s", job_name, exc)
```

- [ ] **Step 4: Call it from compact_tables.py**

`maintenance/` sits one directory deeper than `export_metrics.py`, so under
`spark-submit` its `sys.path[0]` is `/opt/spark/jobs/maintenance` and the
`metrics` package is not visible. Prepend the jobs root before importing it.

In `jobs/spark/maintenance/compact_tables.py`, add below the existing `os` import:

```python
import sys

# spark-submit puts this script's own directory on sys.path, which is one level
# below the jobs root where the `metrics` package lives. Add the parent so the
# same `from metrics.X import Y` form works here as in export_metrics.py.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from metrics.job_metrics import record_job_outcome
```

and replace the `try/finally` inside `main()` with a timed, outcome-recording version:

```python
    start = datetime.now()
    succeeded = False
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
            result = compact_table(spark, table, args.dry_run)
            results.append(result)

        print_summary(results)
        succeeded = not any(r.status == "failed" for r in results)
    finally:
        if not args.dry_run:
            record_job_outcome(
                "compact_tables",
                succeeded,
                (datetime.now() - start).total_seconds(),
            )
        spark.stop()
```

- [ ] **Step 5: Apply the same pattern to expire_snapshots.py**

Read `jobs/spark/maintenance/expire_snapshots.py` first and mirror the structure against that file's own result objects — do not paste the compaction body. Pass `"expire_snapshots"` as the job name, and include the same `sys.path.insert` bootstrap from Step 4: it is in `maintenance/` too, so the `metrics` package is equally invisible to it.

- [ ] **Step 6: Schedule maintenance in the DAG**

In `airflow/dags/iceberg_pipeline.py`, beside `export_table_metrics`:

```python
    compact_tables = BashOperator(
        task_id="compact_tables",
        bash_command=f"{SPARK_SUBMIT} {SPARK_JOBS_PATH}/maintenance/compact_tables.py",
        trigger_rule="all_done",
    )
```

and update the tail dependency:

```python
    [customer_360, sales_dashboard, campaign_dashboard, ga4_engagement_dashboard] >> compact_tables >> export_table_metrics >> end
```

Compaction runs before the metrics export so file counts reflect the post-compaction state.

- [ ] **Step 7: Verify**

Run: `./scripts/run_tests.sh tests/test_metrics_registry.py -v`
Expected: passes with `KNOWN_GAPS` empty.

```bash
docker exec iceberg-spark-master /opt/spark/bin/spark-submit \
  /opt/spark/jobs/maintenance/compact_tables.py --namespace staging
curl -s localhost:9091/metrics | grep maintenance_job
```
Expected: `maintenance_job_last_success_timestamp{maintenance_job="compact_tables"}` present.

- [ ] **Step 8: Commit**

```bash
git add jobs/spark/metrics/job_metrics.py jobs/spark/metrics/registry.py \
        jobs/spark/maintenance/ airflow/dags/iceberg_pipeline.py \
        monitoring/alerts/iceberg_alerts.yaml
git commit -m "feat: record maintenance job outcomes and schedule compaction"
```

---

## Task 7: Pipeline health metrics, replacing the statsd alerts

Alerts 1–3 are rewritten against gauges the DAG emits itself.

**Files:**
- Create: `airflow/dags/callbacks.py`
- Modify: `jobs/spark/metrics/registry.py`
- Modify: `airflow/dags/iceberg_pipeline.py`
- Modify: `monitoring/alerts/iceberg_alerts.yaml`

**Interfaces:**
- Produces: `push_pipeline_health(dag_id: str, succeeded: bool, duration_seconds: float) -> None`, plus `on_pipeline_success(context)` / `on_pipeline_failure(context)` callbacks

The callback module lives under `airflow/dags/` and talks to the Pushgateway over plain `urllib`, so it needs nothing installed in the Airflow image.

- [ ] **Step 1: Register the metrics**

In `registry.py`, add to `PIPELINE_METRICS`:

```python
    MetricDef(
        name="iceberg_pipeline_last_success_timestamp",
        kind="gauge",
        labels=("dag_id",),
        help="Unix time the pipeline DAG last completed successfully",
    ),
    MetricDef(
        name="iceberg_pipeline_last_failure_timestamp",
        kind="gauge",
        labels=("dag_id",),
        help="Unix time the pipeline DAG last failed",
    ),
    MetricDef(
        name="iceberg_pipeline_run_duration_seconds",
        kind="gauge",
        labels=("dag_id",),
        help="Wall-clock duration of the last completed pipeline DAG run",
    ),
```

- [ ] **Step 2: Rewrite alerts 1–3**

Replace the three `pipeline_health` rules in `monitoring/alerts/iceberg_alerts.yaml`:

```yaml
      # The DAG pushes these itself. Airflow's statsd output carries no dag_id
      # label without a hand-written statsd-exporter mapping, and emits nothing
      # equivalent to a last-success timestamp at all.
      - alert: PipelineFailure
        expr: |
          iceberg_pipeline_last_failure_timestamp{dag_id="iceberg_pipeline"}
          > iceberg_pipeline_last_success_timestamp{dag_id="iceberg_pipeline"}
        for: 0m
        labels:
          severity: critical
          team: data-engineering
        annotations:
          summary: "Iceberg pipeline failed"
          description: "The iceberg_pipeline DAG's most recent run failed."
          runbook_url: "https://docs/runbook#pipeline-failure"

      - alert: PipelineDurationHigh
        expr: iceberg_pipeline_run_duration_seconds{dag_id="iceberg_pipeline"} > 600
        for: 5m
        labels:
          severity: warning
          team: data-engineering
        annotations:
          summary: "Pipeline running longer than expected"
          description: "Last run took {{ $value | humanizeDuration }}. Expected < 10 minutes."
          runbook_url: "https://docs/runbook#slow-pipeline"

      - alert: PipelineStale
        expr: |
          time() - iceberg_pipeline_last_success_timestamp{dag_id="iceberg_pipeline"} > 21600
        for: 5m
        labels:
          severity: warning
          team: data-engineering
        annotations:
          summary: "Pipeline hasn't run successfully"
          description: "No successful pipeline run in the last 6 hours."
          runbook_url: "https://docs/runbook#stale-pipeline"
```

`PipelineDurationHigh` now describes the last *completed* run rather than one in flight. That is a deliberate narrowing: Airflow exposes no in-flight duration without the statsd mapping this plan declines to write.

- [ ] **Step 3: Write the callback module**

Create `airflow/dags/callbacks.py`:

```python
"""
DAG Callbacks: Pipeline Health Metrics
======================================

Pushes pipeline-level gauges to the Pushgateway on DAG success and failure.

Uses urllib directly rather than importing the metrics package: those modules
live in the Spark image, not the Airflow one, and this file must not add a
dependency to either.
"""

import logging
import os
import time
import urllib.error
import urllib.request
from typing import Any

logger = logging.getLogger(__name__)

GATEWAY_URL = os.environ.get("PUSHGATEWAY_URL", "http://pushgateway:9091")
_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"


def push_pipeline_health(
    dag_id: str, succeeded: bool, duration_seconds: float
) -> None:
    """Push last-outcome timestamp and duration for a DAG run. Never raises."""
    now = time.time()
    stamp = (
        "iceberg_pipeline_last_success_timestamp" if succeeded
        else "iceberg_pipeline_last_failure_timestamp"
    )
    body = (
        f"# HELP {stamp} Unix time of the last pipeline outcome\n"
        f"# TYPE {stamp} gauge\n"
        f'{stamp}{{dag_id="{dag_id}"}} {now}\n'
        "# HELP iceberg_pipeline_run_duration_seconds Duration of the last run\n"
        "# TYPE iceberg_pipeline_run_duration_seconds gauge\n"
        f'iceberg_pipeline_run_duration_seconds{{dag_id="{dag_id}"}} {duration_seconds}\n'
    )
    url = f"{GATEWAY_URL.rstrip('/')}/metrics/job/pipeline_health_{dag_id}"

    try:
        request = urllib.request.Request(
            url,
            data=body.encode("utf-8"),
            method="POST",
            headers={"Content-Type": _CONTENT_TYPE},
        )
        with urllib.request.urlopen(request, timeout=10):
            logger.info("Pushed pipeline health for %s (success=%s)", dag_id, succeeded)
    except (urllib.error.URLError, OSError) as exc:
        logger.warning("Could not push pipeline health for %s: %s", dag_id, exc)


def _run_duration(context: dict[str, Any]) -> float:
    dag_run = context.get("dag_run")
    if dag_run is None or dag_run.start_date is None:
        return 0.0
    end = dag_run.end_date or dag_run.start_date
    return max((end - dag_run.start_date).total_seconds(), 0.0)


def on_pipeline_success(context: dict[str, Any]) -> None:
    """DAG-level on_success_callback."""
    push_pipeline_health(context["dag"].dag_id, True, _run_duration(context))


def on_pipeline_failure(context: dict[str, Any]) -> None:
    """DAG-level on_failure_callback."""
    push_pipeline_health(context["dag"].dag_id, False, _run_duration(context))
```

- [ ] **Step 4: Wire the callbacks into the DAG**

In `airflow/dags/iceberg_pipeline.py`, add below the existing operator imports:

```python
from callbacks import on_pipeline_failure, on_pipeline_success
```

and add to the `DAG(...)` constructor arguments, after `is_paused_upon_creation=False`:

```python
    on_success_callback=on_pipeline_success,
    on_failure_callback=on_pipeline_failure,
```

- [ ] **Step 5: Verify**

```bash
docker exec iceberg-airflow-scheduler airflow dags list-import-errors
docker exec iceberg-airflow-scheduler airflow dags trigger iceberg_pipeline
# wait for the run to finish, then:
curl -s localhost:9091/metrics | grep iceberg_pipeline_last
```
Expected: `iceberg_pipeline_last_success_timestamp{dag_id="iceberg_pipeline"}` present.

Run: `./scripts/run_tests.sh tests/test_metrics_registry.py -v`
Expected: passes — the rewritten alerts reference registered metrics.

- [ ] **Step 6: Commit**

```bash
git add airflow/dags/callbacks.py airflow/dags/iceberg_pipeline.py \
        jobs/spark/metrics/registry.py monitoring/alerts/iceberg_alerts.yaml
git commit -m "feat: emit pipeline health gauges from the DAG"
```

---

## Task 8: Scrape targets and external metric corrections

> **Status: DONE** — shipped as PR 1 on `fix/prometheus-scrape-targets-and-alerts` (commit `4c0ff21`).
> Verified on the running stack: 4/4 new targets up, 15/15 rules healthy, lag expression cross-checked against `rpk`.

Closes alerts 8, 11, and 12. Nothing here touches the registry — these are all `EXTERNAL_METRIC_PREFIXES` families, and every endpoint fact was verified against the running stack (Step 1).

**Files:**
- Create: `infrastructure/prometheus/blackbox.yml`
- Modify: `infrastructure/docker-compose.yml`
- Modify: `infrastructure/prometheus/prometheus.yml`
- Modify: `infrastructure/clickhouse/config.xml`
- Modify: `monitoring/alerts/iceberg_alerts.yaml`

- [x] **Step 1: Confirm the endpoint facts (already resolved — this is a re-check, not a discovery)**

These were resolved against the running stack on 2026-08-20 before the plan was
finalised. The configs in Steps 3–5 already encode the answers. Re-run these only
to confirm nothing drifted; if any output differs, fix the config rather than the
expectation.

| Question | Answer | Consequence |
|---|---|---|
| Does Trino 440 serve `/metrics`? | **Yes**, but returns **401** unauthenticated | Scrape job needs `basic_auth` (Step 3) |
| How big is Trino's payload? | 1,086,468 bytes / 3,986 series, **2,080 of them planner-rule noise** | Drop `trino_sql_planner_iterative_.*` via `metric_relabel_configs` (Step 3) |
| Does Redpanda expose consumer lag? | **No.** v24.1.1 has only `..._committed_offset`, `..._consumers`, `..._topics` | Lag must be **computed** from two series (Step 5) |
| Is MinIO's metrics endpoint authenticated? | **No** — `MINIO_PROMETHEUS_AUTH_TYPE: public` at `docker-compose.yml:51` | Existing `minio` job is correct as-is |
| Do the MinIO capacity series exist? | **Yes** — `minio_cluster_capacity_usable_free_bytes` / `..._total_bytes` | Rewritten alert 12 is correct (Step 5) |
| Does MinIO's bucket endpoint work? | **Yes** — HTTP 200, 19,704 bytes | `minio-bucket` job is valid |
| Does ClickHouse listen on 9363 already? | **No** — nothing bound | The `<prometheus>` block in Step 4 is required |

```bash
docker exec iceberg-trino curl -s -u prometheus: -o /dev/null -w "trino=%{http_code}\n" localhost:8080/metrics
docker exec iceberg-redpanda curl -s localhost:9644/public_metrics | grep -cE "^# TYPE .*consumer_group"
curl -s localhost:9000/minio/v2/metrics/cluster | grep -c "^minio_cluster_capacity_usable"
```
Expected: `trino=200`, `3`, `2`.

**Why Redpanda lag has to be computed.** `rpk group describe` was used to confirm
the arithmetic: for `flink-shopify-customers-raw` on `shopify.customers`,
`redpanda_kafka_max_offset` equals `rpk`'s LOG-END-OFFSET (333/354/380) and
`redpanda_kafka_consumer_group_committed_offset` equals CURRENT-OFFSET (333/354/380),
with `rpk` reporting LAG 0. So lag is a plain subtraction — **no off-by-one**.
The labels are `redpanda_topic`, `redpanda_partition`, `redpanda_group`, and
`redpanda_namespace` (filter to `"kafka"` to exclude the internal `controller` topic).

The join was validated against the live Prometheus and returned 8 series, one per
consumer group, matching `rpk`'s per-group lag.

- [x] **Step 2: Add blackbox-exporter for the REST catalog**

The Iceberg REST catalog image serves no `/metrics`, so a scrape job would report `up=0` forever and make alert 8 fire constantly. A blackbox HTTP probe is the correct liveness signal.

Create `infrastructure/prometheus/blackbox.yml`:

```yaml
# =============================================================================
# Blackbox Exporter Modules
# =============================================================================
# Liveness probing for services that expose no Prometheus metrics endpoint.
# =============================================================================

modules:
  http_2xx:
    prober: http
    timeout: 5s
    http:
      valid_status_codes: [200]
      preferred_ip_protocol: ip4
```

In `infrastructure/docker-compose.yml`, beside the pushgateway service:

```yaml
  blackbox-exporter:
    image: prom/blackbox-exporter:v0.25.0
    container_name: iceberg-blackbox-exporter
    hostname: blackbox-exporter
    command:
      - '--config.file=/etc/blackbox/blackbox.yml'
    volumes:
      - ./prometheus/blackbox.yml:/etc/blackbox/blackbox.yml:ro
    networks:
      - iceberg-network
```

- [x] **Step 3: Add the scrape jobs**

Append to `scrape_configs` in `infrastructure/prometheus/prometheus.yml`:

```yaml
  # Trino coordinator. /metrics exists on 440 but 401s unauthenticated; Trino
  # accepts Basic with an empty password when no authenticator is configured.
  # The raw payload is ~1 MB / 3,986 series, over half of it per-optimizer-rule
  # counters, so those are dropped at scrape time.
  - job_name: 'trino'
    metrics_path: /metrics
    basic_auth:
      username: prometheus
      password: ''
    static_configs:
      - targets: ['trino:8080']
    metric_relabel_configs:
      - source_labels: [__name__]
        regex: 'trino_sql_planner_iterative_.*'
        action: drop

  # ClickHouse (requires the <prometheus> block in clickhouse/config.xml)
  - job_name: 'clickhouse'
    metrics_path: /metrics
    static_configs:
      - targets: ['clickhouse:9363']

  # MinIO per-bucket series. The cluster endpoint above does not emit these.
  - job_name: 'minio-bucket'
    metrics_path: /minio/v2/metrics/bucket
    static_configs:
      - targets: ['minio:9000']

  # Iceberg REST catalog liveness. It serves no /metrics, so probe it instead.
  - job_name: 'blackbox-iceberg-rest'
    metrics_path: /probe
    params:
      module: [http_2xx]
    static_configs:
      - targets: ['http://iceberg-rest:8181/v1/config']
    relabel_configs:
      - source_labels: [__address__]
        target_label: __param_target
      - source_labels: [__param_target]
        target_label: instance
      - target_label: __address__
        replacement: blackbox-exporter:9115
```

Spark is deliberately absent. Its drivers are ephemeral; its metrics arrive via the Pushgateway from Task 3.

- [x] **Step 4: Enable ClickHouse metrics**

In `infrastructure/clickhouse/config.xml`, insert before the closing `</clickhouse>` on line 81:

```xml
    <!-- Prometheus metrics endpoint, scraped as job "clickhouse" -->
    <prometheus>
        <endpoint>/metrics</endpoint>
        <port>9363</port>
        <metrics>true</metrics>
        <events>true</events>
        <asynchronous_metrics>true</asynchronous_metrics>
    </prometheus>
```

- [x] **Step 5: Rewrite alerts 8, 11, and 12**

`IcebergCatalogDown`:

```yaml
      # The REST catalog has no /metrics endpoint, so liveness comes from a
      # blackbox probe rather than a scrape job's `up` series.
      - alert: IcebergCatalogDown
        expr: probe_success{job="blackbox-iceberg-rest"} == 0
        for: 1m
        labels:
          severity: critical
          team: platform
        annotations:
          summary: "Iceberg REST catalog is down"
          description: "The Iceberg REST catalog service is unreachable. All data operations will fail."
          runbook_url: "https://docs/runbook#catalog-down"
```

`KafkaConsumerLagHigh` — Redpanda v24.1.1 publishes **no lag series** (verified in
Step 1), so lag is computed as end-offset minus committed-offset, joined on
topic and partition. Note the label is `redpanda_topic`, not `topic` — the
original annotation referenced a label that does not exist on any Redpanda series.

```yaml
      # Redpanda exposes no consumer-group lag metric, so derive it:
      # lag = max_offset - committed_offset, per (group, topic, partition).
      # rpk group describe confirms these map exactly to LOG-END-OFFSET and
      # CURRENT-OFFSET, so there is no off-by-one. group_right because one
      # partition's max_offset fans out across every consumer group reading it.
      # The namespace filter excludes Redpanda's internal `controller` topic.
      - alert: KafkaConsumerLagHigh
        expr: |
          sum by (redpanda_group, redpanda_topic) (
            redpanda_kafka_max_offset{redpanda_namespace="kafka"}
            - on (redpanda_topic, redpanda_partition) group_right
            redpanda_kafka_consumer_group_committed_offset
          ) > 10000
        for: 10m
        labels:
          severity: warning
          team: data-engineering
        annotations:
          summary: "Kafka consumer lag is high"
          description: "Consumer group {{ $labels.redpanda_group }} on {{ $labels.redpanda_topic }} has {{ $value }} messages lag."
          runbook_url: "https://docs/runbook#consumer-lag"
```

`MinIOStorageAlmostFull` — no bucket quota is configured, so the original ratio can never resolve. Use cluster free capacity:

```yaml
      # No bucket quota is set, so minio_bucket_quota_bytes never exists.
      # Cluster free capacity is the signal that actually matters here.
      - alert: MinIOStorageAlmostFull
        expr: |
          minio_cluster_capacity_usable_free_bytes
          / minio_cluster_capacity_usable_total_bytes < 0.15
        for: 5m
        labels:
          severity: warning
          team: platform
        annotations:
          summary: "MinIO storage almost full"
          description: "Less than 15% of usable cluster capacity remains."
          runbook_url: "https://docs/runbook#storage-full"
```

- [x] **Step 6: Verify every target is up**

```bash
cd infrastructure && docker-compose up -d
sleep 30
curl -s localhost:9090/api/v1/targets | python3 -c "
import json,sys
for t in json.load(sys.stdin)['data']['activeTargets']:
    print(f\"{t['labels']['job']:28} {t['health']}\")
"
```
Expected: every job `up`. A `down` target means the endpoint does not exist on this image version or the port is wrong — fix or remove the job rather than leaving it down.

- [x] **Step 7: Confirm no rule is unhealthy**

```bash
curl -s localhost:9090/api/v1/rules | python3 -c "
import json,sys
for g in json.load(sys.stdin)['data']['groups']:
    for r in g['rules']:
        print(f\"{r['name']:28} {r['health']}\")
"
```
Expected: all 15 rules `ok`.

- [x] **Step 8: Commit**

```bash
git add infrastructure/prometheus/blackbox.yml infrastructure/prometheus/prometheus.yml \
        infrastructure/docker-compose.yml infrastructure/clickhouse/config.xml \
        monitoring/alerts/iceberg_alerts.yaml
git commit -m "feat: add Trino, ClickHouse, and catalog-probe scrape targets"
```

---

## Task 9: Close the ratchet and document

**Files:**
- Modify: `tests/test_metrics_registry.py`
- Modify: `docs/RUNBOOK.md`
- Modify: `CLAUDE.md`

- [ ] **Step 1: Add the closing assertion**

Append to `tests/test_metrics_registry.py`:

```python
def test_known_gaps_is_empty():
    """
    Every alert metric has a producer. If this fails, a new alert was added
    against a series nothing emits — add the producer, do not add a gap.
    """
    assert KNOWN_GAPS == frozenset(), (
        f"Alerts still reference unproduced metrics: {sorted(KNOWN_GAPS)}"
    )
```

- [ ] **Step 2: Run the whole suite**

Run: `./scripts/run_tests.sh`
Expected: all tests pass, including the pre-existing GA4 suite.

- [ ] **Step 3: Verify idempotency**

Per the CLAUDE.md rule, run the pipeline twice and confirm nothing doubles. The metrics export is read-only, but the added `compact_tables` task mutates files.

```bash
./scripts/reset_and_run.sh --no-datagen
curl -s 'localhost:9090/api/v1/query?query=iceberg_table_row_count' > /tmp/run1.json
./scripts/reset_and_run.sh --no-reset --no-datagen
curl -s 'localhost:9090/api/v1/query?query=iceberg_table_row_count' > /tmp/run2.json
python3 -c "
import json
a={(s['metric']['layer'],s['metric']['table']):s['value'][1] for s in json.load(open('/tmp/run1.json'))['data']['result']}
b={(s['metric']['layer'],s['metric']['table']):s['value'][1] for s in json.load(open('/tmp/run2.json'))['data']['result']}
diff={k:(a[k],b.get(k)) for k in a if a.get(k)!=b.get(k)}
print('IDENTICAL' if not diff else f'DRIFT: {diff}')
"
```
Expected: `IDENTICAL`.

- [ ] **Step 4: Add the runbook section**

In `docs/RUNBOOK.md`, add a Metrics section covering: where metrics come from (Pushgateway for batch, scrape for services), how to run the export by hand, and an anchor for every `runbook_url` the alert file references — `#pipeline-failure`, `#slow-pipeline`, `#stale-pipeline`, `#ingestion-stopped`, `#staging-lag`, `#entity-coverage`, `#duplicate-entities`, `#catalog-down`, `#minio-down`, `#redpanda-down`, `#consumer-lag`, `#storage-full`, `#compaction`, `#snapshot-expiration`, `#compaction-failure`.

- [ ] **Step 5: Record the rule in CLAUDE.md**

Under "Testing and Validation", add:

```markdown
- **Alerts may only reference metrics with a producer.** Every metric the
  pipeline emits is declared in `jobs/spark/metrics/registry.py`;
  `tests/test_metrics_registry.py` fails if an alert expression names anything
  outside that registry or `EXTERNAL_METRIC_PREFIXES`. This shipped broken once:
  11 of 15 alerts referenced series nothing produced, so freshness and
  compaction monitoring looked green while emitting nothing at all. Add the
  producer — never widen the external prefix list to silence the test
- Batch metrics go to the Pushgateway, never a scrape target: `spark-submit`
  drivers only live for the duration of a task. All pipeline metrics are gauges;
  Pushgateway replaces a group on push, so a pushed counter breaks `increase()`
```

- [ ] **Step 6: Final commit**

```bash
git add tests/test_metrics_registry.py docs/RUNBOOK.md CLAUDE.md
git commit -m "docs: document the metric registry contract and close the ratchet"
```

---

## Out of Scope

Deferred to their own plans:

- **OpenLineage** (Airflow provider + Spark listener + Marquez) for table- and column-level lineage across engines.
- **OpenTelemetry tracing** on the FastAPI ingestion path, with `traceparent` propagated into Redpanda headers and persisted alongside `_raw_id`.
- **Grafana dashboards** for the new series. The six existing dashboards are untouched; `iceberg_table_*` panels are a natural follow-up.
- **Alertmanager.** The alerting block in `prometheus.yml` stays commented out; these alerts are visible in the Prometheus UI only.
