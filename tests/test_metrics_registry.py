"""
Tests for the pipeline metric registry and the alert-coverage ratchet.

The ratchet is the point of this file: it fails whenever an alert rule
references a metric that nothing produces, unless that metric is explicitly
listed in KNOWN_GAPS. Tasks that add producers shrink KNOWN_GAPS; the final
task asserts it is empty.

This shipped broken once -- 13 of 15 alerts referenced series nothing emitted,
so freshness and compaction monitoring read as configured while producing
nothing at all. Widening EXTERNAL_METRIC_PREFIXES to silence a failure here
recreates exactly that bug.
"""
from __future__ import annotations

from pathlib import Path

import yaml

from metrics.registry import (
    KNOWN_GAPS,
    PIPELINE_METRICS,
    extract_metric_names,
    is_external,
    metric_names,
)

ROOT = Path(__file__).resolve().parents[1]
ALERTS_PATH = ROOT / "monitoring" / "alerts" / "iceberg_alerts.yaml"

# Every location a metric this pipeline emits can actually be pushed from.
# jobs/spark/**: every Spark batch/maintenance job and its metrics/ package.
# airflow/dags/**: iceberg_pipeline_* is pushed from callbacks.py, which is
# deliberately pure stdlib so it can run in the Airflow image (no pyspark) --
# see the comment on those entries in metrics/registry.py.
PRODUCER_ROOTS = (ROOT / "jobs" / "spark", ROOT / "airflow" / "dags")


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


def test_extract_metric_names_handles_joins_and_grouping():
    """The Redpanda lag expression is the hairiest one in the file."""
    expr = (
        "sum by (redpanda_group, redpanda_topic) (\n"
        '  redpanda_kafka_max_offset{redpanda_namespace="kafka"}\n'
        "  - on (redpanda_topic, redpanda_partition) group_right\n"
        "  redpanda_kafka_consumer_group_committed_offset\n"
        ") > 10000"
    )
    assert extract_metric_names(expr) == {
        "redpanda_kafka_max_offset",
        "redpanda_kafka_consumer_group_committed_offset",
    }


def test_registry_has_no_duplicate_names():
    names = [m.name for m in PIPELINE_METRICS]
    assert len(names) == len(set(names))


def test_registry_metrics_are_all_gauges():
    """Counters do not survive the Pushgateway replace-on-push model."""
    non_gauges = [m.name for m in PIPELINE_METRICS if m.kind != "gauge"]
    assert non_gauges == []


def test_registry_entries_have_help_text():
    missing = [m.name for m in PIPELINE_METRICS if not m.help.strip()]
    assert missing == []


def test_is_external_recognises_third_party_families():
    assert is_external("minio_cluster_capacity_usable_free_bytes")
    assert is_external("probe_success")
    assert not is_external("iceberg_table_row_count")


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


def test_known_gaps_is_empty():
    """
    Every alert metric has a producer. If this fails, an alert was added
    against a series nothing emits -- add the producer, do not add a gap.

    KNOWN_GAPS existed to let the fix land incrementally. It is closed now, and
    reopening it puts the pipeline back in the state this whole change set
    exists to correct: 13 of 15 alerts reading series that were never emitted.
    """
    assert KNOWN_GAPS == frozenset(), (
        f"Alerts still reference unproduced metrics: {sorted(KNOWN_GAPS)}"
    )


def _producer_sources() -> str:
    """Concatenated text of every file that could plausibly push a metric.

    Excludes registry.py itself: declaring a name there is exactly the act
    this test must not accept as a substitute for producing it.
    """
    chunks = []
    for root in PRODUCER_ROOTS:
        for path in sorted(root.rglob("*.py")):
            if path.name == "registry.py":
                continue
            chunks.append(path.read_text())
    return "\n".join(chunks)


def test_every_registered_metric_has_a_push_site():
    """
    Registering a metric in PIPELINE_METRICS is not the same as producing it.
    This shipped broken once already (the 13-of-15-alerts incident this whole
    file exists to prevent), and the PII masking fix wave reproduced it in
    miniature: pipeline_pii_vault_entries and pipeline_pii_tokenization_null_rate
    were declared with no MetricSample(...) call anywhere in the codebase.

    Every metric name must appear as a quoted literal in at least one producer
    source file, i.e. somewhere a MetricSample(...) or equivalent could
    actually construct it -- not just as the `name=` field in its own
    MetricDef in registry.py.
    """
    sources = _producer_sources()
    missing = [
        m.name for m in PIPELINE_METRICS
        if '"{}"'.format(m.name) not in sources and "'{}'".format(m.name) not in sources
    ]
    assert not missing, "Metrics with no push site: {}".format(missing)
