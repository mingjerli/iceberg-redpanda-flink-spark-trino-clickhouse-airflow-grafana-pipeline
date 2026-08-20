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
