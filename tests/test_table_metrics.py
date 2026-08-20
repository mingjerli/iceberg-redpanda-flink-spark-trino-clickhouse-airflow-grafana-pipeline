"""
Tests for Iceberg table metric collection.

Uses the session `spark` fixture from conftest.py, which is backed by a
hadoop-type catalog in a temp dir, so no REST catalog or MinIO is needed.
"""
from __future__ import annotations

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


def test_collect_emits_only_registered_metrics(spark, metrics_table):
    """Anything not in the registry would be rejected at render time."""
    from metrics.registry import metric_names

    samples = collect_table_metrics(spark, [metrics_table])
    assert {s.name for s in samples} <= metric_names()


def test_collect_skips_missing_table_without_raising(spark, metrics_table):
    """A metrics job must never be the reason a pipeline run fails."""
    samples = collect_table_metrics(spark, [metrics_table, "iceberg.raw.does_not_exist"])
    tables = {s.labels["table"] for s in samples}
    assert tables == {"metrics_probe"}


def test_collect_empty_table_list_returns_nothing(spark):
    assert collect_table_metrics(spark, []) == []


def test_list_pipeline_tables_finds_created_table(spark, metrics_table):
    tables = list_pipeline_tables(spark, ["raw"])
    assert "iceberg.raw.metrics_probe" in tables


def test_list_pipeline_tables_skips_missing_namespace(spark):
    """A namespace that does not exist is logged and skipped, not raised."""
    assert list_pipeline_tables(spark, ["no_such_namespace"]) == []
