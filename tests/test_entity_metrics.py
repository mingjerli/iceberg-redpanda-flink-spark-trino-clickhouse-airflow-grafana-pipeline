"""
Tests for entity-resolution metric collection.

Builds a small semantic.entity_index by hand rather than running the full
backfill, so the assertions are about the metric maths, not resolution quality.
"""
from __future__ import annotations

import pytest

from metrics.entity_metrics import collect_entity_metrics


@pytest.fixture
def entity_index(spark):
    """
    Four rows: shopify fully resolved, stripe with one source_id split across
    two entity_ids -- the duplicate-mapping case alert 7 watches for.
    """
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
    assert len(matches) == 1, "expected one {}{}, got {}".format(
        name, labels, len(matches)
    )
    return matches[0]


def test_duplicate_mappings_counts_source_ids_with_two_entities(spark, entity_index):
    samples = collect_entity_metrics(spark)
    assert sample_for(samples, "entity_resolution_duplicate_mappings").value == 1


def test_duplicate_mappings_is_zero_when_every_mapping_is_unique(spark, entity_index):
    spark.sql("DELETE FROM iceberg.semantic.entity_index WHERE entity_id = 'e4'")
    samples = collect_entity_metrics(spark)
    assert sample_for(samples, "entity_resolution_duplicate_mappings").value == 0


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


def test_emits_only_registered_metrics(spark, entity_index):
    from metrics.registry import metric_names

    samples = collect_entity_metrics(spark)
    assert {s.name for s in samples} <= metric_names()


def test_samples_render_without_error(spark, entity_index):
    """The unlabelled duplicate-mappings gauge must survive rendering."""
    from metrics.pushgateway import render_exposition

    body = render_exposition(collect_entity_metrics(spark))
    assert "entity_resolution_duplicate_mappings 1.0" in body


def test_missing_entity_index_returns_empty(spark):
    """A metrics collector must never be the reason a pipeline run fails."""
    spark.sql("DROP TABLE IF EXISTS iceberg.semantic.entity_index")
    assert collect_entity_metrics(spark) == []
