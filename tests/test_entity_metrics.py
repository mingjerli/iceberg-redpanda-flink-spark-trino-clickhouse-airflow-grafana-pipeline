"""
Tests for entity-resolution metric collection.

Uses the `pipeline_tables` fixture and insert_rows() so the table under test is
the real semantic.entity_index DDL, not a hand-written approximation of it.
An earlier version of this file declared its own schema with `entity_id` and
`source_system` columns; the real table uses `unified_id` and `source`, so the
tests passed while the collector returned nothing against a live warehouse.
That is exactly the failure CLAUDE.md's "use pipeline_tables.py" rule exists to
prevent.
"""
from __future__ import annotations

import pytest

from metrics.entity_metrics import collect_entity_metrics
from tests.pipeline_tables import insert_rows

ENTITY_INDEX = "iceberg.semantic.entity_index"


@pytest.fixture
def entity_rows(spark, pipeline_tables):
    """
    Four mappings: shopify fully resolved, stripe with one source_id split
    across two unified_ids -- the duplicate case DuplicateEntityMappings
    watches for.
    """
    insert_rows(spark, ENTITY_INDEX, [
        {"unified_id": "u1", "source": "shopify_customers", "source_id": "s1"},
        {"unified_id": "u2", "source": "shopify_customers", "source_id": "s2"},
        {"unified_id": "u3", "source": "stripe_customers", "source_id": "p1"},
        {"unified_id": "u4", "source": "stripe_customers", "source_id": "p1"},
    ])


def sample_for(samples, name, **labels):
    matches = [
        s for s in samples
        if s.name == name and all(s.labels.get(k) == v for k, v in labels.items())
    ]
    assert len(matches) == 1, "expected one {}{}, got {}".format(
        name, labels, len(matches)
    )
    return matches[0]


def names(samples):
    return {s.name for s in samples}


def test_duplicate_mappings_counts_source_ids_with_two_unified_ids(spark, entity_rows):
    samples = collect_entity_metrics(spark)
    assert sample_for(samples, "entity_resolution_duplicate_mappings").value == 1


def test_duplicate_mappings_is_zero_when_every_mapping_is_unique(spark, entity_rows):
    spark.sql("DELETE FROM {} WHERE unified_id = 'u4'".format(ENTITY_INDEX))
    samples = collect_entity_metrics(spark)
    assert sample_for(samples, "entity_resolution_duplicate_mappings").value == 0


def test_coverage_is_emitted_per_source(spark, entity_rows):
    samples = collect_entity_metrics(spark)
    sources = {
        s.labels["source"] for s in samples
        if s.name == "entity_resolution_coverage_percent"
    }
    assert sources == {"shopify_customers", "stripe_customers"}


def test_coverage_is_full_when_every_row_resolved(spark, entity_rows):
    samples = collect_entity_metrics(spark)
    assert sample_for(
        samples, "entity_resolution_coverage_percent", source="shopify_customers"
    ).value == 100.0


def test_coverage_drops_when_unified_id_is_null(spark, entity_rows):
    insert_rows(spark, ENTITY_INDEX, [
        {"unified_id": None, "source": "shopify_customers", "source_id": "s3"},
    ])
    samples = collect_entity_metrics(spark)
    coverage = sample_for(
        samples, "entity_resolution_coverage_percent", source="shopify_customers"
    ).value
    assert coverage == pytest.approx(200.0 / 3.0)


def test_emits_only_registered_metrics(spark, entity_rows):
    from metrics.registry import metric_names

    assert names(collect_entity_metrics(spark)) <= metric_names()


def test_samples_render_without_error(spark, entity_rows):
    """The unlabelled duplicate-mappings gauge must survive rendering."""
    from metrics.pushgateway import render_exposition

    body = render_exposition(collect_entity_metrics(spark))
    assert "entity_resolution_duplicate_mappings 1.0" in body


def test_queries_the_real_column_names(spark, entity_rows):
    """
    Guards the bug this file was rewritten for: the collector must read the
    columns semantic.entity_index actually has. Any rename breaks this before
    it reaches a live warehouse.
    """
    columns = {f.name for f in spark.table(ENTITY_INDEX).schema.fields}
    assert {"unified_id", "source", "source_id"} <= columns


def test_missing_entity_index_returns_empty(spark):
    """A metrics collector must never be the reason a pipeline run fails."""
    spark.sql("DROP TABLE IF EXISTS {}".format(ENTITY_INDEX))
    assert collect_entity_metrics(spark) == []
