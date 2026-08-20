"""
Tests for Prometheus text-exposition rendering.

Transport itself is not tested here -- push_samples is a thin urllib wrapper and
exercising it would need a live Pushgateway, which the suite deliberately avoids.
"""
from __future__ import annotations

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


def test_render_escapes_quotes_in_label_values():
    body = render_exposition([
        MetricSample("iceberg_table_row_count", {"layer": 'ra"w', "table": "t"}, 1)
    ])
    assert 'layer="ra\\"w"' in body


def test_render_escapes_backslashes_in_label_values():
    body = render_exposition([
        MetricSample("iceberg_table_row_count", {"layer": "ra\\w", "table": "t"}, 1)
    ])
    assert 'layer="ra\\\\w"' in body


def test_render_emits_values_as_floats():
    body = render_exposition([
        MetricSample("iceberg_table_file_count", {"layer": "raw", "table": "t"}, 7)
    ])
    assert "} 7.0" in body


def test_render_handles_metric_with_no_labels():
    """Metrics declared with an empty label tuple must not emit an empty {}."""
    from metrics.registry import PIPELINE_METRICS

    unlabelled = [m for m in PIPELINE_METRICS if not m.labels]
    if not unlabelled:
        pytest.skip("no unlabelled metric registered yet")
    body = render_exposition([MetricSample(unlabelled[0].name, {}, 1)])
    assert "{}" not in body


def test_render_rejects_unregistered_metric():
    with pytest.raises(ValueError, match="not in the registry"):
        render_exposition([MetricSample("made_up_metric", {}, 1)])


def test_render_empty_samples_returns_empty_string():
    assert render_exposition([]) == ""


def test_rendered_body_is_parseable_line_by_line():
    """Every non-comment line must be `name{labels} value`."""
    body = render_exposition([
        MetricSample("iceberg_table_row_count", {"layer": "raw", "table": "a"}, 1),
        MetricSample("iceberg_table_file_count", {"layer": "raw", "table": "a"}, 2),
    ])
    for line in body.strip().split("\n"):
        if line.startswith("#"):
            continue
        name_and_labels, _, value = line.rpartition(" ")
        assert name_and_labels, "line has no metric name: {!r}".format(line)
        float(value)
