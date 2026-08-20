"""
Tests for maintenance job outcome metrics.

Success and failure are separate timestamp gauges rather than a counter:
Pushgateway replaces a group's samples on every push, so increase() over a
pushed counter never reflects reality. CompactionJobFailed compares the two
timestamps instead.
"""
from __future__ import annotations

import time

from metrics.job_metrics import build_job_outcome_samples, record_job_outcome


def sample_for(samples, name):
    matches = [s for s in samples if s.name == name]
    assert len(matches) == 1, "expected one {}, got {}".format(name, len(matches))
    return matches[0]


def names(samples):
    return {s.name for s in samples}


def test_success_records_a_success_timestamp():
    samples = build_job_outcome_samples("compact_tables", True, 12.5)
    assert "maintenance_job_last_success_timestamp" in names(samples)
    assert "maintenance_job_last_failure_timestamp" not in names(samples)


def test_failure_records_a_failure_timestamp():
    samples = build_job_outcome_samples("compact_tables", False, 3.0)
    assert "maintenance_job_last_failure_timestamp" in names(samples)
    assert "maintenance_job_last_success_timestamp" not in names(samples)


def test_duration_is_always_recorded():
    for succeeded in (True, False):
        samples = build_job_outcome_samples("expire_snapshots", succeeded, 7.25)
        assert sample_for(samples, "maintenance_job_duration_seconds").value == 7.25


def test_label_is_maintenance_job_not_job():
    """`job` is the Pushgateway grouping key and would be overwritten."""
    samples = build_job_outcome_samples("compact_tables", True, 1.0)
    for sample in samples:
        assert "job" not in sample.labels
        assert sample.labels["maintenance_job"] == "compact_tables"


def test_timestamp_is_current_unix_time():
    before = time.time()
    samples = build_job_outcome_samples("compact_tables", True, 1.0)
    after = time.time()
    stamp = sample_for(samples, "maintenance_job_last_success_timestamp").value
    assert before <= stamp <= after


def test_emits_only_registered_metrics():
    from metrics.registry import metric_names

    samples = build_job_outcome_samples("compact_tables", False, 1.0)
    assert names(samples) <= metric_names()


def test_samples_render_without_error():
    from metrics.pushgateway import render_exposition

    body = render_exposition(build_job_outcome_samples("compact_tables", True, 2.0))
    assert 'maintenance_job="compact_tables"' in body
    assert body.endswith("\n")


def test_record_job_outcome_never_raises_when_gateway_is_down():
    """
    A metrics failure must not turn a successful maintenance run into a failed
    one. Port 1 is reserved and never listening.
    """
    record_job_outcome(
        "compact_tables", True, 1.0, gateway_url="http://127.0.0.1:1"
    )
