"""
Tests for the DAG-level pipeline health callbacks.

callbacks.py is pure stdlib on purpose -- it runs inside the Airflow image,
which has neither pyspark nor the metrics package. That also means it can be
imported and tested here without a scheduler.

Because it does not import metrics.registry, the names it emits could drift
from what the registry declares and the ratchet would not notice. The
cross-check below is what closes that gap.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from callbacks import (
    build_health_body,
    on_pipeline_failure,
    on_pipeline_success,
    push_pipeline_health,
    run_duration,
)


class _DagRun:
    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date


class _Dag:
    def __init__(self, dag_id):
        self.dag_id = dag_id


def test_success_body_carries_success_timestamp():
    body = build_health_body("iceberg_pipeline", True, 42.0)
    assert "iceberg_pipeline_last_success_timestamp" in body
    assert "iceberg_pipeline_last_failure_timestamp" not in body


def test_failure_body_carries_failure_timestamp():
    body = build_health_body("iceberg_pipeline", False, 42.0)
    assert "iceberg_pipeline_last_failure_timestamp" in body
    assert "iceberg_pipeline_last_success_timestamp" not in body


def test_body_carries_dag_id_label_and_duration():
    body = build_health_body("iceberg_pipeline", True, 42.0)
    assert 'dag_id="iceberg_pipeline"' in body
    assert 'iceberg_pipeline_run_duration_seconds{dag_id="iceberg_pipeline"} 42.0' in body


def test_body_ends_with_newline():
    """Pushgateway rejects a body with no trailing newline."""
    assert build_health_body("iceberg_pipeline", True, 1.0).endswith("\n")


def test_body_has_help_and_type_for_every_metric():
    body = build_health_body("iceberg_pipeline", True, 1.0)
    metric_lines = [
        line for line in body.split("\n")
        if line and not line.startswith("#")
    ]
    assert metric_lines
    for line in metric_lines:
        name = line.split("{")[0]
        assert "# TYPE {} gauge".format(name) in body
        assert "# HELP {} ".format(name) in body


def test_emitted_names_match_the_registry():
    """
    callbacks.py cannot import the registry, so nothing but this test stops the
    two from drifting apart and quietly bypassing the alert-coverage ratchet.
    """
    from metrics.registry import metric_names

    emitted = set()
    for succeeded in (True, False):
        for line in build_health_body("iceberg_pipeline", succeeded, 1.0).split("\n"):
            if line and not line.startswith("#"):
                emitted.add(line.split("{")[0])
    assert emitted <= metric_names(), sorted(emitted - metric_names())


def test_run_duration_uses_start_and_end():
    start = datetime(2026, 8, 20, 10, 0, 0)
    context = {"dag_run": _DagRun(start, start + timedelta(seconds=90))}
    assert run_duration(context) == 90.0


def test_run_duration_is_zero_without_a_dag_run():
    assert run_duration({}) == 0.0


def test_run_duration_is_zero_without_a_start_date():
    assert run_duration({"dag_run": _DagRun(None, None)}) == 0.0


def test_run_duration_falls_back_to_start_when_end_is_missing():
    """A failure callback can fire before end_date is set."""
    start = datetime(2026, 8, 20, 10, 0, 0)
    assert run_duration({"dag_run": _DagRun(start, None)}) == 0.0


def test_run_duration_never_negative():
    start = datetime(2026, 8, 20, 10, 0, 0)
    context = {"dag_run": _DagRun(start, start - timedelta(seconds=5))}
    assert run_duration(context) == 0.0


def test_push_never_raises_when_gateway_is_down():
    """Port 1 is reserved and never listening."""
    push_pipeline_health(
        "iceberg_pipeline", True, 1.0, gateway_url="http://127.0.0.1:1"
    )


def test_callbacks_never_raise_when_gateway_is_down(monkeypatch):
    monkeypatch.setattr("callbacks.GATEWAY_URL", "http://127.0.0.1:1")
    start = datetime(2026, 8, 20, 10, 0, 0)
    context = {
        "dag": _Dag("iceberg_pipeline"),
        "dag_run": _DagRun(start, start + timedelta(seconds=5)),
    }
    on_pipeline_success(context)
    on_pipeline_failure(context)
