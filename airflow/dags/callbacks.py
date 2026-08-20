"""
DAG Callbacks: Pipeline Health Metrics
======================================

Pushes pipeline-level gauges to the Pushgateway when a DAG run succeeds or
fails, so PipelineFailure, PipelineDurationHigh, and PipelineStale have
something to read.

Those three alerts used to point at airflow_dag_* series from statsd. Airflow's
statsd output carries no dag_id label without a hand-written mapping, and it
emits nothing equivalent to a last-success timestamp at all, so none of them
could ever fire. Emitting our own gauges is both simpler and exact.

Deliberately pure stdlib: this runs in the Airflow image, which has neither
pyspark nor the metrics package. That means the metric names here are *not*
checked against metrics/registry.py at import time -- tests/
test_pipeline_health.py cross-checks them instead, so the two cannot drift
apart and silently bypass the alert-coverage ratchet.
"""
from __future__ import annotations

import logging
import os
import time
import urllib.error
import urllib.request

logger = logging.getLogger(__name__)

GATEWAY_URL = os.environ.get("PUSHGATEWAY_URL", "http://pushgateway:9091")
_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"

_HELP = {
    "iceberg_pipeline_last_success_timestamp":
        "Unix time the pipeline DAG last completed successfully",
    "iceberg_pipeline_last_failure_timestamp":
        "Unix time the pipeline DAG last failed",
    "iceberg_pipeline_run_duration_seconds":
        "Wall-clock duration of the last completed pipeline DAG run",
}


def _gauge(name: str, dag_id: str, value: float) -> str:
    """Render one gauge with its HELP and TYPE headers."""
    return (
        "# HELP {name} {help}\n"
        "# TYPE {name} gauge\n"
        '{name}{{dag_id="{dag_id}"}} {value}\n'
    ).format(name=name, help=_HELP[name], dag_id=dag_id, value=float(value))


def build_health_body(dag_id: str, succeeded: bool, duration_seconds: float) -> str:
    """
    Render the exposition body for one DAG outcome.

    Only the matching timestamp is emitted; writing both every run would make
    the "failure newer than success" comparison in PipelineFailure meaningless.
    The body ends with a newline -- Pushgateway 400s without it.
    """
    stamp = (
        "iceberg_pipeline_last_success_timestamp" if succeeded
        else "iceberg_pipeline_last_failure_timestamp"
    )
    return (
        _gauge(stamp, dag_id, time.time())
        + _gauge("iceberg_pipeline_run_duration_seconds", dag_id, duration_seconds)
    )


def push_pipeline_health(
    dag_id: str,
    succeeded: bool,
    duration_seconds: float,
    gateway_url: str = None,
) -> None:
    """
    Push last-outcome timestamp and duration for a DAG run.

    Never raises. A callback that threw would mark an otherwise successful DAG
    run as failed, which is precisely the signal it is meant to report on.
    """
    url = "{}/metrics/job/pipeline_health_{}".format(
        (gateway_url or GATEWAY_URL).rstrip("/"), dag_id
    )
    try:
        request = urllib.request.Request(
            url,
            data=build_health_body(dag_id, succeeded, duration_seconds).encode("utf-8"),
            method="POST",
            headers={"Content-Type": _CONTENT_TYPE},
        )
        with urllib.request.urlopen(request, timeout=10):
            logger.info(
                "Pushed pipeline health for %s (success=%s)", dag_id, succeeded
            )
    except (urllib.error.URLError, OSError) as exc:
        logger.warning("Could not push pipeline health for %s: %s", dag_id, exc)


def run_duration(context: dict) -> float:
    """
    Seconds the DAG run took, or 0.0 when that cannot be determined.

    A failure callback can fire before end_date is set, so end falls back to
    start rather than to now -- reporting a duration that keeps growing after
    the run is over would make PipelineDurationHigh alert on nothing.
    """
    dag_run = context.get("dag_run")
    if dag_run is None or dag_run.start_date is None:
        return 0.0
    end = dag_run.end_date or dag_run.start_date
    return max((end - dag_run.start_date).total_seconds(), 0.0)


def on_pipeline_success(context: dict) -> None:
    """DAG-level on_success_callback."""
    push_pipeline_health(context["dag"].dag_id, True, run_duration(context))


def on_pipeline_failure(context: dict) -> None:
    """DAG-level on_failure_callback."""
    push_pipeline_health(context["dag"].dag_id, False, run_duration(context))
