"""
Maintenance Job Outcome Metrics
===============================

Records whether a maintenance job succeeded, when, and how long it took.

Success and failure are separate timestamp gauges rather than a counter because
Pushgateway replaces a group's samples on each push, so increase() over a pushed
counter never reflects reality. CompactionJobFailed compares the two timestamps
instead: failure more recent than success means the last run failed.

Each job pushes under its own group (`maintenance_<name>`) so one job's outcome
never clobbers another's.
"""
from __future__ import annotations

import logging
import time

from metrics.pushgateway import DEFAULT_GATEWAY_URL, push_samples
from metrics.registry import MetricSample

logger = logging.getLogger(__name__)


def build_job_outcome_samples(
    job_name: str, succeeded: bool, duration_seconds: float
) -> list:
    """
    Build the gauges describing one maintenance run.

    Only the matching timestamp is emitted -- writing both on every run would
    make the "failure newer than success" comparison meaningless.

    The label is `maintenance_job`, not `job`: `job` is the Pushgateway
    grouping key and would be overwritten on push.
    """
    labels = {"maintenance_job": job_name}
    stamp = (
        "maintenance_job_last_success_timestamp" if succeeded
        else "maintenance_job_last_failure_timestamp"
    )
    return [
        MetricSample(stamp, labels, time.time()),
        MetricSample("maintenance_job_duration_seconds", labels, duration_seconds),
    ]


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
    try:
        push_samples(
            build_job_outcome_samples(job_name, succeeded, duration_seconds),
            gateway_url=gateway_url,
            job="maintenance_{}".format(job_name),
        )
    except Exception as exc:
        logger.warning("Could not record outcome for %s: %s", job_name, exc)
