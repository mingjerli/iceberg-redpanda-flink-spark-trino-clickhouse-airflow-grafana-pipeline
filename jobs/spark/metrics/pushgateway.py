"""
Pushgateway Transport
=====================

Renders MetricSample values as Prometheus text exposition format and POSTs them
to a Pushgateway.

Uses urllib from the stdlib rather than prometheus_client: the Spark image is
stock and adding a pip dependency would mean a custom Dockerfile for it.

Spark drivers are ephemeral -- a spark-submit driver lives only for the duration
of its task -- so batch jobs push here instead of being scraped.
"""
from __future__ import annotations

import logging
import os
import urllib.error
import urllib.request

from metrics.registry import PIPELINE_METRICS, MetricSample

logger = logging.getLogger(__name__)

DEFAULT_GATEWAY_URL = os.environ.get("PUSHGATEWAY_URL", "http://pushgateway:9091")

_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"
_DEFS = {m.name: m for m in PIPELINE_METRICS}


def _escape(value: str) -> str:
    """Escape a label value per the exposition format spec."""
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _format_labels(labels: dict) -> str:
    """Render a label set, or the empty string when there are none."""
    if not labels:
        return ""
    pairs = ",".join(
        '{}="{}"'.format(k, _escape(v)) for k, v in sorted(labels.items())
    )
    return "{" + pairs + "}"


def render_exposition(samples: list) -> str:
    """
    Render samples as Prometheus text exposition format.

    Samples are grouped by metric so each family emits one HELP/TYPE header.
    The result always ends with a newline; Pushgateway 400s without it.

    Rendering an unregistered metric is an error rather than a passthrough: the
    registry is what tests/test_metrics_registry.py checks alert rules against,
    so a metric that skips it would be invisible to the ratchet.
    """
    if not samples:
        return ""

    unknown = sorted({s.name for s in samples if s.name not in _DEFS})
    if unknown:
        raise ValueError(
            "metrics not in the registry: {}. "
            "Add them to PIPELINE_METRICS in registry.py first.".format(unknown)
        )

    lines = []
    for name in sorted({s.name for s in samples}):
        definition = _DEFS[name]
        lines.append("# HELP {} {}".format(name, definition.help))
        lines.append("# TYPE {} {}".format(name, definition.kind))
        for sample in [s for s in samples if s.name == name]:
            lines.append(
                "{}{} {}".format(
                    name, _format_labels(sample.labels), float(sample.value)
                )
            )

    return "\n".join(lines) + "\n"


def push_samples(
    samples: list,
    gateway_url: str = DEFAULT_GATEWAY_URL,
    job: str = "iceberg_pipeline",
    timeout: int = 10,
) -> None:
    """
    POST samples to the Pushgateway under the given job group.

    Raises on transport failure so the caller can decide what that means.
    Callers that must not fail their pipeline task should catch and log.
    """
    body = render_exposition(samples)
    if not body:
        logger.info("No samples to push")
        return

    url = "{}/metrics/job/{}".format(gateway_url.rstrip("/"), job)
    request = urllib.request.Request(
        url,
        data=body.encode("utf-8"),
        method="POST",
        headers={"Content-Type": _CONTENT_TYPE},
    )

    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            logger.info(
                "Pushed %d samples to %s (HTTP %d)",
                len(samples), url, response.status,
            )
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            "Pushgateway rejected push to {}: {} {}".format(url, exc.code, detail)
        )
    except urllib.error.URLError as exc:
        raise RuntimeError(
            "Pushgateway unreachable at {}: {}".format(url, exc.reason)
        )
