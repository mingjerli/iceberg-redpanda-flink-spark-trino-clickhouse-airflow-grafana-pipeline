"""
The batch phase must wait for Flink to commit the raw tables (issue #11).

reset_and_run.sh submitted the Flink jobs, slept a fixed 60 seconds, then
started the batch pipeline. Messages reaching Redpanda does not mean Flink has
written the raw Iceberg tables -- those appear only after the first successful
checkpoint commit. On a from-scratch run there is no pre-existing table to
read, so 8 of 10 staging jobs failed against tables that did not exist yet.
The same jobs succeeded minutes later with identical code.

The script already had a raw-table check, but it lived inside
`if [ "$VALIDATE_MODE" = true ]` and only reported pass/fail -- it never gated
the batch phase, and never ran at all without --validate.

These tests exercise the polling logic directly by sourcing the script and
stubbing check_table_exists, so they need no Docker, Flink, or catalog. The
race itself only reproduces on a real cold start; that is verified by running
a from-scratch reset, not here.
"""
from __future__ import annotations

import subprocess
import time
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "reset_and_run.sh"


def run_bash(snippet: str, timeout: int = 60) -> subprocess.CompletedProcess:
    """Source the script and run a snippet against its functions."""
    # `set +e` must come AFTER the source: the script sets `set -e` at its top,
    # and that propagates into this shell, so a helper returning non-zero would
    # kill the subshell before the snippet could report its exit code.
    program = f'source "{SCRIPT}"\nset +e\n{snippet}\n'
    return subprocess.run(
        ["bash", "-c", program],
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def test_script_can_be_sourced_without_running_the_pipeline():
    """Sourcing must not execute main(); otherwise nothing here is testable
    and a stray `source` would wipe someone's volumes."""
    result = run_bash('echo "SOURCED_OK"')

    assert "SOURCED_OK" in result.stdout, result.stdout + result.stderr
    # main() opens with this banner. Its absence proves main did not run.
    assert "Reset and Run" not in result.stdout


def test_wait_for_raw_tables_returns_as_soon_as_all_tables_exist():
    result = run_bash(
        'check_table_exists() { return 0; }\n'
        'wait_for_raw_tables 5 shopify_orders shopify_customers\n'
        'echo "EXIT:$?"'
    )

    assert "EXIT:0" in result.stdout, result.stdout + result.stderr


def test_wait_for_raw_tables_fails_when_a_table_never_appears():
    """A table that never commits must fail the phase loudly, rather than
    letting the batch pipeline start and produce eight opaque staging
    failures."""
    result = run_bash(
        'check_table_exists() { return 1; }\n'
        'wait_for_raw_tables 2 shopify_orders\n'
        'echo "EXIT:$?"'
    )

    assert "EXIT:0" not in result.stdout, result.stdout + result.stderr
    assert "EXIT:" in result.stdout, "wait_for_raw_tables did not return"


def test_wait_for_raw_tables_names_the_table_it_gave_up_on():
    """The failure has to say which table is missing. Naming none of them is
    what made the original incident take several rounds to diagnose."""
    result = run_bash(
        'check_table_exists() { [ "$2" = "shopify_orders" ]; }\n'
        'wait_for_raw_tables 2 shopify_orders stripe_charges\n'
        'echo "EXIT:$?"'
    )

    combined = result.stdout + result.stderr
    assert "stripe_charges" in combined, combined
    assert "EXIT:0" not in result.stdout


def test_wait_for_raw_tables_polls_rather_than_sleeping_a_fixed_span():
    """It must return promptly once the condition holds. A fixed sleep would
    burn the whole budget regardless."""
    started = time.monotonic()
    result = run_bash(
        'check_table_exists() { return 0; }\n'
        'wait_for_raw_tables 30 shopify_orders\n'
        'echo "EXIT:$?"'
    )
    elapsed = time.monotonic() - started

    assert "EXIT:0" in result.stdout, result.stdout + result.stderr
    assert elapsed < 20, f"took {elapsed:.1f}s; looks like a fixed sleep, not a poll"


def test_wait_for_raw_tables_gives_up_within_its_budget():
    """Bounded failure: an unavailable table must not hang the run."""
    started = time.monotonic()
    run_bash(
        'check_table_exists() { return 1; }\n'
        'wait_for_raw_tables 2 shopify_orders\n'
        'echo "EXIT:$?"'
    )
    elapsed = time.monotonic() - started

    assert elapsed < 40, f"took {elapsed:.1f}s; timeout budget not respected"
