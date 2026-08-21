"""
Enforcement ratchet for PII masking.

Modeled on tests/test_metrics_registry.py. That file exists because 13 of 15
alerts once referenced metrics nothing produced, so monitoring read as
configured while emitting nothing. The same failure mode applies here: a
plaintext column reintroduced downstream looks exactly like a masked pipeline
until someone reads the data.
"""
from __future__ import annotations

import re
from pathlib import Path

from pii.registry import PII_CLASSES, PII_DERIVED, PII_FIELDS, derived_columns, pii_columns, token_column

ROOT = Path(__file__).resolve().parents[1]

# Files defining tables at or below the staging boundary.
GUARDED_SOURCES = (
    ROOT / "jobs" / "spark" / "staging_batch.py",
    ROOT / "jobs" / "spark" / "core_views.py",
    ROOT / "jobs" / "spark" / "analytics_incremental.py",
    ROOT / "jobs" / "spark" / "marts_incremental.py",
    ROOT / "infrastructure" / "clickhouse" / "init-analytics.sql",
)

# Bare column names that must never appear in a guarded DDL.
FORBIDDEN = {
    "email", "first_name", "last_name", "full_name", "phone", "mobile_phone",
    "address", "address_line1", "address_line2", "billing_email",
    "billing_name", "billing_phone", "shipping_name", "email_address",
    "email_normalized", "phone_normalized", "shipping_address_line1",
    # added after the brief was written:
    "name", "subscriber_id", "email_id", "mailchimp_subscriber_id",
    "customer_email", "customer_phone", "user_id",
    # added after cross-checking against jobs/spark/pii/registry.py: last_name_prefix
    # is a PII_DERIVED entry (class name_prefix) and is just as forbidden bare as
    # any PII_FIELDS entry.
    "last_name_prefix",
}

DDL_COLUMN = re.compile(
    r"^\s+(\w+)\s+(STRING|String|LowCardinality\(String\))\s*,?\s*(COMMENT|--|$)",
    re.MULTILINE,
)

# core_views.py declares its tables as CREATE TABLE ... AS SELECT ... AS alias,
# not typed `column TYPE,` DDL -- DDL_COLUMN structurally cannot see it (zero
# matches, confirmed). This catches the `AS alias` column list of a CTAS
# SELECT. [ \t]+ / [ \t]* (not \s) keep the match on one line: \s spans
# newlines, which let `CREATE VIEW ... AS\nSELECT` match with alias="SELECT"
# during development -- a real cross-line false-positive risk, not a
# hypothetical one. Requiring end-of-line after the captured word also means
# `CAST(x AS DECIMAL(18, 2))` and `CAST(x AS BIGINT)` never match: `AS` there
# is followed by `)`, not `,`/EOL, so the regex backtracks to the real
# trailing `AS alias,` on the same line instead.
CTAS_ALIAS = re.compile(r"\bAS[ \t]+(\w+)[ \t]*,?[ \t]*$", re.MULTILINE)


def test_every_registry_entry_uses_a_known_class():
    for table, mapping in PII_FIELDS.items():
        for column, pii_class in mapping.items():
            assert pii_class in PII_CLASSES, f"{table}.{column} has unknown class {pii_class}"


def test_every_derived_entry_points_at_a_registered_source():
    for table in PII_FIELDS:
        for new_column, (source_column, pii_class) in derived_columns(table).items():
            assert source_column in pii_columns(table), \
                f"{table}.{new_column} derives from unregistered {source_column}"
            assert pii_class in PII_CLASSES


def test_token_column_naming_is_consistent():
    assert token_column("email") == "email_token"
    assert token_column("billing_email") == "billing_email_token"


def test_forbidden_set_covers_every_registry_column():
    """Cross-check: every bare PII column name the registry knows about must be
    in FORBIDDEN, or the ratchet below could silently miss a reintroduced
    plaintext column just because nobody remembered to list it.

    Iterates the union of PII_FIELDS and PII_DERIVED table keys, not just
    PII_FIELDS -- today every PII_DERIVED table also has a PII_FIELDS entry,
    but that's a coincidence of the current registry, not a guarantee. A
    future PII_DERIVED-only table would be silently skipped by `for table in
    PII_FIELDS`."""
    registry_columns = set()
    for table in set(PII_FIELDS) | set(PII_DERIVED):
        registry_columns.update(pii_columns(table).keys())
        for new_column, (source_column, _pii_class) in derived_columns(table).items():
            registry_columns.add(source_column)
            registry_columns.add(new_column)

    missing = registry_columns - FORBIDDEN
    assert not missing, f"FORBIDDEN is missing registry columns: {sorted(missing)}"
    assert registry_columns, "registry_columns should not be empty"


def test_no_bare_pii_column_below_the_staging_boundary():
    """The ratchet. A plaintext column here means masking is cosmetic.

    Two independent patterns, because the guarded files use two different
    column-declaration styles: DDL_COLUMN for typed `column TYPE,` CREATE
    TABLE statements (staging_batch.py, analytics_incremental.py,
    marts_incremental.py, init-analytics.sql), CTAS_ALIAS for the `AS alias`
    columns of core_views.py's `CREATE TABLE ... AS SELECT` views, which
    DDL_COLUMN cannot see at all -- it declares no typed columns, only SELECT
    aliases."""
    violations = []
    for path in GUARDED_SOURCES:
        text = path.read_text()
        for pattern in (DDL_COLUMN, CTAS_ALIAS):
            for match in pattern.finditer(text):
                column = match.group(1)
                if column in FORBIDDEN:
                    line = text[: match.start()].count("\n") + 1
                    violations.append(f"{path.relative_to(ROOT)}:{line} declares `{column}`")
    assert not violations, "Plaintext PII columns below staging:\n" + "\n".join(violations)
