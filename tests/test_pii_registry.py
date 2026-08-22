"""
Enforcement ratchet for PII masking.

Modeled on tests/test_metrics_registry.py. That file exists because 13 of 15
alerts once referenced metrics nothing produced, so monitoring read as
configured while emitting nothing. The same failure mode applies here: a
plaintext column reintroduced downstream looks exactly like a masked pipeline
until someone reads the data.
"""
from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path

from pii.registry import PII_CLASSES, PII_DERIVED, PII_FIELDS, derived_columns, pii_columns, token_column
from tests.pipeline_tables import insert_rows

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
CTAS_ALIAS = re.compile(r"\bAS[ \t]+(\w+)[ \t]*,?[ \t]*$", re.MULTILINE | re.IGNORECASE)


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


# ---------------------------------------------------------------------------
# Generalizing ratchet: values, not names.
#
# CRITICAL 1 in the PII masking fix wave found raw Mailchimp merge_fields JSON
# carried into staging unchanged, sitting next to the first_name_token/
# last_name_token/phone_token it was extracted and hashed into. The ratchet
# above could never have caught it -- the offending column was called
# merge_fields, not email/first_name/phone, so it was never a candidate for
# FORBIDDEN. This test checks cell values instead of column names: after
# tokenization, no value written to the vault as plaintext may also appear,
# unmasked, in the staging row it came from -- whatever column holds it.
# ---------------------------------------------------------------------------

STAGED_AT = datetime(2026, 8, 21, 12, 0, 0)


# Exact `in` against a set is exact-membership, not containment, and it is
# case-sensitive. The vault stores the *normalized* plaintext
# (pii/tokenize.py normalize(): lower(trim(...)) for email/name/address,
# digits-only for phone), so a reintroduced merge_fields value of
# `{"FNAME": "Leakcheck", "LNAME": "Surname"}` equals none of the vault's
# entries outright -- "Leakcheck" only ever appears as a substring, in its
# original casing, inside a larger JSON string. A prior version of this test
# used exact `in` against a set and would still pass with the column
# reintroduced. This checks case-folded substring containment instead: fail
# if any vaulted plaintext appears anywhere inside str(value).lower().
#
# MIN_LEAK_MATCH_LENGTH excludes PII_DERIVED's last_name_prefix plaintext
# (pii/registry.py, pii/tokenize.py NAME_PREFIX: lower(substring(x, 1, 3)) --
# always exactly 3 characters, e.g. "sur"). A 3-character needle matches
# inside unrelated column values too often (ids, timestamps, free text) to be
# a useful signal. Every other PII class here is comfortably longer: phone is
# enforced >= MIN_PHONE_LENGTH (7) by tokenize.py before it ever reaches the
# vault, and this fixture's email/name values are all >= 6 characters. 4 is
# the smallest floor that clears the 3-character name_prefix while keeping
# every other class's shortest realistic value in scope.
MIN_LEAK_MATCH_LENGTH = 4


def test_no_staging_column_leaks_a_vaulted_plaintext_value(spark, pipeline_tables):
    """Runs the real stage_mailchimp_subscribers -- the regression site -- and
    asserts no cell in the resulting row contains, as a case-folded
    substring, a value semantic.pii_vault holds as plaintext for this run.
    A substring-based check catches the next stray JSON blob regardless of
    what it is named, what case it preserves, or where inside the value the
    plaintext sits."""
    from jobs.spark.staging_batch import stage_mailchimp_subscribers

    spark.sql("DROP TABLE IF EXISTS iceberg.staging.stg_mailchimp_subscribers")

    insert_rows(spark, "iceberg.raw.mailchimp_subscribers", [{
        "subscriber_id": "leak-check-subscriber",
        "email_address": "leak.check@example.com",
        "status": "subscribed",
        "phone": "+15551234567",
        "merge_fields": '{"FNAME": "Leakcheck", "LNAME": "Surname"}',
        "stats": "{}",
        "timestamp_signup": STAGED_AT,
        "_loaded_at": STAGED_AT,
    }])

    stage_mailchimp_subscribers(spark, mode="full")

    plaintext_values = {
        row.plaintext
        for row in spark.table("iceberg.semantic.pii_vault").select("plaintext").collect()
        if row.plaintext is not None
    }
    assert plaintext_values, "vault should hold at least the values just tokenized"

    needles = {
        value.lower() for value in plaintext_values
        if len(value) >= MIN_LEAK_MATCH_LENGTH
    }
    assert needles, "vault should hold at least one plaintext value long enough to check"

    staged_row = spark.table("iceberg.staging.stg_mailchimp_subscribers").collect()[0]
    leaked = [
        column for column, value in staged_row.asDict().items()
        if value is not None
        # Token columns are the masked replacement, not a leak of the
        # plaintext they were derived from -- excluded so the check flags
        # the source of a leak, not its intended, hashed counterpart.
        and not column.endswith("_token")
        and any(needle in str(value).lower() for needle in needles)
    ]
    assert not leaked, (
        f"staging.stg_mailchimp_subscribers leaks vaulted plaintext in columns: {leaked}"
    )


# ---------------------------------------------------------------------------
# Dashboards: the ratchet above only scans DDL/CTAS source files. A Grafana
# panel selecting raw `email` was one of the three findings that motivated
# this whole epic (docs/DESIGN_PII_MASKING.md Section 1) and was fixed by
# hand with no regression test. These two guard monitoring/dashboards/*.json.
# ---------------------------------------------------------------------------

DASHBOARDS_DIR = ROOT / "monitoring" / "dashboards"

RAW_LAYER_QUERY = re.compile(r"FROM\s+iceberg\.raw_", re.IGNORECASE)


def _dashboard_sql_targets():
    """Yield (path, panel_title, rawSql) for every SQL panel target."""
    for path in sorted(DASHBOARDS_DIR.glob("*.json")):
        doc = json.loads(path.read_text())
        for panel in doc.get("panels", []):
            for target in panel.get("targets", []):
                raw_sql = target.get("rawSql")
                if raw_sql:
                    yield path, panel.get("title", "<untitled>"), raw_sql


def test_dashboards_never_query_the_raw_layer():
    """The original leak queried `FROM iceberg.raw_shopify_orders` directly."""
    violations = [
        f"{path.relative_to(ROOT)} panel {title!r}"
        for path, title, raw_sql in _dashboard_sql_targets()
        if RAW_LAYER_QUERY.search(raw_sql)
    ]
    assert not violations, "Dashboard panels query the raw layer:\n" + "\n".join(violations)


def test_dashboards_never_select_a_forbidden_bare_column():
    violations = [
        f"{path.relative_to(ROOT)} panel {title!r} selects `{name}`"
        for path, title, raw_sql in _dashboard_sql_targets()
        for name in FORBIDDEN
        if re.search(r"\b{}\b".format(re.escape(name)), raw_sql)
    ]
    assert not violations, (
        "Dashboard panels select forbidden PII columns:\n" + "\n".join(violations)
    )
