"""
Idempotency of the entity-resolution write path (issue #10).

`--mode initial` reads every staging customer with no watermark filter, so its
write must replace rather than accumulate. It appended, which meant each batch
run stacked a full recomputation on the previous one -- silently, because
duplicate rows are not an error. In a live run this reached 2,984 entity_index
rows for 985 distinct unified_ids, cascading to 43,217 marts.customer_360 rows
for 605 real customers.

CLAUDE.md states the rule this violates: an unfiltered read pairs with
createOrReplace or a MERGE on the grain key, never append. It also prescribes
the check -- two consecutive runs, every row count identical -- and warns to
assert below the layer you changed, since checking only raw passes while
everything downstream doubles.

`--mode range` is watermark-filtered, so append is correct there; the last
test guards against over-correcting it into a replace.
"""
from __future__ import annotations

from datetime import datetime

from pyspark.sql.functions import lit

from pii.tokenize import normalize, token_expr
from tests.pipeline_tables import insert_rows

PEPPER = "test-pepper-do-not-use-in-production"
STAGED_AT = datetime(2026, 8, 21, 12, 0, 0)
LATER = datetime(2026, 8, 22, 12, 0, 0)

ENTITY_INDEX = "iceberg.semantic.entity_index"
BLOCKING_INDEX = "iceberg.semantic.blocking_index"


def token_for(spark, value, pii_class):
    df = spark.range(1).withColumn("v", lit(value))
    return df.select(token_expr(normalize("v", pii_class), pii_class, PEPPER).alias("t")).collect()[0]["t"]


def _resolve_and_write(spark, mode="initial", start_date=None, end_date=None):
    """Run the same sequence main() runs for a resolution mode."""
    from jobs.spark.entity_backfill import (
        get_all_staging_customers,
        perform_initial_resolution,
        write_results,
    )

    staging_data = get_all_staging_customers(spark, start_date, end_date)
    entity_index_df, blocking_index_df = perform_initial_resolution(
        spark, staging_data, dry_run=False
    )
    write_results(spark, entity_index_df, blocking_index_df, dry_run=False, mode=mode)


def _counts(spark):
    return (
        spark.table(ENTITY_INDEX).count(),
        spark.table(BLOCKING_INDEX).count(),
    )


def test_initial_mode_rerun_does_not_duplicate(spark, pipeline_tables):
    """Two consecutive --mode initial runs must leave identical row counts."""
    email_token = token_for(spark, "ada@example.com", "email")
    other_token = token_for(spark, "grace@example.com", "email")

    insert_rows(spark, "iceberg.staging.stg_shopify_customers", [
        {"customer_id": "S1", "email_token": email_token, "_staged_at": STAGED_AT},
        {"customer_id": "S2", "email_token": other_token, "_staged_at": STAGED_AT},
    ])
    insert_rows(spark, "iceberg.staging.stg_hubspot_contacts", [
        {"contact_id": "H1", "email_token": email_token, "_staged_at": STAGED_AT},
    ])

    _resolve_and_write(spark)
    first_entity, first_blocking = _counts(spark)

    # Guard against a vacuous pass: if the first run wrote nothing, equal
    # counts below would prove nothing at all.
    assert first_entity == 3, f"expected 3 entity_index rows, got {first_entity}"
    assert first_blocking > 0, "expected at least one blocking key"

    _resolve_and_write(spark)
    second_entity, second_blocking = _counts(spark)

    assert second_entity == first_entity, (
        f"entity_index grew {first_entity} -> {second_entity} on rerun"
    )
    assert second_blocking == first_blocking, (
        f"blocking_index grew {first_blocking} -> {second_blocking} on rerun"
    )


def test_initial_mode_rerun_keeps_one_unified_id_per_source_record(spark, pipeline_tables):
    """Duplication also corrupts grouping: a source record must not end up
    holding two unified_ids after a rerun."""
    email_token = token_for(spark, "ada@example.com", "email")

    insert_rows(spark, "iceberg.staging.stg_shopify_customers", [
        {"customer_id": "S1", "email_token": email_token, "_staged_at": STAGED_AT},
    ])
    insert_rows(spark, "iceberg.staging.stg_hubspot_contacts", [
        {"contact_id": "H1", "email_token": email_token, "_staged_at": STAGED_AT},
    ])

    _resolve_and_write(spark)
    _resolve_and_write(spark)

    rows = spark.table(ENTITY_INDEX).select("unified_id", "source", "source_id").collect()
    assert rows, "entity_index should not be empty"

    per_source = {}
    for r in rows:
        per_source.setdefault((r["source"], r["source_id"]), set()).add(r["unified_id"])

    multi = {k: v for k, v in per_source.items() if len(v) > 1}
    assert not multi, f"source records hold more than one unified_id after rerun: {multi}"


def test_range_mode_still_appends(spark, pipeline_tables):
    """--mode range is watermark-filtered, so it must keep appending. This
    guards against fixing initial mode by making every write a replace."""
    first_token = token_for(spark, "ada@example.com", "email")
    second_token = token_for(spark, "grace@example.com", "email")

    insert_rows(spark, "iceberg.staging.stg_shopify_customers", [
        {"customer_id": "S1", "email_token": first_token, "_staged_at": STAGED_AT},
        {"customer_id": "S2", "email_token": second_token, "_staged_at": LATER},
    ])

    _resolve_and_write(spark, mode="range",
                       start_date="2026-08-21 00:00:00", end_date="2026-08-21 23:59:59")
    after_first, _ = _counts(spark)
    assert after_first == 1, f"expected the day-one row only, got {after_first}"

    _resolve_and_write(spark, mode="range",
                       start_date="2026-08-22 00:00:00", end_date="2026-08-22 23:59:59")
    after_second, _ = _counts(spark)

    assert after_second == 2, (
        f"range mode must accumulate across windows: {after_first} -> {after_second}"
    )
