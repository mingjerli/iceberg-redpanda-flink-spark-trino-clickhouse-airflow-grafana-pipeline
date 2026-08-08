# GA4 Implementation Status

**Status**: Phase 1 complete and verified end to end.

- Unit suite: 30 passed
- `./scripts/reset_and_run.sh --validate`: ALL VALIDATIONS PASSED, 36/36 tables populated
- Airflow `iceberg_pipeline`: 3 consecutive runs, 29/29 tasks green, and row
  counts identical between consecutive runs — the pipeline is idempotent

Counts from the verification run, chosen because each one corroborates the
transformation rather than merely showing it ran:

| Table | Rows | Why that number is right |
|-------|------|--------------------------|
| `raw.ga4_events` | 2227 | exactly the events generated — MERGE lost and duplicated nothing |
| `staging.stg_ga4_events` | 2227 | matches raw; dedup dropped no distinct natural key |
| `staging.stg_ga4_sessions` | 308 | matches the generator's distinct session count — the 30-min gap rule recovers them precisely |
| `analytics.ga4_engagement_metrics` | 30 | one row per day |
| `analytics.ga4_engagement_by_channel` | 229 | |
| `analytics.ga4_page_performance` | 1123 | |
| `analytics.ga4_funnel_analysis` | 150 | 30 days × 5 funnel steps |
| `marts.ga4_engagement_dashboard` | 30 | one row per day of metrics — proves the marts row-explosion fix |

Fixing GA4 also unblocked entity resolution for **every** source:
`get_all_staging_customers()` unions five staging tables, so the missing
`stg_ga4_sessions` had been taking the whole join down. At the verification run
`semantic.entity_index` went 0 → 892 and `marts.customer_360` 0 → 954. Those two
keep growing while the continuous datagen service runs, so treat them as
"non-zero and rising", not fixed values.

---

## Completed Tasks

| # | Task | Where it lives |
|---|------|----------------|
| 1 | Test infrastructure | `tests/conftest.py`, `tests/pipeline_tables.py` |
| 2 | GA4Provider (TDD) | `datagen/providers/ga4_provider.py`, `tests/test_ga4_provider.py` |
| 3 | Generator integration | `datagen/generator.py` (`generate_ga4_data`, `"ga4"` CLI choice) |
| 4 | Batch ingestion job | `jobs/spark/ga4_batch_ingest.py` (MERGE INTO on `_raw_id`) |
| 5 | Infrastructure | `infrastructure/docker-compose.yml` volume mounts, `.env.example` GA4 block |
| 6 | Staging events + dedup | `staging_batch.py::stage_ga4_events` |
| 7 | Staging sessions | `staging_batch.py::compute_ga4_sessions` (30-min gap rule) |
| 8 | Entity resolution | `entity_backfill.py::get_all_staging_customers`, `::rebuild_blocking_index` |
| 9 | Analytics (4 tables) | `analytics_incremental.py`, registered in `ANALYTICS_FUNCTIONS` |
| 10 | Marts | `marts_incremental.py::build_ga4_engagement_dashboard`; `customer_360` gains `has_ga4`, `ga4_total_sessions` |
| 11 | Airflow DAG | `airflow/dags/iceberg_pipeline.py` (8 tasks) |
| 12 | ClickHouse + monitoring | `infrastructure/clickhouse/iceberg_setup.sql`, `monitoring/dashboards/batch_business.json` |
| 13 | Scripts | `scripts/reset_and_run.sh`, `scripts/validate_tables.sh` |
| 14 | Docs + E2E tests | `tests/test_ga4_e2e.py`, `CLAUDE.md`, `README.md` |

Reference DDL for the GA4 analytics and marts tables lives in
`sql/04_analytics/ga4_*.sql` and `sql/05_marts/ga4_engagement_dashboard.sql`.
Like the rest of `sql/`, those files are documentation — the executable DDL is
inline in the Spark jobs.

---

## Critical Gaps Addressed

- **Event deduplication** (RC-2): `ROW_NUMBER()` over `(client_id, event_timestamp, event_name)` in `stage_ga4_events`, applied in both full and incremental modes
- **Idempotency** (RC-3): `MERGE INTO ... ON target._raw_id = source._raw_id` in `ga4_batch_ingest.py`
- **FIRST/LAST ordering** (RC-4): deterministic window functions in `compute_ga4_sessions`
- **`_raw_id` column** (RC-1): SHA-256 of the natural key, in `raw.ga4_events`
- **Funnel analysis** (PM gap): `analytics.ga4_funnel_analysis`
- **Page performance** (PM gap): `analytics.ga4_page_performance`

---

## Defects Found by Actually Running It

GA4 was committed but never executed — not the tests, not the batch path, not
the DAG. Exercising each layer surfaced 17 defects, all since fixed. They fall
into a pattern worth naming: **code that reported success while doing nothing,
or doing damage.**

### Found by making the committed tests runnable

`pytest` was not installed anywhere and no requirements file declared it.

| Where | Defect |
|-------|--------|
| `ga4_batch_ingest.py` | `current_timestamp()` / `input_file_name()` in the MERGE source — Spark rejects non-deterministic expressions there, so the ingest job failed on **every** run |
| `staging_batch.py` | `get_watermark_timestamp()` called but never defined — `NameError` in incremental mode |
| `staging_batch.py` | `hour`, `lag`, `first`, `sum`, `min`, `max`, `count`, `udf`, `unix_timestamp` used without importing them |
| `staging_batch.py` | local `count` variable shadowed the imported Spark `count` |
| `marts_incremental.py` | top-N windows partitioned by `(session_date, <dimension>)` instead of `session_date`, so `row_number() == 1` kept every row; the dashboard fanned out to one row per date × source × device × country (132 rows for 15 days) |
| `ga4_provider.py` | seeded the global `random` module, so two providers built with the same seed shared one stream and produced different output |
| `iceberg_pipeline.py`, `reset_and_run.sh` | invoked `ga4_batch_ingest.py` without the required `--input`, and with a `--mode` value outside its `append\|overwrite` choices |

### Found by running the full pipeline

| Where | Defect |
|-------|--------|
| `ingestion/requirements.txt` | `prometheus-fastapi-instrumentator<7` against an unbounded `fastapi<1.0` resolved to a Starlette whose `_IncludedRouter` the 6.x middleware cannot read → `AttributeError` on **every** request. All 920 webhook posts returned 500 and every topic stayed empty |
| `reset_and_run.sh` | `generator.py --source ga4` was never invoked, so no Parquet existed. `GA4_EXPORT_FILES`/`GA4_SESSIONS_PER_FILE` were defined, documented and printed but drove nothing |
| `datagen/requirements.txt` + venv bootstrap | `pandas`/`pyarrow` missing for the Parquet writer; the bootstrap's hand-kept list also lacked `orjson`. Now installs from the declared requirements files |
| `reset_and_run.sh`, `iceberg_pipeline.py` | `--table stg_ga4_events` — the `STAGING_FUNCTIONS` keys are `ga4_events`/`ga4_sessions`; argparse rejected both before Spark started |
| `reset_and_run.sh` (×6) | `$SPARK_SUBMIT … \| tail -N \|\| log_warning` then unconditional `log_success`. The pipe yields tail's status, so a failed entity resolution printed `✓ Entity resolution complete` |
| `reset_and_run.sh` | `post_mock_data.py` reported `Total Failed: 920`; the script piped it to grep, discarded the status and carried on |
| `reset_and_run.sh` | `rpk topic consume --num 1` blocks forever on an empty topic — the check for "no messages" hung 58 minutes instead of reporting |

### Found by triggering the Airflow DAG

Only reachable on a **second** run, which is why full-mode verification and a
green test suite both missed them.

| Where | Defect |
|-------|--------|
| `ga4_batch_ingest.py` | `WHEN MATCHED THEN UPDATE SET *` rewrote `_loaded_at` on re-ingest, pushing every row past the staging watermark so staging appended a second full copy. Now insert-only |
| 5 GA4 aggregations | `compute_ga4_sessions` + the four `compute_ga4_*` analytics jobs read their **entire** source then `append` on incremental, stacking a full recomputation each run: 308 sessions → 616 → 924. Now `createOrReplace`, matching the unfiltered read |
| `test_e2e_ga4_idempotency` | asserted only that **raw** was stable — true even while every table beneath it doubled. Now asserts staging and sessions do not move on a second pass |

`marts` was the only job using `MERGE`, so it was the one failing loudly with
`MERGE_CARDINALITY_VIOLATION` — the canary, not the culprit. Had it appended
like the others, this would have corrupted silently and indefinitely.

---

## Running the Tests

```bash
./scripts/run_tests.sh
```

Runs inside the Spark image; needs no other service. See the Testing sections of
`README.md` and `CLAUDE.md`.

---

## Key Design Decisions

- Batch ingestion via Parquet (no BigQuery emulator)
- MERGE INTO for idempotency
- `ROW_NUMBER()` for event deduplication
- Window functions for deterministic first/last attribution
- 30-minute inactivity gap for session computation
- Microsecond timestamp precision preserved
- `user_id` = email for demo entity resolution
