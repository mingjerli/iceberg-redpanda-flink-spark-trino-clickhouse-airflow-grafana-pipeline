# GA4 Implementation Status

**Status**: Phase 1 complete. All 14 tasks shipped, test suite green (30 tests).

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

## Defects Found by Running the Tests

The suite was committed but had never been executed. Running it surfaced seven
defects, all since fixed:

| Where | Defect |
|-------|--------|
| `ga4_batch_ingest.py` | `current_timestamp()` / `input_file_name()` in the MERGE source — Spark rejects non-deterministic expressions there, so the ingest job failed on **every** run |
| `staging_batch.py` | `get_watermark_timestamp()` called but never defined — `NameError` in incremental mode |
| `staging_batch.py` | `hour`, `lag`, `first`, `sum`, `min`, `max`, `count`, `udf`, `unix_timestamp` used without importing them |
| `staging_batch.py` | local `count` variable shadowed the imported Spark `count` |
| `marts_incremental.py` | top-N windows partitioned by `(session_date, <dimension>)` instead of `session_date`, so `row_number() == 1` kept every row; the dashboard fanned out to one row per date × source × device × country (132 rows for 15 days) |
| `ga4_provider.py` | seeded the global `random` module, so two providers built with the same seed shared one stream and produced different output |
| `iceberg_pipeline.py`, `reset_and_run.sh` | invoked `ga4_batch_ingest.py` without the required `--input`, and with a `--mode` value outside its `append\|overwrite` choices |

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
