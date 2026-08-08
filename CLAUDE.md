# CLAUDE.md

## Project Overview

End-to-end data pipeline integrating five sources through a unified Apache Iceberg lakehouse: Shopify, Stripe, HubSpot, and Mailchimp arrive as webhooks; GA4 arrives as batch Parquet exports (standing in for a BigQuery Export). Combines real-time streaming (Flink) with batch processing (Spark), entity resolution, and monitoring (Grafana/Prometheus). Runs entirely in Docker (13+ services).

## Tech Stack

- **Storage:** MinIO (S3-compatible)
- **Table Format:** Apache Iceberg (v2, Parquet + zstd)
- **Catalog:** Iceberg REST Catalog backed by PostgreSQL
- **Streaming:** Redpanda (Kafka-compatible) + Apache Flink SQL
- **Batch ETL:** Apache Spark (PySpark)
- **Query Engines:** Trino, Spark SQL, ClickHouse
- **Orchestration:** Apache Airflow
- **Ingestion API:** FastAPI (webhook receiver with HMAC validation)
- **Monitoring:** Prometheus + Grafana (4 pre-built dashboards)
- **Data Generation:** Faker-based mock providers

## Directory Structure

```
infrastructure/          # Docker Compose, .env, per-service configs
jobs/flink/              # Flink SQL streaming jobs (*_full.sql)
jobs/spark/              # Spark batch jobs (Python)
sql/                     # DDL and transforms organized by layer
  00_raw/ 01_staging/ 02_semantic/ 03_core/ 04_analytics/ 05_marts/
airflow/dags/            # Airflow DAG definitions
ingestion/app/           # FastAPI webhook ingestion service
datagen/                 # Mock data generators and webhook simulator
monitoring/              # Prometheus alerts, Grafana dashboards
scripts/                 # Automation scripts (reset_and_run.sh, run_tests.sh, validate_tables.sh)
tests/                   # Pytest suite (conftest.py fixtures, pipeline_tables.py DDL helpers)
requirements-dev.txt     # Test dependencies
schemas/                 # API JSON schemas
docs/                    # Architecture docs, diagrams, runbook
  index.html             # Control panel, served by the `homepage` container on :8087
```

## Build and Run

```bash
# Full setup from scratch
cd infrastructure && docker-compose up -d
./scripts/reset_and_run.sh

# Options
./scripts/reset_and_run.sh --validate       # With detailed validation
./scripts/reset_and_run.sh --no-reset       # Skip reset, just run pipeline
./scripts/reset_and_run.sh --reset-only     # Only reset, don't run
./scripts/reset_and_run.sh --no-datagen     # Don't start continuous data gen
```

No Makefile. Uses shell scripts and direct `docker-compose` commands.

## Data Pipeline Architecture

Five-layer medallion architecture:

```
Webhooks → FastAPI → Redpanda → Flink (streaming) → raw (Iceberg)
GA4 Parquet ────────→ Spark ga4_batch_ingest.py ──→ raw (Iceberg)
  → Spark staging_batch.py       → staging layer
  → Spark entity_backfill.py     → semantic layer (entity resolution)
  → Spark core_views.py          → core layer (unified objects)
  → Spark analytics_incremental.py → analytics layer
  → Spark marts_incremental.py   → marts layer → Grafana dashboards
```

Two ingress paths converge on the raw layer. The four webhook sources stream
through Flink; GA4 is batch-only — `datagen/` writes Parquet to the volume
mounted at `/opt/spark/data`, and `ga4_batch_ingest.py` MERGEs it into
`raw.ga4_events` keyed on `_raw_id`, so re-running a file is idempotent. From
staging onward every source follows the same path.

Airflow orchestrates batch jobs on a 4-hour schedule by default.

## Coding Conventions

### Python (Spark jobs, ingestion, datagen)
- Every file starts with a multi-line docstring explaining purpose and usage
- `logging.basicConfig(...)` with named logger per module
- Type hints on function signatures
- `argparse` for CLI args (`--mode full|incremental|range`, `--table`, `--start-date`)
- Environment variables via `os.environ.get("VAR", "default")`
- Watermark pattern: track `_staged_at`, `_loaded_at` for incremental processing

### SQL (Flink and Spark)
- Section headers with `-- ====` comment blocks
- 2-space indentation, backticks for reserved words
- Column comments (`COMMENT 'description'`)
- Month-based partitioning on `created_at`
- Iceberg table properties: `'format-version' = '2'`, `'write.parquet.compression-codec' = 'zstd'`

### Shell Scripts
- `set -e` at top
- Colored output (`RED`, `GREEN`, `NC` variables)
- `SCRIPT_DIR` / `PROJECT_DIR` derivation pattern
- Environment sourcing with `set -a` / `set +a`

### Naming Conventions
- Tables: `raw.*`, `staging.stg_*`, `analytics.*`, `marts.*`
- Metadata columns: `_raw_id`, `_webhook_topic`, `_loaded_at`
- Flink jobs: `*_full.sql`
- Spark jobs: descriptive snake_case Python files
- **`--table` takes the registry key, not the table name.** `STAGING_FUNCTIONS`
  is keyed `ga4_events`, so the CLI arg is `--table ga4_events` even though the
  table is `staging.stg_ga4_events`. The `stg_` form is rejected by argparse
  before Spark starts

### Idempotency (read before adding a job)

Everything reruns — Airflow retries, the 4-hour schedule, manual triggers. The
write mode must match the read scope:

| Read | Write |
|------|-------|
| Filtered by watermark (`_loaded_at > last`) | `append` |
| Unfiltered, i.e. the whole source table | `createOrReplace`, or `MERGE` on the grain key |

An unfiltered read plus `append` stacks a full recomputation every run and fails
silently, because duplicate rows are not an error. This shipped: GA4 sessions
went 308 → 616 → 924 until the marts `MERGE` finally died with
`MERGE_CARDINALITY_VIOLATION`.

Also:
- Never `UPDATE` the watermark column in a re-ingest MERGE. Rewriting
  `_loaded_at` pushes every row past the staging watermark, so the next
  incremental run restages the whole table. `ga4_batch_ingest.py` is
  insert-only for exactly this reason
- Assert idempotency *below* the layer you changed — checking only `raw` passes
  while everything downstream doubles
- Verify with two consecutive runs: every row count must be identical

### Shell scripts: never mask an exit code

`cmd 2>&1 | tail -5 || log_warning "..."` reports **tail's** status, so the guard
never fires and the `log_success` after it lies. Six call sites did this, which
is why a failed entity resolution printed `✓ Entity resolution complete` while
`entity_index`, `core.customers` and `customer_360` sat empty. Use
`run_spark_job()` (`scripts/reset_and_run.sh:200`) or capture `${PIPESTATUS[0]}`.

### Airflow DAGs
- `default_args` with `owner: "data-engineering"`, retries: 2, retry_delay: 5 min
- Spark submit commands built from environment variables
- Clear task dependency chains with `>>` operator

## Configuration

All configuration lives in `infrastructure/.env` (137 parameters). Template at `infrastructure/.env.example`. Covers ports, credentials, resource limits, scale factors.

## Testing and Validation

```bash
./scripts/run_tests.sh                        # whole suite
./scripts/run_tests.sh tests/test_ga4_dedup.py
./scripts/run_tests.sh -k dedup -vv           # pytest args pass through
```

- Pytest suite in `tests/`, dependencies in `requirements-dev.txt`
- Tests run inside the `infrastructure-spark-master` image (Java 11, Spark 3.5.3,
  Iceberg 1.5.0). Running them on the host would need a JDK plus a matching local
  PySpark, so always go through the script
- No other service needs to be up: `tests/conftest.py` registers a hadoop-type
  Iceberg catalog named `iceberg` in a temp dir, so MinIO, Postgres, and the REST
  catalog can all be down
- `tests/pipeline_tables.py` holds the DDL for every source table plus
  `insert_rows()`, which builds DataFrames from the table's own schema. Use it
  rather than `spark.createDataFrame(pd.DataFrame(...))` — inference fails on
  all-NULL columns and mistypes decimals
- The `pipeline_tables` fixture creates all five staging sources even when a test
  populates only one, because `get_all_staging_customers()` unions across all of them
- `tests/test_ga4_e2e.py` calls the same functions the Airflow DAG invokes, so
  caller/callee signature drift fails there rather than in a scheduled run
- `scripts/validate_tables.sh` — row count validation against a running stack
- Docker health checks on all services
- Faker-based mock data in `datagen/` for realistic test data
- `--dry-run` mode on entity backfill

## Key Design Decisions

- PostgreSQL catalog backend (not SQLite) for concurrent Spark/Flink access
- Flink SQL over DataStream API for maintainability
- Exactly-once guarantees via Flink checkpointing
- Fuzzy entity resolution with blocking index for efficiency
- Incremental watermark-based processing to avoid reprocessing

## Workflow Rules

### Feature / Integration Design
When a feature or integration design is done, always act as a staff engineer to review the doc. Then take the suggestions to modify the original plan before implementation.

### Multi-Step Implementation
When implementing a multi-step feature or integration, always implement step by step:
1. Create tests for the current step first
2. Implement the step
3. Make sure all tests pass. If something is confusing, ask a staff engineer to review the pros and cons, then decide whether to modify the test or modify the implementation
4. Commit the code only after all tests pass
5. Move to the next step

### Post-Implementation
When the feature/integration implementation is completed:
1. Update integration tests
2. Update documentation
3. Final commit
