# CLAUDE.md

## Project Overview

End-to-end data pipeline integrating Shopify, Stripe, and HubSpot webhooks through a unified Apache Iceberg lakehouse. Combines real-time streaming (Flink) with batch processing (Spark), entity resolution, and monitoring (Grafana/Prometheus). Runs entirely in Docker (13+ services).

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
scripts/                 # Automation scripts (reset_and_run.sh, validate_tables.sh)
schemas/                 # API JSON schemas
docs/                    # Architecture docs, diagrams, runbook
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
  → Spark staging_batch.py       → staging layer
  → Spark entity_backfill.py     → semantic layer (entity resolution)
  → Spark core_views.py          → core layer (unified objects)
  → Spark analytics_incremental.py → analytics layer
  → Spark marts_incremental.py   → marts layer → Grafana dashboards
```

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

### Airflow DAGs
- `default_args` with `owner: "data-engineering"`, retries: 2, retry_delay: 5 min
- Spark submit commands built from environment variables
- Clear task dependency chains with `>>` operator

## Configuration

All configuration lives in `infrastructure/.env` (137 parameters). Template at `infrastructure/.env.example`. Covers ports, credentials, resource limits, scale factors.

## Testing and Validation

- `scripts/validate_tables.sh` — row count validation across all layers
- Docker health checks on all services
- Faker-based mock data in `datagen/` for realistic test data
- `--dry-run` mode on entity backfill
- No formal unit test framework yet; validation is end-to-end

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
