# Operational Runbook

This runbook provides procedures for operating and troubleshooting the Iceberg Incremental Demo pipeline.

## Table of Contents

- [Service Overview](#service-overview)
- [Common Operations](#common-operations)
- [Troubleshooting Guide](#troubleshooting-guide)
- [Alert Response Procedures](#alert-response-procedures)
- [Maintenance Procedures](#maintenance-procedures)
- [Recovery Procedures](#recovery-procedures)
- [Metrics and Alerting](#metrics-and-alerting)

---

## Service Overview

### Architecture

```
Webhooks → Ingestion API → Redpanda → Flink → Iceberg (Raw)
                                         ↓
         Airflow DAG → Spark Jobs → Iceberg (Staging → Semantic → Analytics → Marts)
                                         ↓
                              Query Engines (Trino, Spark, ClickHouse)
```

### Service Dependencies

| Service | Port | Purpose | Dependencies |
|---------|------|---------|--------------|
| MinIO | 9000/9001 | Object storage | None |
| Iceberg REST | 8181 | Metadata catalog | MinIO, PostgreSQL |
| Redpanda | 9092/8080 | Message queue | None |
| Flink | 8083 | Streaming ETL | MinIO, Iceberg REST, Redpanda |
| Spark | 8084 | Batch ETL | MinIO, Iceberg REST |
| Airflow | 8086 | Orchestration | PostgreSQL |
| Trino | 8085 | Ad-hoc queries | MinIO, Iceberg REST |
| ClickHouse | 8123 | OLAP queries | MinIO |
| Ingestion API | 8090 | Webhook receiver | Redpanda |

### Health Check URLs

| Service | Health Endpoint |
|---------|-----------------|
| MinIO | `curl http://localhost:9000/minio/health/live` |
| Iceberg REST | `curl http://localhost:8181/v1/config` |
| Redpanda | `curl http://localhost:9644/v1/status/ready` |
| Airflow | `curl http://localhost:8086/health` |
| Trino | `curl http://localhost:8085/v1/info` |
| Ingestion API | `curl http://localhost:8090/health` |

---

## Common Operations

### Starting the Platform

```bash
cd infrastructure
docker-compose up -d
```

Wait for all services to be healthy:
```bash
docker-compose ps
```

### Stopping the Platform

```bash
cd infrastructure
docker-compose down
```

To also remove data volumes:
```bash
docker-compose down -v
```

### Viewing Logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f spark-master

# Last 100 lines
docker-compose logs --tail 100 airflow-scheduler
```

### Triggering the Pipeline

```bash
# Manual trigger
docker exec iceberg-airflow-scheduler airflow dags trigger iceberg_pipeline

# Check status
docker exec iceberg-airflow-scheduler airflow dags list-runs -d iceberg_pipeline
```

### Querying Data

**Trino:**
```bash
docker exec -it iceberg-trino trino --execute "SELECT COUNT(*) FROM iceberg.staging.stg_shopify_orders"
```

**Spark SQL:**
```bash
docker exec iceberg-spark-master /opt/spark/bin/spark-sql \
    --conf 'spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog' \
    --conf 'spark.sql.catalog.iceberg.type=rest' \
    --conf 'spark.sql.catalog.iceberg.uri=http://iceberg-rest:8181' \
    -e "SELECT COUNT(*) FROM iceberg.staging.stg_shopify_orders"
```

---

## Troubleshooting Guide

### Pipeline Failure

**Symptoms:** Airflow DAG shows failed status

**Diagnosis:**
```bash
# Check DAG run status
docker exec iceberg-airflow-scheduler airflow dags list-runs -d iceberg_pipeline

# Check task logs
docker exec iceberg-airflow-scheduler airflow tasks logs iceberg_pipeline <task_id> <execution_date>
```

**Common causes:**
1. Iceberg REST catalog unavailable
2. Spark job OOM error
3. MinIO connection timeout

### Slow Pipeline

**Symptoms:** Pipeline duration > 10 minutes

**Diagnosis:**
```bash
# Check Spark job status
docker exec iceberg-spark-master /opt/spark/bin/spark-class org.apache.spark.deploy.client.ApplicationClient status spark://spark-master:7077 <app-id>

# Check table file count (many small files = slow queries)
docker exec iceberg-trino trino --execute "SELECT COUNT(*) FROM iceberg.staging.stg_shopify_orders.files"
```

**Resolution:**
- Run compaction if file count > 100
- Increase Spark executor memory
- Check for data skew in partitions

### Ingestion Stopped

**Symptoms:** No new data in raw layer

**Diagnosis:**
```bash
# Check Redpanda topics
docker exec iceberg-redpanda rpk topic list

# Check consumer lag
docker exec iceberg-redpanda rpk group describe flink-raw-consumer

# Check Flink jobs
curl http://localhost:8083/jobs/overview
```

**Resolution:**
1. Verify Ingestion API is receiving webhooks
2. Check Flink job status and restart if needed
3. Verify Redpanda connectivity

**If the API returns 500 on every webhook**, check its logs before anything
else — a dependency mismatch there stops all four streaming sources at once
while the data poster still prints progress:

```bash
docker logs iceberg-ingestion-api --tail 50 | grep -i "error\|traceback"
```

### GA4 Tables Empty

**Symptoms:** every other source has data; only `raw.ga4_events` is 0

GA4 uses no webhooks, Redpanda or Flink, so none of the checks above apply. It
is a file drop: `datagen/generator.py --source ga4` writes
`datagen/output/ga4/events.parquet`, mounted read-only into Spark at
`/opt/spark/data/ga4/events.parquet`, and `ga4_batch_ingest.py` MERGEs it in.

**Diagnosis:**
```bash
# Does the export exist on the host?
ls -la datagen/output/ga4/

# Can Spark see it through the mount?
docker exec iceberg-spark-master ls -la /opt/spark/data/ga4/

# Can the generator write Parquet at all? (needs pandas + pyarrow)
docker exec iceberg-datagen python -c "import pandas, pyarrow"
```

**Resolution:**
1. No file on the host → generation never ran, or its deps are missing
2. File on the host but not in the container → the volume mount is wrong
3. File visible but `raw.ga4_events` still 0 → check the ingest task log; its
   `--input` must match `GA4_EXPORT_PATH`

### GA4 Row Counts Growing Every Run

**Symptoms:** `stg_ga4_sessions` doubles on each scheduled DAG run
(308 → 616 → 924); `marts.ga4_engagement_dashboard` eventually fails with
`MERGE_CARDINALITY_VIOLATION`

**Cause:** a job reading its whole source table while writing with `append`.
Duplicate rows raise no error, so this compounds silently until the one job
keyed by `MERGE` rejects the ambiguity.

**Diagnosis:**
```bash
docker exec iceberg-trino trino --execute "
SELECT COUNT(*) FROM (
  SELECT client_id, event_timestamp, event_name
  FROM iceberg.staging.stg_ga4_events
  GROUP BY 1,2,3 HAVING COUNT(*) > 1)"
```
Non-zero means duplication has already happened.

**Resolution:**
1. Rebuild the affected tables with `--mode full` — `createOrReplace` clears them
2. Fix the offending job against the idempotency contract in
   [ARCHITECTURE.md](../ARCHITECTURE.md#the-idempotency-contract)
3. Confirm by triggering the DAG twice: counts must be identical

### Staging Data Stale

**Symptoms:** Raw count >> Staging count

**Diagnosis:**
```bash
# Compare counts
docker exec iceberg-trino trino --execute "
SELECT 'raw' as layer, COUNT(*) as cnt FROM iceberg.raw.shopify_orders
UNION ALL
SELECT 'staging', COUNT(*) FROM iceberg.staging.stg_shopify_orders
"
```

**Resolution:**
1. Check Airflow task logs for staging job
2. Manually trigger staging job
3. Check for data quality issues blocking staging

### Entity Coverage Low

**Symptoms:** Entity coverage < 90%

**Diagnosis:**
```bash
# Run entity quality check
docker exec iceberg-spark-master spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark/jobs/maintenance/entity_quality_check.py --quick
```

**Resolution:**
1. Check for new data sources not in entity resolution
2. Verify email normalization is working
3. Run entity backfill job

---

## Alert Response Procedures

### PipelineFailure (Critical)

**Impact:** Data not flowing to downstream layers

**Response:**
1. Check Airflow UI for failed task
2. Review task logs for error
3. Fix underlying issue
4. Manually re-trigger pipeline
5. Verify data integrity

### IcebergCatalogDown (Critical)

**Impact:** All Iceberg operations fail

**Response:**
1. Check container status: `docker ps | grep iceberg-rest`
2. Check logs: `docker logs iceberg-rest`
3. Verify PostgreSQL is healthy: `docker exec iceberg-airflow-postgres pg_isready`
4. Restart if needed: `docker-compose restart iceberg-rest`
5. Verify catalog: `curl http://localhost:8181/v1/config`

### MinIODown (Critical)

**Impact:** Storage unavailable, all operations fail

**Response:**
1. Check container: `docker ps | grep minio`
2. Check logs: `docker logs iceberg-minio`
3. Check disk space: `docker exec iceberg-minio df -h`
4. Restart: `docker-compose restart minio`
5. Verify: `curl http://localhost:9000/minio/health/live`

### KafkaConsumerLagHigh (Warning)

**Impact:** Data processing delayed

**Response:**
1. Check Flink job status
2. If job failed, restart it
3. If running slowly, check for resource constraints
4. Consider adding more Flink taskmanagers

### TableNeedsCompaction (Info)

**Impact:** Queries may be slow

**Response:**
1. Schedule or run compaction job
2. Monitor file count afterward

---

## Maintenance Procedures

### Table Compaction

Run compaction to merge small files:

```bash
docker exec iceberg-spark-master spark-submit \
    --master spark://spark-master:7077 \
    --conf 'spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog' \
    --conf 'spark.sql.catalog.iceberg.type=rest' \
    --conf 'spark.sql.catalog.iceberg.uri=http://iceberg-rest:8181' \
    --conf 'spark.sql.catalog.iceberg.warehouse=s3a://warehouse/' \
    --conf 'spark.sql.catalog.iceberg.io-impl=org.apache.iceberg.aws.s3.S3FileIO' \
    --conf 'spark.sql.catalog.iceberg.s3.endpoint=http://minio:9000' \
    --conf 'spark.sql.catalog.iceberg.s3.access-key-id=admin' \
    --conf 'spark.sql.catalog.iceberg.s3.secret-access-key=admin123' \
    --conf 'spark.sql.catalog.iceberg.s3.path-style-access=true' \
    /opt/spark/jobs/maintenance/compact_tables.py
```

### Snapshot Expiration

Remove old snapshots to free storage:

```bash
docker exec iceberg-spark-master spark-submit \
    --master spark://spark-master:7077 \
    --conf 'spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog' \
    # ... same configs as above ...
    /opt/spark/jobs/maintenance/expire_snapshots.py --retention-days 7
```

### Entity Quality Check

Run quality check and review report:

```bash
docker exec iceberg-spark-master spark-submit \
    --master spark://spark-master:7077 \
    # ... same configs ...
    /opt/spark/jobs/maintenance/entity_quality_check.py \
    --output /tmp/entity_report.json
```

### Backup Procedures

**Backup Iceberg metadata:**
```bash
# Export PostgreSQL catalog
docker exec iceberg-airflow-postgres pg_dump -U airflow iceberg_catalog > iceberg_catalog_backup.sql
```

**Backup Airflow metadata:**
```bash
docker exec iceberg-airflow-postgres pg_dump -U airflow airflow > airflow_backup.sql
```

---

## Recovery Procedures

### Full Platform Reset

Complete reset with data loss:

```bash
./scripts/reset_and_run.sh
```

For detailed validation during reset:

```bash
./scripts/reset_and_run.sh --validate
```

### Restore from Backup

1. Stop services
2. Restore PostgreSQL databases
3. Start services
4. Verify data integrity

### Recover Missing Data

If data is missing from a layer:

1. Identify the gap (compare counts across layers)
2. Run full refresh for affected layer:
```bash
docker exec iceberg-airflow-scheduler airflow dags trigger iceberg_full_refresh
```

### Time Travel Recovery

Recover data from previous snapshot:

```sql
-- View available snapshots
SELECT * FROM iceberg.staging.stg_shopify_orders.snapshots;

-- Query historical data
SELECT * FROM iceberg.staging.stg_shopify_orders FOR VERSION AS OF <snapshot_id>;

-- Rollback to snapshot (if needed)
CALL iceberg.system.rollback_to_snapshot('iceberg.staging.stg_shopify_orders', <snapshot_id>);
```

---

---

## Metrics and Alerting

### Where metrics come from

Two paths, and the split matters:

| Source | Mechanism | Examples |
|--------|-----------|----------|
| Long-lived services | Prometheus **scrapes** them | Redpanda, MinIO, Trino, ClickHouse, Flink, ingestion API |
| Batch Spark jobs | The job **pushes** to Pushgateway | `iceberg_table_*`, `entity_resolution_*`, `maintenance_job_*` |
| Airflow DAG runs | `on_success_callback` / `on_failure_callback` push | `iceberg_pipeline_*` |
| Iceberg REST catalog | blackbox-exporter **probes** it | `probe_success` |

Spark is deliberately not a scrape target: a `spark-submit` driver lives only
for the duration of its task, so a job pointed at a driver UI would be down more
often than up.

The catalog is deliberately probed rather than scraped: it serves no `/metrics`
endpoint, so a scrape job would sit at `up=0` forever and fire
`IcebergCatalogDown` permanently.

### The metric registry

Every metric the pipeline emits is declared in `jobs/spark/metrics/registry.py`.
`tests/test_metrics_registry.py` fails if an alert expression names anything
outside that registry or `EXTERNAL_METRIC_PREFIXES`.

This exists because it already went wrong: 13 of 15 alerts referenced series
nothing produced, so freshness, compaction, entity-coverage, and catalog
liveness monitoring all read as configured while emitting nothing. **Add the
producer — never widen the external prefix list to silence the test.**

All pipeline metrics are gauges. Pushgateway replaces a group's samples on each
push rather than accumulating them, so a pushed counter breaks `increase()`.
Failure tracking uses `*_last_failure_timestamp` compared against
`*_last_success_timestamp`.

### Publishing metrics by hand

```bash
# Print what would be pushed, without pushing
docker exec iceberg-spark-master /opt/spark/bin/spark-submit \
  /opt/spark/jobs/export_metrics.py --dry-run

# Publish for real
docker exec iceberg-spark-master /opt/spark/bin/spark-submit \
  /opt/spark/jobs/export_metrics.py

# Confirm Prometheus picked them up (allow one 15s scrape interval)
curl -s 'localhost:9090/api/v1/query?query=iceberg_table_row_count' | head -c 400
```

### Checking alert health

`health: ok` means the rule evaluates. `state: inactive` means it evaluated and
the condition was false — that is the healthy resting state, not a problem.
`health: unknown` means the rule has not evaluated yet; the `maintenance` group
runs on a 5-minute interval.

```bash
curl -s localhost:9090/api/v1/rules | python3 -c "
import json,sys
for g in json.load(sys.stdin)['data']['groups']:
    for r in g['rules']:
        print(f\"{r['name']:28} {r['health']:8} {r.get('state')}\")
"
```

---

## Alert Reference

### catalog-down

`IcebergCatalogDown` — the blackbox probe against `http://iceberg-rest:8181/v1/config`
failed. Every Spark and Flink job that touches Iceberg will fail while this is true.

```bash
docker ps --filter name=iceberg-rest
docker logs iceberg-rest --tail 50
curl -s localhost:8181/v1/config | head -c 200
docker-compose restart iceberg-rest
```

The catalog depends on `airflow-postgres` (JDBC backend) and MinIO. Check both
before restarting.

### minio-down

`MinIODown` — the `minio` scrape target is down. All storage operations fail.

```bash
docker ps --filter name=iceberg-minio
docker logs iceberg-minio --tail 50
curl -s localhost:9000/minio/health/live -o /dev/null -w "%{http_code}\n"
```

### redpanda-down

`RedpandaDown` — webhook ingestion stops; batch layers keep working from data
already in Iceberg.

```bash
docker exec iceberg-redpanda rpk cluster health
docker logs iceberg-redpanda --tail 50
```

### consumer-lag

`KafkaConsumerLagHigh` — Flink is consuming slower than webhooks arrive, or has
stopped. Redpanda publishes no lag metric, so the alert derives it as
`max_offset - committed_offset`.

```bash
docker exec iceberg-redpanda rpk group list
docker exec iceberg-redpanda rpk group describe <group>
```

Note this alert reports nothing when no consumer group exists — a stopped Flink
eventually has its offsets expire. "Flink stopped entirely" is caught by
`RawDataIngestionStopped`, not here.

### staging-lag

`StagingDataStale` — the staging layer is more than 10% behind raw. Usually a
failed or skipped staging task.

```bash
curl -s 'localhost:9090/api/v1/query?query=iceberg_table_row_count' | head -c 600
docker exec iceberg-airflow-scheduler airflow tasks states-for-dag-run iceberg_pipeline <run_id>
```

Re-run one staging table:

```bash
docker exec iceberg-spark-master /opt/spark/bin/spark-submit \
  /opt/spark/jobs/staging_batch.py --table ga4_events --mode incremental
```

Remember `--table` takes the registry key (`ga4_events`), not the table name
(`stg_ga4_events`).

### stale-pipeline

`PipelineStale` — no successful DAG run in 6 hours. The schedule is every 4
hours, so this means two consecutive misses.

```bash
docker exec iceberg-airflow-scheduler airflow dags list-runs iceberg_pipeline -o plain | head
docker exec iceberg-airflow-scheduler airflow dags list-import-errors
```

If the DAG is queued but never starts, check that the scheduler is running and
that `max_active_runs=1` is not blocking behind an older run.

### entity-coverage

`EntityCoverageLow` — a source's rows in `semantic.entity_index` are resolving
to `entity_id` less than 90% of the time. Usually means a staging source changed
shape and the blocking keys no longer match.

```bash
docker exec iceberg-spark-master /opt/spark/bin/spark-submit \
  /opt/spark/jobs/entity_backfill.py --mode initial --dry-run
```

### duplicate-entities

`DuplicateEntityMappings` — one `(source_system, source_id)` pair resolved to
more than one `entity_id`. Resolution has split a single identity, so joins
against the index fan out and inflate every aggregate above it. Treat as a data
correctness incident, not a warning.

```sql
SELECT source_system, source_id, COUNT(DISTINCT entity_id) AS entities
FROM iceberg.semantic.entity_index
GROUP BY source_system, source_id
HAVING COUNT(DISTINCT entity_id) > 1;
```

### compaction

`TableNeedsCompaction` — a table has more than 100 data files. Streaming
ingestion writes one file per append, so raw tables accumulate quickly.

```bash
docker exec iceberg-spark-master /opt/spark/bin/spark-submit \
  /opt/spark/jobs/maintenance/compact_tables.py --namespace raw
```

If compaction reports "Skipping: Only 0 files" for every table, its Spark
session is missing `spark.sql.catalog.iceberg.s3.endpoint` — metadata reads
fail with an S3 301 and the count comes back zero.

### compaction-failure

`CompactionJobFailed` — `maintenance_job_last_failure_timestamp` is newer than
the matching success timestamp.

```bash
curl -s localhost:9091/metrics | grep maintenance_job
docker exec iceberg-spark-master /opt/spark/bin/spark-submit \
  /opt/spark/jobs/maintenance/compact_tables.py --namespace staging --dry-run
```

A dry run deliberately records no outcome, so it cannot clear this alert.

### storage-full

`MinIOStorageAlmostFull` — less than 15% of usable cluster capacity remains.
Compaction and snapshot expiration are the first levers.

```bash
curl -s localhost:9000/minio/v2/metrics/cluster | grep capacity_usable
docker exec iceberg-spark-master /opt/spark/bin/spark-submit \
  /opt/spark/jobs/maintenance/expire_snapshots.py --retention-days 3 --remove-orphans
```

## Contact Information

- **Data Engineering Team:** data-eng@company.com
- **Platform Team:** platform@company.com
- **On-Call:** Follow PagerDuty escalation policy

## Related Documentation

- [Architecture Documentation](../ARCHITECTURE.md)
- [Setup Guide](../README.md)
- [Iceberg Documentation](https://iceberg.apache.org/docs/latest/)
