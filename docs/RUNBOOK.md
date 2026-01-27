# Operational Runbook

This runbook provides procedures for operating and troubleshooting the Iceberg Incremental Demo pipeline.

## Table of Contents

- [Service Overview](#service-overview)
- [Common Operations](#common-operations)
- [Troubleshooting Guide](#troubleshooting-guide)
- [Alert Response Procedures](#alert-response-procedures)
- [Maintenance Procedures](#maintenance-procedures)
- [Recovery Procedures](#recovery-procedures)

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
./scripts/full_reset_and_validate.sh
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

## Contact Information

- **Data Engineering Team:** data-eng@company.com
- **Platform Team:** platform@company.com
- **On-Call:** Follow PagerDuty escalation policy

## Related Documentation

- [Architecture Documentation](../ARCHITECTURE.md)
- [Setup Guide](../README.md)
- [Iceberg Documentation](https://iceberg.apache.org/docs/latest/)
