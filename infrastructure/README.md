# Infrastructure: Iceberg + Redpanda + Flink + Spark + Trino + ClickHouse + Airflow + Grafana

Docker Compose configuration for running the complete data platform locally.

For detailed rationale on why each tool was chosen, see [ARCHITECTURE.md](../ARCHITECTURE.md#infrastructure-why-each-tool).

## Services Overview

| Service | Port | Role in Pipeline |
|---------|------|------------------|
| **MinIO** | 9000 (API), 9001 (Console) | Object storage for Iceberg data files |
| **Iceberg REST Catalog** | 8181 | Shared metadata catalog for all engines |
| **PostgreSQL** | 5432 | Catalog backend + Airflow metadata |
| **Redpanda** | 9092 (Kafka), 8081 (Schema Registry), 8080 (Console) | Message queue between ingestion and Flink |
| **Flink** | 8083 (JobManager UI) | Streaming: Kafka → Iceberg raw layer |
| **Spark** | 8084 (Master UI) | Batch: raw → staging → semantic → analytics → marts |
| **Airflow** | 8086 (Webserver) | Orchestration of batch jobs |
| **Trino** | 8085 | Ad-hoc SQL queries on Iceberg |
| **ClickHouse** | 8123 (HTTP) | Fast OLAP for dashboards |
| **Ingestion API** | 8090 | Webhook receiver (Shopify, Stripe, HubSpot) |
| **Grafana** | 3000 | Monitoring dashboards |
| **Prometheus** | 9090 | Metrics collection |

---

## Service Configuration & Troubleshooting

For rationale on why each tool was chosen, see [ARCHITECTURE.md](../ARCHITECTURE.md#infrastructure-why-each-tool).

### MinIO

| Setting | Value |
|---------|-------|
| Credentials | `admin` / `admin123` |
| Bucket | `warehouse` (created on startup) |
| Path style | Required (`s3.path-style-access=true`) |

**Troubleshooting:**
- "Table not found" → verify bucket exists in Console
- Slow queries → check if data files are fragmented (too many small files)

### Iceberg REST Catalog

| Setting | Value |
|---------|-------|
| Backend | PostgreSQL JDBC (not SQLite) |
| Warehouse | `s3a://warehouse/` |

**Troubleshooting:**
- "Table not found" but data exists → catalog out of sync, restart catalog
- Concurrent job failures → check PostgreSQL for lock contention

### PostgreSQL

| Setting | Value |
|---------|-------|
| Iceberg database | `iceberg_catalog` |
| Airflow database | `airflow` |
| Credentials | `airflow` / `airflow123` |

**Troubleshooting:**
- Catalog errors → verify PostgreSQL is healthy (`docker logs`)
- Airflow issues → check connection pool exhaustion

### Redpanda

| Setting | Value |
|---------|-------|
| Topics | `shopify.orders`, `shopify.customers`, `stripe.charges`, `hubspot.contacts` |
| Partitions | 1 per topic (demo scale) |
| Auth | None (demo only) |

**Troubleshooting:**
- Data not in raw layer → check topic lag in Console (:8080)
- Flink job failing → verify topics exist (`rpk topic list`)

### Apache Flink

| Setting | Value |
|---------|-------|
| Task slots | 4 per TaskManager |
| Checkpoints | MinIO (exactly-once) |
| Jobs | `/jobs/flink/*.sql` |

**Troubleshooting:**
- Raw layer not updating → check job status at `:8083`
- Duplicate data → verify checkpointing is working

### Apache Spark

| Setting | Value |
|---------|-------|
| Workers | 1 (4 cores, 4GB RAM) |
| Jobs | `/jobs/spark/*.py` |
| S3 config | Both Hadoop S3A and Iceberg S3 required |

**Troubleshooting:**
- Job failures → check Master UI at `:8084`
- OOM errors → increase executor memory in spark-submit
- Slow jobs → check for data skew in Spark UI

### Apache Airflow

| Setting | Value |
|---------|-------|
| Executor | CeleryExecutor with Redis |
| DAGs | `/airflow/dags/iceberg_pipeline.py` |
| Credentials | `admin` / `admin123` |

**Troubleshooting:**
- Pipeline not running → check scheduler logs
- Task failures → view task logs in UI (:8086)

### Trino

| Setting | Value |
|---------|-------|
| Catalog | `iceberg` (configured in `/trino/catalog/`) |
| Memory | 1GB per query (demo scale) |

**Troubleshooting:**
- Query errors → verify catalog configuration matches Iceberg REST
- Slow queries → tables may need compaction

### ClickHouse

| Setting | Value |
|---------|-------|
| Credentials | `default` / `admin123` |
| Iceberg setup | `/clickhouse/iceberg_setup.sql` |

**Troubleshooting:**
- Dashboard latency → check materialized view freshness
- Data mismatch → verify sync with Iceberg tables

### Ingestion API

| Setting | Value |
|---------|-------|
| Port | 8090 |
| Auth | None (demo only) |
| Health | `/health` |

**Troubleshooting:**
- No data arriving → verify API is receiving requests (`docker logs`)
- Malformed data → check API validation errors in logs

### Grafana + Prometheus

| Setting | Value |
|---------|-------|
| Grafana credentials | `admin` / `admin123` |
| Dashboards | `/monitoring/dashboards/` |
| Scrape targets | Flink, Spark, Airflow |

**Troubleshooting:**
- Missing metrics → verify Prometheus targets are up (:9090/targets)
- Dashboard gaps → check scrape interval and retention

---

## Quick Start

```bash
# 1. Copy environment file
cp .env.example .env

# 2. Start core services (MinIO, Iceberg REST, Redpanda, ClickHouse, Trino)
docker compose up -d minio
docker compose up minio-init
docker compose up -d iceberg-rest redpanda
docker compose up redpanda-init
docker compose up -d clickhouse trino redpanda-console

# 3. Check status
docker compose ps

# 4. (Optional) Build and start Spark/Flink
docker compose build spark-master spark-worker
docker compose build flink-jobmanager flink-taskmanager
docker compose up -d spark-master spark-worker flink-jobmanager flink-taskmanager
```

## Stopping Services

```bash
docker compose down
```

To remove volumes (data):

```bash
docker compose down -v
```

## Service URLs

After starting, access services at:

- **MinIO Console**: http://localhost:9001 (admin/admin123)
- **Redpanda Console**: http://localhost:8080
- **Iceberg REST**: http://localhost:8181/v1/config
- **ClickHouse**: http://localhost:8123 (default/admin123)
- **Trino**: http://localhost:8084

## Testing Connectivity

```bash
# Test Iceberg catalog
curl http://localhost:8181/v1/config

# Test ClickHouse
curl "http://default:admin123@localhost:8123/?query=SELECT%20version()"

# Test Trino
docker exec iceberg-trino trino --execute "SHOW CATALOGS"

# Test Redpanda topics
docker exec iceberg-redpanda rpk topic list
```

## Configuration

All configurable values are in `.env.example`. Key settings:

- `MINIO_ROOT_USER/PASSWORD`: MinIO credentials
- `CLICKHOUSE_PASSWORD`: ClickHouse default user password
- `REDPANDA_DEFAULT_PARTITIONS`: Default Kafka partition count
- `EXTERNAL_*_PORT`: Host-mapped ports (change if conflicts)

## Notes

- **Spark and Flink** require building Docker images before use (they include Iceberg connectors)
- All data is stored in Docker volumes (prefixed with `iceberg-demo-`)
- Services communicate on the `iceberg-demo-network` bridge network
