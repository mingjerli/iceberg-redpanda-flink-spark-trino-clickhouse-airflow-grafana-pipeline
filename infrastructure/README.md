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

## Service Details

### MinIO (Object Storage)

**What it does:** Stores all Iceberg data files (Parquet) and metadata.

**Why it's needed:** Iceberg separates compute from storage. MinIO provides S3-compatible storage that works identically to AWS S3, enabling local development without cloud costs.

**Key configuration:**
- Path-style access required (`s3.path-style-access=true`)
- Bucket `warehouse` created on startup
- Credentials: `admin` / `admin123`

**When to check it:**
- "Table not found" errors → verify bucket exists
- Slow queries → check if data files are fragmented

---

### Iceberg REST Catalog

**What it does:** Tracks where Iceberg tables live and their current schema/snapshot state.

**Why it's needed:** Multiple engines (Spark, Flink, Trino) need a shared, consistent view of table metadata. Without a catalog, each engine would have its own (potentially stale) view.

**Key configuration:**
- PostgreSQL JDBC backend (not SQLite) for concurrent access
- Warehouse location: `s3a://warehouse/`

**When to check it:**
- "Table not found" but data exists → catalog out of sync
- Concurrent job failures → check PostgreSQL locks

---

### PostgreSQL

**What it does:** Stores Iceberg catalog metadata and Airflow metadata.

**Why it's needed:** SQLite (Iceberg's default) locks under concurrent access. PostgreSQL handles multiple Spark/Flink jobs writing simultaneously.

**Key configuration:**
- Database `iceberg_catalog` for Iceberg
- Database `airflow` for Airflow
- User: `airflow` / `airflow123`

**When to check it:**
- Catalog errors → verify PostgreSQL is healthy
- Airflow issues → check connection pool

---

### Redpanda (Message Queue)

**What it does:** Buffers webhook events between the Ingestion API and Flink.

**Why it's needed:** Webhooks arrive in real-time bursts. Flink processes at a steady rate. Redpanda decouples these speeds and enables replay on failure.

**Key configuration:**
- Topics: `shopify.orders`, `shopify.customers`, `stripe.charges`, `hubspot.contacts`
- Single partition per topic (demo scale)
- No authentication (demo only)

**When to check it:**
- Data not appearing in raw layer → check topic lag in Console
- Flink job failing → verify topics exist

---

### Apache Flink (Streaming)

**What it does:** Reads from Kafka topics and writes to Iceberg raw tables in real-time.

**Why it's needed:** Sub-second latency from webhook to queryable data. Spark Structured Streaming has higher latency; Flink provides true streaming.

**Key configuration:**
- Checkpoints to MinIO for exactly-once delivery
- 4 task slots per TaskManager
- Jobs defined in `/jobs/flink/*.sql`

**When to check it:**
- Raw layer not updating → check job status at `:8083`
- Duplicate data → verify checkpointing is working

---

### Apache Spark (Batch Processing)

**What it does:** Transforms data through staging → semantic → analytics → marts layers.

**Why it's needed:** Complex transformations (entity resolution, aggregations, joins) require distributed processing. Flink SQL is limited for these operations.

**Key configuration:**
- 1 worker with 4 cores, 4GB RAM (demo scale)
- Jobs defined in `/jobs/spark/*.py`
- Requires both Hadoop S3A and Iceberg S3 configs

**When to check it:**
- Job failures → check Master UI at `:8084`
- OOM errors → increase executor memory
- Slow jobs → check for data skew

---

### Apache Airflow (Orchestration)

**What it does:** Schedules and monitors batch job execution with dependency management.

**Why it's needed:** Staging must complete before analytics. Failures need retries. Operators need visibility into what ran and when.

**Key configuration:**
- CeleryExecutor with Redis broker
- DAG in `/airflow/dags/clgraph_pipeline.py`
- Credentials: `admin` / `admin123`

**When to check it:**
- Pipeline not running → check scheduler logs
- Task failures → view task logs in UI

---

### Trino (Query Engine)

**What it does:** Provides interactive SQL access to Iceberg tables for ad-hoc analysis.

**Why it's needed:** Spark SQL requires spinning up executors. Trino gives sub-second query startup for exploration.

**Key configuration:**
- Catalog `iceberg` configured in `/trino/catalog/iceberg.properties`
- Memory: 1GB per query (demo scale)

**When to check it:**
- Query errors → verify catalog configuration
- Slow queries → check if tables need compaction

---

### ClickHouse (OLAP)

**What it does:** Provides millisecond query response for dashboard queries.

**Why it's needed:** Some dashboard queries need faster response than Trino can provide on large datasets. ClickHouse pre-computes and caches.

**Key configuration:**
- Iceberg integration via `/clickhouse/iceberg_setup.sql`
- Credentials: `default` / `admin123`

**When to check it:**
- Dashboard latency → check materialized view freshness
- Data mismatch → verify sync with Iceberg

---

### Ingestion API (FastAPI)

**What it does:** Receives webhooks from Shopify, Stripe, HubSpot and publishes to Kafka topics.

**Why it's needed:** External systems push data via webhooks. The API validates payloads and routes to appropriate topics.

**Key configuration:**
- Port 8090
- No authentication (demo only)
- Health check: `/health`

**When to check it:**
- No data arriving → verify API is receiving requests
- Malformed data → check API logs

---

### Grafana + Prometheus (Monitoring)

**What it does:** Collects and visualizes metrics from all services.

**Why it's needed:** Operators need visibility into pipeline health, job durations, and resource usage.

**Key configuration:**
- Grafana: `admin` / `admin123`
- Pre-built dashboards in `/monitoring/dashboards/`
- Prometheus scrapes Flink, Spark, Airflow metrics

**When to check it:**
- Performance issues → review historical metrics
- Capacity planning → monitor resource usage trends

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
