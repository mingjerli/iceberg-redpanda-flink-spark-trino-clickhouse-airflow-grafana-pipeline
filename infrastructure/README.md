# Infrastructure Setup

This directory contains Docker Compose configuration for the Iceberg Incremental Demo infrastructure.

## Services

| Service | Port | Description |
|---------|------|-------------|
| **MinIO** | 9000 (API), 9001 (Console) | S3-compatible object storage |
| **Iceberg REST Catalog** | 8181 | Apache Iceberg metadata catalog |
| **Redpanda** | 19092 (Kafka), 8081 (Schema Registry), 8080 (Console) | Kafka-compatible message queue |
| **ClickHouse** | 8123 (HTTP), 19000 (Native) | OLAP database |
| **Trino** | 8084 | Ad-hoc SQL query engine |
| **Flink** | 8083 (JobManager UI) | Streaming ETL (requires build) |
| **Spark** | 8084 (Master UI) | Batch ETL (requires build) |

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

- **MinIO Console**: http://localhost:9001 (admin/admin123456)
- **Redpanda Console**: http://localhost:8080
- **Iceberg REST**: http://localhost:8181/v1/config
- **ClickHouse**: http://localhost:8123 (default/clickhouse123)
- **Trino**: http://localhost:8084

## Testing Connectivity

```bash
# Test Iceberg catalog
curl http://localhost:8181/v1/config

# Test ClickHouse
curl "http://default:clickhouse123@localhost:8123/?query=SELECT%20version()"

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
