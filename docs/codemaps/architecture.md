# Architecture Codemap

> Freshness: 2026-02-08 | Auto-generated

## System Overview

Medallion lakehouse pipeline: 4 webhook sources -> streaming ingestion -> 5-layer batch transforms -> 3 query engines -> dashboards.

## Service Topology (21 containers)

```
Webhooks ─> [ingestion-api :8090] ─> [redpanda :19092] ─> [flink-jm :8083]
                                                             [flink-tm]
                                                               │
                                                        ┌──────▼──────┐
                                                        │ MinIO :9000 │
                                                        │  (Iceberg)  │
                                                        └──────┬──────┘
                                                               │
                                    ┌──────────────────────────┼────────────────────┐
                                    │                          │                    │
                             [spark-master :8084]    [trino :8085]    [clickhouse :8123]
                             [spark-worker]                                    │
                                    │                                   [grafana :3001]
                             [airflow :8086]                            [prometheus :9090]
                             [airflow-scheduler]
                             [airflow-worker]
```

## Data Flow

```
Source        Ingestion      Streaming      Batch (Spark)           Serving
────────      ─────────      ─────────      ─────────────           ───────
Shopify   ─┐                               raw ─> staging ─┐
Stripe    ─┤─> FastAPI ─> Redpanda ─> Flink                ├─> semantic ─> core ─> analytics ─> marts
HubSpot   ─┤   (HMAC)     (8 topics)  (8 jobs)             │   (entity      (unified   (metrics)  (360/
Mailchimp ─┘                                                │    resolution)  objects)              dashboards)
                                                            └─────────────────────────────────────────────┘
```

## Directory Map

```
/
├── airflow/dags/              # Orchestration (iceberg_pipeline.py)
├── datagen/                   # Mock data (generator.py, simulate_webhooks.py)
│   └── providers/             # Faker providers per source (4 files)
├── docs/                      # Architecture docs, design docs, screenshots
├── infrastructure/            # Docker Compose + per-service configs
│   ├── airflow/clickhouse/flink/grafana/minio/postgres/prometheus/redpanda/spark/trino/
│   └── docker-compose.yml     # 18 services, 844 lines
├── ingestion/app/             # FastAPI webhook receiver
│   ├── webhooks/              # Per-source handlers (4 files)
│   ├── validators/            # HMAC signature validation
│   └── producers/             # Redpanda producer
├── jobs/
│   ├── flink/                 # 16 SQL files (streaming raw ingestion)
│   └── spark/                 # 6 batch jobs + 3 maintenance
├── monitoring/
│   ├── dashboards/            # 6 Grafana JSON dashboards
│   └── alerts/                # Prometheus alerting rules
├── schemas/                   # JSON schemas per source (4 files)
├── scripts/                   # reset_and_run.sh, validate_tables.sh, post_mock_data.py
└── sql/                       # DDL reference (00_raw through 05_marts, 27 files)
```

## Orchestration (Airflow DAG)

```
start
  ├── stg_shopify_orders ──────────────────────────┐
  ├── stg_shopify_customers ───────────────────────┤
  ├── stg_stripe_charges ──────────────────────────┤
  ├── stg_stripe_customers ────────────────────────┤
  ├── stg_hubspot_contacts ────────────────────────┤
  ├── stg_mailchimp_campaigns ─> campaign_metrics ─┤
  ├── stg_mailchimp_events ────┘                   ├── entity_index ─> core_customers ─> customer_metrics ─┐
  └── stg_mailchimp_subscribers ───────────────────┘   core_orders ──> order_summary ──────────────────────┤
                                                                       payment_metrics ────────────────────┤
                                                                                                           ├── customer_360 ─┐
                                                                                             campaign_dashboard ──────────────┤
                                                                                             sales_dashboard ─────────────────┤
                                                                                             executive_summary ────────────────┤
                                                                                                                              end
```

## Key Config

| Parameter | Location | Default |
|-----------|----------|---------|
| All credentials & ports | `infrastructure/.env` | 139 params |
| Flink tuning | `infrastructure/flink/flink-conf.yaml` | JM 1024m, 2 slots |
| Spark defaults | `infrastructure/spark/spark-defaults.conf` | Iceberg extensions |
| Airflow schedule | `airflow/dags/iceberg_pipeline.py` | 4-hour interval |
| Prometheus scrape | `infrastructure/prometheus/prometheus.yml` | 15s interval |
