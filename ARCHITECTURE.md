# Iceberg Incremental Demo - Architecture

This document provides a detailed overview of the demo's architecture, data flow, and key design decisions.

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              EXTERNAL DATA SOURCES                               │
│                     Shopify    │    Stripe    │    HubSpot                       │
│                   (Webhooks)   │  (Webhooks)  │  (Webhooks)                      │
└───────────────────────┬────────────────┬────────────────┬────────────────────────┘
                        │                │                │
                        ▼                ▼                ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         INGESTION LAYER (FastAPI)                                │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │  Ingestion API (Port 8090)                                              │    │
│  │  - Receives webhooks from Shopify, Stripe, HubSpot                      │    │
│  │  - Validates webhook signatures                                          │    │
│  │  - Publishes events to Kafka topics                                      │    │
│  └───────────────────────────────────┬─────────────────────────────────────┘    │
└──────────────────────────────────────┼──────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         MESSAGE QUEUE (Redpanda)                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │  Kafka-Compatible Topics:                                                │    │
│  │  • shopify.orders       • shopify.customers                             │    │
│  │  • stripe.charges       • stripe.customers                              │    │
│  │  • hubspot.contacts     • hubspot.companies                             │    │
│  └───────────────────────────────────┬─────────────────────────────────────┘    │
└──────────────────────────────────────┼──────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                      STREAMING LAYER (Apache Flink)                              │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │  Flink SQL Jobs:                                                         │    │
│  │  • shopify_orders_full.sql    → raw.shopify_orders                      │    │
│  │  • shopify_customers_full.sql → raw.shopify_customers                   │    │
│  │  • stripe_charges_full.sql    → raw.stripe_charges                      │    │
│  │  • hubspot_contacts_full.sql  → raw.hubspot_contacts                    │    │
│  └───────────────────────────────────┬─────────────────────────────────────┘    │
└──────────────────────────────────────┼──────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         STORAGE LAYER (Apache Iceberg)                           │
│                                                                                  │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────────────────┐   │
│  │  MinIO (S3)      │  │  Iceberg REST    │  │  PostgreSQL Catalog          │   │
│  │  - Data files    │  │  - Catalog API   │  │  - Table metadata            │   │
│  │  - warehouse/    │  │  - Port 8181     │  │  - Namespace definitions     │   │
│  │  - Parquet files │  │                  │  │  - Snapshot history          │   │
│  └──────────────────┘  └──────────────────┘  └──────────────────────────────┘   │
│                                                                                  │
│  Data Layers (Iceberg Tables):                                                   │
│  ┌────────┐  ┌──────────┐  ┌──────────┐  ┌───────────┐  ┌────────┐             │
│  │  RAW   │→ │ STAGING  │→ │ SEMANTIC │→ │ ANALYTICS │→ │ MARTS  │             │
│  └────────┘  └──────────┘  └──────────┘  └───────────┘  └────────┘             │
└─────────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                      BATCH PROCESSING (Apache Spark)                             │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │  Spark Jobs:                                                             │    │
│  │  • staging_batch.py       - Raw → Staging transformations               │    │
│  │  • entity_backfill.py     - Entity resolution across sources            │    │
│  │  • analytics_incremental.py - Staging/Core → Analytics metrics          │    │
│  │  • marts_incremental.py   - Analytics → Business marts                  │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                      ORCHESTRATION (Apache Airflow)                              │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │  DAG: clgraph_iceberg_pipeline                                          │    │
│  │  • Staging tasks (4 tables)                                             │    │
│  │  • Semantic tasks (entity resolution)                                   │    │
│  │  • Analytics tasks (3 metrics)                                          │    │
│  │  • Marts tasks (dashboards)                                             │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          QUERY ENGINES                                           │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────────────────┐  │
│  │  Trino          │  │  Spark SQL      │  │  ClickHouse                     │  │
│  │  - Ad-hoc       │  │  - Batch        │  │  - OLAP                         │  │
│  │  - Port 8085    │  │  - Port 8084    │  │  - Real-time analytics          │  │
│  └─────────────────┘  └─────────────────┘  └─────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Data Flow

### Phase 1: Webhook Ingestion

```
Shopify/Stripe/HubSpot → Ingestion API → Redpanda Topics
```

1. External systems send webhook events to the Ingestion API
2. The API validates the request and extracts the payload
3. Events are published to source-specific Kafka topics

### Phase 2: Raw Layer (Streaming)

```
Redpanda Topics → Flink SQL → Iceberg Raw Tables
```

1. Flink SQL jobs continuously read from Kafka topics
2. Events are written as-is to Iceberg raw tables (append-only)
3. Raw tables preserve the original webhook format with metadata

**Raw Tables:**
- `raw.shopify_orders` - Order events with line items, addresses
- `raw.shopify_customers` - Customer creation/update events
- `raw.stripe_charges` - Payment charge events
- `raw.hubspot_contacts` - Contact events with properties

### Phase 3: Staging Layer (Batch)

```
Iceberg Raw → Spark (staging_batch.py) → Iceberg Staging
```

1. Spark reads from raw tables using watermark-based incremental processing
2. Data is cleaned, typed, and standardized
3. Nested structures are flattened where appropriate
4. Invalid/null records are filtered

**Staging Tables:**
- `staging.stg_shopify_orders` - Cleaned orders with derived fields
- `staging.stg_shopify_customers` - Normalized customer records
- `staging.stg_stripe_charges` - Standardized payment data
- `staging.stg_hubspot_contacts` - Flattened contact properties

### Phase 4: Semantic Layer (Batch)

```
Staging Tables → Spark (entity_backfill.py) → Semantic Tables
```

1. Entity resolution identifies the same customer across sources
2. Exact matching on email and phone
3. Blocking index created for efficient matching
4. Unified customer identity assigned

**Semantic Tables:**
- `semantic.entity_index` - Cross-source customer mappings
- `semantic.blocking_index` - Efficient lookup structures
- `semantic.entity_resolution_stats` - Resolution metrics

### Phase 5: Analytics & Marts (Batch)

```
Staging/Semantic → Spark → Analytics → Marts
```

1. Analytics aggregates metrics from staging and semantic layers
2. Marts create business-ready denormalized views

**Analytics Tables:**
- `analytics.customer_metrics` - Customer-level aggregations
- `analytics.order_summary` - Order metrics by period
- `analytics.payment_metrics` - Payment/revenue analytics

**Marts Tables:**
- `marts.customer_360` - Unified customer view
- `marts.sales_dashboard_daily` - Sales reporting data

## Component Details

### Iceberg REST Catalog

The catalog uses **PostgreSQL JDBC backend** instead of SQLite for:
- Concurrent access from multiple Spark/Flink jobs
- No lock contention during parallel writes
- Reliable metadata storage

**Configuration:**
```yaml
CATALOG_CATALOG__IMPL: org.apache.iceberg.jdbc.JdbcCatalog
CATALOG_URI: jdbc:postgresql://airflow-postgres:5432/iceberg_catalog
CATALOG_WAREHOUSE: s3a://warehouse/
```

### S3 Path Style Access

MinIO requires path-style access (not virtual-hosted style). All components must be configured with:

```
s3.path-style-access=true
s3.endpoint=http://minio:9000
```

For Spark jobs, both Hadoop and Iceberg S3 configs are needed:
```bash
# Hadoop S3A filesystem
--conf 'spark.hadoop.fs.s3a.endpoint=http://minio:9000'
--conf 'spark.hadoop.fs.s3a.path.style.access=true'

# Iceberg S3FileIO
--conf 'spark.sql.catalog.iceberg.s3.endpoint=http://minio:9000'
--conf 'spark.sql.catalog.iceberg.s3.path-style-access=true'
```

### Airflow DAG Structure

```
                           ┌─────────────┐
                           │    start    │
                           └──────┬──────┘
                                  │
         ┌────────────────────────┼────────────────────────┐
         ▼                        ▼                        ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ stg_shopify_    │    │ stg_shopify_    │    │ stg_stripe_     │
│ orders          │    │ customers       │    │ charges         │
└────────┬────────┘    └────────┬────────┘    └────────┬────────┘
         │                      │                      │
         │             ┌────────┴────────┐             │
         │             ▼                 ▼             │
         │    ┌─────────────────┐ ┌─────────────────┐  │
         │    │ stg_hubspot_    │ │ entity_index    │  │
         │    │ contacts        │ │                 │  │
         │    └────────┬────────┘ └────────┬────────┘  │
         │             │                   │           │
         │             │          ┌────────┴────────┐  │
         │             │          ▼                 │  │
         │             │ ┌─────────────────┐        │  │
         │             │ │ blocking_index  │        │  │
         │             │ └────────┬────────┘        │  │
         │             │          │                 │  │
         └─────────────┼──────────┼─────────────────┘  │
                       │          │                    │
                       ▼          ▼                    ▼
              ┌─────────────────────────────────────────────┐
              │              Analytics Layer                 │
              │  customer_metrics │ order_summary │ payment_ │
              └────────────────────────┬────────────────────┘
                                       │
                                       ▼
              ┌─────────────────────────────────────────────┐
              │                Marts Layer                   │
              │     customer_360  │  sales_dashboard         │
              └────────────────────────┬────────────────────┘
                                       │
                                       ▼
                               ┌─────────────┐
                               │     end     │
                               └─────────────┘
```

## Data Schemas

### Raw Layer Schema (Example: shopify_orders)

```sql
CREATE TABLE raw.shopify_orders (
    id STRING,
    admin_graphql_api_id STRING,
    email STRING,
    order_number BIGINT,
    total_price STRING,
    subtotal_price STRING,
    total_tax STRING,
    currency STRING,
    financial_status STRING,
    fulfillment_status STRING,
    customer STRING,           -- JSON nested object
    line_items STRING,         -- JSON array
    shipping_address STRING,   -- JSON object
    billing_address STRING,    -- JSON object
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    _webhook_topic STRING,
    _loaded_at TIMESTAMP
)
```

### Staging Layer Schema (Example: stg_shopify_orders)

```sql
CREATE TABLE staging.stg_shopify_orders (
    order_id STRING,
    order_number BIGINT,
    customer_id STRING,
    email STRING,
    total_amount DECIMAL(18,2),
    subtotal_amount DECIMAL(18,2),
    tax_amount DECIMAL(18,2),
    discount_amount DECIMAL(18,2),
    shipping_amount DECIMAL(18,2),
    currency STRING,
    line_item_count INT,
    financial_status STRING,
    fulfillment_status STRING,
    is_paid BOOLEAN,
    is_fulfilled BOOLEAN,
    shipping_city STRING,
    shipping_state STRING,
    shipping_country STRING,
    billing_city STRING,
    billing_state STRING,
    billing_country STRING,
    order_date DATE,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    _raw_id STRING,
    _staged_at TIMESTAMP
)
```

### Semantic Layer Schema (Entity Index)

```sql
CREATE TABLE semantic.entity_index (
    entity_id STRING,          -- Unified customer ID
    source STRING,             -- 'shopify', 'stripe', 'hubspot'
    source_id STRING,          -- Original ID in source system
    email STRING,
    email_normalized STRING,
    phone STRING,
    phone_normalized STRING,
    first_name STRING,
    last_name STRING,
    full_name STRING,
    match_confidence DECIMAL(5,2),
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
```

## Incremental Processing

### Watermark-Based Incremental Loading

Each staging table maintains a watermark in `metadata.incremental_watermarks`:

```sql
CREATE TABLE metadata.incremental_watermarks (
    source_table STRING,
    pipeline_name STRING,
    last_sync_timestamp TIMESTAMP,
    records_processed BIGINT,
    updated_at TIMESTAMP
)
```

**Processing Logic:**
1. Read last watermark for the table
2. Filter source data: `WHERE _loaded_at > last_watermark`
3. Process only new records
4. Update watermark after successful write

### Benefits of Incremental Loading

- **Cost Efficiency**: Only process new data, not full table scans
- **Lower Latency**: Faster job completion for smaller data volumes
- **Resource Optimization**: Reduced Spark executor memory/time
- **Fault Tolerance**: Resume from last successful point

## Security Considerations

### Credentials Management

| Component | Credential Type | Storage |
|-----------|-----------------|---------|
| MinIO | Access Key/Secret | Environment variables |
| PostgreSQL | Username/Password | Environment variables |
| Airflow | Admin password | Environment variables |
| Webhook secrets | HMAC keys | Not implemented (demo) |

### Network Isolation

All services run on a Docker bridge network (`iceberg-demo-network`). External access is only through exposed ports.

## Scaling Considerations

### Current Demo Limits

- Single Spark worker (2 cores, 4GB RAM)
- Single Flink TaskManager (2 slots)
- Single MinIO instance (no replication)

### Production Recommendations

1. **Spark**: Deploy on Kubernetes with dynamic allocation
2. **Flink**: Run on YARN/K8s with checkpointing to S3
3. **MinIO**: Use distributed mode with erasure coding
4. **PostgreSQL**: Use managed service (RDS, Cloud SQL)
5. **Airflow**: Deploy with CeleryExecutor or KubernetesExecutor

## Monitoring

### Available UIs

| Service | URL | Purpose |
|---------|-----|---------|
| Airflow | :8086 | DAG monitoring, task logs |
| Spark Master | :8084 | Job status, executor metrics |
| Flink Dashboard | :8083 | Streaming job metrics |
| MinIO Console | :9001 | Storage usage, bucket browser |
| Redpanda Console | :8080 | Topic lag, consumer groups |

### Key Metrics to Monitor

- **Flink**: Checkpoint duration, backpressure, records lag
- **Spark**: Job duration, shuffle read/write, GC time
- **Airflow**: DAG success rate, task duration, queue depth
- **Iceberg**: Snapshot count, file count, data size

## Design Decisions

### Why Iceberg?

1. **Time Travel**: Query historical data states
2. **Schema Evolution**: Add/modify columns without rewrites
3. **ACID Transactions**: Consistent reads during writes
4. **Engine Agnostic**: Same tables for Spark, Flink, Trino

### Why PostgreSQL for Catalog?

SQLite (default) causes lock contention with concurrent jobs. PostgreSQL supports:
- Multiple simultaneous readers/writers
- Transaction isolation
- Better connection pooling

### Why Flink for Raw Layer?

1. **True Streaming**: Sub-second latency from Kafka
2. **Exactly-Once**: Checkpoint-based delivery guarantees
3. **SQL Interface**: Easy to maintain table definitions

### Why Spark for Batch Layers?

1. **Mature Ecosystem**: Well-tested Iceberg integration
2. **PySpark**: Python-friendly data transformations
3. **Resource Efficiency**: Process large batches efficiently
