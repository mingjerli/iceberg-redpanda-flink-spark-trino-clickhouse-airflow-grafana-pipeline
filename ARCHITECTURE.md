# Architecture: Iceberg + Redpanda + Flink + Spark + Trino + ClickHouse + Airflow + Grafana

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

---

## Infrastructure: Why Each Tool

### Storage: MinIO (S3-Compatible Object Storage)

**What does it do:** Stores all data files (Parquet) with S3-compatible API. Enables the "lakehouse" pattern—warehouse-like ACID transactions on cheap object storage.

| Cloud | Equivalent |
|-------|------------|
| AWS | S3 |
| GCP | Cloud Storage |
| Azure | Blob Storage / ADLS Gen2 |

### Table Format: Apache Iceberg

**What does it do:** Adds database-like capabilities to files on object storage—ACID transactions, time travel, schema evolution. Every write creates an immutable snapshot (like git for data), so readers see consistent data even during writes. Works with any engine (Spark, Flink, Trino).

| Cloud | Equivalent |
|-------|------------|
| AWS | Iceberg on S3 (native support), or fully managed: Athena/EMR with Iceberg |
| GCP | Iceberg on GCS, or fully managed: BigQuery / BigLake |
| Azure | Iceberg on ADLS, or fully managed: Fabric / Databricks Unity |

### Catalog: Iceberg REST Catalog with PostgreSQL Backend

**What does it do:** Tracks where tables live and their current state. Multiple engines (Spark, Flink, Trino) share this single source of truth via REST API. PostgreSQL backend handles concurrent access without locking (SQLite, the default, doesn't).

| Cloud | Equivalent |
|-------|------------|
| AWS | AWS Glue Data Catalog |
| GCP | BigLake Metastore / Dataplex |
| Azure | Unity Catalog (Databricks) / Purview |

### Message Queue: Redpanda (Kafka-Compatible)

**What does it do:** Buffers webhook events between ingestion and processing. Decouples arrival speed from processing speed. Enables replay—if a consumer fails, it re-reads from where it left off. Kafka-compatible but simpler to operate (single binary, no ZooKeeper).

| Cloud | Equivalent |
|-------|------------|
| AWS | Amazon MSK (Kafka) / Kinesis Data Streams |
| GCP | Pub/Sub / Confluent Cloud |
| Azure | Event Hubs / HDInsight Kafka |

### Streaming: Apache Flink

**What does it do:** Moves data from Kafka to Iceberg with sub-second latency. True event-at-a-time streaming (not micro-batches). Checkpoint-based recovery provides exactly-once delivery guarantees. Flink SQL makes pipeline definitions readable.

| Cloud | Equivalent |
|-------|------------|
| AWS | Amazon Managed Service for Apache Flink |
| GCP | Dataflow (Beam-based, similar purpose) |
| Azure | Stream Analytics / HDInsight Flink |

### Batch Processing: Apache Spark

**What does it do:** Handles complex transformations that streaming can't—joins across sources, aggregations, entity resolution. Distributes work across multiple executors for large datasets. PySpark makes it accessible to Python-oriented data teams.

| Cloud | Equivalent |
|-------|------------|
| AWS | EMR / Glue |
| GCP | Dataproc |
| Azure | Synapse Spark / HDInsight / Databricks |

### Query Engine: Trino

**What does it do:** Provides interactive SQL access for exploration and ad-hoc analysis. Sub-second query startup (unlike Spark which needs executor spin-up). Can federate queries across Iceberg, PostgreSQL, and other sources in a single query.

| Cloud | Equivalent |
|-------|------------|
| AWS | Athena |
| GCP | BigQuery (different model but similar use case) |
| Azure | Synapse Serverless SQL |

### OLAP: ClickHouse

**What does it do:** Delivers millisecond query response for dashboard queries that even Trino can't achieve on large datasets. Columnar storage with extreme compression. Materialized views pre-compute aggregations automatically.

| Cloud | Equivalent |
|-------|------------|
| AWS | Redshift / ClickHouse Cloud |
| GCP | BigQuery |
| Azure | Synapse Dedicated SQL / ClickHouse Cloud |

### Orchestration: Apache Airflow

**What does it do:** Schedules batch jobs with dependency management—staging must complete before analytics. Tracks execution history, provides logs and retry logic. DAG-based UI shows what ran, when, and whether it succeeded.

| Cloud | Equivalent |
|-------|------------|
| AWS | MWAA (Managed Workflows for Apache Airflow) |
| GCP | Cloud Composer |
| Azure | Data Factory / MWAA on Azure |

### Observability: Prometheus + Grafana

**What does it do:** Prometheus scrapes metrics from Flink, Spark, Airflow, and MinIO. Grafana visualizes pipeline health, job durations, checkpoint status, and resource usage.

| Cloud | Equivalent |
|-------|------------|
| AWS | CloudWatch / Amazon Managed Grafana |
| GCP | Cloud Monitoring |
| Azure | Azure Monitor |

### Dashboards: Grafana (lightweight) or Apache Superset

**What does it do:** Connects to ClickHouse and Trino to query marts tables for business dashboards. We use Grafana here as a shortcut since it's already running for observability. For a dedicated BI tool, Apache Superset offers richer visualization options, SQL Lab for exploration, and role-based access control.

| Cloud | Equivalent |
|-------|------------|
| AWS | QuickSight / Managed Grafana |
| GCP | Looker / Looker Studio |
| Azure | Power BI |

---

## Data Layers: Why Each Layer Exists

Each layer solves a specific problem. Skipping layers creates technical debt.

### Raw Layer

**Purpose:** Preserve the original data exactly as received—your "source of truth tape recording."

**Why it exists:**
- **Debugging**: When downstream data looks wrong, trace back to what actually arrived
- **Reprocessing**: If transformation logic changes, replay from raw
- **Compliance**: Some regulations require preserving original records
- **Schema flexibility**: Raw accepts any shape; validation happens later

**What belongs here:**
- Append-only webhook payloads
- Full JSON objects, nested structures intact
- Metadata: `_loaded_at`, `_source_topic`, `_webhook_id`

**What doesn't belong:**
- Cleaned or transformed data
- Aggregations
- Business logic

Never modify raw. If you need to fix data, fix it in staging.

### Staging Layer

**Purpose:** Clean, type, and standardize data for downstream consumption—the "contract layer" that downstream code depends on.

**Why it exists:**
- **Type safety**: Convert strings to decimals, timestamps to proper types
- **Flattening**: Extract nested JSON into columns
- **Null handling**: Apply default values or filter invalid records
- **Standardization**: Normalize field names (`customer_id` not `cust_id` or `CustomerID`)
- **Deduplication**: Handle duplicate webhook deliveries

**What belongs here:**
- One staging table per source table (1:1 mapping)
- Typed columns with consistent naming
- Derived fields (e.g., `is_paid` from `financial_status`)
- Audit columns: `_raw_id`, `_staged_at`

**What doesn't belong:**
- Joins across sources (that's semantic/core layer)
- Business metrics or aggregations
- Denormalized views

### Semantic Layer (Entity Resolution)

**Purpose:** Create unified identities across multiple source systems.

**Why it exists:**
- **Unified customer profile**: The same person exists in Shopify (by email), Stripe (by customer ID), and HubSpot (by contact ID)
- **Deduplication**: Multiple records in the same system may represent the same entity
- **Attribution**: Connect orders to the right customer across systems

**What belongs here:**
- `entity_index`: Maps source IDs to unified entity IDs
- `blocking_index`: Enables efficient matching (see below)
- Match confidence scores
- Fuzzy matching results

**What doesn't belong:**
- Business metrics
- Transactional data
- Anything not about identity resolution

Entity resolution is hard—email matching catches 70-80%, phone adds 10%, fuzzy name matching adds 5-10% but introduces false positives. The semantic layer isolates this complexity from other layers.

#### What is a Blocking Index?

Entity resolution requires comparing records to find matches. The naive approach compares every record to every other record:

```
100,000 records × 100,000 records = 10 billion comparisons
```

Too slow. **Blocking** groups records by shared attributes and only compares within groups.

**Example: Blocking by email domain**

```
Without blocking:              With blocking:

A ↔ B ↔ C ↔ D ↔ E             Block: gmail.com
(10 comparisons)                 A ↔ B (both @gmail.com)

                               Block: yahoo.com
                                 C ↔ D (both @yahoo.com)

                               Block: company.com
                                 E (only one, skip)

                               (2 comparisons)
```

**Common blocking keys:**

| Blocking Key | Groups By | Trade-off |
|--------------|-----------|-----------|
| Email domain | `@gmail.com`, `@company.com` | Misses matches across different email providers |
| Phone area code | `(415)`, `(212)` | Misses matches with multiple phone numbers |
| First 3 chars of last name | `SMI`, `JOH` | Misses typos (`JON` vs `JOH`) |
| Normalized email (exact) | Full email | Only catches exact duplicates |

**The `blocking_index` table structure:**

```sql
CREATE TABLE semantic.blocking_index (
    blocking_key STRING,    -- e.g., 'gmail.com' or '415'
    blocking_type STRING,   -- 'email_domain', 'phone_area', 'name_prefix'
    source STRING,          -- 'shopify', 'stripe', 'hubspot'
    source_id STRING,       -- Original ID in source system
    created_at TIMESTAMP
)
```

**How it's used:**

1. Extract blocking keys from each record (email domain, phone prefix, etc.)
2. Insert into `blocking_index`
3. For entity resolution, query records with matching `blocking_key`
4. Only compare records within the same block
5. Assign unified `entity_id` to matches

**Multiple blocking passes:** Production systems often run multiple passes—first block by email domain, then by phone area code, then by name prefix—to balance speed and recall. Records missed by one blocking strategy may be caught by another.

### Core Layer

**Purpose:** Create unified, entity-resolved views of business objects. Core answers "what is a customer?" and "what is an order?"

**Why it exists:**
- **Single source of truth**: One `customers` table, not three
- **Consistent joins**: All downstream analytics use the same customer definition
- **Entity-aware**: Transactions linked to unified entity IDs

**What belongs here:**
- `core.customers`: Unified customer view with best-available attributes from all sources
- `core.orders`: Orders linked to unified customer entities
- Cross-source aggregations at the entity level

**What doesn't belong:**
- Source-specific details (keep those in staging)
- Time-series aggregations (that's analytics)
- Dashboard-specific denormalization (that's marts)

### Analytics Layer

**Purpose:** Pre-compute metrics and aggregations. Analytics answers "how many?" and "what's the trend?"

**Why it exists:**
- **Query performance**: Aggregating millions of orders on every dashboard load is slow
- **Consistency**: Everyone uses the same metric definitions
- **Historical tracking**: Store daily/weekly/monthly snapshots

**What belongs here:**
- `order_summary`: Daily order counts, revenue by region/channel
- `customer_metrics`: Lifetime value, order frequency, recency
- `payment_metrics`: Revenue by payment method, refund rates

**What doesn't belong:**
- Raw transactional data
- Entity resolution logic
- Dashboard-specific formatting

### Marts Layer

**Purpose:** Denormalized, query-optimized views for specific use cases—the delivery layer optimized for consumption, not transformation.

**Why it exists:**
- **BI tool performance**: Dashboards need fast, simple queries
- **Use-case specific**: Sales team needs different views than marketing
- **Pre-joined**: No complex joins at query time

**What belongs here:**
- `customer_360`(marketing term): All customer attributes from all sources in one wide table
- `sales_dashboard_daily`: Pre-formatted for the sales dashboard
- Role-specific views (marketing, finance, operations)

**What doesn't belong:**
- Granular transactional data
- Complex business logic (that should be in analytics)
- Data that multiple marts need (put it in analytics instead)

If you're writing complex SQL in a mart, the logic probably belongs in analytics.

---

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
