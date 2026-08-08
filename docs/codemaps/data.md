# Data Models Codemap

> Freshness: 2026-02-08 | Auto-generated

## Table Inventory (27 tables across 6 layers)

### Layer 0: Raw (8 tables) — Flink writes, append-only

| Table | Source | Partition | Key Columns |
|-------|--------|-----------|-------------|
| `raw.shopify_orders` | shopify.orders topic | months(created_at) | order_id |
| `raw.shopify_customers` | shopify.customers topic | months(created_at) | customer_id |
| `raw.stripe_charges` | stripe.charges topic | months(created) | charge_id |
| `raw.stripe_customers` | stripe.customers topic | months(created) | customer_id |
| `raw.hubspot_contacts` | hubspot.contacts topic | months(createdate) | contact_id |
| `raw.mailchimp_campaigns` | mailchimp.campaigns topic | months(send_time) | campaign_id |
| `raw.mailchimp_events` | mailchimp.events topic | months(event_timestamp) | event_id |
| `raw.mailchimp_subscribers` | mailchimp.subscribers topic | months(timestamp_signup) | subscriber_id |
| `raw.ga4_events` | Parquet export (no topic) | event_date | _raw_id (sha256 of client_id\|event_timestamp\|event_name) |

**Common metadata:** `_raw_id STRING`, `_webhook_topic STRING`, `_loaded_at TIMESTAMP`

### Layer 1: Staging (8 tables) — Spark staging_batch.py

| Table | Rows (typical) | Key Transforms |
|-------|----------------|----------------|
| `staging.stg_shopify_orders` | ~8K | Type casting, line_item flattening, discount/tax extraction |
| `staging.stg_shopify_customers` | ~7K | customer_tier derivation, address normalization |
| `staging.stg_stripe_charges` | ~8K | Amount cents->dollars, risk scoring, card metadata |
| `staging.stg_stripe_customers` | ~8K | Balance normalization, delinquency flags |
| `staging.stg_ga4_events` | ~2K | JSON extraction, dedup on (client_id, event_timestamp, event_name) |
| `staging.stg_ga4_sessions` | ~300 | Sessionization by 30-min inactivity gap, first/last attribution |
| `staging.stg_hubspot_contacts` | ~8K | Lifecycle stage normalization, engagement scoring |
| `staging.stg_mailchimp_campaigns` | ~8K | Campaign type, rate calculations |
| `staging.stg_mailchimp_events` | ~9K | Action normalization, engagement flags |
| `staging.stg_mailchimp_subscribers` | ~7K | Email normalization, dedup by subscriber_id (window) |

**Common metadata:** `_raw_id STRING`, `_loaded_at TIMESTAMP`, `_staged_at TIMESTAMP`

### Layer 2: Semantic (2-3 tables) — Spark entity_backfill.py

| Table | Purpose | Partition |
|-------|---------|-----------|
| `semantic.entity_index` | Unified customer IDs across sources | entity_type |
| `semantic.blocking_index` | Fuzzy matching keys for entity resolution | (blocking_key_type, entity_type) |
| `semantic.entity_resolution_stats` | Quality metrics (optional) | months(started_at) |

**Entity sources:** shopify_customers, stripe_customers, hubspot_contacts, mailchimp_subscribers
**Match types:** exact_email, exact_phone, fuzzy

### Layer 3: Core (2 tables) — Spark core_views.py

| Table | Key Columns | Join Logic |
|-------|-------------|------------|
| `core.customers` | customer_id (=unified_id), email, full_name, source_count, has_shopify, has_stripe, has_hubspot | entity_index LEFT JOIN all staging sources |
| `core.orders` | order_id, customer_id (unified), total_price, financial_status | stg_shopify_orders JOIN entity_index for customer_id mapping |

### Layer 4: Analytics (4 tables) — Spark analytics_incremental.py

| Table | Grain | Key Metrics |
|-------|-------|-------------|
| `analytics.customer_metrics` | Per customer_id | total_orders, total_revenue, avg_order_value, days_since_last_order, lifetime_value |
| `analytics.order_summary` | Per order_date | order_count, total_revenue, avg_order_value, unique_customers |
| `analytics.payment_metrics` | Per payment_date | charge_count, total_charged, total_refunded, success_rate, avg_charge |
| `analytics.campaign_metrics` | Per campaign_id | emails_sent, open_rate, click_rate, bounce_rate, engagement_score, performance_tier |

### Layer 5: Marts (3-4 tables) — Spark marts_incremental.py

| Table | Consumers | Key Columns |
|-------|-----------|-------------|
| `marts.customer_360` | Grafana, Trino | All customer + metrics + Mailchimp engagement (has_mailchimp, email_open_rate, etc.) |
| `marts.sales_dashboard_daily` | Grafana | date_key, gross_revenue, net_revenue, total_orders, avg_order_value |
| `marts.campaign_dashboard` | Grafana | campaign_id, send_month, open_rate, click_rate, engagement_score |
| `marts.executive_summary` | Grafana | Period-based business KPIs |

### Metadata (1 table)

| Table | Purpose |
|-------|---------|
| `metadata.incremental_watermarks` | Tracks last_sync_timestamp per source_table/pipeline_name |

## Redpanda Topics (8)

| Topic | Partitions | Producer | Consumer |
|-------|------------|----------|----------|
| `shopify.orders` | 3 | ingestion-api | Flink |
| `shopify.customers` | 3 | ingestion-api | Flink |
| `stripe.charges` | 3 | ingestion-api | Flink |
| `stripe.customers` | 3 | ingestion-api | Flink |
| `hubspot.contacts` | 3 | ingestion-api | Flink |
| `mailchimp.campaigns` | 3 | ingestion-api | Flink |
| `mailchimp.events` | 3 | ingestion-api | Flink |
| `mailchimp.subscribers` | 3 | ingestion-api | Flink |

## ClickHouse Views (24)

ClickHouse reads Iceberg tables directly from MinIO via `iceberg()` table function. Views are read-only.

```
iceberg.raw_*              (8 views)  -> raw layer
iceberg.stg_*              (8 views)  -> staging layer
iceberg.entity_index       (1 view)   -> semantic layer
iceberg.customer_metrics   (1 view)  ┐
iceberg.order_summary      (1 view)  ├─ analytics layer
iceberg.payment_metrics    (1 view)  │
iceberg.campaign_metrics   (1 view)  ┘
iceberg.customer_360       (1 view)  ┐
iceberg.sales_dashboard_daily (1 view) ├─ marts layer
iceberg.campaign_dashboard (1 view)  ┘
```

## Iceberg Table Properties

All tables use:
- Format version: 2
- Compression: zstd (Parquet)
- Partition: month-based on primary timestamp
- Catalog: REST (backed by PostgreSQL)
- Storage: MinIO (`s3a://warehouse/{layer}/{table}/`)
