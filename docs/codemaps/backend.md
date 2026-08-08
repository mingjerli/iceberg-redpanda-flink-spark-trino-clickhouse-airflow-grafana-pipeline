# Backend Codemap

> Freshness: 2026-02-08 | Auto-generated

## Ingestion Service (FastAPI)

```
ingestion/app/
├── main.py                    # App factory, /health, /ready, /metrics
├── config.py                  # pydantic Settings (env: INGESTION_*)
├── producers/
│   └── redpanda.py            # AIOKafka async producer wrapper
├── validators/
│   ├── __init__.py            # Re-exports validate_* functions
│   └── signatures.py          # HMAC: Shopify (SHA256), Stripe (SHA256), HubSpot (SHA256), Mailchimp (URL secret)
└── webhooks/
    ├── shopify.py             # POST /webhooks/shopify/{orders,customers}  -> shopify.{orders,customers}
    ├── stripe.py              # POST /webhooks/stripe/webhook             -> stripe.{charges,customers}
    ├── hubspot.py             # POST /webhooks/hubspot/webhook            -> hubspot.contacts
    └── mailchimp.py           # POST /webhooks/mailchimp/webhook          -> mailchimp.{campaigns,events,subscribers}
```

**Routing pattern:** Each webhook handler validates signature, parses payload, determines Redpanda topic by event type, publishes via async producer.

## Batch Jobs (PySpark)

```
jobs/spark/
├── staging_batch.py           # raw -> staging (8 functions in STAGING_FUNCTIONS dict)
│                              #   --table {name} --mode {full|incremental}
├── entity_backfill.py         # staging -> semantic (entity_index + blocking_index)
│                              #   --mode {initial|incremental|dry-run}
├── entity_resolution_fuzzy.py # Fuzzy matching using blocking_index
├── core_views.py              # semantic+staging -> core (customers, orders)
├── analytics_incremental.py   # staging+core -> analytics (4 functions in ANALYTICS_FUNCTIONS dict)
│                              #   --table {name} --mode {full|incremental}
├── marts_incremental.py       # analytics+core+staging -> marts (4 functions in MARTS_FUNCTIONS dict)
│                              #   --table {name} --mode {full|incremental}
└── maintenance/
    ├── compact_tables.py      # Iceberg file compaction
    ├── expire_snapshots.py    # Snapshot cleanup
    └── entity_quality_check.py # Entity resolution quality metrics
```

**Common patterns:**
- `argparse` CLI: `--table`, `--mode full|incremental|range`, `--start-date`, `--end-date`
- Watermark tracking in `iceberg.metadata.incremental_watermarks`
- Functions registered in `*_FUNCTIONS` dict at module bottom
- Inline DDL (CREATE TABLE IF NOT EXISTS) before transforms

## Streaming Jobs (Flink SQL)

```
jobs/flink/
├── metadata_setup.sql         # Creates metadata database
├── raw_ingestion_setup.sql    # Creates raw database + Kafka connectors
├── staging_setup.sql          # Creates staging database
├── shopify_orders_full.sql    # Kafka(shopify.orders) -> raw.shopify_orders
├── shopify_customers_full.sql # Kafka(shopify.customers) -> raw.shopify_customers
├── stripe_charges_full.sql    # Kafka(stripe.charges) -> raw.stripe_charges
├── stripe_customers_full.sql  # Kafka(stripe.customers) -> raw.stripe_customers
├── hubspot_contacts_full.sql  # Kafka(hubspot.contacts) -> raw.hubspot_contacts
├── mailchimp_campaigns_full.sql    # Kafka(mailchimp.campaigns) -> raw.mailchimp_campaigns
├── mailchimp_events_full.sql       # Kafka(mailchimp.events) -> raw.mailchimp_events
├── mailchimp_subscribers_full.sql  # Kafka(mailchimp.subscribers) -> raw.mailchimp_subscribers
├── entity_resolution_exact.sql     # Real-time exact entity matching
│   # NB: GA4 has no Flink job -- it is batch-ingested by
│   #     jobs/spark/ga4_batch_ingest.py straight into raw.ga4_events
└── stg_shopify_orders_full.sql     # Streaming staging example
```

**Flink job pattern (3 sections):**
1. Iceberg catalog setup (REST catalog, S3/MinIO creds)
2. Kafka source table (Redpanda topic, JSON format)
3. Iceberg sink table + INSERT INTO ... SELECT

## Data Generation

```
datagen/
├── generator.py               # CLI orchestrator: --source, --count, --seed
├── simulate_webhooks.py       # HTTP POST to ingestion API with retries
└── providers/
    ├── shopify_provider.py    # generate_customer(), generate_order(), generate_product()
    ├── stripe_provider.py     # generate_customer(), generate_charge()
    ├── hubspot_provider.py    # generate_contact()
    └── mailchimp_provider.py  # generate_subscriber(), generate_campaign(), generate_event()
```

## Automation Scripts

```
scripts/
├── reset_and_run.sh           # 8-phase pipeline: reset -> infra -> catalog -> datagen -> flink -> spark -> clickhouse -> airflow
├── validate_tables.sh         # Row count validation via Trino (27 tables)
├── post_mock_data.py          # Direct HTTP mock data posting (all 4 sources)
└── benchmarks/
    └── query_performance.py   # Query benchmarks across Spark/Trino/ClickHouse
```

## Key Dependencies

| Component | Key Libraries |
|-----------|--------------|
| Ingestion | fastapi, uvicorn, aiokafka, pydantic, prometheus-fastapi-instrumentator |
| Datagen | faker, click, tqdm, requests, orjson |
| Spark jobs | pyspark (bundled in Docker image) |
| Flink jobs | Pure SQL (Iceberg + Kafka connectors in Docker image) |
| Mock poster | click, httpx, faker |
