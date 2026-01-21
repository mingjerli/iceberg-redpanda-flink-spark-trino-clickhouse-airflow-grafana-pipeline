# Iceberg Incremental Demo

A realistic enterprise data platform demo combining **Apache Iceberg**, **incremental loading**, and a **semantic layer** for entity resolution across multiple SaaS sources.

## Overview

This demo simulates a modern data platform that:
- Ingests data from **Shopify**, **Stripe**, and **HubSpot** via webhooks
- Stores data in **Apache Iceberg** format for cross-engine compatibility
- Uses **incremental loading** patterns for cost-efficient processing
- Implements **entity resolution** to unify customers across sources
- Supports queries from **ClickHouse**, **Spark**, and **Trino**

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      DATA SOURCES                               │
│              Shopify │ Stripe │ HubSpot (via Webhooks)         │
└─────────────┬────────┴────────┴────────┬────────────────────────┘
              │                          │
              ▼                          ▼
┌─────────────────────────────────────────────────────────────────┐
│              INGESTION API (FastAPI)                            │
│  - Receive webhooks, validate signatures, publish to Redpanda   │
└─────────────────────────────┬───────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    REDPANDA (Kafka-compatible)                  │
│  Topics: shopify.orders, stripe.charges, hubspot.contacts       │
└─────────────────────────────┬───────────────────────────────────┘
                              │
              ┌───────────────┴───────────────┐
              ▼                               ▼
┌─────────────────────────┐     ┌─────────────────────────────────┐
│  RAW FILE WRITER        │     │  ICEBERG WRITER (Flink)         │
│  (Flink → GCS/S3)       │     │  → raw.shopify_orders           │
└─────────────────────────┘     │  → raw.stripe_charges           │
                                │  → raw.hubspot_contacts          │
                                └─────────────┬───────────────────┘
                                              │
                                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    PROCESSING LAYERS                            │
│  Raw → Staging → Semantic → Core (Views) → Analytics → Marts    │
└─────────────────────────────────────────────────────────────────┘
```

## Quick Start

### 1. Generate Mock Data

```bash
cd datagen
pip install -r requirements.txt

# Generate all sources with default scale
python generator.py --source all --output-dir ./output

# Generate with custom scale (2x = double the records)
python generator.py --source all --scale 2.0 --output-dir ./output

# Generate only Shopify data
python generator.py --source shopify --scale 1.0 --output-dir ./output
```

### 2. Start Infrastructure

```bash
cd infrastructure
docker-compose up -d
```

### 3. Run SQL Transformations

SQL files in `sql/` are organized by layer:
- `00_raw/` - Iceberg table DDLs
- `01_staging/` - Cleaning and typing
- `02_semantic/` - Entity resolution
- `03_core/` - Unified business entity views
- `04_analytics/` - Aggregations
- `05_marts/` - Denormalized tables

## Directory Structure

```
iceberg-incremental-demo/
├── README.md                    # This file
├── schemas/                     # API-accurate JSON schemas
│   ├── shopify.json            # Shopify REST/Webhook format
│   ├── stripe.json             # Stripe REST API format
│   └── hubspot.json            # HubSpot CRM API format
├── datagen/                     # Mock data generation
│   ├── requirements.txt
│   ├── generator.py            # Main CLI tool
│   ├── providers/              # Source-specific Faker providers
│   │   ├── shopify_provider.py
│   │   ├── stripe_provider.py
│   │   └── hubspot_provider.py
│   └── output/                 # Generated data (gitignored)
├── infrastructure/              # Docker services
│   ├── docker-compose.yml
│   ├── minio/                  # S3-compatible storage
│   ├── redpanda/               # Kafka-compatible streaming
│   ├── iceberg-rest/           # Iceberg REST catalog
│   ├── flink/                  # Streaming ETL
│   ├── spark/                  # Batch ETL
│   └── clickhouse/             # OLAP queries
├── ingestion/                   # FastAPI webhook service
│   └── app/
├── sql/                         # SQL transformations
│   ├── 00_raw/
│   ├── 01_staging/
│   ├── 02_semantic/
│   ├── 03_core/
│   ├── 04_analytics/
│   └── 05_marts/
├── jobs/                        # ETL jobs
│   ├── flink/
│   └── spark/
├── airflow/                     # Orchestration DAGs
│   └── dags/
└── scripts/                     # Utility scripts
```

## Data Schemas

### Shopify (REST/Webhook Format)
Based on [Shopify Admin REST API 2024-10](https://shopify.dev/docs/api/admin-rest/2024-10/resources/order)

Tables:
- `orders` - Order headers with nested customer, addresses, line_items
- `customers` - Customer records with addresses
- `products` - Product catalog with variants
- `line_items` - Denormalized from orders
- `addresses` - Denormalized from customers

### Stripe (REST API)
Based on [Stripe API Reference](https://docs.stripe.com/api)

Tables:
- `customers` - Customer records
- `charges` - Payment charges with outcome/risk data
- `payment_intents` - Payment lifecycle
- `subscriptions` - Recurring billing
- `refunds` - Refund transactions
- `disputes` - Chargebacks

### HubSpot (CRM API)
Based on [HubSpot CRM API](https://developers.hubspot.com/docs/api/crm/contacts)

Tables:
- `contacts` - Contact records with lifecycle stage
- `companies` - Company records
- `deals` - Sales opportunities with pipeline stages
- `engagements` - Activities (calls, emails, meetings)

## Entity Resolution

The semantic layer unifies customers across sources using:
- **Exact matching**: Email, phone number
- **Fuzzy matching**: Name similarity, address matching

See `sql/02_semantic/entity_index.sql` for implementation.

## Related Documentation

- [Iceberg + Incremental Design](../../plans/iceberg-incremental-design.md)
- [Semantic Layer Design](../../plans/semantic-layer-design.md)

## API Documentation Sources

- [Shopify REST Admin API](https://shopify.dev/docs/api/admin-rest/2024-10/resources/order)
- [Shopify Webhooks](https://shopify.dev/docs/api/webhooks)
- [Stripe API Reference](https://docs.stripe.com/api)
- [Stripe Webhooks](https://docs.stripe.com/webhooks)
- [HubSpot CRM API](https://developers.hubspot.com/docs/api/crm/contacts)
- [HubSpot Webhooks](https://developers.hubspot.com/docs/api/webhooks)
