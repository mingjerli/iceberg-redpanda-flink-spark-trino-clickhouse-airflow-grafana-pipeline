# Design: GA4 Analytics Integration (Final - Approved for Implementation)

**Status**: APPROVED WITH CHANGES (Staff Engineer Review 2026-02-08)

**Review History**:
- Product Manager Review: B+ (Very Good, Not Excellent) - Identified 7 critical gaps
- Staff Data Engineer Review: Recommended Parquet-based batch ingestion (no BigQuery emulator for Phase 1)
- Senior Engineer: Created comprehensive 2-phase implementation plan (20 steps, ~73 hours)
- Staff Engineer: APPROVED WITH CHANGES - Identified 5 required changes, all addressed below

---

## Executive Summary

This document describes the integration of Google Analytics 4 (GA4) as a **batch data source** into the existing Iceberg lakehouse pipeline. Unlike the webhook-based sources (Shopify, Stripe, HubSpot, Mailchimp), GA4 follows a **file-based batch ingestion pattern** that bypasses Redpanda and Flink entirely.

**Key Design Decisions**:
- ✅ Batch ingestion via Parquet files (simulates BigQuery Export)
- ✅ Sessions computed from events in staging layer (30-minute inactivity gap rule)
- ✅ Event deduplication at staging level (critical gap addressed)
- ✅ Funnel analysis table (Phase 1 - critical gap addressed)
- ✅ Page performance table (Phase 1 - critical gap addressed)
- ✅ Idempotency protection in batch ingest (MERGE INTO pattern)
- ✅ TIMESTAMP(6) microsecond precision through raw/staging
- ✅ Entity resolution via user_id (demo: user_id = email)

**Phases**:
- **Phase 1** (Core Integration): Batch ingestion, staging, analytics (engagement, funnel, page performance), marts, monitoring - ~46 hours
- **Phase 2** (Advanced Analytics): Cohort retention, e-commerce item extraction, multi-touch attribution, Grafana dashboard - ~27 hours

---

## 1. Architecture Comparison

```
EXISTING (Shopify/Stripe/HubSpot/Mailchimp):
  Webhook → Ingestion API → Redpanda → Flink → raw Iceberg → Spark staging

GA4 (this design):
  Mock BigQuery Export (Parquet files) → Spark → raw Iceberg → Spark staging
```

This introduces a **second ingestion pattern** into the pipeline, demonstrating that real data platforms handle multiple ingestion modes.

---

## 2. Critical Gaps Addressed (PM Feedback)

| Gap | Priority | Solution |
|-----|----------|----------|
| Event deduplication | CRITICAL | Dedup by (client_id, event_timestamp, event_name) in staging using ROW_NUMBER() |
| Funnel analysis | CRITICAL | `analytics.funnel_analysis` table with step-by-step conversion tracking |
| Page performance | CRITICAL | `analytics.page_performance` table with page-level metrics |
| Idempotency | CRITICAL | MERGE INTO pattern in batch ingest (Staff Engineer RC-3) |
| FIRST/LAST ordering | CRITICAL | Window functions with explicit ordering (Staff Engineer RC-4) |

---

## 3. Data Model

### Single Entity: Events

GA4 has one primary entity: **events**. Sessions are **derived** from events in the staging layer.

### Event Fields (BigQuery Export Format)

```
_raw_id            STRING   -- Hash of (client_id|event_timestamp|event_name)
client_id          STRING   -- GA4 client ID (e.g. "1234567890.1706500000")
user_id            STRING   -- Optional cross-device user ID
event_name         STRING   -- page_view, purchase, add_to_cart, etc.
event_timestamp    BIGINT   -- Microseconds since epoch (GA4 native)
event_date         STRING   -- YYYYMMDD (BigQuery Export convention)
event_params       STRING   -- JSON array of {key, value} pairs
traffic_source     STRING   -- JSON: {source, medium, campaign}
device             STRING   -- JSON: {category, os, browser}
geo                STRING   -- JSON: {country, region, city}
page_location      STRING   -- Full URL
engagement_time_ms BIGINT   -- Milliseconds of engagement
is_conversion      BOOLEAN  -- Conversion flag
session_id         STRING   -- GA4 session identifier
```

---

## 4. Implementation Plan Summary

### Phase 1 Components (14 Steps)

1. **Test Infrastructure** - pytest fixtures, SparkSession setup
2. **GA4 Provider** (TDD) - Mock data generation with session coherence
3. **Generator Integration** (TDD) - Parquet output
4. **Batch Ingestion** (TDD) - MERGE INTO for idempotency
5. **Infrastructure** - Docker volume mounts, env vars
6. **Staging Events** (TDD) - Deduplication + JSON extraction
7. **Staging Sessions** (TDD) - 30-min gap rule + window functions
8. **Entity Resolution** - GA4 user_id matching
9. **Analytics** (TDD) - Engagement + Page Performance + Funnel
10. **Marts** (TDD) - Engagement dashboard + Customer 360 updates
11. **Airflow DAG** - Task orchestration
12. **ClickHouse** - Views and monitoring
13. **Scripts** - reset_and_run.sh, validate_tables.sh
14. **Integration Tests** - E2E validation

### Estimated Effort: ~46 hours (2 weeks)

---

## 5. Key Files (Phase 1)

### New Files (19)

**Data Generation**:
- `datagen/providers/ga4_provider.py`

**Batch Ingestion**:
- `jobs/spark/ga4_batch_ingest.py`

**Schema (Reference DDL)**:
- `sql/00_raw/ga4/events.sql`
- `sql/01_staging/stg_ga4_events.sql`
- `sql/01_staging/stg_ga4_sessions.sql`
- `sql/04_analytics/engagement_metrics.sql`
- `sql/04_analytics/engagement_by_channel.sql`
- `sql/04_analytics/page_performance.sql` ⭐ (Critical gap)
- `sql/04_analytics/funnel_analysis.sql` ⭐ (Critical gap)
- `sql/05_marts/engagement_dashboard.sql`

**Tests** (9 files for TDD):
- `tests/conftest.py`
- `tests/test_ga4_provider.py`
- `tests/test_ga4_batch_ingest.py`
- `tests/test_ga4_dedup.py`
- `tests/test_ga4_staging.py`
- `tests/test_ga4_analytics.py`
- `tests/test_ga4_e2e.py`
- etc.

### Modified Files (14)

- `datagen/generator.py` - Add GA4Provider, Parquet save
- `infrastructure/docker-compose.yml` - Volume mounts
- `infrastructure/.env.example` - GA4 env vars
- `jobs/spark/staging_batch.py` - Add GA4 staging functions
- `jobs/spark/entity_backfill.py` - Add GA4 union + LEFT JOIN
- `jobs/spark/analytics_incremental.py` - Add 4 analytics functions
- `jobs/spark/marts_incremental.py` - Update customer_360
- `airflow/dags/iceberg_pipeline.py` - Add 8 GA4 tasks
- `sql/02_semantic/entity_index.sql` - Add 'ga4' docs
- `sql/05_marts/customer_360.sql` - Add GA4 columns
- `infrastructure/clickhouse/iceberg_setup.sql` - Add views
- `scripts/reset_and_run.sh` - Add GA4 steps
- `scripts/validate_tables.sh` - Add GA4 validation
- `monitoring/dashboards/batch_business.json` - Add panel

---

## 6. Required Changes Implemented (Staff Engineer Review)

### RC-1: `_raw_id` Column Added
- Raw table includes `_raw_id STRING` as hash of natural key
- Staging references `_raw_id` for lineage tracking
- Follows project convention (per MEMORY.md)

### RC-2: Event Deduplication Implemented
- Dedup by `(client_id, event_timestamp, event_name)` triple
- Uses `ROW_NUMBER()` window with `_loaded_at DESC`
- Keeps latest record on duplicate
- Same pattern as `stg_mailchimp_subscribers`

### RC-3: Idempotency Protection
- Batch ingest uses `MERGE INTO` with `_raw_id` as merge key
- Prevents duplicates on Airflow retry
- `WHEN MATCHED THEN UPDATE SET *`
- `WHEN NOT MATCHED THEN INSERT *`

### RC-4: FIRST/LAST Ordering Fixed
- Uses explicit window functions instead of aggregate FIRST/LAST
- `row_number()` over partition by session ordered by timestamp
- Separate windows for ascending (first event) and descending (last event)
- Deterministic ordering guaranteed

### RC-5: Docker Volume Mount Clarified
- Volume mount on both `spark-master` and `spark-worker` services
- Path: `../datagen/output:/opt/spark/data:ro`
- Read-only mount (`:ro` flag)

---

## 7. Validation Criteria (Phase 1 Complete)

### Must Pass:

- [ ] `raw.ga4_events` exists with `_raw_id` column
- [ ] Duplicate Parquet ingestion does not inflate row count (idempotency test)
- [ ] `stg_ga4_events` deduplication works (insert same event twice, count = 1)
- [ ] `stg_ga4_sessions` respects 30-min gap rule (boundary test)
- [ ] `analytics.funnel_analysis` shows step-by-step dropoff
- [ ] `analytics.page_performance` has page-level bounce rate
- [ ] Entity resolution links GA4 users to existing entities via email
- [ ] `customer_360` has `has_ga4`, `total_sessions` columns
- [ ] Airflow DAG runs end-to-end (all 8 GA4 tasks green)
- [ ] All unit tests pass: `pytest tests/test_ga4*.py -v --cov`
- [ ] Test coverage >= 80%

### Key Queries Must Work:

```sql
-- Funnel analysis (critical gap)
SELECT step_name, step_users, step_completion_rate, dropoff_rate
FROM iceberg.analytics.funnel_analysis
WHERE metric_date = CURRENT_DATE AND funnel_name = 'ecommerce'
ORDER BY step_number;

-- Page performance (critical gap)
SELECT page_location, page_views, unique_visitors, bounce_rate
FROM iceberg.analytics.page_performance
WHERE metric_date = CURRENT_DATE
ORDER BY page_views DESC
LIMIT 20;

-- Engagement metrics
SELECT metric_date, total_sessions, engagement_rate, bounce_rate, conversion_rate
FROM iceberg.analytics.engagement_metrics
ORDER BY metric_date DESC
LIMIT 30;

-- Customer 360 with GA4
SELECT entity_id, has_ga4, total_sessions, last_session_date, source_count
FROM iceberg.marts.customer_360
WHERE has_ga4 = TRUE
LIMIT 10;
```

---

## 8. TDD Workflow (Test-Driven Development)

### Red-Green-Refactor Cycle

For each component:

1. **RED**: Write failing test first
2. **GREEN**: Write minimal code to pass test
3. **REFACTOR**: Improve code while keeping tests green
4. **VERIFY**: Check coverage >= 80%

### Example: GA4Provider

```python
# Step 1: Write test (RED)
def test_generate_event_returns_valid_schema(ga4_provider):
    event = ga4_provider.generate_event()
    assert "client_id" in event
    assert "event_timestamp" in event
    assert isinstance(event["event_timestamp"], int)
    assert event["event_timestamp"] > 0

# Step 2: Implement (GREEN)
class GA4Provider:
    def generate_event(self):
        return {"client_id": "...", "event_timestamp": int(time.time() * 1_000_000)}

# Step 3: Refactor (IMPROVE)
# Add validation, improve data realism, etc.
```

### Priority Test Coverage

| Component | Target % | Critical Tests |
|-----------|----------|----------------|
| GA4Provider | 95% | Session coherence, seed reproducibility |
| Deduplication | 90% | Exact duplicates, partial duplicates, cross-batch |
| Session computation | 90% | 30-min boundary, FIRST/LAST ordering |
| Batch ingest | 85% | Idempotency, MERGE behavior |
| Funnel analysis | 85% | Step completion, dropoff calculation |

---

## 9. Phase 2 (Future Work)

### Cohort Retention Analysis
```sql
CREATE TABLE analytics.cohort_retention (
    cohort_period       DATE,
    period_offset       INT,
    cohort_size         BIGINT,
    retained_users      BIGINT,
    retention_rate      DECIMAL(5, 4)
);
```

### E-commerce Item Extraction
```sql
CREATE TABLE staging.stg_ga4_ecommerce_items (
    event_timestamp     TIMESTAMP,
    item_id             STRING,
    item_name           STRING,
    item_category       STRING,
    price               DECIMAL(18, 2),
    quantity            INT
);
```

### Multi-Touch Attribution
```sql
CREATE TABLE analytics.attribution (
    attribution_model   STRING,  -- last_touch, first_touch, linear
    channel_group       STRING,
    attributed_conversions DECIMAL(18, 4),
    attributed_value    DECIMAL(18, 2)
);
```

**Estimated effort**: ~27 hours

---

## 10. ADR: Architectural Decision Records

### ADR-001: Batch Ingestion (No Webhooks)

**Context**: GA4 doesn't send webhooks. Real production systems use BigQuery Export.

**Decision**: Use Parquet-based batch ingestion via Spark, bypassing Redpanda/Flink.

**Consequences**:
- ✅ Architecturally honest (matches production reality)
- ✅ Demonstrates second ingestion pattern
- ✅ Lower complexity than BigQuery emulator
- ⚠️ 24-hour data latency (acceptable for analytics)

### ADR-002: Sessions Computed in Staging

**Context**: GA4 sessions are derived from events using 30-min inactivity gap.

**Decision**: Compute sessions in staging layer, not as separate raw entity.

**Consequences**:
- ✅ Matches GA4's internal model
- ✅ Demonstrates window-function sessionization
- ✅ Allows validation against mock session_id
- ⚠️ More complex staging logic

### ADR-003: MERGE INTO for Idempotency

**Context**: Airflow retries without idempotency would duplicate data.

**Decision**: Use MERGE INTO with `_raw_id` hash as merge key.

**Consequences**:
- ✅ Prevents duplicates on retry
- ✅ Enables safe reprocessing
- ⚠️ Requires Iceberg v2 with upsert support
- ⚠️ Slightly slower than append-only

### ADR-004: Funnel + Page Performance in Phase 1

**Context**: PM identified these as critical gaps for web analytics.

**Decision**: Include in Phase 1 despite adding 2 additional analytics tables.

**Consequences**:
- ✅ Delivers complete web analytics capability
- ✅ Enables key business questions
- ⚠️ Adds ~8 hours to Phase 1 effort

---

## 11. Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│ Phase 1: Data Generation                                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  datagen/providers/ga4_provider.py                              │
│    └─> generate_export_batch()                                 │
│         └─> List[Dict] (GA4 events)                            │
│                                                                 │
│  datagen/generator.py                                           │
│    └─> save_to_files() → Parquet                               │
│         └─> datagen/output/ga4/events.parquet                  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                            │
                            │ Docker volume mount
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│ Phase 2: Batch Ingestion (New Pattern)                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  jobs/spark/ga4_batch_ingest.py                                 │
│    ├─> spark.read.parquet()                                    │
│    ├─> Add _raw_id (hash), _loaded_at, _source_file           │
│    └─> MERGE INTO iceberg.raw.ga4_events                       │
│         (idempotency via _raw_id)                              │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│ Phase 3: Staging (Deduplication + Sessionization)               │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  jobs/spark/staging_batch.py                                    │
│                                                                 │
│  stage_ga4_events()                                             │
│    ├─> Dedup by (client_id, event_timestamp, event_name)      │
│    ├─> Convert timestamp (microseconds → TIMESTAMP(6))        │
│    ├─> Extract JSON (traffic, device, geo)                    │
│    └─> staging.stg_ga4_events                                  │
│                                                                 │
│  compute_ga4_sessions()                                         │
│    ├─> LAG window for 30-min gap detection                    │
│    ├─> Window functions for first/last event attribution      │
│    ├─> Aggregate to session level                             │
│    └─> staging.stg_ga4_sessions                                │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                            │
                ┌───────────┴───────────┐
                │                       │
                ▼                       ▼
┌──────────────────────────┐  ┌──────────────────────────────────┐
│ Entity Resolution        │  │ Analytics (3 Critical Tables)    │
├──────────────────────────┤  ├──────────────────────────────────┤
│                          │  │                                  │
│ entity_backfill.py       │  │ analytics_incremental.py         │
│  └─> GA4 union           │  │                                  │
│  └─> LEFT JOIN           │  │ ├─> engagement_metrics           │
│  └─> entity_index        │  │ │    (daily aggregation)         │
│                          │  │ │                                │
│                          │  │ ├─> page_performance ⭐          │
│                          │  │ │    (critical gap)              │
│                          │  │ │                                │
│                          │  │ └─> funnel_analysis ⭐           │
│                          │  │      (critical gap)              │
│                          │  │                                  │
└──────────────────────────┘  └──────────────────────────────────┘
                │                       │
                └───────────┬───────────┘
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│ Marts (Business Intelligence Layer)                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  marts_incremental.py                                           │
│                                                                 │
│  ├─> engagement_dashboard_daily                                │
│  │    (rolling averages, device split, top country)           │
│  │                                                             │
│  └─> customer_360 (updated with GA4 columns)                  │
│       ├─> has_ga4                                              │
│       ├─> total_sessions                                       │
│       ├─> total_page_views                                     │
│       ├─> last_session_date                                    │
│       └─> source_count (now 5: Shopify, Stripe, HubSpot,      │
│                              Mailchimp, GA4)                   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│ Monitoring & Visualization                                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ClickHouse Views → Grafana Dashboards                          │
│                                                                 │
│  Key Metrics:                                                   │
│  - Sessions, Engagement Rate, Bounce Rate                      │
│  - Conversion Funnel (Step-by-step dropoff)                   │
│  - Top Pages by Performance                                    │
│  - Channel Attribution                                         │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 12. Next Steps

### Implementation Order

1. **Week 1**: Steps 1-7 (Data gen → Staging)
   - Day 1-2: GA4Provider + Generator (TDD)
   - Day 3: Batch Ingest (TDD + idempotency tests)
   - Day 4-5: Staging events + sessions (TDD + session boundary tests)

2. **Week 2**: Steps 8-14 (Analytics → E2E)
   - Day 6-7: Analytics tables (engagement, page perf, funnel)
   - Day 8: Entity resolution + Marts
   - Day 9: Airflow + ClickHouse + Scripts
   - Day 10: Integration tests + Documentation

### Definition of Done (Phase 1)

- [ ] All 19 new files created
- [ ] All 14 modified files updated
- [ ] All unit tests pass (pytest -v)
- [ ] Test coverage >= 80% (pytest --cov)
- [ ] Integration test passes (E2E through Docker stack)
- [ ] `./scripts/reset_and_run.sh --validate` passes
- [ ] All critical queries return expected results
- [ ] Documentation updated (CLAUDE.md, DESIGN_GA4_FINAL.md)
- [ ] Code reviewed (security, performance, maintainability)
- [ ] Commit and create PR with comprehensive summary

---

**Status**: Ready for implementation. All staff engineer required changes addressed. ✅
