# Design: Mailchimp Integration

This document describes the integration of Mailchimp as a new streaming data source into the existing pipeline. The design follows every convention established by the Shopify, Stripe, and HubSpot integrations.

---

## 1. Architecture Overview

Mailchimp follows the **same streaming path** as all existing sources:

```
datagen (mock)  →  POST /webhooks/mailchimp/*  →  Redpanda  →  Flink  →  raw Iceberg
                                                                            ↓
                                                              Spark staging/analytics/marts
```

This is architecturally consistent because Mailchimp actually sends real webhooks in production — unlike GA4, there is no impedance mismatch here.

---

## 2. Data Model

### 2.1 Entities & Topics

| Entity | Redpanda Topic | Description |
|--------|---------------|-------------|
| Campaigns | `mailchimp.campaigns` | Campaign metadata (email + SMS) |
| Events | `mailchimp.events` | Send, open, click, bounce, unsubscribe, sms_sent, sms_click |
| Subscribers | `mailchimp.subscribers` | List member profiles and subscription status |

All entities include metadata columns added by the ingestion layer. The Redpanda producer (`ingestion/app/producers/redpanda.py`) enriches every message with `_webhook_received_at`, `_source`, and `_event_type`. Each source also defines its own source-specific metadata field:

- **Shopify**: `_webhook_topic` (from `X-Shopify-Topic` header)
- **Stripe**: `_webhook_event_id` (from Stripe event ID)
- **HubSpot**: `_webhook_subscription_type` (from subscription type field)
- **Mailchimp**: `_webhook_event_type` (from the `type` field in the webhook payload)

The Flink source tables read `_webhook_received_at` and the source-specific field from the Kafka JSON. The Flink sink tables propagate these plus `_loaded_at` (set to `CURRENT_TIMESTAMP`). The `_source` and `_event_type` fields are available in the Kafka source but are NOT propagated to the Iceberg sink (consistent with existing sources).

Mailchimp entity metadata columns:
```
_webhook_received_at   STRING   -- ISO8601 timestamp (set by Redpanda producer, overwrites any provider value)
_webhook_event_type    STRING   -- Mailchimp webhook type (e.g., "subscribe", "campaign", "click")
```

These columns appear in the Flink source table definitions, raw table schemas, and propagate through staging as `_loaded_at` and `_staged_at`.

### 2.2 Campaign Fields

```
campaign_id        STRING   -- Mailchimp campaign ID (10-char hex)
type               STRING   -- regular, plaintext, absplit, rss, variate, automation, sms
status             STRING   -- save, paused, schedule, sending, sent
list_id            STRING   -- Audience/list ID
subject_line       STRING
preview_text       STRING
from_name          STRING
from_email         STRING
reply_to           STRING
send_time          STRING   -- ISO8601
content_type       STRING   -- template, html, url, multichannel
emails_sent        INT
opens              INT
unique_opens       INT
clicks             INT
unique_clicks      INT
unsubscribes       INT
bounces            INT
open_rate          DOUBLE
click_rate         DOUBLE
settings           STRING   -- JSON: additional campaign settings
tracking           STRING   -- JSON: {opens, html_clicks, text_clicks, google_analytics}
_webhook_received_at STRING -- (set by Redpanda producer)
_webhook_event_type  STRING -- (Mailchimp webhook type field)
```

### 2.3 Event Fields

```
event_id           STRING   -- Unique event ID
campaign_id        STRING
email_id           STRING   -- Unique per member-campaign combo
email_address      STRING
action             STRING   -- sent, open, click, bounce, unsub, abuse, sms_sent, sms_click
timestamp          STRING   -- ISO8601
url                STRING   -- Clicked URL (for click events)
ip                 STRING   -- IP address (for opens/clicks)
user_agent         STRING   -- Browser/client
location           STRING   -- JSON: {latitude, longitude, country_code, region}
bounce_type        STRING   -- hard, soft (for bounce events)
list_id            STRING
_webhook_received_at STRING -- (set by Redpanda producer)
_webhook_event_type  STRING -- (Mailchimp webhook type field)
```

### 2.4 Subscriber Fields

```
subscriber_id      STRING   -- MD5 hash of lowercased email (Mailchimp convention)
email_address      STRING
email_type         STRING   -- html, text
status             STRING   -- subscribed, unsubscribed, cleaned, pending, transactional
merge_fields       STRING   -- JSON: {FNAME, LNAME, PHONE, ADDRESS, ...}
stats              STRING   -- JSON: {avg_open_rate, avg_click_rate}
list_id            STRING
tags               STRING   -- JSON array of tag objects
ip_signup          STRING
timestamp_signup   STRING   -- ISO8601
ip_opt             STRING
timestamp_opt      STRING   -- ISO8601
last_changed       STRING   -- ISO8601
language           STRING
vip                BOOLEAN
source             STRING   -- API, import, popup, landing_page
phone              STRING   -- SMS-enabled phone number (E.164)
sms_status         STRING   -- subscribed, unsubscribed, non_subscribed
_webhook_received_at STRING -- (set by Redpanda producer)
_webhook_event_type  STRING -- (Mailchimp webhook type field)
```

---

## 3. Files to Create / Modify

### 3.1 Mock Data Generation

| File | Action | Description |
|------|--------|-------------|
| `datagen/providers/mailchimp_provider.py` | **Create** | Faker provider for campaigns, events, subscribers |
| `datagen/generator.py` | **Modify** | Add `generate_mailchimp_data()`, extend shared customer pool |
| `datagen/simulate_webhooks.py` | **Modify** | Add Mailchimp webhook posting methods |

**Provider details**:

- **`MailchimpProvider`** class following the existing pattern (`ShopifyProvider`, etc.)
- Sequential ID counters: `_campaign_id`, `_event_id`, `_subscriber_id`
- Methods: `generate_campaign()`, `generate_event(campaign, subscriber)`, `generate_subscriber(shared=None)`
- `campaign_id`: 10-char hex string (matches real Mailchimp format)
- `subscriber_id`: `hashlib.md5(email.lower().encode()).hexdigest()` (matches Mailchimp convention)
- `email_address`: drawn from shared customer pool (30% overlap with other sources)
- `merge_fields`: `{"FNAME": ..., "LNAME": ..., "PHONE": ...}` using same names/phones as shared customers
- `action` distribution: `sent (40%), open (25%), click (15%), bounce (8%), unsub (5%), sms_sent (5%), sms_click (2%)`
- `phone`: E.164 format, matching shared customer pool for SMS events

**Generator changes** (`generator.py`):

```python
# New import
from providers.mailchimp_provider import MailchimpProvider

# In __init__:
self.mailchimp = MailchimpProvider(seed=seed)

# New method:
def generate_mailchimp_data(
    self,
    subscribers: int = 100,
    campaigns: int = 20,
    events: int = 500,
) -> Dict[str, List[Dict]]:
    ...

# In generate_all(): add mailchimp call
# In CLI --source choices: add "mailchimp"
```

**Webhook simulator changes** (`simulate_webhooks.py`):

The datagen sends payloads as **JSON** (matching the existing Shopify/Stripe/HubSpot pattern), not form-encoded data. In production, Mailchimp webhooks are form-encoded, but the datagen and ingestion handler use JSON for consistency with the rest of the pipeline. The ingestion handler code must include a comment documenting this difference so a future migration to real Mailchimp webhooks knows to add form-decoding middleware.

```python
# New methods:
def send_mailchimp_subscriber(self) -> bool: ...
def send_mailchimp_event(self) -> bool: ...
def send_mailchimp_campaign(self) -> bool: ...

# In simulate(): add mailchimp senders
# In CLI --source choices: add "mailchimp"
# In CLI --entity choices: add "subscribers", "events", "campaigns"
#   (these are used by the entity filter in the simulate() method)
```

### 3.2 Ingestion API

| File | Action | Description |
|------|--------|-------------|
| `ingestion/app/webhooks/mailchimp.py` | **Create** | FastAPI router for Mailchimp webhooks |
| `ingestion/app/webhooks/__init__.py` | **Modify** | Add `from .mailchimp import router as mailchimp_router` to exports |
| `ingestion/app/validators/signatures.py` | **Modify** | Add `validate_mailchimp_signature()` |
| `ingestion/app/config.py` | **Modify** | Add `mailchimp_webhook_secret` and `mailchimp_enabled` settings |
| `ingestion/app/main.py` | **Modify** | Register Mailchimp router |

**Webhook handler** (`mailchimp.py`):

Router uses `APIRouter(prefix="/mailchimp", tags=["mailchimp"])`, registered in `main.py` via `app.include_router(mailchimp_router, prefix="/webhooks")`. This makes endpoints available at `/webhooks/mailchimp/webhook`.

Mailchimp uses a single webhook URL for all events. The handler routes by the `type` field in the JSON payload:

```
POST /webhooks/mailchimp/webhook → routes by payload["type"]:
  subscribe/unsubscribe/profile/upemail/cleaned → mailchimp.subscribers
  campaign                                       → mailchimp.campaigns
  send/open/click/bounce/unsub/abuse             → mailchimp.events

GET /webhooks/mailchimp/webhook → returns 200 (Mailchimp validation ping)
```

Record key: `payload["data"]["email"]` or `payload["data"]["id"]`

**Signature validation**: Mailchimp uses a shared secret key passed as a URL query parameter (`?secret=...`). This is **not HMAC-based** — it is a simple string equality check, fundamentally weaker than the Shopify/Stripe/HubSpot signature patterns. In production, this must be over HTTPS to avoid leaking the secret. Additionally, any reverse proxy or load balancer in front of the ingestion API must be configured to redact query parameters from access logs, as the secret would otherwise appear in plain text in HTTP logs and error monitoring tools.

```python
def validate_mailchimp_signature(secret_param: str, expected_secret: str) -> bool:
    """
    Validate Mailchimp webhook by comparing the query parameter secret
    against the configured secret. This is NOT HMAC — Mailchimp uses
    a shared secret in the URL, not a request body signature.
    """
    return hmac.compare_digest(secret_param, expected_secret)
```

**Config changes** (`config.py`):

The Settings class uses `env_prefix = "INGESTION_"`, so these fields map to `INGESTION_MAILCHIMP_WEBHOOK_SECRET` and `INGESTION_MAILCHIMP_ENABLED` env vars:

```python
# Mailchimp webhook settings
mailchimp_webhook_secret: Optional[str] = Field(
    default=None,
    description="Mailchimp webhook shared secret (query parameter validation)"
)
mailchimp_enabled: bool = Field(
    default=True,
    description="Enable Mailchimp webhook endpoints"
)
```

**Router registration** (`main.py`):

```python
if settings.mailchimp_enabled:
    app.include_router(mailchimp_router, prefix="/webhooks")
```

**`__init__.py` export**:

```python
from .mailchimp import router as mailchimp_router

__all__ = ["shopify_router", "stripe_router", "hubspot_router", "mailchimp_router"]
```

**Error handling for unknown event types**:
```python
topic = EVENT_TYPE_TO_TOPIC.get(event_type)
if topic is None:
    logger.warning(f"Unknown Mailchimp event type: {event_type}")
    return {"status": "ignored", "reason": "unknown_event_type"}
```

### 3.3 Infrastructure

| File | Action | Description |
|------|--------|-------------|
| `infrastructure/redpanda/init-topics.sh` | **Modify** | Add 3 new topics |
| `infrastructure/.env.example` | **Modify** | Add Mailchimp env vars |

**New Redpanda topics** (3 partitions each):

```
mailchimp.campaigns
mailchimp.events
mailchimp.subscribers
```

**New environment variables**:

```bash
INGESTION_MAILCHIMP_WEBHOOK_SECRET=mailchimp_dev_secret
```

Note: The `ingestion/app/config.py` Settings class uses `env_prefix = "INGESTION_"`, so the env var must be prefixed with `INGESTION_`. The existing `.env.example` lists `SHOPIFY_WEBHOOK_SECRET` without the prefix — this is an existing inconsistency (those env vars are likely not loaded by the Settings class, but signature validation is skipped in dev via `INGESTION_SKIP_SIGNATURE_VALIDATION=true`). The Mailchimp design uses the correct prefixed name.

Note: No `MAILCHIMP_API_KEY` is needed — this integration is webhook-based (Mailchimp pushes to us). An API key would only be needed for a future pull-based backfill, which is out of scope.

### 3.4 Flink Streaming Jobs (Raw Layer)

| File | Action |
|------|--------|
| `jobs/flink/mailchimp_campaigns_full.sql` | **Create** |
| `jobs/flink/mailchimp_events_full.sql` | **Create** |
| `jobs/flink/mailchimp_subscribers_full.sql` | **Create** |

Each follows the established 3-section pattern:

1. `CREATE TEMPORARY TABLE ... WITH ('connector'='kafka', 'topic'='mailchimp.*')`
2. `CREATE TABLE IF NOT EXISTS raw.mailchimp_* (...)`
3. `INSERT INTO raw.mailchimp_* SELECT ... CURRENT_TIMESTAMP as _loaded_at FROM source`

The Kafka source table must include `_webhook_received_at STRING` and `_webhook_event_type STRING` to match the fields added by the ingestion layer. The source table should also declare `_source STRING` and `_event_type STRING` (added by the Redpanda producer) but these are NOT propagated to the Iceberg sink table, consistent with existing sources.

**Flink consumer group IDs** (follow existing `flink-<source>-<entity>-raw` convention):
- `flink-mailchimp-campaigns-raw`
- `flink-mailchimp-events-raw`
- `flink-mailchimp-subscribers-raw`

**Type conversions**:
- ISO8601 timestamps → `TIMESTAMP(3)` via `TO_TIMESTAMP(REPLACE(REPLACE(...)))`
- JSON fields (`merge_fields`, `stats`, `location`, `settings`, `tracking`, `tags`) → stored as `STRING`
- `open_rate`, `click_rate` (DOUBLE) → `DECIMAL(5, 4)` via `CAST`

### 3.5 Raw Table Schemas

| File | Action |
|------|--------|
| `sql/00_raw/mailchimp/campaigns.sql` | **Create** |
| `sql/00_raw/mailchimp/events.sql` | **Create** |
| `sql/00_raw/mailchimp/subscribers.sql` | **Create** |

All follow existing raw layer conventions: append-only, `format-version = '2'`, `write.upsert.enabled = 'false'`, zstd compression, `_webhook_received_at` and `_loaded_at` metadata columns.

**Important**: The SQL files under `sql/` are **reference documentation only** — they are NOT executed by the pipeline. The actual `CREATE TABLE` statements are inline in the Flink SQL jobs (for raw tables) and in the Spark Python jobs (for staging/analytics/marts). The Flink-created tables use minimal properties (`format-version`, `write.upsert.enabled`) while the SQL reference files document the full intended properties including `zstd` compression.

### 3.6 Staging Schemas & Transforms

| File | Action |
|------|--------|
| `sql/01_staging/stg_mailchimp_campaigns.sql` | **Create** |
| `sql/01_staging/stg_mailchimp_events.sql` | **Create** |
| `sql/01_staging/stg_mailchimp_subscribers.sql` | **Create** |
| `jobs/spark/staging_batch.py` | **Modify** |

**Spark staging_batch.py** — new function registrations. Each staging function must contain its own inline `CREATE TABLE IF NOT EXISTS` statement (the SQL files under `sql/01_staging/` are reference docs only, not executed by the pipeline):

```python
STAGING_FUNCTIONS = {
    ...  # existing entries
    "mailchimp_campaigns": stage_mailchimp_campaigns,
    "mailchimp_events": stage_mailchimp_events,
    "mailchimp_subscribers": stage_mailchimp_subscribers,
}
```

**`stg_mailchimp_campaigns`**:

Derived fields:
- `click_to_open_rate`: `unique_clicks / NULLIF(unique_opens, 0)`
- `is_sms`: `campaign_type = 'sms'`
- `is_automated`: `campaign_type = 'automation'`

Partitioned by `months(send_time)`.

**`stg_mailchimp_events`**:

Key transformations:
- `email_normalized`: `lower(trim(email_address))`
- `location_country`, `location_region`: extracted from `location` JSON
- `is_sms_event`: `action IN ('sms_sent', 'sms_click')`
- `is_positive_engagement`: `action IN ('open', 'click', 'sms_click')`
- `is_negative_event`: `action IN ('bounce', 'unsub', 'abuse')`
- `event_date`: derived from `event_timestamp`

Partitioned by `months(event_timestamp)`.

**`stg_mailchimp_subscribers`**:

Subscribers are mutable (status changes, profile updates). The raw layer is append-only, so the staging transform must deduplicate to get the latest state per subscriber within each incremental batch:

```python
# Dedup: keep latest record per subscriber_id within the batch
window = Window.partitionBy("subscriber_id").orderBy(col("_loaded_at").desc())
deduped_df = raw_df.withColumn("_rn", row_number().over(window)) \
    .filter(col("_rn") == 1) \
    .drop("_rn")
```

This follows the same `row_number()` pattern used in `entity_backfill.py` and `marts_incremental.py`. Note: HubSpot contacts are also mutable but the existing staging code does not dedup — this is a known gap. The Mailchimp integration adds dedup from the start to avoid accumulating stale rows.

**Cross-batch dedup strategy**: The dedup above is within-batch only (deduplicates multiple raw records for the same subscriber within a single incremental window). Across batches, the staging table will contain multiple records per subscriber — one per batch that included an update. Downstream consumers (`customer_360`, entity resolution) must resolve the latest state by selecting the record with the most recent `_staged_at` per `subscriber_id`. The `customer_360` build in `marts_incremental.py` uses a `row_number()` window ordered by `_staged_at DESC` to pick the latest subscriber record per entity, consistent with how other mutable entities are handled at the marts layer.

Key transformations (entity resolution depends on `email_normalized`, `first_name`, `last_name`, `full_name`, `phone_normalized`):
- `email_normalized`: `lower(trim(email_address))`
- `first_name`, `last_name`, `phone`: extracted from `merge_fields` JSON
- `full_name`: `concat(first_name, ' ', last_name)` — required by entity backfill union schema
- `phone_normalized`: digits only (for entity resolution matching)
- `avg_open_rate`, `avg_click_rate`: extracted from `stats` JSON
- `has_sms`: `phone IS NOT NULL AND sms_status = 'subscribed'`
- `is_active`: `status = 'subscribed'`
- `days_since_signup`: `datediff(current_date(), signup_timestamp)`

**Partitioned by `months(COALESCE(signup_timestamp, _loaded_at))`**. This is preferred over `months(_staged_at)` because it groups subscribers by cohort for analytical queries. The `COALESCE` fallback to `_loaded_at` handles cases where `signup_timestamp` is NULL (e.g., subscribers imported via API or cleaned records without signup data). The trade-off is that late-arriving updates to old subscribers create small files in historical partitions, but Iceberg compaction handles this adequately at the expected data volume.

### 3.7 Entity Resolution

| File | Action | Description |
|------|--------|-------------|
| `sql/02_semantic/entity_index.sql` | **Modify** | Add `'mailchimp'` to source documentation |
| `jobs/spark/entity_backfill.py` | **Modify** | Add Mailchimp to `get_all_staging_customers()` and `rebuild_blocking_index()` |

Mailchimp has direct email and phone matching — identical to existing sources. The query must match the union schema used by Shopify/Stripe/HubSpot (`source, source_id, email, first_name, last_name, full_name, phone, address, city, state, zip, country, created_at, _staged_at`):

```python
mailchimp_customers = spark.sql(f"""
    SELECT
        'mailchimp_subscribers' AS source,
        subscriber_id AS source_id,
        email_normalized AS email,
        first_name,
        last_name,
        full_name,
        phone_normalized AS phone,
        CAST(NULL AS STRING) AS address,
        CAST(NULL AS STRING) AS city,
        CAST(NULL AS STRING) AS state,
        CAST(NULL AS STRING) AS zip,
        CAST(NULL AS STRING) AS country,
        signup_timestamp AS created_at,
        _staged_at
    FROM iceberg.staging.stg_mailchimp_subscribers
    WHERE 1=1 {date_filter}
""")
```

Note: No `WHERE status` filter. The entity index tracks identity, not engagement status. An unsubscribed user who has records in Shopify/Stripe/HubSpot should still be entity-resolved. Status-based filtering belongs in downstream analytics and marts layers.

**Known limitation — email plus-addressing**: Users who subscribe to Mailchimp with alias emails (e.g., `john+newsletter@gmail.com`) will not match their Shopify/Stripe/HubSpot records under `john@gmail.com`. This is a known limitation of exact email matching. Normalizing `+` aliases (stripping `+tag` from Gmail-like addresses) in `email_normalized` is a potential future improvement but is out of scope for v1 due to the risk of false positives with non-Gmail providers where `+` is part of the actual address.

This unions with the existing Shopify/Stripe/HubSpot customer queries via `shopify.union(hubspot).union(stripe).union(mailchimp)`. The blocking index picks up Mailchimp subscribers by email and phone automatically.

Note: Mailchimp subscribers lack address fields, so all address columns are `CAST(NULL AS STRING)`. This is analogous to how any source with missing fields handles the union. Campaigns and events do not feed into entity resolution — only subscribers carry identity data. This mirrors how `shopify.products` exists as a topic but does not participate in entity resolution.

**`rebuild_blocking_index()` changes** (`entity_backfill.py`):

The `rebuild_blocking_index()` function hardcodes LEFT JOINs per source to resolve entity attributes. A 4th LEFT JOIN must be added for Mailchimp, and all COALESCE chains must include Mailchimp columns:

```python
# In the entities query inside rebuild_blocking_index():
entities = spark.sql("""
    SELECT
        ei.unified_id,
        ei.source,
        ei.source_id,
        LOWER(TRIM(COALESCE(hc.email, sc.email, stc.email, mc.email_normalized))) AS normalized_email,
        REGEXP_REPLACE(COALESCE(hc.phone, hc.mobile_phone, sc.phone, stc.phone, mc.phone_normalized), '[^0-9+]', '') AS normalized_phone,
        COALESCE(hc.last_name, sc.last_name, stc.last_name, mc.last_name) AS last_name,
        COALESCE(hc.zip, sc.zip, stc.postal_code) AS zip
    FROM iceberg.semantic.entity_index ei
    LEFT JOIN iceberg.staging.stg_shopify_customers sc
        ON ei.source = 'shopify_customers'
        AND ei.source_id = CAST(sc.customer_id AS STRING)
    LEFT JOIN iceberg.staging.stg_hubspot_contacts hc
        ON ei.source = 'hubspot_contacts'
        AND ei.source_id = hc.contact_id
    LEFT JOIN iceberg.staging.stg_stripe_customers stc
        ON ei.source = 'stripe_customers'
        AND ei.source_id = stc.customer_id
    LEFT JOIN iceberg.staging.stg_mailchimp_subscribers mc
        ON ei.source = 'mailchimp_subscribers'
        AND ei.source_id = mc.subscriber_id
    WHERE ei.entity_type = 'customer'
      AND ei.linked_to_unified_id IS NULL
""")
```

Note: This per-source LEFT JOIN pattern works at 4 sources but becomes a maintenance concern at 6+. A tech debt item should be filed to evaluate a source registry / configuration-driven pattern before adding source #6.

### 3.8 Analytics Layer

| File | Action |
|------|--------|
| `sql/04_analytics/campaign_metrics.sql` | **Create** |
| `jobs/spark/analytics_incremental.py` | **Modify** |

**Spark analytics_incremental.py** — new function registration:

```python
ANALYTICS_FUNCTIONS = {
    ...  # existing entries
    "campaign_metrics": compute_campaign_metrics,
}
```

**`analytics.campaign_metrics`**:

Joins `stg_mailchimp_campaigns` with aggregated `stg_mailchimp_events` to compute per-campaign metrics:

- Volume: `total_sent`, `total_delivered` (sent - bounces)
- Engagement: `total_opens`, `unique_opens`, `total_clicks`, `unique_clicks`
- Negative: `total_bounces`, `hard_bounces`, `soft_bounces`, `total_unsubscribes`
- Rates: `delivery_rate`, `open_rate`, `click_rate`, `click_to_open_rate`, `bounce_rate`, `unsubscribe_rate`
- SMS: `sms_sent`, `sms_clicks`, `sms_click_rate`
- Engagement score (rate-based, not count-based):

```sql
engagement_score = (
    (open_rate * 25) +
    (click_rate * 50) +
    (click_to_open_rate * 25) -
    (bounce_rate * 25) -
    (unsubscribe_rate * 50)
)
-- Theoretical range: -75 to +100
-- All input rates are 0.0–1.0, so the actual range is bounded
-- Tier thresholds (calibrate against real data after initial load):
--   excellent > 60, good > 40, average > 20, poor <= 20
```

Partitioned by `months(send_time)`.

### 3.9 Marts Layer

| File | Action | Description |
|------|--------|-------------|
| `sql/05_marts/customer_360.sql` | **Modify** | Add Mailchimp columns |
| `sql/05_marts/campaign_dashboard.sql` | **Create** | Campaign performance dashboard |
| `jobs/spark/marts_incremental.py` | **Modify** | Add campaign dashboard build, update customer_360 |

**Spark marts_incremental.py** — new function registration:

```python
MARTS_FUNCTIONS = {
    ...  # existing entries
    "campaign_dashboard": build_campaign_dashboard,
}
```

**New `customer_360` columns** (joined via `entity_id → mailchimp subscriber`):

Note: The actual Spark code in `marts_incremental.py` uses `has_shopify`, `has_hubspot`, `has_stripe` (not `has_*_profile`). The SQL reference file `sql/05_marts/customer_360.sql` uses `has_shopify_profile` etc. The Mailchimp columns follow the **Spark code convention** since that is what actually executes:

```sql
has_mailchimp           BOOLEAN,    -- Consistent with has_shopify, has_hubspot, has_stripe in Spark code
mailchimp_subscriber_id STRING,
mailchimp_status        STRING,     -- subscribed, unsubscribed, etc.
email_open_rate         DECIMAL(5, 4),
email_click_rate        DECIMAL(5, 4),
total_emails_received   BIGINT,
total_emails_opened     BIGINT,
total_emails_clicked    BIGINT,
total_sms_received      BIGINT,
total_sms_clicked       BIGINT,
has_sms                 BOOLEAN,
last_email_open_date    DATE,
last_email_click_date   DATE,
days_since_last_email   INT,
source_count            INT,        -- Now counts up to 4 sources
```

Note: `has_stripe` is added dynamically at runtime via a LEFT JOIN (not in the `CREATE TABLE`). The Mailchimp columns (`has_mailchimp`, `mailchimp_subscriber_id`, etc.) should follow the same pattern — added dynamically via a LEFT JOIN with `stg_mailchimp_subscribers` aggregated data, with a fallback to default values when the join does not match.

**`marts.campaign_dashboard`**: Denormalized campaign scorecard with `performance_tier` derived from engagement score thresholds.

Partitioned by `send_month` (STRING, `YYYY-MM` format).

### 3.10 Airflow DAG

| File | Action |
|------|--------|
| `airflow/dags/iceberg_pipeline.py` | **Modify** |

**New tasks**:

```python
# STAGING (parallel with existing staging tasks):
stg_mailchimp_campaigns = BashOperator(
    task_id="stg_mailchimp_campaigns",
    bash_command=f"{SPARK_SUBMIT} {SPARK_JOBS_PATH}/staging_batch.py --table mailchimp_campaigns --mode incremental"
)
stg_mailchimp_events = BashOperator(
    task_id="stg_mailchimp_events",
    bash_command=f"{SPARK_SUBMIT} {SPARK_JOBS_PATH}/staging_batch.py --table mailchimp_events --mode incremental"
)
stg_mailchimp_subscribers = BashOperator(
    task_id="stg_mailchimp_subscribers",
    bash_command=f"{SPARK_SUBMIT} {SPARK_JOBS_PATH}/staging_batch.py --table mailchimp_subscribers --mode incremental"
)

# ANALYTICS (parallel with existing analytics tasks):
campaign_metrics = BashOperator(
    task_id="campaign_metrics",
    bash_command=f"{SPARK_SUBMIT} {SPARK_JOBS_PATH}/analytics_incremental.py --table campaign_metrics --mode incremental"
)

# MARTS (parallel with existing marts tasks):
campaign_dashboard = BashOperator(
    task_id="campaign_dashboard",
    bash_command=f"{SPARK_SUBMIT} {SPARK_JOBS_PATH}/marts_incremental.py --table campaign_dashboard --mode incremental"
)
```

**Dependencies**:

```python
# Start includes Mailchimp staging (parallel with existing)
start >> [stg_mailchimp_campaigns, stg_mailchimp_events, stg_mailchimp_subscribers]

# Mailchimp staging → analytics → marts
[stg_mailchimp_campaigns, stg_mailchimp_events] >> campaign_metrics >> campaign_dashboard

# Mailchimp subscribers feed into entity resolution → customer_360
stg_mailchimp_subscribers >> entity_index

# campaign_dashboard feeds into end (updated from existing)
[customer_360, sales_dashboard, campaign_dashboard] >> end
```

**Clarification on `core_customers`**: The existing dependency is `[entity_index, stg_shopify_customers, stg_stripe_customers, stg_hubspot_contacts] >> core_customers`. Mailchimp subscribers do NOT feed into `core_customers` — they flow through entity resolution directly to `customer_360`. This is consistent with the design decision in Section 3.14 (no changes to `core_views.py`).

### 3.11 Monitoring & ClickHouse

| File | Action |
|------|--------|
| `monitoring/dashboards/streaming_business.json` | **Modify** — add Mailchimp message rate panel |
| `monitoring/dashboards/batch_business.json` | **Modify** — add campaign summary panel |
| `infrastructure/clickhouse/iceberg_setup.sql` | **Modify** — add Iceberg views for all Mailchimp tables |

**ClickHouse views** — add views for all new tables following the existing pattern. The existing setup creates views for raw, staging, analytics, and marts layers. All Mailchimp tables need corresponding views (8 total):

```sql
-- Raw layer
CREATE OR REPLACE VIEW iceberg.raw_mailchimp_campaigns AS
SELECT * FROM iceberg('http://minio:9000/warehouse/raw/mailchimp_campaigns/',
                      '__MINIO_USER__', '__MINIO_PASSWORD__');

CREATE OR REPLACE VIEW iceberg.raw_mailchimp_events AS
SELECT * FROM iceberg('http://minio:9000/warehouse/raw/mailchimp_events/',
                      '__MINIO_USER__', '__MINIO_PASSWORD__');

CREATE OR REPLACE VIEW iceberg.raw_mailchimp_subscribers AS
SELECT * FROM iceberg('http://minio:9000/warehouse/raw/mailchimp_subscribers/',
                      '__MINIO_USER__', '__MINIO_PASSWORD__');

-- Staging layer
CREATE OR REPLACE VIEW iceberg.stg_mailchimp_campaigns AS
SELECT * FROM iceberg('http://minio:9000/warehouse/staging/stg_mailchimp_campaigns/',
                      '__MINIO_USER__', '__MINIO_PASSWORD__');

CREATE OR REPLACE VIEW iceberg.stg_mailchimp_events AS
SELECT * FROM iceberg('http://minio:9000/warehouse/staging/stg_mailchimp_events/',
                      '__MINIO_USER__', '__MINIO_PASSWORD__');

CREATE OR REPLACE VIEW iceberg.stg_mailchimp_subscribers AS
SELECT * FROM iceberg('http://minio:9000/warehouse/staging/stg_mailchimp_subscribers/',
                      '__MINIO_USER__', '__MINIO_PASSWORD__');

-- Analytics layer
CREATE OR REPLACE VIEW iceberg.campaign_metrics AS
SELECT * FROM iceberg('http://minio:9000/warehouse/analytics/campaign_metrics/',
                      '__MINIO_USER__', '__MINIO_PASSWORD__');

-- Marts layer
CREATE OR REPLACE VIEW iceberg.campaign_dashboard AS
SELECT * FROM iceberg('http://minio:9000/warehouse/marts/campaign_dashboard/',
                      '__MINIO_USER__', '__MINIO_PASSWORD__');
```

**Monitoring panels**:

| Dashboard | Panel | Type | Metric |
|-----------|-------|------|--------|
| `streaming_business.json` | Mailchimp Message Rate | timeseries | `messages_per_second{topic=~"mailchimp.*"}` |
| `batch_business.json` | Mailchimp Campaign Summary | stat | Row count from `analytics.campaign_metrics` |

### 3.12 API Schema

| File | Action | Description |
|------|--------|-------------|
| `schemas/mailchimp.json` | **Create** | JSON schema for Mailchimp webhook payloads |

Follows the existing pattern of `schemas/shopify.json`, `schemas/stripe.json`, `schemas/hubspot.json`.

### 3.13 Scripts

| File | Action | Description |
|------|--------|-------------|
| `scripts/validate_tables.sh` | **Modify** | Add Mailchimp tables to validation query |
| `scripts/reset_and_run.sh` | **Modify** | Add Mailchimp Flink jobs to `submit_flink_jobs()` loop |

**`validate_tables.sh`** — add row count checks for all new Mailchimp tables:

```sql
UNION ALL SELECT 'raw.mailchimp_campaigns', COUNT(*) FROM iceberg.raw.mailchimp_campaigns
UNION ALL SELECT 'raw.mailchimp_events', COUNT(*) FROM iceberg.raw.mailchimp_events
UNION ALL SELECT 'raw.mailchimp_subscribers', COUNT(*) FROM iceberg.raw.mailchimp_subscribers
UNION ALL SELECT 'staging.stg_mailchimp_campaigns', COUNT(*) FROM iceberg.staging.stg_mailchimp_campaigns
UNION ALL SELECT 'staging.stg_mailchimp_events', COUNT(*) FROM iceberg.staging.stg_mailchimp_events
UNION ALL SELECT 'staging.stg_mailchimp_subscribers', COUNT(*) FROM iceberg.staging.stg_mailchimp_subscribers
UNION ALL SELECT 'analytics.campaign_metrics', COUNT(*) FROM iceberg.analytics.campaign_metrics
UNION ALL SELECT 'marts.campaign_dashboard', COUNT(*) FROM iceberg.marts.campaign_dashboard
```

**`reset_and_run.sh`** — multiple hardcoded loops must be updated for Mailchimp. The script has **5 loops** that enumerate sources/tables/topics:

1. **`submit_flink_jobs()` — Flink job submission loop (~line 411)**:
```bash
# Current:
for job in shopify_orders shopify_customers stripe_charges stripe_customers hubspot_contacts; do
# Updated:
for job in shopify_orders shopify_customers stripe_charges stripe_customers hubspot_contacts mailchimp_campaigns mailchimp_events mailchimp_subscribers; do
```

2. **`submit_flink_jobs()` — raw table validation loop (~line 427)**:
```bash
# Updated:
for table in shopify_orders shopify_customers stripe_charges stripe_customers hubspot_contacts mailchimp_campaigns mailchimp_events mailchimp_subscribers; do
```

3. **`submit_flink_jobs()` — Redpanda topic validation loop (~line 436)**:
```bash
# Updated:
for topic in shopify.orders shopify.customers stripe.charges stripe.customers hubspot.contacts mailchimp.campaigns mailchimp.events mailchimp.subscribers; do
```

4. **`run_batch_pipeline()` — staging batch loop (~line 505)**:
```bash
# Updated:
for table in shopify_orders shopify_customers stripe_charges stripe_customers hubspot_contacts mailchimp_campaigns mailchimp_events mailchimp_subscribers; do
```

5. **`run_batch_pipeline()` — staging table validation loop (~line 514)**:
```bash
# Updated:
for table in stg_shopify_orders stg_shopify_customers stg_stripe_charges stg_stripe_customers stg_hubspot_contacts stg_mailchimp_campaigns stg_mailchimp_events stg_mailchimp_subscribers; do
```

6. **`validate_tables()` — tables array (~line 621)**: Add all Mailchimp tables to the `tables=()` array.

7. **`run_batch_pipeline()` — marts validation loop (~line 572)**: Add `campaign_dashboard` to the marts table list.

8. **`generate_mock_data()` — data generation settings and log output (~lines 61-65, 382-384)**: Add Mailchimp defaults (`MAILCHIMP_SUBSCRIBERS`, `MAILCHIMP_CAMPAIGNS`, `MAILCHIMP_EVENTS`) and extend `post_mock_data.py` CLI args.

### 3.14 Core Layer

No changes needed for `jobs/spark/core_views.py`. The core layer defines unified business objects (`core_customers`, `core_orders`), and Mailchimp does not introduce new object types that fit the core model. Mailchimp subscriber data flows into `customer_360` via the entity resolution path instead.

---

## 4. File Change Summary

### New Files (14)

| # | File | Layer |
|---|------|-------|
| 1 | `datagen/providers/mailchimp_provider.py` | Data Generation |
| 2 | `ingestion/app/webhooks/mailchimp.py` | Ingestion API |
| 3 | `jobs/flink/mailchimp_campaigns_full.sql` | Streaming (Raw) |
| 4 | `jobs/flink/mailchimp_events_full.sql` | Streaming (Raw) |
| 5 | `jobs/flink/mailchimp_subscribers_full.sql` | Streaming (Raw) |
| 6 | `sql/00_raw/mailchimp/campaigns.sql` | Schema (Raw) |
| 7 | `sql/00_raw/mailchimp/events.sql` | Schema (Raw) |
| 8 | `sql/00_raw/mailchimp/subscribers.sql` | Schema (Raw) |
| 9 | `sql/01_staging/stg_mailchimp_campaigns.sql` | Schema (Staging) |
| 10 | `sql/01_staging/stg_mailchimp_events.sql` | Schema (Staging) |
| 11 | `sql/01_staging/stg_mailchimp_subscribers.sql` | Schema (Staging) |
| 12 | `sql/04_analytics/campaign_metrics.sql` | Schema (Analytics) |
| 13 | `sql/05_marts/campaign_dashboard.sql` | Schema (Marts) |
| 14 | `schemas/mailchimp.json` | API Schema |

### Modified Files (22)

| # | File | Changes |
|---|------|---------|
| 1 | `datagen/generator.py` | Add `MailchimpProvider`, `generate_mailchimp_data()`, extend shared pool |
| 2 | `datagen/simulate_webhooks.py` | Add Mailchimp webhook posting methods |
| 3 | `datagen/providers/__init__.py` | Add `MailchimpProvider` to imports and `__all__` |
| 4 | `ingestion/app/webhooks/__init__.py` | Export `mailchimp_router` |
| 5 | `ingestion/app/validators/signatures.py` | Add `validate_mailchimp_signature()` |
| 6 | `ingestion/app/validators/__init__.py` | Export `validate_mailchimp_signature` |
| 7 | `ingestion/app/config.py` | Add `mailchimp_webhook_secret` and `mailchimp_enabled` settings |
| 8 | `ingestion/app/main.py` | Register Mailchimp router (gated by `mailchimp_enabled`) |
| 9 | `infrastructure/redpanda/init-topics.sh` | Add 3 topics |
| 10 | `infrastructure/.env.example` | Add `INGESTION_MAILCHIMP_WEBHOOK_SECRET` |
| 11 | `jobs/spark/staging_batch.py` | Add 3 staging functions (with inline `CREATE TABLE`) + `STAGING_FUNCTIONS` entries |
| 12 | `jobs/spark/entity_backfill.py` | Add Mailchimp to `get_all_staging_customers()` union and `rebuild_blocking_index()` LEFT JOIN |
| 13 | `jobs/spark/analytics_incremental.py` | Add `compute_campaign_metrics` + `ANALYTICS_FUNCTIONS` entry |
| 14 | `jobs/spark/marts_incremental.py` | Add `build_campaign_dashboard` + `MARTS_FUNCTIONS` entry, update `customer_360` (dynamic Mailchimp columns via LEFT JOIN) |
| 15 | `airflow/dags/iceberg_pipeline.py` | Add 5 tasks, update dependencies (including `end` task) |
| 16 | `sql/02_semantic/entity_index.sql` | Add `'mailchimp'` to source documentation |
| 17 | `sql/05_marts/customer_360.sql` | Add Mailchimp columns (reference doc) |
| 18 | `infrastructure/clickhouse/iceberg_setup.sql` | Add 8 Iceberg views (3 raw + 3 staging + 1 analytics + 1 marts) |
| 19 | `monitoring/dashboards/streaming_business.json` | Add Mailchimp rate panel |
| 20 | `monitoring/dashboards/batch_business.json` | Add campaign summary panel |
| 21 | `scripts/validate_tables.sh` | Add Mailchimp tables to validation query |
| 22 | `scripts/reset_and_run.sh` | Update all 8 hardcoded loops (see Section 3.13) |

---

## 5. Data Flow

```
datagen/providers/mailchimp_provider.py
  │
  ▼
datagen/simulate_webhooks.py
  │  POST /webhooks/mailchimp/webhook  (JSON body, ?secret=... query param)
  ▼
ingestion/app/webhooks/mailchimp.py
  │  validate secret → route by payload["type"] → enrich with _webhook_event_type → publish to Redpanda
  ▼
Redpanda: mailchimp.campaigns | .events | .subscribers
  │
  ▼
Flink: mailchimp_*_full.sql
  │  Kafka → Iceberg (append-only)
  ▼
raw.mailchimp_campaigns | raw.mailchimp_events | raw.mailchimp_subscribers
  │
  ▼ (Airflow triggers Spark)
staging.stg_mailchimp_campaigns | .stg_mailchimp_events | .stg_mailchimp_subscribers
  │                                                           │
  ├──► analytics.campaign_metrics ──► marts.campaign_dashboard │
  │                                                           │
  └───────────────────────────────────────────────────────────►│
                                                               ▼
                                              semantic.entity_index (email/phone match)
                                                               ▼
                                              marts.customer_360 (updated)
```

---

## 6. Implementation Phases

Each phase is implemented, tested, and committed before moving to the next. This ensures fail-fast behavior and keeps each commit in a working state.

**Test execution environment**: Unit tests run via `pytest` with PySpark local mode (no Docker required). Integration and E2E tests require the full Docker Compose stack (`docker-compose up -d`). All test commands are run from the host machine; Spark jobs execute inside the `iceberg-spark` container via `docker exec`.

### Phase 1 — Infrastructure

**Goal**: Redpanda topics exist, environment variables are configured.

**Files**:
- `infrastructure/redpanda/init-topics.sh` — add 3 topics
- `infrastructure/.env.example` — add `INGESTION_MAILCHIMP_WEBHOOK_SECRET`
- `scripts/reset_and_run.sh` — add 3 Mailchimp Flink jobs to `submit_flink_jobs()` loop

**Tests**:
| Type | What | How | Pass Criteria |
|------|------|-----|---------------|
| Integration | Topics created | `rpk topic list` inside Redpanda container | `mailchimp.campaigns`, `mailchimp.events`, `mailchimp.subscribers` all present with 3 partitions each |
| Integration | Env var loaded | `docker-compose config` or shell echo | `INGESTION_MAILCHIMP_WEBHOOK_SECRET` resolves to expected value |

### Phase 2 — Data Generation & Ingestion

**Goal**: Mock data can be generated and posted to webhook endpoints that route messages to the correct Redpanda topics.

**Files (create)**:
- `datagen/providers/mailchimp_provider.py`
- `ingestion/app/webhooks/mailchimp.py`
- `schemas/mailchimp.json`

**Files (modify)**:
- `datagen/generator.py`
- `datagen/simulate_webhooks.py`
- `datagen/providers/__init__.py`
- `ingestion/app/webhooks/__init__.py`
- `ingestion/app/validators/signatures.py`
- `ingestion/app/validators/__init__.py`
- `ingestion/app/config.py`
- `ingestion/app/main.py`

**Tests**:
| Type | What | How | Pass Criteria |
|------|------|-----|---------------|
| Unit | `MailchimpProvider` output format | Call each generate method, validate field types and value ranges | All fields present, `campaign_id` is 10-char hex, `subscriber_id` is MD5, `action` distribution within tolerance |
| Unit | `validate_mailchimp_signature()` | Pass matching/mismatching secrets | Returns `True` for match, `False` for mismatch; uses constant-time comparison |
| Unit | Event type routing | Call handler with each `type` value | Correct topic returned for each type; unknown types return `{"status": "ignored"}` |
| Integration | `POST /webhooks/mailchimp/webhook` | Send sample payloads via `httpx` / `TestClient` | 200 response, message appears in correct Redpanda topic |
| Integration | `GET /webhooks/mailchimp/webhook` | Send GET request (Mailchimp validation ping) | 200 response with empty or acknowledgment body |
| Integration | Secret validation on endpoint | POST without `?secret=` param or with wrong secret | 401/403 response |
| Integration | Shared customer pool | Generate data for all sources, check email overlap | >= 25% email overlap between Mailchimp and at least one other source |

### Phase 3 — Streaming (Flink Raw Layer)

**Goal**: Flink SQL jobs consume from Redpanda topics and write to raw Iceberg tables.

**Files (create)**:
- `jobs/flink/mailchimp_campaigns_full.sql`
- `jobs/flink/mailchimp_events_full.sql`
- `jobs/flink/mailchimp_subscribers_full.sql`
- `sql/00_raw/mailchimp/campaigns.sql`
- `sql/00_raw/mailchimp/events.sql`
- `sql/00_raw/mailchimp/subscribers.sql`

**Tests**:
| Type | What | How | Pass Criteria |
|------|------|-----|---------------|
| Integration | Raw table creation | Run DDL via Spark SQL or Trino | Tables exist in `raw` namespace with correct column types |
| Integration | Flink job submission | Submit each SQL job to Flink cluster | Jobs enter RUNNING state without errors |
| E2E | End-to-end raw ingest | Post webhooks → wait → query raw tables | Row count > 0 in all 3 raw tables; `_webhook_received_at`, `_webhook_event_type`, and `_loaded_at` populated; timestamps parse correctly |
| Data quality | Type conversions | Query raw tables for specific fields | ISO8601 strings converted to `TIMESTAMP(3)`; `open_rate`/`click_rate` are `DECIMAL(5,4)`; JSON fields stored as `STRING` |

### Phase 4 — Batch (Staging Layer)

**Goal**: Spark staging transforms produce clean, deduplicated, enriched tables.

**Files (create)**:
- `sql/01_staging/stg_mailchimp_campaigns.sql`
- `sql/01_staging/stg_mailchimp_events.sql`
- `sql/01_staging/stg_mailchimp_subscribers.sql`

**Files (modify)**:
- `jobs/spark/staging_batch.py`

**Tests**:
| Type | What | How | Pass Criteria |
|------|------|-----|---------------|
| Unit | Staging SQL transforms | Spark local session with fixture data | Derived fields computed correctly: `click_to_open_rate`, `is_sms`, `email_normalized`, `is_positive_engagement`, `full_name`, `has_sms`, `is_active`, `days_since_signup` |
| Unit | Subscriber dedup | Insert 3 records for same `subscriber_id` with different `_loaded_at` | Only latest record retained after transform |
| Integration | `staging_batch.py --table mailchimp_campaigns` | Run Spark job against raw tables with data | Staging table populated; row count matches expected; `_staged_at` populated |
| Integration | Incremental watermark | Run staging twice with new raw data between runs | Second run only processes new records (verify via `_staged_at` range) |
| Data quality | Partitioning | Query `stg_mailchimp_events` metadata | Partitioned by `months(event_timestamp)` |

### Phase 5 — Semantic, Analytics & Marts

**Goal**: Entity resolution includes Mailchimp subscribers; campaign metrics computed; customer_360 and campaign dashboard populated.

**Files (create)**:
- `sql/04_analytics/campaign_metrics.sql`
- `sql/05_marts/campaign_dashboard.sql`

**Files (modify)**:
- `sql/02_semantic/entity_index.sql`
- `sql/05_marts/customer_360.sql`
- `jobs/spark/entity_backfill.py`
- `jobs/spark/analytics_incremental.py`
- `jobs/spark/marts_incremental.py`

**Tests**:
| Type | What | How | Pass Criteria |
|------|------|-----|---------------|
| Unit | Entity resolution query | Run `get_all_staging_customers()` with Mailchimp data | Mailchimp subscribers appear in union with `source = 'mailchimp_subscribers'` |
| Unit | Engagement score formula | Compute score for known input rates | Score matches expected value; edge cases (all zeros, max rates) produce values within theoretical range (-75 to +100); test with realistic Mailchimp-like distributions (e.g., open_rate ~0.20, click_rate ~0.03) to validate tier thresholds |
| Integration | Entity matching | Run `entity_backfill.py` with shared customers across sources | Mailchimp subscribers matched to existing entities by email/phone; `source_count` incremented |
| Integration | `campaign_metrics` | Run `analytics_incremental.py --table campaign_metrics` | Metrics table populated; `delivery_rate` = `(sent - bounces) / sent`; rates between 0.0 and 1.0 |
| Integration | `customer_360` update | Run `marts_incremental.py` | New Mailchimp columns populated: `has_mailchimp`, `mailchimp_subscriber_id`, `mailchimp_status`, `email_open_rate`, etc. |
| Integration | `campaign_dashboard` | Run `marts_incremental.py --table campaign_dashboard` | Dashboard table populated; `performance_tier` values are one of `excellent`, `good`, `average`, `poor` |
| Data quality | No status filter in entity index | Query entity index for unsubscribed Mailchimp users | Unsubscribed users present in entity index (status filtering belongs in marts) |

### Phase 6 — Orchestration & Monitoring

**Goal**: Airflow DAG includes Mailchimp tasks with correct dependencies; ClickHouse views and Grafana panels configured.

**Files (modify)**:
- `airflow/dags/iceberg_pipeline.py` — add 5 tasks, update dependency chains including `end` task
- `infrastructure/clickhouse/iceberg_setup.sql` — add 8 Iceberg views (3 raw + 3 staging + 1 analytics + 1 marts)
- `monitoring/dashboards/streaming_business.json`
- `monitoring/dashboards/batch_business.json`
- `scripts/validate_tables.sh` — add all Mailchimp tables to validation query
- `scripts/reset_and_run.sh` — update all 8 hardcoded loops (see Section 3.13)

**Tests**:
| Type | What | How | Pass Criteria |
|------|------|-----|---------------|
| Unit | DAG parse test | `python -c "from iceberg_pipeline import dag; assert dag"` | DAG loads without import errors |
| Unit | Task dependencies | Inspect DAG task graph | `stg_mailchimp_campaigns >> campaign_metrics >> campaign_dashboard`; `stg_mailchimp_subscribers >> entity_index`; `campaign_dashboard >> end` |
| Integration | ClickHouse views | Run `SELECT 1 FROM iceberg.raw_mailchimp_campaigns LIMIT 1` in ClickHouse | Query succeeds (views resolve to Iceberg tables via MinIO) |
| Integration | Grafana dashboard JSON | Validate JSON syntax and panel references | `streaming_business.json` contains Mailchimp message rate panel; `batch_business.json` contains campaign summary panel |
| E2E | Full pipeline run | `./scripts/reset_and_run.sh --validate` | All Mailchimp tables have rows; `validate_tables.sh` passes for all layers |

---

## 7. Future Work (Out of Scope)


The following items are explicitly out of scope for this integration but documented for future reference:

- **`monitoring/dashboards/campaign_analytics.json`**: Dedicated Grafana dashboard for campaign performance. Deferred to a follow-up after the base integration is validated.
- **Mailchimp API pull-based backfill**: If historical data needs to be backfilled from the Mailchimp API, a separate `MAILCHIMP_API_KEY` env var and backfill script would be needed.
- **HubSpot staging dedup**: The subscriber dedup pattern added here should be backported to `stage_hubspot_contacts` as a separate improvement.
- **Source registry pattern**: At 4 sources, the per-source LEFT JOINs in `rebuild_blocking_index()` and the manual file modifications per source (14 new, 20 modified) are manageable. Before adding source #6, evaluate a configuration-driven source registry that auto-generates Flink jobs, staging functions, entity resolution JOINs, and validation queries from a source definition file. This would reduce the per-source integration effort from ~34 files to ~5 source-specific files.
- **Email plus-addressing normalization**: Stripping `+tag` from Gmail-like addresses in `email_normalized` to improve entity resolution match rates. Requires careful handling to avoid false positives with non-Gmail providers.
- **Prometheus alerting rules**: Define alerts for Mailchimp topic consumer lag, staging job failures, and zero-row-count conditions. Currently only dashboard panels exist.
- **Fix `.env.example` webhook secret naming**: The existing `.env.example` lists `SHOPIFY_WEBHOOK_SECRET`, `STRIPE_WEBHOOK_SECRET`, and `HUBSPOT_CLIENT_SECRET` without the `INGESTION_` prefix required by `config.py`'s `env_prefix = "INGESTION_"`. These env vars are likely never loaded by the Settings class (signature validation is skipped in dev). Should be corrected to `INGESTION_SHOPIFY_WEBHOOK_SECRET`, etc.
