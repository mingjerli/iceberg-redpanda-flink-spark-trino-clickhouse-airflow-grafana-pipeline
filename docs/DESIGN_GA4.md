# Design: GA4 Analytics Integration (Batch)

This document describes the integration of Google Analytics 4 (GA4) as a **batch data source** into the existing pipeline. Unlike the existing webhook-based sources (Shopify, Stripe, HubSpot, Mailchimp), GA4 follows a **file-based batch ingestion pattern** that bypasses Redpanda and Flink entirely.

---

## 1. Why Batch?

GA4 does not send webhooks. In production, GA4 data is accessed via:

1. **BigQuery Export** — GA4 exports raw event-level data to BigQuery daily (free tier) or via streaming export (GA4 360, paid). This is the standard production path.
2. **GA4 Data API** — Pull-based REST API for pre-aggregated reports. Returns dimensional aggregations, not raw events.
3. **Measurement Protocol** — Write-only. Sends events *to* GA4 (server-side tracking), not *from* it.

For this demo, we mock a **BigQuery Export-style dataset** as CSV/Parquet files that a Spark job reads directly into the raw Iceberg layer. This is architecturally honest about how GA4 data actually flows in production systems.

### Architecture Comparison

```
EXISTING (Shopify/Stripe/HubSpot/Mailchimp):
  Webhook → Ingestion API → Redpanda → Flink → raw Iceberg → Spark staging

GA4 (this design):
  Mock BigQuery Export (Parquet files) → Spark → raw Iceberg → Spark staging
```

This introduces a **second ingestion pattern** into the pipeline, which is valuable for demonstrating that real data platforms always handle multiple ingestion modes.

---

## 2. Data Model

### 2.1 Single Entity: Events

GA4 has one primary entity: **events**. Everything in GA4 is an event — page views, clicks, purchases, sessions starts, etc.

Sessions are **not a separate entity**. They are derived from events by the pipeline using the 30-minute inactivity gap rule. This is how GA4 works internally and how BigQuery Export data is structured.

| Entity | Description |
|--------|-------------|
| `ga4_events` | Raw event-level data matching BigQuery Export schema |

Sessions (`stg_ga4_sessions`) are computed in the staging layer from raw events.

### 2.2 Event Fields (BigQuery Export Format)

```
client_id          STRING   -- GA4 client ID (cookie-based, e.g. "1234567890.1706500000")
user_id            STRING   -- Optional cross-device user ID (set by application)
event_name         STRING   -- page_view, purchase, add_to_cart, sign_up, scroll, click, etc.
event_timestamp    BIGINT   -- Microseconds since epoch (GA4 native format)
event_date         STRING   -- YYYYMMDD (GA4 BigQuery Export convention)
event_params       STRING   -- JSON array of {key, value} pairs
user_properties    STRING   -- JSON array of {key, value} pairs
traffic_source     STRING   -- JSON: {source, medium, campaign}
device             STRING   -- JSON: {category, os, browser, screen_resolution}
geo                STRING   -- JSON: {country, region, city}
page_location      STRING   -- Full URL
page_title         STRING   -- HTML title
page_referrer      STRING   -- Referrer URL
engagement_time_ms BIGINT   -- Milliseconds of active engagement
is_conversion      BOOLEAN  -- Whether event is marked as a conversion
currency           STRING   -- ISO 4217 (for ecommerce events)
value              DOUBLE   -- Monetary value (for ecommerce events)
session_id         STRING   -- GA4 session identifier (numeric string)
ga_session_number  INT      -- Session sequence number per user
```

Note: `event_timestamp` uses microseconds (not milliseconds). We preserve this precision through the raw and staging layers using `TIMESTAMP(6)`, truncating only at the analytics/marts layer.

---

## 3. Mock Data Generation

### 3.1 GA4 Provider

| File | Action |
|------|--------|
| `datagen/providers/ga4_provider.py` | **Create** |

**`GA4Provider`** class following the existing provider pattern:

```python
class GA4Provider:
    """Generate mock GA4 BigQuery Export data."""

    EVENT_WEIGHTS = {
        "page_view": 50,
        "scroll": 15,
        "click": 10,
        "view_item": 5,
        "add_to_cart": 8,
        "begin_checkout": 4,
        "purchase": 5,
        "sign_up": 3,
    }

    TRAFFIC_SOURCES = ["google", "facebook", "twitter", "email", "direct", "(none)"]
    TRAFFIC_MEDIUMS = ["organic", "cpc", "social", "email", "referral", "(none)"]
    DEVICE_CATEGORIES = ["desktop", "mobile", "tablet"]
    BROWSERS = ["Chrome", "Safari", "Firefox", "Edge", "Samsung Internet"]
    OS_LIST = ["Windows", "macOS", "iOS", "Android", "Linux"]

    def __init__(self, seed=None):
        self.fake = Faker()
        # ...
        self._session_counter = 1000000

    def generate_event(self, client_id=None, session_id=None, user_id=None) -> Dict: ...
    def generate_session_events(self, client_id=None, user_id=None, event_count=None) -> List[Dict]: ...
    def generate_export_batch(self, num_users, events_per_user_range, shared_customers=None) -> List[Dict]: ...
```

**Key generation logic**:

- **`client_id`**: Format `"{10_digit_random}.{unix_timestamp}"` to match GA4 cookie format
- **`event_timestamp`**: Microseconds since epoch (`int(time.time() * 1_000_000)`)
- **`event_date`**: `YYYYMMDD` string derived from event_timestamp (matches BigQuery Export)
- **`session_id`**: Numeric string. Events within a session share the same `session_id`. Sessions are bounded by 30-minute inactivity gaps.
- **`event_params`**: JSON array of `{key: str, value: {string_value/int_value/float_value/double_value}}` matching BigQuery Export nested schema
- **`user_id`**: For shared customers, set to the customer's email address (enables entity resolution in the demo)

**Session-coherent event generation** (`generate_session_events`):

The provider generates events in realistic session sequences, not as independent random events:

```python
def generate_session_events(self, client_id=None, user_id=None, event_count=None):
    """Generate a sequence of events forming a coherent session."""
    session_id = str(self._next_session_id())
    event_count = event_count or random.randint(2, 15)
    base_time = int(time.time() * 1_000_000) - random.randint(0, 86400 * 30) * 1_000_000

    events = []
    # First event is always session_start
    events.append(self._make_event("session_start", client_id, session_id, user_id, base_time))
    # Second event is always page_view (landing page)
    events.append(self._make_event("page_view", client_id, session_id, user_id, base_time + random.randint(100_000, 500_000)))

    # Remaining events follow weighted distribution with realistic timing
    for i in range(event_count - 2):
        event_name = random.choices(list(self.EVENT_WEIGHTS.keys()), weights=list(self.EVENT_WEIGHTS.values()))[0]
        offset = base_time + (i + 2) * random.randint(5_000_000, 300_000_000)  # 5s to 5min gaps
        events.append(self._make_event(event_name, client_id, session_id, user_id, offset))

    return events
```

This approach ensures:
- Events within a session share the same `session_id` and `client_id`
- Timestamps are monotonically increasing within a session
- Session starts always have a `session_start` event
- The staging layer can verify its session computation matches the mock `session_id`

**Export batch generation** (`generate_export_batch`):

```python
def generate_export_batch(self, num_users=200, events_per_user_range=(3, 20), shared_customers=None):
    """Generate a batch of events simulating a BigQuery Export table."""
    all_events = []
    for i in range(num_users):
        client_id = self._generate_client_id()

        # 30% of users get a user_id from the shared customer pool
        user_id = None
        if shared_customers and random.random() < 0.3:
            shared = random.choice(shared_customers)
            user_id = shared["email"]  # user_id = email for entity resolution demo

        num_events = random.randint(*events_per_user_range)
        # Generate 1-3 sessions per user
        num_sessions = random.randint(1, min(3, num_events // 2))
        for _ in range(num_sessions):
            session_events = self.generate_session_events(
                client_id=client_id,
                user_id=user_id,
                event_count=num_events // num_sessions,
            )
            all_events.extend(session_events)

    return all_events
```

### 3.2 Generator Integration

| File | Action |
|------|--------|
| `datagen/generator.py` | **Modify** |

Changes:
- Import `GA4Provider`
- Initialize `self.ga4 = GA4Provider(seed=seed)` in `__init__`
- Add `generate_ga4_data()` method
- Add GA4 to `generate_all()`
- Add `"ga4"` to CLI `--source` choices
- **Output format**: GA4 data is saved as **Parquet** (not JSONL) to simulate BigQuery Export

```python
def generate_ga4_data(
    self,
    num_users: int = 200,
    events_per_user: tuple = (3, 20),
) -> Dict[str, List[Dict]]:
    """Generate GA4 BigQuery Export-style data."""
    events = self.ga4.generate_export_batch(
        num_users=num_users,
        events_per_user_range=events_per_user,
        shared_customers=self._shared_customers,
    )
    return {"events": events}

def save_to_files(self, data, output_dir, format="jsonl"):
    # Existing logic for JSONL sources...
    # For GA4: save as Parquet using pandas/pyarrow
    if "ga4" in data:
        ga4_dir = output_dir / "ga4"
        ga4_dir.mkdir(parents=True, exist_ok=True)
        df = pd.DataFrame(data["ga4"]["events"])
        df.to_parquet(ga4_dir / "events.parquet", index=False)
```

### 3.3 No Webhook Simulator Changes

GA4 is batch-only. There is no `simulate_webhooks.py` change — no webhooks to simulate. The data flows through the batch ingestion path instead.

---

## 4. Batch Ingestion (New Pattern)

### 4.1 Spark Batch Ingestion Job

| File | Action |
|------|--------|
| `jobs/spark/ga4_batch_ingest.py` | **Create** |

This is a **new ingestion pattern** — Spark reads Parquet files and writes directly to the raw Iceberg layer, bypassing Redpanda and Flink:

```python
"""
GA4 Batch Ingestion: Parquet → raw.ga4_events (Iceberg)

Simulates ingesting a GA4 BigQuery Export. In production, this would be:
  - A BigQuery-to-Iceberg connector
  - An Airflow task that queries BigQuery and writes to Iceberg
  - A scheduled Cloud Function that exports and loads data

For the demo, we read Parquet files from the mounted volume.
"""

def ingest_ga4_export(spark, input_path, mode="append"):
    """Read GA4 export Parquet and write to raw Iceberg."""

    df = spark.read.parquet(input_path)

    # Add ingestion metadata (matching the _loaded_at convention from Flink jobs)
    df = df.withColumn("_loaded_at", current_timestamp()) \
           .withColumn("_source_file", input_file_name())

    df.write \
        .format("iceberg") \
        .mode(mode) \
        .saveAsTable("iceberg.raw.ga4_events")
```

**Volume mount**: The Spark container already mounts `../jobs/spark` to `/opt/spark/jobs`. The GA4 Parquet files need to be accessible — mount the datagen output directory:

```yaml
# In docker-compose.yml, spark-master service:
volumes:
  - ../datagen/output:/opt/spark/data:ro  # GA4 export files
```

### 4.2 No Redpanda Topics, No Flink Jobs

GA4 does **not** get Redpanda topics or Flink streaming jobs. The data path is:

```
Parquet file → Spark (ga4_batch_ingest.py) → raw.ga4_events (Iceberg)
```

This is a deliberate architectural decision — not every data source is real-time, and the pipeline should demonstrate both patterns.

---

## 5. Raw Table Schema

| File | Action |
|------|--------|
| `sql/00_raw/ga4/events.sql` | **Create** |

```sql
CREATE TABLE IF NOT EXISTS raw.ga4_events (
    client_id          STRING,
    user_id            STRING,
    event_name         STRING,
    event_timestamp    BIGINT    COMMENT 'Microseconds since epoch (GA4 native)',
    event_date         STRING    COMMENT 'YYYYMMDD (GA4 BigQuery Export format)',
    event_params       STRING    COMMENT 'JSON array of {key, value} pairs',
    user_properties    STRING    COMMENT 'JSON array',
    traffic_source     STRING    COMMENT 'JSON: {source, medium, campaign}',
    device             STRING    COMMENT 'JSON: {category, os, browser, screen_resolution}',
    geo                STRING    COMMENT 'JSON: {country, region, city}',
    page_location      STRING,
    page_title         STRING,
    page_referrer      STRING,
    engagement_time_ms BIGINT,
    is_conversion      BOOLEAN,
    currency           STRING,
    value              DOUBLE,
    session_id         STRING,
    ga_session_number  INT,
    -- Ingestion metadata
    _loaded_at         TIMESTAMP COMMENT 'When loaded into Iceberg',
    _source_file       STRING    COMMENT 'Source Parquet file path'
) USING iceberg
PARTITIONED BY (event_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.upsert.enabled' = 'false',
    'write.parquet.compression-codec' = 'zstd'
)
```

Note: Partitioned by `event_date` (STRING in YYYYMMDD format) — this matches the GA4 BigQuery Export partitioning convention and enables efficient date-range queries without timestamp parsing.

---

## 6. Staging Schemas & Transforms

| File | Action |
|------|--------|
| `sql/01_staging/stg_ga4_events.sql` | **Create** |
| `sql/01_staging/stg_ga4_sessions.sql` | **Create** |
| `jobs/spark/staging_batch.py` | **Modify** |

### 6.1 `stg_ga4_events`

Cleaned event-level data with JSON fields extracted:

```sql
CREATE TABLE staging.stg_ga4_events (
    client_id               STRING,
    user_id                 STRING,
    session_id              STRING,
    event_name              STRING,
    event_timestamp         TIMESTAMP  COMMENT 'Converted from microseconds, TIMESTAMP(6) precision',
    -- Extracted from event_params JSON
    page_location           STRING,
    page_title              STRING,
    page_referrer           STRING,
    engagement_time_ms      BIGINT,
    -- Extracted from traffic_source JSON
    traffic_source          STRING,
    traffic_medium          STRING,
    traffic_campaign        STRING,
    -- Extracted from device JSON
    device_category         STRING,   -- desktop, mobile, tablet
    device_os               STRING,
    device_browser          STRING,
    -- Extracted from geo JSON
    geo_country             STRING,
    geo_region              STRING,
    geo_city                STRING,
    -- Event attributes
    is_conversion           BOOLEAN,
    event_value             DECIMAL(18, 2),
    currency                STRING,
    -- Derived fields
    event_date              DATE,     -- Derived from event_timestamp
    hour_of_day             INT,      -- 0-23
    is_ecommerce_event      BOOLEAN,  -- purchase, add_to_cart, begin_checkout, view_item
    is_engagement_event     BOOLEAN,  -- scroll, click, video_*, file_download
    -- Lineage
    _raw_client_id          STRING,
    _loaded_at              TIMESTAMP,
    _staged_at              TIMESTAMP
) USING iceberg
PARTITIONED BY (months(event_timestamp))
```

**Key transformations in Spark**:
- `event_timestamp`: `from_unixtime(event_timestamp / 1000000)` → preserve microseconds
- `traffic_source/medium/campaign`: `get_json_object(traffic_source, '$.source')` etc.
- `device_category/os/browser`: `get_json_object(device, '$.category')` etc.
- `geo_country/region/city`: `get_json_object(geo, '$.country')` etc.
- `is_ecommerce_event`: `event_name IN ('purchase', 'add_to_cart', 'begin_checkout', 'view_item')`

### 6.2 `stg_ga4_sessions` (Computed from Events)

Sessions are **derived from events** using the 30-minute inactivity gap rule:

```sql
CREATE TABLE staging.stg_ga4_sessions (
    session_id              STRING,
    client_id               STRING,
    user_id                 STRING,
    session_start           TIMESTAMP,
    session_end             TIMESTAMP,
    session_duration_sec    INT,
    event_count             INT,
    page_view_count         INT,
    is_engaged_session      BOOLEAN,
    -- Traffic attribution (from first event in session)
    traffic_source          STRING,
    traffic_medium          STRING,
    traffic_campaign        STRING,
    channel_group           STRING,   -- Derived: organic_search, paid_search, social, direct, email, referral
    -- Pages
    landing_page            STRING,   -- first page_location in session
    exit_page               STRING,   -- last page_location in session
    -- Device & geo (from first event)
    device_category         STRING,
    device_os               STRING,
    geo_country             STRING,
    geo_region              STRING,
    -- Engagement
    total_engagement_ms     BIGINT,
    conversions             INT,
    total_value             DECIMAL(18, 2),
    -- Derived
    session_date            DATE,
    is_bounce               BOOLEAN,   -- page_view_count <= 1 AND NOT is_engaged_session
    -- Lineage
    _loaded_at              TIMESTAMP,
    _staged_at              TIMESTAMP
) USING iceberg
PARTITIONED BY (months(session_start))
```

**Session computation in Spark** (`staging_batch.py`):

```python
def compute_ga4_sessions(spark):
    """Derive sessions from raw GA4 events using 30-min inactivity gap."""

    # Step 1: Detect session boundaries
    events_with_gaps = spark.sql("""
        SELECT *,
            event_timestamp - LAG(event_timestamp) OVER (
                PARTITION BY client_id ORDER BY event_timestamp
            ) as time_since_prev_event
        FROM staging.stg_ga4_events
    """)

    # Step 2: Mark new sessions (gap > 30 min or first event)
    events_with_sessions = spark.sql("""
        SELECT *,
            SUM(CASE
                WHEN time_since_prev_event IS NULL THEN 1
                WHEN time_since_prev_event > INTERVAL '30' MINUTE THEN 1
                ELSE 0
            END) OVER (PARTITION BY client_id ORDER BY event_timestamp) as computed_session_num
        FROM events_with_gaps
    """)

    # Step 3: Aggregate to session level
    sessions = spark.sql("""
        SELECT
            -- Use the source session_id if available, else generate from client_id + session_num
            COALESCE(
                FIRST(session_id),
                CONCAT(client_id, '_', CAST(computed_session_num AS STRING))
            ) as session_id,
            client_id,
            FIRST(user_id) as user_id,
            MIN(event_timestamp) as session_start,
            MAX(event_timestamp) as session_end,
            CAST(TIMESTAMPDIFF(SECOND, MIN(event_timestamp), MAX(event_timestamp)) AS INT) as session_duration_sec,
            COUNT(*) as event_count,
            SUM(CASE WHEN event_name = 'page_view' THEN 1 ELSE 0 END) as page_view_count,
            -- Engaged session: engagement_time > 10s OR 2+ page_views OR has conversion
            (SUM(engagement_time_ms) > 10000
             OR SUM(CASE WHEN event_name = 'page_view' THEN 1 ELSE 0 END) >= 2
             OR SUM(CASE WHEN is_conversion THEN 1 ELSE 0 END) > 0) as is_engaged_session,
            -- First-event attribution
            FIRST(traffic_source) as traffic_source,
            FIRST(traffic_medium) as traffic_medium,
            FIRST(traffic_campaign) as traffic_campaign,
            -- Landing/exit pages
            FIRST(CASE WHEN event_name = 'page_view' THEN page_location END) as landing_page,
            LAST(CASE WHEN event_name = 'page_view' THEN page_location END) as exit_page,
            -- Device/geo from first event
            FIRST(device_category) as device_category,
            FIRST(device_os) as device_os,
            FIRST(geo_country) as geo_country,
            FIRST(geo_region) as geo_region,
            -- Engagement aggregates
            SUM(engagement_time_ms) as total_engagement_ms,
            SUM(CASE WHEN is_conversion THEN 1 ELSE 0 END) as conversions,
            SUM(event_value) as total_value,
            -- Derived
            CAST(MIN(event_timestamp) AS DATE) as session_date,
            CURRENT_TIMESTAMP() as _staged_at
        FROM events_with_sessions
        GROUP BY client_id, computed_session_num
    """)
```

**Channel group derivation**:

```python
def derive_channel_group(source, medium):
    """Map traffic source/medium to channel group (GA4 default channel grouping logic)."""
    if source == '(direct)' or (source == '(none)' and medium == '(none)'):
        return 'direct'
    elif medium in ('organic', 'organic search'):
        return 'organic_search'
    elif medium in ('cpc', 'ppc', 'paid search'):
        return 'paid_search'
    elif medium in ('social', 'social-network', 'social media'):
        return 'social'
    elif medium == 'email':
        return 'email'
    elif medium == 'referral':
        return 'referral'
    elif medium == 'sms':
        return 'sms'
    else:
        return 'other'
```

---

## 7. Entity Resolution

| File | Action |
|------|--------|
| `sql/02_semantic/entity_index.sql` | **Modify** — add `'ga4'` to source docs |
| `jobs/spark/entity_backfill.py` | **Modify** — add GA4 to customer union |

**GA4 entity resolution approach**:

GA4 has no direct email or phone. Entity resolution depends on `user_id`, which is set by the application. In the demo, `user_id` is set to the customer's email for shared customers, enabling direct matching.

```python
ga4_customers = spark.sql("""
    SELECT DISTINCT
        'ga4' as source,
        user_id as source_id,
        user_id as email,              -- In demo, user_id = email
        CAST(NULL AS STRING) as first_name,
        CAST(NULL AS STRING) as last_name,
        CAST(NULL AS STRING) as phone,
        CAST(NULL AS STRING) as external_user_id,
        MIN(session_start) as created_at,
        MAX(_staged_at) as _staged_at
    FROM staging.stg_ga4_sessions
    WHERE user_id IS NOT NULL
    GROUP BY user_id
""")
```

**Production note**: In a real system, `user_id` would be an opaque application ID (not email). You would need a lookup table (`semantic.ga4_user_mapping`) maintained by the application to map `ga4_user_id → email`. This lookup is not implemented in the demo — the `user_id = email` simplification is documented and intentional.

---

## 8. Analytics Layer

| File | Action |
|------|--------|
| `sql/04_analytics/engagement_metrics.sql` | **Create** |
| `jobs/spark/analytics_incremental.py` | **Modify** |

**`analytics.engagement_metrics`** — daily aggregation from `stg_ga4_sessions`:

```sql
CREATE TABLE analytics.engagement_metrics (
    metric_date             DATE,
    -- Traffic
    total_sessions          BIGINT,
    engaged_sessions        BIGINT,
    engagement_rate         DECIMAL(5, 4),
    total_users             BIGINT,       -- distinct client_id
    new_users               BIGINT,
    returning_users         BIGINT,
    -- Content
    total_page_views        BIGINT,
    avg_pages_per_session   DECIMAL(8, 2),
    avg_session_duration    DECIMAL(8, 2),
    bounce_rate             DECIMAL(5, 4),
    -- Conversions
    total_conversions       BIGINT,
    conversion_rate         DECIMAL(5, 4),
    total_conversion_value  DECIMAL(18, 2),
    -- Channel counts
    organic_sessions        BIGINT,
    paid_sessions           BIGINT,
    direct_sessions         BIGINT,
    email_sessions          BIGINT,
    social_sessions         BIGINT,
    referral_sessions       BIGINT,
    sms_sessions            BIGINT,
    -- Device counts
    desktop_sessions        BIGINT,
    mobile_sessions         BIGINT,
    tablet_sessions         BIGINT,
    -- Top country
    top_country             STRING,
    top_country_sessions    BIGINT,
    -- Lineage
    _computed_at            TIMESTAMP
) USING iceberg
PARTITIONED BY (metric_date)
```

**`analytics.engagement_by_channel`** — per-channel daily breakdown:

```sql
CREATE TABLE analytics.engagement_by_channel (
    metric_date         DATE,
    channel_group       STRING,
    sessions            BIGINT,
    engaged_sessions    BIGINT,
    engagement_rate     DECIMAL(5, 4),
    conversions         BIGINT,
    conversion_value    DECIMAL(18, 2),
    conversion_rate     DECIMAL(5, 4),
    avg_session_duration DECIMAL(8, 2),
    bounce_rate         DECIMAL(5, 4),
    _computed_at        TIMESTAMP
) USING iceberg
PARTITIONED BY (metric_date)
```

---

## 9. Marts Layer

| File | Action |
|------|--------|
| `sql/05_marts/customer_360.sql` | **Modify** — add GA4 columns |
| `sql/05_marts/engagement_dashboard.sql` | **Create** |
| `jobs/spark/marts_incremental.py` | **Modify** |

**New `customer_360` columns** (joined via `entity_id → ga4 user_id`):

```sql
has_ga4                 BOOLEAN,
total_sessions          BIGINT,
total_page_views        BIGINT,
total_conversions       INT,
total_conversion_value  DECIMAL(18, 2),
avg_session_duration    DECIMAL(8, 2),
last_session_date       DATE,
top_traffic_source      STRING,
top_device_category     STRING,
days_since_last_visit   INT,
source_count            INT,        -- Now counts up to 5 sources (with Mailchimp)
```

**`marts.engagement_dashboard_daily`**: Daily engagement metrics with rolling averages for Grafana dashboards.

```sql
CREATE TABLE marts.engagement_dashboard_daily (
    date_key                DATE,
    day_of_week             INT,
    day_name                STRING,
    week_of_year            INT,
    month_key               STRING,    -- YYYY-MM
    -- Session metrics
    total_sessions          BIGINT,
    engaged_sessions        BIGINT,
    engagement_rate         DECIMAL(5, 4),
    unique_users            BIGINT,
    new_users               BIGINT,
    -- Content
    total_page_views        BIGINT,
    pages_per_session       DECIMAL(8, 2),
    avg_session_duration    DECIMAL(8, 2),
    bounce_rate             DECIMAL(5, 4),
    -- Conversions
    conversions             BIGINT,
    conversion_rate         DECIMAL(5, 4),
    conversion_value        DECIMAL(18, 2),
    -- Channels
    organic_sessions        BIGINT,
    paid_sessions           BIGINT,
    direct_sessions         BIGINT,
    email_sessions          BIGINT,
    social_sessions         BIGINT,
    sms_sessions            BIGINT,
    -- Device split
    desktop_pct             DECIMAL(5, 4),
    mobile_pct              DECIMAL(5, 4),
    -- Rolling averages
    sessions_7d_avg         DECIMAL(10, 2),
    sessions_30d_avg        DECIMAL(10, 2),
    conversion_rate_7d_avg  DECIMAL(5, 4),
    -- Top country
    top_country             STRING,
    _computed_at            TIMESTAMP
) USING iceberg
PARTITIONED BY (month_key)
```

---

## 10. Airflow DAG

| File | Action |
|------|--------|
| `airflow/dags/iceberg_pipeline.py` | **Modify** |

**New tasks**:

```
GA4 BATCH INGEST (before staging, new stage):
  + ga4_batch_ingest       → ga4_batch_ingest.py --input /opt/spark/data/ga4/events.parquet

STAGING (parallel):
  + stg_ga4_events         → staging_batch.py --table ga4_events
  + compute_ga4_sessions   → staging_batch.py --table ga4_sessions  (derives sessions from events)

ANALYTICS (parallel):
  + engagement_metrics     → analytics_incremental.py --table engagement_metrics

MARTS (parallel):
  + engagement_dashboard   → marts_incremental.py --table engagement_dashboard_daily
```

**Dependencies**:

```
start → ga4_batch_ingest → stg_ga4_events → compute_ga4_sessions → engagement_metrics → engagement_dashboard → end
                                                   ↓
                                           entity_index → customer_360
```

The `ga4_batch_ingest` task runs at the **beginning** of the pipeline (alongside the staging tasks for other sources) since GA4 data is loaded from files, not from Redpanda.

---

## 11. Infrastructure Changes

| File | Action | Description |
|------|--------|-------------|
| `infrastructure/docker-compose.yml` | **Modify** | Mount datagen output to Spark container |
| `infrastructure/.env.example` | **Modify** | Add GA4 env vars |

**Docker-compose change**:

```yaml
spark-master:
  volumes:
    - ../datagen/output:/opt/spark/data:ro  # GA4 export files
```

**New environment variables**:

```bash
# GA4 (batch ingestion, no webhook secret needed)
GA4_EXPORT_PATH=/opt/spark/data/ga4/events.parquet
GA4_MEASUREMENT_ID=G-XXXXXXXXXX  # For documentation only
```

**No Redpanda topics** — GA4 does not use Redpanda.

**No Flink jobs** — GA4 does not use Flink.

### 11.1 Monitoring & ClickHouse

| File | Action |
|------|--------|
| `monitoring/dashboards/batch_business.json` | **Modify** — add engagement summary panel |
| `monitoring/dashboards/engagement_analytics.json` | **Create** — GA4 engagement dashboard (Phase 2) |
| `infrastructure/clickhouse/iceberg_setup.sql` | **Modify** — add engagement_dashboard_daily table |

---

## 12. File Change Summary

### New Files (9)

| # | File | Layer |
|---|------|-------|
| 1 | `datagen/providers/ga4_provider.py` | Data Generation |
| 2 | `jobs/spark/ga4_batch_ingest.py` | Batch Ingestion (new pattern) |
| 3 | `sql/00_raw/ga4/events.sql` | Schema (Raw) |
| 4 | `sql/01_staging/stg_ga4_events.sql` | Schema (Staging) |
| 5 | `sql/01_staging/stg_ga4_sessions.sql` | Schema (Staging) |
| 6 | `sql/04_analytics/engagement_metrics.sql` | Schema (Analytics) |
| 7 | `sql/04_analytics/engagement_by_channel.sql` | Schema (Analytics) |
| 8 | `sql/05_marts/engagement_dashboard.sql` | Schema (Marts) |
| 9 | `monitoring/dashboards/engagement_analytics.json` | Monitoring |

### Modified Files (8)

| # | File | Changes |
|---|------|---------|
| 1 | `datagen/generator.py` | Add GA4Provider, `generate_ga4_data()`, Parquet output |
| 2 | `infrastructure/docker-compose.yml` | Mount datagen output to Spark |
| 3 | `infrastructure/.env.example` | Add GA4 env vars |
| 4 | `jobs/spark/staging_batch.py` | Add GA4 events staging + session computation |
| 5 | `jobs/spark/entity_backfill.py` | Add GA4 to customer union |
| 6 | `jobs/spark/analytics_incremental.py` | Add engagement_metrics, engagement_by_channel |
| 7 | `jobs/spark/marts_incremental.py` | Add engagement_dashboard, update customer_360 |
| 8 | `airflow/dags/iceberg_pipeline.py` | Add batch ingest task + 4 downstream tasks |
| 9 | `sql/02_semantic/entity_index.sql` | Add 'ga4' to source docs |
| 10 | `sql/05_marts/customer_360.sql` | Add GA4 engagement columns |
| 11 | `infrastructure/clickhouse/iceberg_setup.sql` | Add engagement_dashboard_daily table |
| 12 | `monitoring/dashboards/batch_business.json` | Add engagement summary panel |

---

## 13. Data Flow

```
datagen/providers/ga4_provider.py
  │  generate_export_batch()
  ▼
datagen/generator.py
  │  save_to_files() → output/ga4/events.parquet
  ▼
Docker volume mount: ../datagen/output → /opt/spark/data
  │
  ▼
jobs/spark/ga4_batch_ingest.py       ← NEW PATTERN (Parquet → Iceberg, no Redpanda/Flink)
  │  spark.read.parquet() → raw.ga4_events
  ▼
raw.ga4_events (Iceberg, append-only)
  │
  ▼ (Airflow triggers Spark staging)
staging.stg_ga4_events (cleaned, JSON extracted)
  │
  ├──► staging.stg_ga4_sessions (COMPUTED via 30-min gap rule)
  │       │
  │       ├──► analytics.engagement_metrics (daily aggregation)
  │       │       │
  │       │       ├──► analytics.engagement_by_channel (per-channel breakdown)
  │       │       │
  │       │       └──► marts.engagement_dashboard_daily
  │       │
  │       └──► semantic.entity_index (user_id matching, demo: user_id = email)
  │                   │
  │                   └──► marts.customer_360 (updated with GA4 columns)
  │
  └──► (event-level data available for ad-hoc queries via Trino)
```

---

## 14. Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| Batch (not streaming) | GA4 doesn't send webhooks. BigQuery Export is the real production path. |
| Parquet output (not JSONL) | Matches BigQuery Export format. Efficient for columnar reads by Spark. |
| No Redpanda topics | No real-time event stream for GA4. Data arrives as batch files. |
| No Flink jobs | Flink handles streaming ingestion. GA4 is batch — Spark handles it. |
| Sessions computed in staging | Sessions are derived from events, not a separate data product. Demonstrates window-function aggregation. |
| `TIMESTAMP(6)` in raw/staging | Preserves GA4's native microsecond precision. Truncates at analytics layer. |
| `event_date` partition (raw) | Matches BigQuery Export convention. Efficient date-range pruning. |
| `user_id = email` for demo | Simplification. Production needs a mapping table. Documented. |
| `engagement_by_channel` table | Enables the primary analytical question: which channels perform best. |
