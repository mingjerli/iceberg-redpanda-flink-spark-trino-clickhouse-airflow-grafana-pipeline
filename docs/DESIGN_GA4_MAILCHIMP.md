# Design: GA4 Analytics & Mailchimp Integration

This document describes the integration of two new data sources — **Google Analytics 4 (GA4)** and **Mailchimp** — into the existing pipeline. The design follows every convention established by the Shopify, Stripe, and HubSpot integrations.

---

## 1. Data Model Overview

### GA4 — Engagement Events

GA4 tracks user engagement on web and app properties. We model two entities:

| Entity | Topic | Description |
|--------|-------|-------------|
| `ga4.events` | Page views, clicks, conversions, custom events | Core engagement stream |
| `ga4.sessions` | Session-level aggregations with traffic source | Session attribution |

**Key fields per event**:

```
client_id          STRING   -- GA4 client ID (cookie-based, e.g. "1234567890.1706500000")
user_id            STRING   -- Optional cross-device user ID (set by app)
event_name         STRING   -- page_view, purchase, add_to_cart, sign_up, custom, etc.
event_timestamp    BIGINT   -- Microseconds since epoch (GA4 native format)
event_params       STRING   -- JSON array of {key, value} pairs
user_properties    STRING   -- JSON array of {key, value} pairs
traffic_source     STRING   -- JSON: {source, medium, campaign}
device             STRING   -- JSON: {category, os, browser, screen_resolution}
geo                STRING   -- JSON: {country, region, city}
page_location      STRING   -- Full URL
page_title         STRING   -- HTML title
page_referrer      STRING   -- Referrer URL
engagement_time_ms BIGINT   -- Milliseconds of active engagement
is_conversion      BOOLEAN  -- Whether event is a conversion
currency           STRING   -- ISO 4217 (for ecommerce events)
value              DOUBLE   -- Monetary value (for ecommerce events)
session_id         STRING   -- GA4 session identifier
```

**Key fields per session**:

```
session_id              STRING
client_id               STRING
user_id                 STRING
session_start           BIGINT    -- Microseconds since epoch
session_end             BIGINT
session_duration_sec    INT
event_count             INT
page_view_count         INT
engaged_session         BOOLEAN   -- engagement_time > 10s or 2+ page_views or conversion
traffic_source          STRING    -- JSON: {source, medium, campaign}
landing_page            STRING
exit_page               STRING
device                  STRING    -- JSON
geo                     STRING    -- JSON
is_direct               BOOLEAN
is_organic              BOOLEAN
is_paid                 BOOLEAN
total_engagement_ms     BIGINT
conversions             INT
total_value             DOUBLE
```

### Mailchimp — Email & SMS Campaigns

Mailchimp tracks campaign sends, opens, clicks, bounces, and subscriber activity. We model three entities:

| Entity | Topic | Description |
|--------|-------|-------------|
| `mailchimp.campaigns` | Campaign metadata (email + SMS) | Campaign definitions |
| `mailchimp.events` | Send, open, click, bounce, unsubscribe, sms_sent, sms_click | Member-level events |
| `mailchimp.subscribers` | List member profiles and subscription status | Subscriber master |

**Key fields per campaign**:

```
campaign_id        STRING   -- Mailchimp campaign ID (e.g. "abc123def4")
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
```

**Key fields per event**:

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
```

**Key fields per subscriber**:

```
subscriber_id      STRING   -- Mailchimp member ID (MD5 hash of email)
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
phone              STRING   -- SMS-enabled phone number
sms_status         STRING   -- subscribed, unsubscribed, non_subscribed
```

---

## 2. Files to Create / Modify

### 2.1 Mock Data Generation

| File | Action | Description |
|------|--------|-------------|
| `datagen/providers/ga4_provider.py` | **Create** | Faker provider for GA4 events and sessions |
| `datagen/providers/mailchimp_provider.py` | **Create** | Faker provider for campaigns, events, subscribers |
| `datagen/generator.py` | **Modify** | Add `generate_ga4_data()` and `generate_mailchimp_data()` methods; include GA4/Mailchimp customers in shared customer pool |
| `datagen/simulate_webhooks.py` | **Modify** | Add GA4 and Mailchimp webhook posting loops |

**GA4 Provider Details**:
- `client_id`: format `"{10_digit_random}.{unix_timestamp}"` to match GA4 cookie format
- `event_name`: weighted random from `[page_view (50%), scroll (15%), click (10%), purchase (5%), add_to_cart (8%), begin_checkout (4%), sign_up (3%), view_item (5%)]`
- `event_timestamp`: microseconds since epoch (multiply `time.time()` by 1,000,000)
- `event_params`: JSON array of `{key, value}` matching GA4 BigQuery export schema
- `session_id`: numeric string, grouped per client_id with 30-min inactivity gap
- Shared customer pool: link `user_id` to same emails used by Shopify/Stripe/HubSpot for entity resolution
- Webhook metadata: `_webhook_received_at` (ISO8601), `_webhook_source: "ga4"`

**Mailchimp Provider Details**:
- `campaign_id`: 10-char alphanumeric (lowercase)
- `subscriber_id`: MD5 hash of lowercased email (matches Mailchimp convention)
- `email_address`: draw from shared customer pool (30% overlap with other sources)
- `merge_fields`: `{"FNAME": ..., "LNAME": ..., "PHONE": ...}` — use same names/phones as other sources
- `action` distribution: `sent (40%), open (25%), click (15%), bounce (8%), unsub (5%), sms_sent (5%), sms_click (2%)`
- `phone`: for SMS events, use E.164 format matching shared customer pool
- Webhook metadata: `_webhook_received_at`, `_webhook_event_id`

### 2.2 Ingestion API

| File | Action | Description |
|------|--------|-------------|
| `ingestion/app/webhooks/ga4.py` | **Create** | FastAPI router for GA4 Measurement Protocol webhooks |
| `ingestion/app/webhooks/mailchimp.py` | **Create** | FastAPI router for Mailchimp webhooks |
| `ingestion/app/validators/signatures.py` | **Modify** | Add `validate_ga4_signature()` and `validate_mailchimp_signature()` |
| `ingestion/app/config.py` | **Modify** | Add `ga4_enabled`, `mailchimp_enabled`, `ga4_api_secret`, `mailchimp_webhook_secret` settings |
| `ingestion/app/main.py` | **Modify** | Register GA4 and Mailchimp routers |

**GA4 webhook handler** (`ga4.py`):

```
POST /webhooks/ga4/events   → topic: ga4.events
POST /webhooks/ga4/sessions → topic: ga4.sessions
Record key: payload.client_id
```

GA4 Measurement Protocol uses an `api_secret` query parameter for authentication (no HMAC body signing). The validator checks the `api_secret` query param against the configured secret.

**Mailchimp webhook handler** (`mailchimp.py`):

```
POST /webhooks/mailchimp/webhook → routes by event type:
  subscribe/unsubscribe/profile/upemail/cleaned → mailchimp.subscribers
  campaign/send/open/click/bounce/unsub/abuse   → mailchimp.events

GET /webhooks/mailchimp/webhook → returns 200 (Mailchimp validation ping)
Record key: payload.data.email or payload.id
```

Mailchimp webhooks use a shared secret key passed as a URL query parameter. The validator checks the `secret` query param. Additionally, Mailchimp sends a GET request to verify the endpoint exists before activating — the handler must respond with HTTP 200 to GET requests.

### 2.3 Infrastructure

| File | Action | Description |
|------|--------|-------------|
| `infrastructure/redpanda/init-topics.sh` | **Modify** | Add 5 new topics |
| `infrastructure/.env.example` | **Modify** | Add GA4 and Mailchimp env vars |

**New Redpanda topics**:

```bash
ga4.events              # Partitions: 3
ga4.sessions            # Partitions: 3
mailchimp.campaigns     # Partitions: 3
mailchimp.events        # Partitions: 3
mailchimp.subscribers   # Partitions: 3
```

**New environment variables**:

```bash
# GA4
GA4_API_SECRET=ga4_dev_secret
GA4_MEASUREMENT_ID=G-XXXXXXXXXX

# Mailchimp
MAILCHIMP_WEBHOOK_SECRET=mailchimp_dev_secret
MAILCHIMP_API_KEY=dummy-us21
```

### 2.4 Flink Streaming Jobs (Raw Layer)

| File | Action | Description |
|------|--------|-------------|
| `jobs/flink/ga4_events_full.sql` | **Create** | Kafka → Iceberg for ga4.events |
| `jobs/flink/ga4_sessions_full.sql` | **Create** | Kafka → Iceberg for ga4.sessions |
| `jobs/flink/mailchimp_campaigns_full.sql` | **Create** | Kafka → Iceberg for mailchimp.campaigns |
| `jobs/flink/mailchimp_events_full.sql` | **Create** | Kafka → Iceberg for mailchimp.events |
| `jobs/flink/mailchimp_subscribers_full.sql` | **Create** | Kafka → Iceberg for mailchimp.subscribers |

Each follows the established pattern:

```sql
-- 1. CREATE TEMPORARY TABLE ... WITH ('connector'='kafka', 'topic'='ga4.events', ...)
-- 2. CREATE TABLE IF NOT EXISTS raw.ga4_events (...)
-- 3. INSERT INTO raw.ga4_events SELECT ... CURRENT_TIMESTAMP as _loaded_at FROM source
```

**Type conversions in Flink**:
- GA4 `event_timestamp` (BIGINT microseconds) → `TIMESTAMP(3)` via `TO_TIMESTAMP_LTZ(event_timestamp / 1000, 3)`
- Mailchimp ISO8601 timestamps → `TIMESTAMP(3)` via `TO_TIMESTAMP(REPLACE(REPLACE(...)))`
- GA4 `value` (DOUBLE) → `DECIMAL(18, 2)` via `CAST`
- JSON fields (`event_params`, `traffic_source`, `device`, `geo`, `merge_fields`, `stats`) → stored as `STRING`

### 2.5 Raw Table Schemas

| File | Action |
|------|--------|
| `sql/00_raw/ga4/events.sql` | **Create** |
| `sql/00_raw/ga4/sessions.sql` | **Create** |
| `sql/00_raw/mailchimp/campaigns.sql` | **Create** |
| `sql/00_raw/mailchimp/events.sql` | **Create** |
| `sql/00_raw/mailchimp/subscribers.sql` | **Create** |

### 2.6 Staging Table Schemas & Transforms

| File | Action | Description |
|------|--------|-------------|
| `sql/01_staging/stg_ga4_events.sql` | **Create** | Cleaned engagement events |
| `sql/01_staging/stg_ga4_sessions.sql` | **Create** | Cleaned session data |
| `sql/01_staging/stg_mailchimp_campaigns.sql` | **Create** | Cleaned campaign metadata |
| `sql/01_staging/stg_mailchimp_events.sql` | **Create** | Cleaned member-level events |
| `sql/01_staging/stg_mailchimp_subscribers.sql` | **Create** | Cleaned subscriber profiles |
| `jobs/spark/staging_batch.py` | **Modify** | Add GA4 and Mailchimp staging transforms |

**Staging: `stg_ga4_events`**:

```sql
CREATE TABLE staging.stg_ga4_events (
    client_id               STRING,
    user_id                 STRING,
    session_id              STRING,
    event_name              STRING,
    event_timestamp         TIMESTAMP,
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
    is_ecommerce_event      BOOLEAN,  -- purchase, add_to_cart, begin_checkout, etc.
    is_engagement_event     BOOLEAN,  -- scroll, click, video_*, file_download
    -- Lineage
    _raw_client_id          STRING,
    _loaded_at              TIMESTAMP,
    _staged_at              TIMESTAMP
) USING iceberg
PARTITIONED BY (months(event_timestamp))
```

**Staging: `stg_ga4_sessions`**:

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
    -- Traffic attribution
    traffic_source          STRING,
    traffic_medium          STRING,
    traffic_campaign        STRING,
    channel_group           STRING,   -- Derived: organic_search, paid_search, social, direct, email, referral, sms
    -- Pages
    landing_page            STRING,
    exit_page               STRING,
    -- Device & geo
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
    is_bounce               BOOLEAN,   -- page_view_count <= 1 AND NOT engaged
    -- Lineage
    _loaded_at              TIMESTAMP,
    _staged_at              TIMESTAMP
) USING iceberg
PARTITIONED BY (months(session_start))
```

**Staging: `stg_mailchimp_campaigns`**:

```sql
CREATE TABLE staging.stg_mailchimp_campaigns (
    campaign_id             STRING,
    campaign_type           STRING,   -- regular, automation, sms
    status                  STRING,
    list_id                 STRING,
    subject_line            STRING,
    preview_text            STRING,
    from_name               STRING,
    from_email              STRING,
    content_type            STRING,
    send_time               TIMESTAMP,
    emails_sent             INT,
    -- Performance metrics
    opens                   INT,
    unique_opens            INT,
    clicks                  INT,
    unique_clicks           INT,
    unsubscribes            INT,
    bounces                 INT,
    open_rate               DECIMAL(5, 4),  -- 0.0000 to 1.0000
    click_rate              DECIMAL(5, 4),
    -- Derived
    click_to_open_rate      DECIMAL(5, 4),  -- unique_clicks / unique_opens
    is_sms                  BOOLEAN,        -- campaign_type = 'sms'
    is_automated            BOOLEAN,        -- campaign_type = 'automation'
    -- Lineage
    _loaded_at              TIMESTAMP,
    _staged_at              TIMESTAMP
) USING iceberg
PARTITIONED BY (months(send_time))
```

**Staging: `stg_mailchimp_events`**:

```sql
CREATE TABLE staging.stg_mailchimp_events (
    event_id                STRING,
    campaign_id             STRING,
    list_id                 STRING,
    email_address           STRING,
    email_normalized        STRING,   -- lower(trim(email))
    action                  STRING,   -- sent, open, click, bounce, unsub, sms_sent, sms_click
    event_timestamp         TIMESTAMP,
    -- Click details
    url                     STRING,
    -- Bounce details
    bounce_type             STRING,   -- hard, soft
    -- Location (extracted from JSON)
    location_country        STRING,
    location_region         STRING,
    -- Derived
    is_sms_event            BOOLEAN,  -- action IN ('sms_sent', 'sms_click')
    is_positive_engagement  BOOLEAN,  -- action IN ('open', 'click', 'sms_click')
    is_negative_event       BOOLEAN,  -- action IN ('bounce', 'unsub', 'abuse')
    event_date              DATE,
    -- Lineage
    _loaded_at              TIMESTAMP,
    _staged_at              TIMESTAMP
) USING iceberg
PARTITIONED BY (months(event_timestamp))
```

**Staging: `stg_mailchimp_subscribers`**:

```sql
CREATE TABLE staging.stg_mailchimp_subscribers (
    subscriber_id           STRING,
    email_address           STRING,
    email_normalized        STRING,   -- lower(trim(email))
    email_type              STRING,
    status                  STRING,   -- subscribed, unsubscribed, cleaned, pending
    list_id                 STRING,
    -- Extracted from merge_fields JSON
    first_name              STRING,
    last_name               STRING,
    full_name               STRING,   -- Derived: concat(first_name, ' ', last_name)
    phone                   STRING,
    phone_normalized        STRING,   -- digits only
    -- Extracted from stats JSON
    avg_open_rate           DECIMAL(5, 4),
    avg_click_rate          DECIMAL(5, 4),
    -- SMS
    sms_status              STRING,
    has_sms                 BOOLEAN,  -- phone IS NOT NULL AND sms_status = 'subscribed'
    -- Subscription lifecycle
    signup_timestamp        TIMESTAMP,
    opt_in_timestamp        TIMESTAMP,
    last_changed            TIMESTAMP,
    language                STRING,
    vip                     BOOLEAN,
    source                  STRING,   -- API, import, popup, landing_page
    -- Derived
    is_active               BOOLEAN,  -- status = 'subscribed'
    days_since_signup       INT,
    -- Lineage
    _loaded_at              TIMESTAMP,
    _staged_at              TIMESTAMP
) USING iceberg
PARTITIONED BY (status)
```

### 2.7 Semantic Layer (Entity Resolution)

| File | Action | Description |
|------|--------|-------------|
| `sql/02_semantic/entity_index.sql` | **Modify** | Add `'ga4'` and `'mailchimp'` to `source` column documentation |
| `jobs/spark/entity_backfill.py` | **Modify** | Add GA4 and Mailchimp to `get_all_staging_customers()` union |

**Entity resolution approach per source**:

- **GA4**: GA4 has `user_id` (when set by app) which maps to the application's user identifier. When `user_id` is present, it can be matched to emails/phones from other sources via a lookup table or direct match. When only `client_id` is available, entity resolution is deferred (cookie-only identity). GA4 records enter the entity index only when `user_id` is populated.

  ```python
  # In get_all_staging_customers():
  ga4_customers = spark.sql("""
      SELECT DISTINCT
          'ga4' as source,
          user_id as source_id,
          CAST(NULL AS STRING) as email,       -- GA4 has no email directly
          CAST(NULL AS STRING) as first_name,
          CAST(NULL AS STRING) as last_name,
          CAST(NULL AS STRING) as phone,
          user_id as external_user_id,         -- For cross-reference matching
          MIN(session_start) as created_at,
          MAX(_staged_at) as _staged_at
      FROM staging.stg_ga4_sessions
      WHERE user_id IS NOT NULL
      GROUP BY user_id
  """)
  ```

  GA4 `user_id` matching requires a supplementary step: if the application sets `user_id` to the same email or customer ID used in Shopify/Stripe/HubSpot, the entity resolution can match on that field directly.

- **Mailchimp**: Direct email and phone matching — identical to existing sources.

  ```python
  mailchimp_customers = spark.sql("""
      SELECT
          'mailchimp' as source,
          subscriber_id as source_id,
          email_normalized as email,
          first_name,
          last_name,
          phone_normalized as phone,
          CAST(NULL AS STRING) as external_user_id,
          signup_timestamp as created_at,
          _staged_at
      FROM staging.stg_mailchimp_subscribers
      WHERE status = 'subscribed'
  """)
  ```

### 2.8 Analytics Layer

| File | Action | Description |
|------|--------|-------------|
| `sql/04_analytics/engagement_metrics.sql` | **Create** | GA4 engagement aggregations |
| `sql/04_analytics/campaign_metrics.sql` | **Create** | Mailchimp campaign performance |
| `jobs/spark/analytics_incremental.py` | **Modify** | Add `engagement_metrics` and `campaign_metrics` tables |

**`analytics.engagement_metrics`**:

```sql
CREATE TABLE analytics.engagement_metrics (
    metric_date             DATE,
    -- Traffic
    total_sessions          BIGINT,
    engaged_sessions        BIGINT,
    engagement_rate         DECIMAL(5, 4),   -- engaged_sessions / total_sessions
    total_users             BIGINT,          -- distinct client_id
    new_users               BIGINT,          -- first session in period
    returning_users         BIGINT,
    -- Page performance
    total_page_views        BIGINT,
    avg_pages_per_session   DECIMAL(8, 2),
    avg_session_duration    DECIMAL(8, 2),   -- seconds
    bounce_rate             DECIMAL(5, 4),
    -- Conversions
    total_conversions       BIGINT,
    conversion_rate         DECIMAL(5, 4),   -- conversions / sessions
    total_conversion_value  DECIMAL(18, 2),
    -- Channel breakdown (top-level counts)
    organic_sessions        BIGINT,
    paid_sessions           BIGINT,
    direct_sessions         BIGINT,
    email_sessions          BIGINT,
    social_sessions         BIGINT,
    referral_sessions       BIGINT,
    sms_sessions            BIGINT,
    -- Device breakdown
    desktop_sessions        BIGINT,
    mobile_sessions         BIGINT,
    tablet_sessions         BIGINT,
    -- Geo (top country)
    top_country             STRING,
    top_country_sessions    BIGINT,
    -- Lineage
    _computed_at            TIMESTAMP
) USING iceberg
PARTITIONED BY (metric_date)
```

**`analytics.campaign_metrics`**:

```sql
CREATE TABLE analytics.campaign_metrics (
    campaign_id             STRING,
    campaign_type           STRING,     -- regular, automation, sms
    list_id                 STRING,
    subject_line            STRING,
    send_time               TIMESTAMP,
    send_date               DATE,
    -- Volume
    total_sent              BIGINT,
    total_delivered          BIGINT,     -- sent - bounces
    -- Engagement
    total_opens             BIGINT,
    unique_opens            BIGINT,
    total_clicks            BIGINT,
    unique_clicks           BIGINT,
    -- Negative
    total_bounces           BIGINT,
    hard_bounces            BIGINT,
    soft_bounces            BIGINT,
    total_unsubscribes      BIGINT,
    -- Rates
    delivery_rate           DECIMAL(5, 4),
    open_rate               DECIMAL(5, 4),
    click_rate              DECIMAL(5, 4),
    click_to_open_rate      DECIMAL(5, 4),
    bounce_rate             DECIMAL(5, 4),
    unsubscribe_rate        DECIMAL(5, 4),
    -- SMS specific
    sms_sent                BIGINT,
    sms_clicks              BIGINT,
    sms_click_rate          DECIMAL(5, 4),
    -- Derived
    is_sms                  BOOLEAN,
    engagement_score        DECIMAL(5, 2),  -- Weighted: opens*1 + clicks*3 - unsubs*5
    -- Lineage
    _computed_at            TIMESTAMP
) USING iceberg
PARTITIONED BY (months(send_time))
```

### 2.9 Marts Layer

| File | Action | Description |
|------|--------|-------------|
| `sql/05_marts/customer_360.sql` | **Modify** | Add engagement and campaign columns |
| `sql/05_marts/engagement_dashboard.sql` | **Create** | Daily engagement dashboard table |
| `sql/05_marts/campaign_dashboard.sql` | **Create** | Campaign performance dashboard table |
| `jobs/spark/marts_incremental.py` | **Modify** | Add engagement and campaign dashboard builds |

**Changes to `marts.customer_360`** — new columns:

```sql
-- GA4 engagement (joined via entity_id → ga4 user_id)
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

-- Mailchimp engagement (joined via entity_id → mailchimp subscriber)
has_mailchimp           BOOLEAN,
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

-- Updated multi-source flags
source_count            INT,        -- Now counts up to 5 sources
```

**`marts.engagement_dashboard_daily`**:

```sql
CREATE TABLE marts.engagement_dashboard_daily (
    date_key                DATE,
    day_of_week             INT,
    day_name                STRING,
    week_of_year            INT,
    month_key               STRING,
    -- Session metrics
    total_sessions          BIGINT,
    engaged_sessions        BIGINT,
    engagement_rate         DECIMAL(5, 4),
    unique_users            BIGINT,
    new_users               BIGINT,
    -- Content metrics
    total_page_views        BIGINT,
    pages_per_session       DECIMAL(8, 2),
    avg_session_duration    DECIMAL(8, 2),
    bounce_rate             DECIMAL(5, 4),
    -- Conversions
    conversions             BIGINT,
    conversion_rate         DECIMAL(5, 4),
    conversion_value        DECIMAL(18, 2),
    -- Channel performance
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

**`marts.campaign_dashboard`**:

```sql
CREATE TABLE marts.campaign_dashboard (
    campaign_id             STRING,
    campaign_type           STRING,
    subject_line            STRING,
    send_date               DATE,
    send_month              STRING,
    -- Volume
    total_sent              BIGINT,
    total_delivered          BIGINT,
    delivery_rate           DECIMAL(5, 4),
    -- Engagement
    unique_opens            BIGINT,
    unique_clicks           BIGINT,
    open_rate               DECIMAL(5, 4),
    click_rate              DECIMAL(5, 4),
    click_to_open_rate      DECIMAL(5, 4),
    -- Negative
    bounces                 BIGINT,
    unsubscribes            BIGINT,
    bounce_rate             DECIMAL(5, 4),
    unsubscribe_rate        DECIMAL(5, 4),
    -- SMS
    sms_sent                BIGINT,
    sms_clicks              BIGINT,
    sms_click_rate          DECIMAL(5, 4),
    -- Score
    engagement_score        DECIMAL(5, 2),
    performance_tier        STRING,   -- 'excellent', 'good', 'average', 'poor'
    _computed_at            TIMESTAMP
) USING iceberg
PARTITIONED BY (send_month)
```

### 2.10 Airflow Orchestration

| File | Action | Description |
|------|--------|-------------|
| `airflow/dags/iceberg_pipeline.py` | **Modify** | Add GA4 and Mailchimp tasks to all stages |

**Updated DAG task graph**:

```
STAGING (parallel, 10 tasks):
  [existing 5 tasks]
  + stg_ga4_events           → staging_batch.py --table stg_ga4_events
  + stg_ga4_sessions         → staging_batch.py --table stg_ga4_sessions
  + stg_mailchimp_campaigns  → staging_batch.py --table stg_mailchimp_campaigns
  + stg_mailchimp_events     → staging_batch.py --table stg_mailchimp_events
  + stg_mailchimp_subscribers → staging_batch.py --table stg_mailchimp_subscribers

SEMANTIC (unchanged structure, updated logic):
  entity_index               → entity_backfill.py (now includes GA4 + Mailchimp)
  blocking_index             → entity_resolution_fuzzy.py

CORE (unchanged):
  core_customers, core_orders

ANALYTICS (parallel, 5 tasks):
  [existing 3 tasks]
  + engagement_metrics       → analytics_incremental.py --table engagement_metrics
  + campaign_metrics         → analytics_incremental.py --table campaign_metrics

MARTS (parallel, 5 tasks):
  [existing 3 tasks]
  + engagement_dashboard     → marts_incremental.py --table engagement_dashboard_daily
  + campaign_dashboard       → marts_incremental.py --table campaign_dashboard
```

**Dependencies**:

```
stg_ga4_events, stg_ga4_sessions → engagement_metrics → engagement_dashboard
stg_mailchimp_* → campaign_metrics → campaign_dashboard
stg_mailchimp_subscribers → entity_index (for entity resolution)
stg_ga4_sessions → entity_index (for GA4 user_id matching)
entity_index → customer_360 (updated with GA4 + Mailchimp columns)
```

### 2.11 Monitoring & Dashboards

| File | Action | Description |
|------|--------|-------------|
| `monitoring/dashboards/engagement_analytics.json` | **Create** | GA4 engagement Grafana dashboard |
| `monitoring/dashboards/campaign_analytics.json` | **Create** | Mailchimp campaign Grafana dashboard |
| `monitoring/dashboards/streaming_business.json` | **Modify** | Add GA4 and Mailchimp message rate panels |
| `monitoring/dashboards/batch_business.json` | **Modify** | Add engagement and campaign summary panels |

**Engagement Analytics Dashboard** (panels):

| Panel | Type | Query Source |
|-------|------|-------------|
| Daily Active Users | timeseries | `engagement_dashboard_daily.unique_users` |
| Engagement Rate | stat + timeseries | `engagement_dashboard_daily.engagement_rate` |
| Bounce Rate | stat + timeseries | `engagement_dashboard_daily.bounce_rate` |
| Sessions by Channel | piechart | `engagement_dashboard_daily` organic/paid/direct/email/social/sms |
| Conversions & Value | timeseries (dual axis) | `engagement_dashboard_daily.conversions, conversion_value` |
| Device Split | piechart | `engagement_dashboard_daily` desktop/mobile/tablet |
| Pages per Session | timeseries | `engagement_dashboard_daily.pages_per_session` |
| Top Countries | table | `engagement_metrics` grouped by top_country |

**Campaign Analytics Dashboard** (panels):

| Panel | Type | Query Source |
|-------|------|-------------|
| Campaigns Sent (MTD) | stat | `COUNT(*) FROM campaign_dashboard WHERE send_month = current` |
| Avg Open Rate | stat | `AVG(open_rate) FROM campaign_dashboard` |
| Avg Click Rate | stat | `AVG(click_rate) FROM campaign_dashboard` |
| Campaign Performance | table | `campaign_dashboard` sorted by send_date DESC |
| Open Rate Trend | timeseries | `campaign_dashboard` grouped by send_date |
| Email vs SMS Performance | bar chart | Grouped by campaign_type |
| Bounce & Unsubscribe Rate | timeseries | `campaign_dashboard.bounce_rate, unsubscribe_rate` |
| Performance Tier Distribution | piechart | `campaign_dashboard.performance_tier` |
| Top Campaigns by Engagement | table | `campaign_dashboard` ORDER BY engagement_score DESC LIMIT 10 |

### 2.12 ClickHouse Integration

| File | Action | Description |
|------|--------|-------------|
| `infrastructure/clickhouse/iceberg_setup.sql` | **Modify** | Add Iceberg table functions for new marts tables |

**New ClickHouse table definitions**:

```sql
-- GA4 engagement dashboard
CREATE TABLE IF NOT EXISTS iceberg.engagement_dashboard_daily
ENGINE = IcebergS3('http://minio:9000/warehouse/marts/engagement_dashboard_daily', ...)

-- Campaign dashboard
CREATE TABLE IF NOT EXISTS iceberg.campaign_dashboard
ENGINE = IcebergS3('http://minio:9000/warehouse/marts/campaign_dashboard', ...)
```

---

## 3. Complete File Change Summary

### New Files (22)

| # | File | Layer |
|---|------|-------|
| 1 | `datagen/providers/ga4_provider.py` | Data Generation |
| 2 | `datagen/providers/mailchimp_provider.py` | Data Generation |
| 3 | `ingestion/app/webhooks/ga4.py` | Ingestion API |
| 4 | `ingestion/app/webhooks/mailchimp.py` | Ingestion API |
| 5 | `jobs/flink/ga4_events_full.sql` | Streaming (Raw) |
| 6 | `jobs/flink/ga4_sessions_full.sql` | Streaming (Raw) |
| 7 | `jobs/flink/mailchimp_campaigns_full.sql` | Streaming (Raw) |
| 8 | `jobs/flink/mailchimp_events_full.sql` | Streaming (Raw) |
| 9 | `jobs/flink/mailchimp_subscribers_full.sql` | Streaming (Raw) |
| 10 | `sql/00_raw/ga4/events.sql` | Schema (Raw) |
| 11 | `sql/00_raw/ga4/sessions.sql` | Schema (Raw) |
| 12 | `sql/00_raw/mailchimp/campaigns.sql` | Schema (Raw) |
| 13 | `sql/00_raw/mailchimp/events.sql` | Schema (Raw) |
| 14 | `sql/00_raw/mailchimp/subscribers.sql` | Schema (Raw) |
| 15 | `sql/01_staging/stg_ga4_events.sql` | Schema (Staging) |
| 16 | `sql/01_staging/stg_ga4_sessions.sql` | Schema (Staging) |
| 17 | `sql/01_staging/stg_mailchimp_campaigns.sql` | Schema (Staging) |
| 18 | `sql/01_staging/stg_mailchimp_events.sql` | Schema (Staging) |
| 19 | `sql/01_staging/stg_mailchimp_subscribers.sql` | Schema (Staging) |
| 20 | `sql/04_analytics/engagement_metrics.sql` | Schema (Analytics) |
| 21 | `sql/04_analytics/campaign_metrics.sql` | Schema (Analytics) |
| 22 | `sql/05_marts/engagement_dashboard.sql` | Schema (Marts) |
| 23 | `sql/05_marts/campaign_dashboard.sql` | Schema (Marts) |
| 24 | `monitoring/dashboards/engagement_analytics.json` | Monitoring |
| 25 | `monitoring/dashboards/campaign_analytics.json` | Monitoring |

### Modified Files (13)

| # | File | Changes |
|---|------|---------|
| 1 | `datagen/generator.py` | Add `generate_ga4_data()`, `generate_mailchimp_data()`, extend shared customer pool |
| 2 | `datagen/simulate_webhooks.py` | Add GA4 and Mailchimp webhook posting |
| 3 | `ingestion/app/validators/signatures.py` | Add GA4 and Mailchimp validation functions |
| 4 | `ingestion/app/config.py` | Add GA4/Mailchimp settings |
| 5 | `ingestion/app/main.py` | Register new routers |
| 6 | `infrastructure/redpanda/init-topics.sh` | Add 5 new topics |
| 7 | `infrastructure/.env.example` | Add GA4/Mailchimp env vars |
| 8 | `jobs/spark/staging_batch.py` | Add 5 new staging transforms |
| 9 | `jobs/spark/entity_backfill.py` | Add GA4/Mailchimp to customer union |
| 10 | `jobs/spark/analytics_incremental.py` | Add engagement_metrics, campaign_metrics |
| 11 | `jobs/spark/marts_incremental.py` | Add engagement_dashboard, campaign_dashboard; update customer_360 |
| 12 | `airflow/dags/iceberg_pipeline.py` | Add 10 new tasks, update dependencies |
| 13 | `infrastructure/clickhouse/iceberg_setup.sql` | Add new Iceberg table definitions |
| 14 | `monitoring/dashboards/streaming_business.json` | Add GA4/Mailchimp message rate panels |
| 15 | `monitoring/dashboards/batch_business.json` | Add engagement/campaign summary panels |
| 16 | `sql/05_marts/customer_360.sql` | Add GA4 and Mailchimp columns |

---

## 4. Data Flow Diagram

```
                    ┌─────────────────────────────────────────────────────┐
                    │              MOCK DATA GENERATION                   │
                    │                                                     │
                    │  ga4_provider.py ──────► GA4 events & sessions      │
                    │  mailchimp_provider.py ► Campaigns, events, subs    │
                    │                                                     │
                    │  simulate_webhooks.py (shared customer pool 30%)    │
                    └──────────────┬──────────────────────────────────────┘
                                   │ POST /webhooks/*
                    ┌──────────────▼──────────────────────────────────────┐
                    │              INGESTION API (FastAPI)                │
                    │                                                     │
                    │  /webhooks/ga4/events     → ga4.events topic        │
                    │  /webhooks/ga4/sessions   → ga4.sessions topic      │
                    │  /webhooks/mailchimp/webhook                        │
                    │    → mailchimp.campaigns | .events | .subscribers   │
                    └──────────────┬──────────────────────────────────────┘
                                   │
                    ┌──────────────▼──────────────────────────────────────┐
                    │              REDPANDA (5 new topics)                │
                    │                                                     │
                    │  ga4.events  ga4.sessions                           │
                    │  mailchimp.campaigns  mailchimp.events              │
                    │  mailchimp.subscribers                              │
                    └──────────────┬──────────────────────────────────────┘
                                   │
                    ┌──────────────▼──────────────────────────────────────┐
                    │              FLINK STREAMING (5 new jobs)           │
                    │                                                     │
                    │  → raw.ga4_events                                   │
                    │  → raw.ga4_sessions                                 │
                    │  → raw.mailchimp_campaigns                          │
                    │  → raw.mailchimp_events                             │
                    │  → raw.mailchimp_subscribers                        │
                    └──────────────┬──────────────────────────────────────┘
                                   │
                    ┌──────────────▼──────────────────────────────────────┐
                    │              SPARK STAGING (5 new transforms)       │
                    │                                                     │
                    │  → staging.stg_ga4_events                           │
                    │  → staging.stg_ga4_sessions                         │
                    │  → staging.stg_mailchimp_campaigns                  │
                    │  → staging.stg_mailchimp_events                     │
                    │  → staging.stg_mailchimp_subscribers                │
                    └──────┬───────────────────────────────┬──────────────┘
                           │                               │
          ┌────────────────▼────────────┐    ┌─────────────▼──────────────┐
          │  ENTITY RESOLUTION          │    │  ANALYTICS (2 new tables)  │
          │                             │    │                            │
          │  entity_index updated with: │    │  → engagement_metrics      │
          │  - mailchimp (email/phone)  │    │    (GA4 daily aggregations)│
          │  - ga4 (user_id matching)   │    │  → campaign_metrics        │
          │                             │    │    (Mailchimp per-campaign) │
          └────────────┬────────────────┘    └─────────────┬──────────────┘
                       │                                   │
          ┌────────────▼───────────────────────────────────▼──────────────┐
          │              MARTS (2 new + 1 updated)                       │
          │                                                              │
          │  → engagement_dashboard_daily (GA4 daily metrics)            │
          │  → campaign_dashboard (Mailchimp campaign scorecard)         │
          │  → customer_360 (updated: +GA4 engagement, +Mailchimp cols)  │
          └──────────────┬───────────────────────────────────────────────┘
                         │
          ┌──────────────▼───────────────────────────────────────────────┐
          │              CLICKHOUSE + GRAFANA                            │
          │                                                              │
          │  Dashboard: Engagement Analytics (GA4)                       │
          │    - DAU, sessions, bounce rate, conversions, channels       │
          │  Dashboard: Campaign Analytics (Mailchimp)                   │
          │    - Open/click rates, email vs SMS, performance tiers       │
          │  Dashboard: Customer 360 (updated)                           │
          │    - Now includes engagement + campaign dimensions           │
          └──────────────────────────────────────────────────────────────┘
```

---

## 5. Entity Resolution Strategy

```
Source         Identity Fields           Match Strategy
─────────────────────────────────────────────────────────────
Shopify        email, phone, name        Existing (email/phone exact, name fuzzy)
Stripe         email, phone, name        Existing (email/phone exact, name fuzzy)
HubSpot        email, phone, name        Existing (email/phone exact, name fuzzy)
Mailchimp      email, phone, name        Same as above — direct email/phone match
GA4            user_id only              Match user_id against known customer IDs
                                         (only when user_id is set by app)
```

Mailchimp subscribers are strong candidates for entity resolution because they carry `email_address` and `phone` — the same primary keys used by existing sources. GA4 is weaker because it relies on cookie-based `client_id` by default; only records with an application-set `user_id` participate in entity resolution.

---

## 6. Shared Customer Pool (Data Generation)

The existing 30% overlap pool expands to include GA4 and Mailchimp:

```python
# In generator.py — shared_customers list
# Each shared customer has consistent:
#   - email (used by all sources)
#   - first_name, last_name
#   - phone (used by Shopify, Stripe, HubSpot, Mailchimp)
#   - user_id (used by GA4, set to email for cross-matching)

# Source participation per shared customer:
#   Shopify:    customer record
#   Stripe:     customer record
#   HubSpot:    contact record
#   Mailchimp:  subscriber record (email + phone + merge_fields)
#   GA4:        sessions with user_id = email
```

This ensures entity resolution can unify the same person across all 5 sources in the demo environment.
