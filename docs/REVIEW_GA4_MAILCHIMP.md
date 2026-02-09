# Staff Architecture Review: GA4 & Mailchimp Integration Design

**Reviewing**: `docs/DESIGN_GA4_MAILCHIMP.md`
**Reviewer perspective**: Staff Data Architect

---

## Overall Assessment

The design is thorough and follows established codebase conventions well. It correctly mirrors the Shopify/Stripe/HubSpot patterns across all layers. However, several architectural concerns warrant discussion before implementation.

---

## 1. GA4 Data Ingestion Model Is Wrong for the Domain

### Problem

The design models GA4 as a webhook source, but GA4 does not send webhooks. GA4 data is accessed via:

1. **BigQuery Export** (batch, daily/streaming) — the standard production path
2. **GA4 Data API** (pull-based, for reporting queries)
3. **Measurement Protocol** (write-only, for sending events *to* GA4, not receiving them)

The design's `POST /webhooks/ga4/events` endpoint simulates receiving GA4 data as webhooks, which conflates the Measurement Protocol (which sends data *to* GA4) with data *extraction from* GA4. In a real system, you would never receive GA4 data via HTTP push.

### Suggestion

Since this is a demo/mock pipeline, the webhook simulation is acceptable for generating realistic data flow. However, the design document should explicitly acknowledge this deviation and note that a production implementation would use either:

- A BigQuery-to-Kafka connector (batch pull)
- The GA4 Data API with a scheduled extractor (Airflow sensor + API pull)

**Pros of keeping the webhook simulation**:
- Consistent architecture across all sources — same ingestion path
- Simpler infrastructure — no additional connectors or API polling
- Good enough for demonstrating the downstream pipeline

**Cons**:
- Misleading for anyone using this as a production reference
- Hides the real complexity of GA4 data extraction (BigQuery export schemas, sampling, data freshness delays)
- The `api_secret` validation in the design imitates Measurement Protocol auth, which is backwards (that secret is for *sending* to GA4, not receiving from it)

### Recommendation

Add a "Production Considerations" callout box in the design doc that explains the real GA4 data access patterns. Keep the webhook simulation for the demo but rename the endpoint to something like `/ingest/ga4/events` instead of `/webhooks/ga4/events` to avoid confusion.

---

## 2. GA4 Event Timestamps: Microsecond Precision Creates Silent Data Issues

### Problem

GA4 natively uses microseconds since epoch for `event_timestamp`. The design converts this to `TIMESTAMP(3)` (millisecond precision) via `TO_TIMESTAMP_LTZ(event_timestamp / 1000, 3)`.

This integer division silently truncates the last 3 digits of precision. For event ordering within a single page load, this matters — multiple events can fire within the same millisecond.

### Suggestion

Use `TIMESTAMP(6)` for raw and staging GA4 tables, then truncate to milliseconds only at the analytics/marts layer where sub-millisecond precision has no analytical value.

**Pros**:
- Preserves source fidelity in raw/staging layers
- Avoids event ordering ambiguity
- Aligns with the "raw layer stores data as-received" convention

**Cons**:
- Slightly larger storage footprint in Parquet files (negligible)
- Inconsistent with other sources that use `TIMESTAMP(3)` — but this is justified by the source format difference

### Recommendation

Use `TIMESTAMP(6)` in raw and staging. Truncate to `TIMESTAMP(3)` or `DATE` in analytics and marts where it genuinely doesn't matter.

---

## 3. Session Aggregation Belongs in Staging, Not at Ingestion

### Problem

The design has a separate `ga4.sessions` topic and `POST /webhooks/ga4/sessions` endpoint, treating sessions as a first-class ingested entity. In reality, GA4 sessions are derived from events — they're an aggregation defined by the 30-minute inactivity gap rule.

Creating sessions at the data generation layer means the session logic is baked into mock data rather than being computed by the pipeline itself. This is an inversion of responsibility.

### Suggestion

**Option A (Recommended)**: Drop the `ga4.sessions` topic entirely. Ingest only `ga4.events`. Compute sessions in the staging layer (Spark) using window functions:

```sql
-- Session boundaries via 30-min gap detection
SELECT
    client_id,
    event_timestamp,
    SUM(new_session_flag) OVER (PARTITION BY client_id ORDER BY event_timestamp) as session_id
FROM (
    SELECT *,
        CASE WHEN event_timestamp - LAG(event_timestamp) OVER (...) > INTERVAL '30' MINUTE
             THEN 1 ELSE 0 END as new_session_flag
    FROM raw.ga4_events
)
```

**Option B**: Keep `ga4.sessions` as a convenience, but document that it's a pre-aggregated feed for the demo, not how sessions would be computed in production.

**Pros of Option A**:
- Sessions are derived, testable, and reproducible
- Single source of truth (events) — sessions are a view, not a separate data product
- Matches how GA4 actually works internally
- Eliminates a topic, a Flink job, a Faker provider method, and a webhook endpoint — reduces scope

**Cons of Option A**:
- More complex staging logic
- Session computation in Spark adds processing time
- Harder to generate realistic session data (need to generate correlated event sequences)

**Pros of Option B**:
- Simpler implementation — sessions arrive pre-built
- Faster to demonstrate the full pipeline end-to-end

**Cons of Option B**:
- Architecturally incorrect — hides a critical data transformation
- The mock generator must maintain session logic that should live in the pipeline

### Recommendation

Option A. Compute sessions in staging. This is a data engineering pipeline demo — showing how raw events become sessions is more valuable than pre-computing them.

---

## 4. Mailchimp Webhook Handler: Single-Endpoint Routing Is Fragile

### Problem

The design routes all Mailchimp events through a single `POST /webhooks/mailchimp/webhook` endpoint, then dispatches to three different topics based on the event type field. This means the handler must parse the payload before knowing which topic to route to.

The existing Shopify handler uses *separate endpoints per entity* (`/customers/create`, `/orders/create`), while Stripe uses a single endpoint but maps cleanly via `event.type`. The Mailchimp design is closer to the Stripe pattern, which is fine — but the payload structure is less predictable.

### Suggestion

The single-endpoint approach is correct for Mailchimp because that's how Mailchimp webhooks actually work (you register one URL). Keep the design as-is, but add explicit error handling for unknown event types:

```python
topic = EVENT_TYPE_TO_TOPIC.get(event_type)
if topic is None:
    logger.warning(f"Unknown Mailchimp event type: {event_type}")
    return {"status": "ignored", "reason": "unknown_event_type"}
```

**Pros**: Matches the real Mailchimp API behavior
**Cons**: None significant — this is the right pattern

### Recommendation

Keep the single-endpoint design. Add a dead-letter topic or explicit logging for unrecognized event types. Consider adding the `GET` handler validation ping as a separate, clearly documented endpoint.

---

## 5. Entity Resolution for GA4 Is Underspecified

### Problem

The design says GA4 records enter entity resolution "only when `user_id` is populated" and that `user_id` is matched against known customer IDs. But it doesn't specify *how* this matching works.

The entity resolution system uses email and phone as blocking keys. GA4's `user_id` is neither — it's an opaque application identifier. The design assumes `user_id = email` (line 998: "set to email for cross-matching"), but this is a mock data convenience, not a real resolution strategy.

In production, you'd need a **lookup table** mapping `ga4.user_id → email`, maintained by the application that sets the user ID. Without that table, GA4 data cannot participate in entity resolution.

### Suggestion

**For the demo**: The current approach (set `user_id = email` in mock data) works. Document it clearly as a simplification.

**For production readiness**: Add a `ga4_user_mapping` table concept:

```sql
-- Bridge table: application-maintained
CREATE TABLE semantic.ga4_user_mapping (
    ga4_user_id    STRING,
    email          STRING,
    mapped_at      TIMESTAMP,
    mapping_source STRING  -- 'login_event', 'signup', 'manual'
)
```

**Pros of adding the mapping table concept**:
- Makes the entity resolution dependency explicit
- Highlights a real integration challenge teams face with GA4
- Demonstrates a more realistic architecture

**Cons**:
- Adds another table and maintenance burden for the demo
- Overcomplicates what is otherwise a straightforward extension

### Recommendation

Keep the `user_id = email` approach for the demo. Add a callout in the design document explaining the production approach with a mapping table. Do not implement the mapping table in the demo.

---

## 6. Too Many New Grafana Dashboards at Once

### Problem

The design creates 2 new dashboards and modifies 2 existing ones. Each dashboard has 8-9 panels. That's ~20 new panels total. Dashboard JSON files are notoriously tedious to create and maintain.

### Suggestion

**Phase 1**: Add GA4 and Mailchimp message rate panels to the existing `streaming_business.json` dashboard only. This validates data flow end-to-end.

**Phase 2**: Create the `engagement_analytics.json` dashboard with 3-4 key panels (DAU, engagement rate, conversions, channel breakdown).

**Phase 3**: Create the `campaign_analytics.json` dashboard with 3-4 key panels (open rate, click rate, campaign table, bounce trend).

**Pros of phased approach**:
- Faster time to demonstrable value
- Each phase is independently testable
- Avoids the "20 panels but nothing works yet" scenario

**Cons**:
- Takes longer overall
- Design document describes the final state, not intermediate states

### Recommendation

Implement in phases. The design document can describe the final state but should note recommended implementation order.

---

## 7. `stg_mailchimp_subscribers` Partitioned by `status` — Bad Partition Strategy

### Problem

The design partitions `stg_mailchimp_subscribers` by `status` (subscribed, unsubscribed, cleaned, pending, transactional). This creates 4-5 partitions, which is fine for cardinality, but problematic because:

1. Status changes over time — a subscriber who unsubscribes creates a new file in a different partition, but their old record remains in the `subscribed` partition. Without compaction or upsert, you get duplicates across partitions.
2. Most queries will filter on `status = 'subscribed'`, making partition pruning useful — but this benefit is offset by the data management complexity.

### Suggestion

Partition by `months(signup_timestamp)` instead. This is:
- Immutable (signup date never changes)
- Consistent with the partitioning strategy used by every other staging table
- Better for time-based incremental processing

**Pros**:
- Consistent partitioning strategy across all staging tables
- No cross-partition data management issues
- Better alignment with watermark-based incremental processing

**Cons**:
- Queries filtering by status lose partition pruning (but these tables are small enough that full scans are cheap)

### Recommendation

Change to `PARTITIONED BY (months(signup_timestamp))`. If status-based filtering is a performance concern at scale, add a secondary index or materialized view in ClickHouse.

---

## 8. Analytics Layer: `engagement_metrics` Is Date-Grain Only — Too Coarse

### Problem

`analytics.engagement_metrics` aggregates everything to `metric_date` grain. This means you lose the ability to drill into:

- Hourly traffic patterns
- Per-page performance
- Per-channel metrics at a granular level (the design stores top-level counts like `organic_sessions` but not per-channel conversion rates)

The `engagement_dashboard_daily` mart inherits this limitation.

### Suggestion

Add a `metric_hour` column or create a separate hourly metrics table. At minimum, keep the daily grain but add a few critical per-dimension tables:

```sql
-- Option: Add channel-level daily metrics
CREATE TABLE analytics.engagement_by_channel (
    metric_date     DATE,
    channel_group   STRING,  -- organic, paid, direct, email, social, sms, referral
    sessions        BIGINT,
    engaged_sessions BIGINT,
    conversions     BIGINT,
    conversion_value DECIMAL(18, 2),
    _computed_at    TIMESTAMP
) USING iceberg
PARTITIONED BY (metric_date)
```

**Pros**:
- Enables channel performance comparison (the main analytical question for GA4 data)
- Still manageable table size (7 channels x 365 days = ~2,500 rows/year)

**Cons**:
- Another table to maintain
- More complexity in the Spark analytics job

### Recommendation

Keep the daily `engagement_metrics` table as designed. Add a `engagement_by_channel` table at the analytics layer. Skip hourly granularity unless there's a specific dashboard need — the raw and staging layers preserve full event-level detail for ad-hoc analysis.

---

## 9. Scope Is Large — Consider Splitting Into Two PRs

### Problem

The design creates 25 new files and modifies 16 existing files. That's 41 file changes in a single implementation. Code review will be painful, testing will be complex, and a bug in one area (say, the Flink jobs) blocks the entire feature.

### Suggestion

**PR 1: Mailchimp** (simpler, direct email/phone matching, fits existing patterns perfectly)
- Provider, webhook handler, Flink jobs, raw/staging schemas, entity resolution update
- ~20 files, all following established patterns closely

**PR 2: GA4** (more complex, session computation, weaker entity resolution)
- Provider, webhook handler, Flink jobs, raw/staging schemas, session aggregation logic
- ~15 files, includes the session computation logic

**PR 3: Analytics + Marts + Dashboards** (depends on PR 1 and PR 2)
- Analytics tables, marts updates, customer_360 changes, dashboard JSON
- ~10 files, purely downstream

**Pros**:
- Each PR is independently reviewable and testable
- Mailchimp can ship first and provide value immediately
- GA4 session logic can be iterated on without blocking Mailchimp
- Rollback is granular

**Cons**:
- More PRs to manage
- Some shared infrastructure changes (topics, env vars) need to be in PR 1 or a separate infra PR
- Cross-PR dependencies need coordination

### Recommendation

Split into at least two PRs: Mailchimp first, GA4 second. Bundle analytics/marts/dashboards with whichever PR completes second, or as a third PR.

---

## 10. Missing: Data Quality Checks and Validation

### Problem

The design has no mention of data quality checks at any layer. The existing codebase also doesn't have explicit quality checks, but adding two new sources is an opportunity to establish the pattern.

### Suggestion

Add lightweight validation at the staging layer:

```python
# In staging_batch.py, after each transform
quality_checks = {
    "stg_ga4_events": [
        ("no_null_client_id", "SELECT COUNT(*) FROM stg_ga4_events WHERE client_id IS NULL"),
        ("valid_event_names", "SELECT COUNT(*) FROM stg_ga4_events WHERE event_name NOT IN (...)"),
        ("timestamp_not_future", "SELECT COUNT(*) FROM stg_ga4_events WHERE event_timestamp > CURRENT_TIMESTAMP + INTERVAL 1 HOUR"),
    ],
    "stg_mailchimp_events": [
        ("no_null_email", "SELECT COUNT(*) FROM stg_mailchimp_events WHERE email_address IS NULL AND NOT is_sms_event"),
        ("valid_actions", "SELECT COUNT(*) FROM stg_mailchimp_events WHERE action NOT IN (...)"),
    ],
}
```

**Pros**:
- Catches data issues before they propagate to analytics/marts
- Establishes a pattern for all future sources
- Minimal implementation effort

**Cons**:
- Adds processing time to each staging run
- Need to decide: warn or fail? (Recommend: warn + log, don't block pipeline)

### Recommendation

Add quality checks as logged warnings in staging. Don't block the pipeline on failures. This can be a follow-up PR after the core integration ships.

---

## 11. `campaign_metrics` Engagement Score Formula Is Arbitrary

### Problem

The design defines:
```
engagement_score DECIMAL(5, 2)  -- Weighted: opens*1 + clicks*3 - unsubs*5
```

This formula mixes absolute counts with no normalization. A campaign sent to 100,000 people will always score higher than one sent to 1,000 people, regardless of engagement quality. The score has no bounded range, making tier assignment (`excellent`, `good`, `average`, `poor`) dependent on the data distribution.

### Suggestion

Normalize the score by volume:

```sql
engagement_score = (
    (open_rate * 25) +
    (click_rate * 50) +
    (click_to_open_rate * 25) -
    (bounce_rate * 25) -
    (unsubscribe_rate * 50)
)
-- Range: approximately -75 to +100
-- Tier thresholds: excellent > 60, good > 40, average > 20, poor <= 20
```

**Pros**:
- Volume-independent — compares campaigns fairly
- Bounded range — tier thresholds are stable
- Interpretable — based on industry-standard email metrics

**Cons**:
- More complex formula
- Weights are still arbitrary (but at least normalized)

### Recommendation

Use rate-based scoring instead of count-based. Define tier thresholds explicitly in the design document.

---

## 12. Minor Issues

### 12a. File count mismatch in summary
Section 3 header says "New Files (22)" but lists 25 files (numbered 1-25). Similarly, "Modified Files (13)" lists 16 files. Update the counts.

### 12b. `customer_360` partition key
The existing `customer_360` is partitioned by `customer_segment`. Adding GA4 and Mailchimp columns doesn't change the partition key, but the `source_count` field now goes up to 5. Verify that the `customer_segment` derivation logic still holds with the expanded source set.

### 12c. Mailchimp `campaign_id` format
The design says "10-char alphanumeric (lowercase)" but real Mailchimp campaign IDs are hex strings. Use hex format for more realistic mock data.

### 12d. Missing `reply_to` in staging
The raw `mailchimp.campaigns` schema includes `reply_to` but the staging schema drops it. If this is intentional (not analytically useful), document the omission. If accidental, add it.

---

## Summary of Recommendations

| # | Recommendation | Priority | Effort |
|---|---------------|----------|--------|
| 1 | Document GA4 webhook simulation as demo-only; rename endpoint | Medium | Low |
| 2 | Use TIMESTAMP(6) for GA4 raw/staging layers | Low | Low |
| 3 | Compute sessions in staging from events (drop ga4.sessions topic) | High | Medium |
| 4 | Add dead-letter handling for unknown Mailchimp events | Low | Low |
| 5 | Document GA4 entity resolution as simplified; note production mapping table | Medium | Low |
| 6 | Phase dashboard implementation | Medium | Low |
| 7 | Change subscriber partitioning to `months(signup_timestamp)` | High | Low |
| 8 | Add `engagement_by_channel` analytics table | Medium | Medium |
| 9 | Split implementation into 2-3 PRs | High | Low |
| 10 | Add data quality checks at staging layer | Medium | Medium |
| 11 | Use rate-based engagement scoring formula | Medium | Low |
| 12 | Fix file counts, minor schema corrections | Low | Low |
