# Senior Data Scientist Review - GA4 Integration (Data Gen → Staging)

**Reviewer**: Senior Data Scientist
**Date**: 2026-02-09
**Scope**: Tasks #1-7 (Data Generation → Staging Layer)
**Status**: ⚠️ **CONDITIONAL APPROVAL WITH REQUIRED FIXES**

---

## Executive Summary

The GA4 data generation and staging implementation demonstrates solid technical execution with correct handling of critical requirements (deduplication, session computation). However, **3 CRITICAL data quality issues** and **4 MEDIUM priority improvements** must be addressed before proceeding to Analytics layer.

**Overall Grade**: B+ (Very Good, Not Excellent)

**Recommendation**: Fix critical issues before proceeding to Task #8 (Entity Resolution).

---

## 1. GA4Provider Session Coherence Review

### ✅ **Strengths**

1. **Session Structure (Lines 226-277)**
   - ✅ Correctly generates coherent sessions with `session_start` → `page_view` → weighted events
   - ✅ Realistic timing: 0.1-0.5s for first page_view, then 5s-5min gaps
   - ✅ All events share same `client_id` and `session_id` (correct invariant)
   - ✅ Timestamps are monotonically increasing (line 240-276)

2. **Seed Reproducibility (Lines 35-42)**
   - ✅ Handles `seed=0` correctly with `if seed is not None` (per MEMORY.md pitfall)
   - ✅ Seeds both `random` and `Faker` for full determinism

3. **Multi-Session Generation (Lines 299-310)**
   - ✅ Generates 1-3 sessions per user (realistic)
   - ✅ Distributes events across sessions appropriately

### 🔴 **CRITICAL ISSUE #1: Missing Session Boundary Validation**

**Problem**: `generate_session_events()` does **not** enforce 30-minute gap between sessions for the same client_id.

**Location**: Lines 226-277 (GA4Provider.generate_session_events)

**Impact**: When generating multiple sessions for the same user (lines 299-310), sessions can have overlapping or too-close timestamps. This violates the 30-minute inactivity rule that `compute_ga4_sessions()` expects.

**Example Bug**:
```python
# Session 1 ends at: base_time + 600_000_000 (10 minutes)
# Session 2 starts at: new base_time (could be only 5 minutes later!)
# This creates invalid test data
```

**Fix Required**:
```python
# In generate_export_batch, track last_session_end per client_id
last_session_end = {}

for session in range(num_sessions):
    if client_id in last_session_end:
        # Enforce 31+ minute gap
        min_start = last_session_end[client_id] + 1860_000_000  # 31 min
        base_time = max(base_time, min_start)

    session_events = self.generate_session_events(...)
    last_session_end[client_id] = session_events[-1]["event_timestamp"]
```

**Priority**: 🔴 **CRITICAL** - Must fix before Analytics layer (will cause funnel analysis failures)

---

### 🟡 **MEDIUM ISSUE #1: Unrealistic Session Length Distribution**

**Problem**: Event count per session is uniform random (2-15 events), but real GA4 data is heavily skewed toward short sessions.

**Location**: Line 237 (`event_count or random.randint(2, 15)`)

**Impact**: Staging analytics will show unrealistic engagement patterns (too many long sessions).

**Real-world Distribution**:
- 60% of sessions: 1-3 events
- 30% of sessions: 4-10 events
- 10% of sessions: 11+ events

**Suggested Fix**:
```python
def _realistic_event_count():
    r = random.random()
    if r < 0.6: return random.randint(1, 3)
    elif r < 0.9: return random.randint(4, 10)
    else: return random.randint(11, 25)
```

**Priority**: 🟡 **MEDIUM** - Improves realism but not blocking

---

## 2. Deduplication Logic Review (stage_ga4_events)

### ✅ **Strengths**

1. **Correct Natural Key (Line 1188)**
   - ✅ Dedup by `(client_id, event_timestamp, event_name)` - matches RC-2 requirement
   - ✅ Uses `ROW_NUMBER()` window with `_loaded_at DESC` ordering
   - ✅ Keeps latest record on duplicate (correct for late-arriving data)

2. **Efficient Filtering (Line 1189)**
   - ✅ Filters `_dedup_rank == 1` immediately after window calculation
   - ✅ Drops helper column before writing

3. **Timestamp Conversion (Line 1191)**
   - ✅ Converts microseconds to `TIMESTAMP(6)` with correct precision
   - ✅ Formula: `CAST(event_timestamp / 1000000 AS TIMESTAMP)` is correct

### 🔴 **CRITICAL ISSUE #2: Missing Deduplication for FULL Mode**

**Problem**: Deduplication only applies to incremental batches. Running in `mode="full"` will skip deduplication entirely.

**Location**: Lines 1179-1186 (conditional loading) + Line 1188 (dedup window)

**Impact**: If `stage_ga4_events()` runs in full refresh mode, duplicates from `raw.ga4_events` will pass through to staging.

**Scenario**:
1. Batch ingest runs twice with same Parquet file (Airflow retry)
2. MERGE INTO prevents raw duplicates (✅)
3. BUT: If staging runs in full mode, it reads ALL raw events
4. Dedup window only works on in-memory DataFrame, not historical staging data
5. Result: Staging table gets duplicates ❌

**Fix Required**:
```python
# Option A: Always deduplicate (safer)
dedup_window = Window.partitionBy(
    "client_id", "event_timestamp", "event_name"
).orderBy(col("_loaded_at").desc())

staged_df = raw_df \
    .withColumn("_dedup_rank", row_number().over(dedup_window)) \
    .filter(col("_dedup_rank") == 1) \
    .drop("_dedup_rank")

# Option B: Use createOrReplace which truncates first (current approach OK if documented)
# Add assertion check after write to verify no duplicates
```

**Priority**: 🔴 **CRITICAL** - Could cause double-counting in analytics

---

### 🟡 **MEDIUM ISSUE #2: Event Classification Logic**

**Problem**: Hardcoded event type lists are incomplete and may drift from real GA4 events.

**Location**: Lines 1205-1206

```python
is_ecommerce_event = when(col("event_name").isin(
    "purchase", "add_to_cart", "begin_checkout", "view_item"), lit(True))
```

**Missing Events**:
- E-commerce: `remove_from_cart`, `add_shipping_info`, `add_payment_info`
- Engagement: `video_progress`, `video_complete`, `form_start`, `form_submit`

**Suggested Fix**: Move to config constant at top of file for maintainability.

**Priority**: 🟡 **MEDIUM** - Analytics may miss some event types

---

## 3. Session Computation Review (compute_ga4_sessions)

### ✅ **Strengths**

1. **30-Minute Gap Rule (Lines 1240-1243)** ✅ **EXCELLENT**
   - Correct LAG window: `Window.partitionBy("client_id").orderBy("event_timestamp")`
   - Gap calculation: `unix_timestamp(current) - unix_timestamp(prev)`
   - Threshold: `gap_sec > 1800` (30 minutes in seconds)
   - Cumulative sum for session numbering: `sum("new_sess").over(client_win)`

2. **First/Last Attribution (Lines 1245-1257)** ✅ **EXCELLENT** (RC-4 Fix)
   - Uses explicit `row_number()` windows instead of aggregate FIRST/LAST
   - Separate windows for first event (ascending) and last event (descending)
   - Handles edge case: Landing page only if `event_name == "page_view"`

3. **Channel Grouping (Lines 1272-1282)** ✅ **CORRECT**
   - Matches GA4's default channel grouping logic
   - Handles edge cases: `(direct)`, `(none)`, etc.

4. **Engaged Session Definition (Line 1289)** ✅ **CORRECT**
   - Matches GA4's definition: `>10s engagement OR ≥2 page views OR conversions`

5. **Bounce Rate Logic (Line 1290)** ✅ **CORRECT**
   - Bounce = single page view AND not engaged
   - Correct boolean logic with negation

### 🔴 **CRITICAL ISSUE #3: Session Aggregation Loses event_params**

**Problem**: Session aggregation (lines 1259-1270) does **not** preserve `event_params`, which are needed for funnel analysis.

**Location**: Lines 1259-1270 (groupBy aggregation)

**Impact**:
- **Funnel analysis** (Task #9) requires event-level params to identify e-commerce items
- **Page performance** (Task #9) requires page-level metrics from event_params
- Current implementation loses this critical data

**Fix Required**:

**Option A**: Keep `stg_ga4_events` table and join it in analytics layer (RECOMMENDED)
```python
# No change to compute_ga4_sessions
# Analytics layer will join:
# FROM stg_ga4_sessions s JOIN stg_ga4_events e ON s.session_id = e.session_id
```

**Option B**: Add aggregated params to sessions table
```python
.agg(
    ...existing aggregations...,
    collect_list(
        struct("event_name", "event_timestamp", "event_params", "page_location")
    ).alias("events")
)
```

**Priority**: 🔴 **CRITICAL** - Blocks funnel analysis implementation

**Decision**: I recommend **Option A** (keep events table). This follows the design doc which specifies separate `stg_ga4_events` and `stg_ga4_sessions` tables.

---

### 🟡 **MEDIUM ISSUE #3: Missing Session Duration Validation**

**Problem**: No validation that `session_duration_sec` is non-negative or realistic.

**Location**: Line 1263

**Edge Case**: If events are out of order (data quality issue), duration could be 0 or very large.

**Suggested Fix**:
```python
.withColumn("session_duration_sec",
    when(col("session_duration_sec") < 0, lit(0))
    .when(col("session_duration_sec") > 86400, lit(86400))  # Cap at 24 hours
    .otherwise(col("session_duration_sec"))
)
```

**Priority**: 🟡 **MEDIUM** - Defensive programming

---

### 🟡 **MEDIUM ISSUE #4: Channel Grouping UDF Performance**

**Problem**: Python UDF (lines 1272-1282) is less performant than Spark SQL.

**Impact**: For large datasets (millions of sessions), UDF creates serialization overhead.

**Suggested Fix**: Rewrite as Spark SQL `when().when()...otherwise()` chain.

**Priority**: 🟡 **MEDIUM** - Performance optimization (not blocking)

---

## 4. Data Quality & Edge Cases

### ✅ **Handled Correctly**

1. ✅ Null user_id (70% of users don't have user_id - realistic)
2. ✅ Watermark-based incremental processing (lines 1180-1181)
3. ✅ Empty dataset handling (lines 1185-1186, 1238)
4. ✅ Partitioning by month (correct for time-series data)

### ⚠️ **Missing Validations**

1. **Missing**: Assert that `event_timestamp` is not in the future
2. **Missing**: Assert that `event_date` matches `event_timestamp` date
3. **Missing**: Validate JSON structure of nested fields before extraction

**Suggested**: Add validation step before dedup:
```python
validated_df = raw_df.filter(
    (col("event_timestamp") <= current_timestamp() * 1_000_000) &
    (col("event_timestamp") > 0)
)
```

---

## 5. Test Coverage Assessment

### ✅ **Good Coverage**

Based on `tests/test_ga4_provider.py`:
- ✅ Schema validation
- ✅ Format validation (client_id, timestamps, dates)
- ✅ JSON structure validation
- ✅ Session coherence (same client_id, same session_id, monotonic timestamps)
- ✅ Seed reproducibility (including seed=0 edge case)

### ⚠️ **Missing Test Cases**

**CRITICAL Missing Tests**:
1. ❌ **Multi-session gap validation** (Issue #1)
2. ❌ **Deduplication in full mode** (Issue #2)
3. ❌ **Session computation with out-of-order events**
4. ❌ **Session boundary edge case** (event exactly at 30:00 mark)

**Recommended Additional Tests**:
```python
def test_multi_session_respects_30min_gap():
    """Test that multiple sessions for same user have 31+ min gaps."""
    provider = GA4Provider(seed=42)
    events = provider.generate_export_batch(num_users=10)

    # Group by client_id
    for client_id, client_events in group_by_client(events):
        sessions = group_by_session(client_events)
        for i in range(1, len(sessions)):
            gap_sec = (sessions[i][0]["event_timestamp"] -
                      sessions[i-1][-1]["event_timestamp"]) / 1_000_000
            assert gap_sec >= 1860, f"Session gap {gap_sec}s < 31 minutes"

def test_dedup_full_mode_with_duplicates(spark):
    """Test that full mode deduplicates correctly."""
    # Insert duplicate events
    # Run stage_ga4_events(spark, mode="full")
    # Assert row count matches unique events, not total events
```

---

## 6. Summary of Required Fixes

### 🔴 **CRITICAL (Must Fix Before Proceeding)**

| # | Issue | Location | Fix Complexity | ETA |
|---|-------|----------|----------------|-----|
| 1 | Multi-session gap enforcement | GA4Provider.generate_export_batch | 30 min | High |
| 2 | Deduplication in full mode | stage_ga4_events | 15 min | Low |
| 3 | Event params preservation | (Design decision - keep events table) | 5 min | Low |

**Total Critical Fix Time**: ~50 minutes

### 🟡 **MEDIUM (Should Fix Before Production)**

| # | Issue | Fix Complexity | ETA |
|---|-------|----------------|-----|
| 1 | Realistic session length distribution | 20 min | Low |
| 2 | Event classification completeness | 10 min | Low |
| 3 | Session duration validation | 10 min | Low |
| 4 | Channel grouping UDF → SQL | 30 min | Medium |

**Total Medium Fix Time**: ~70 minutes

---

## 7. Approval Decision

**Status**: ⚠️ **CONDITIONAL APPROVAL**

**Conditions**:
1. Fix **CRITICAL ISSUE #1** (multi-session gap)
2. Fix **CRITICAL ISSUE #2** (dedup in full mode)
3. Confirm **CRITICAL ISSUE #3** (design decision: keep events table for analytics)

**Once Fixed**:
- ✅ Safe to proceed to Task #8 (Entity Resolution)
- ✅ Safe to proceed to Task #9 (Analytics Tables)

**Estimated Fix Time**: 1 hour (critical fixes only)

---

## 8. Recommendations for Next Phase (Analytics Layer)

1. **Funnel Analysis** will need to:
   - Join `stg_ga4_events` (for event_params)
   - Join `stg_ga4_sessions` (for session context)
   - Use window functions for step-by-step tracking

2. **Page Performance** will need:
   - Event-level aggregation (not session-level)
   - Bounce rate calculation at page level
   - Join to sessions for traffic attribution

3. **Engagement Metrics** can use:
   - `stg_ga4_sessions` table directly (already has engagement flags)

---

**Senior Data Scientist Sign-Off**: 🟡 **APPROVED WITH CONDITIONS**

**Next Step**: Implement critical fixes, then proceed to PM review before starting Analytics layer.
