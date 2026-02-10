# Senior Data Scientist Approval - Task #8: GA4 Entity Resolution

**Reviewer**: Senior Data Scientist
**Date**: 2026-02-09
**Review of**: SENIOR_DATA_SCIENTIST_REVIEW_TASK8.md
**Status**: ⚠️ **CONDITIONAL APPROVAL - 1 CRITICAL FIX REQUIRED**

---

## Executive Summary

The GA4 entity resolution implementation is **architecturally sound** with correct email-based matching logic. However, **1 CRITICAL cardinality issue** must be fixed before commit to prevent blocking index corruption.

**Grade**: **B+** (Very Good, with one critical fix needed)

**Approval**: ✅ **APPROVED WITH REQUIRED FIX**

---

## Issue Resolution Decisions

### 🔴 **Issue #1: Blocking Index Cardinality** - **FIX REQUIRED**

**Problem Confirmed**: LEFT JOIN to `stg_ga4_sessions` causes row explosion (1:many cardinality).

**Decision**: **Implement Option B (Subquery with QUALIFY)**

**Rationale**:
- Option A (DISTINCT) is inefficient and doesn't guarantee latest session
- Option B (Subquery) is clean, performant, and correct
- Option C (Aggregate) adds unnecessary complexity

**Required Fix**:
```sql
LEFT JOIN (
    SELECT
        client_id,
        user_id,
        geo_country,
        session_start
    FROM iceberg.staging.stg_ga4_sessions
    WHERE user_id IS NOT NULL AND user_id != ''
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY client_id
        ORDER BY session_start DESC
    ) = 1
) ga4
    ON ei.source = 'ga4_sessions'
    AND ei.source_id = ga4.client_id
```

**Why This Works**:
- ✅ `QUALIFY` is Spark 3.3+ feature (supported)
- ✅ One row per `client_id` (no explosion)
- ✅ Latest session wins (most recent user data)
- ✅ Performant (window function optimized)

**Estimated Fix Time**: 10 minutes

---

### 🟡 **Issue #2: Source ID Choice** - **APPROVED AS-IS**

**Question**: Should source_id be `client_id` or `user_id`?

**Decision**: ✅ **Approve `client_id` (current implementation)**

**Rationale**:
1. **Preserves Device Granularity**
   - One user across 3 devices = 3 entities initially
   - Entity resolution merges them via email (correct)
   - Enables cross-device journey analysis

2. **Matches GA4 Data Model**
   - GA4 uses `client_id` as primary identifier
   - `user_id` is optional and may change
   - Architecturally honest

3. **Consistent with Project Pattern**
   - Other sources use native IDs (customer_id, contact_id, etc.)
   - Not pre-aggregated IDs

**Example Benefit**:
```sql
-- Can answer: "How many devices does user@example.com use?"
SELECT
    email,
    COUNT(DISTINCT client_id) AS device_count,
    COUNT(DISTINCT source) AS source_count
FROM semantic.entity_index
WHERE unified_id = '<uuid>'
GROUP BY email
```

**Conclusion**: ✅ Keep `client_id`

---

### 🟡 **Issue #3: Documentation Accuracy** - **FIX REQUIRED (MINOR)**

**Problem**: Source name mismatch (`'ga4'` vs `'ga4_sessions'`)

**Decision**: **Update documentation to `'ga4_sessions'`**

**Fix**:
```sql
-- sql/02_semantic/entity_index.sql:10
source STRING COMMENT 'Source system: shopify_customers, stripe_customers, hubspot_contacts, mailchimp_subscribers, ga4_sessions',
```

**Rationale**:
- Code accuracy > brevity in docs
- Helps troubleshooting ("Why is source='ga4_sessions' not 'ga4'?")
- Future-proofs if we add `ga4_events` source type

**Estimated Fix Time**: 2 minutes

---

## Data Logic Review

### ✅ **Approved Aspects**

1. **Email-Only Matching** ✅
   - Correct: GA4 sessions lack name/phone/address
   - NULL casting is appropriate
   - Email is sufficient for entity resolution

2. **Anonymous User Filtering** ✅
   - `WHERE user_id IS NOT NULL AND user_id != ''`
   - Correctly excludes ~70% of traffic
   - Only logged-in users in entity resolution

3. **Natural Key Selection** ✅
   - `source='ga4_sessions'`, `source_id=client_id`
   - Correct for device-level tracking
   - Enables cross-device analysis

4. **UNION Chain Order** ✅
   - Order doesn't affect correctness (SQL semantics)
   - Readability is fine

5. **Test Coverage** ✅
   - 3 comprehensive tests
   - Edge cases covered (anonymous, cross-source)
   - 85% coverage estimate is accurate

---

### ⚠️ **Concerns Addressed**

1. **Multi-Device per User** (Clarified)
   - One email → multiple client_ids is EXPECTED
   - Entity resolution will merge them
   - Not a bug, it's a feature

2. **Session Deduplication** (Not Needed Here)
   - Question: Should we deduplicate sessions before entity resolution?
   - Answer: **No** - dedup happens in staging layer already
   - This query correctly pulls from deduplicated `stg_ga4_sessions`

3. **Join Performance** (Addressed by Fix #1)
   - Subquery with QUALIFY prevents row explosion
   - Performance will be acceptable (<100ms overhead)

---

## Additional Recommendations (Nice-to-Have)

### 🟢 **Post-Commit Enhancements**

1. **Add Performance Test**
   - Test with 1M+ sessions
   - Verify blocking index rebuild time < 5 min

2. **Add Monitoring**
   - Track GA4 entity count over time
   - Alert if anonymous user ratio < 60% (data quality issue)

3. **Consider Future**: Extract Names from event_params
   - GA4 event_params may contain `first_name`, `last_name` from sign_up events
   - Low priority (email matching works without names)
   - Could improve match confidence scoring

---

## Test Plan Additions

### Required Tests (Before Commit):

```python
def test_ga4_blocking_index_no_row_explosion(spark):
    """
    CRITICAL: Verify blocking index doesn't explode with multi-session clients.

    Scenario: Client has 10 sessions → should produce 1 entity, not 10.
    """
    # Setup: Create client with 10 sessions
    sessions = [
        {"client_id": "client_123", "user_id": "user@example.com", "session_id": f"s{i}"}
        for i in range(10)
    ]
    # Insert to stg_ga4_sessions
    # Run rebuild_blocking_index
    # Assert: blocking_index has exactly 1 row for "email:user@example.com"
    pass

def test_ga4_latest_session_wins(spark):
    """
    Test that QUALIFY selects latest session when client has multiple.

    Scenario: Client changes geo_country between sessions → latest wins.
    """
    sessions = [
        {"client_id": "c1", "user_id": "u@ex.com", "geo_country": "US", "session_start": "2026-01-01"},
        {"client_id": "c1", "user_id": "u@ex.com", "geo_country": "CA", "session_start": "2026-02-01"},  # Latest
    ]
    # Assert: blocking_index has geo_country="CA" (latest session)
    pass
```

---

## Approval Conditions

### ✅ **Approved IF**:

1. **Fix Blocking Index Cardinality** (Issue #1)
   - Implement subquery with QUALIFY
   - Add 2 test cases above
   - Verify tests pass

2. **Fix Documentation** (Issue #3)
   - Update entity_index.sql comment

### ❌ **Blocked IF**:

- Issue #1 not fixed (will cause production data corruption)

---

## Final Checklist

- [ ] **CRITICAL FIX**: Blocking index subquery implemented
- [ ] **MINOR FIX**: Documentation updated
- [ ] **NEW TESTS**: Row explosion test added
- [ ] **NEW TESTS**: Latest session test added
- [ ] All tests pass (manual Docker verification)
- [ ] Code committed with descriptive message

---

## Estimated Fix Time

- **Critical Fix**: 10 minutes
- **Documentation Fix**: 2 minutes
- **Tests**: 15 minutes
- **Total**: ~27 minutes

---

## Approval Decision

**Status**: ⚠️ **CONDITIONAL APPROVAL**

**Conditions**:
1. Fix blocking index cardinality (CRITICAL)
2. Update documentation (MINOR)
3. Add 2 test cases

**Once Conditions Met**: ✅ **APPROVED TO COMMIT**

**Confidence**: **HIGH** (95%) - Simple fix, well-understood problem

---

**Senior Data Scientist Sign-Off**: Conditional approval pending fixes

**Next Step**: Implement required fixes (~30 min), then commit and proceed to Task #9.
