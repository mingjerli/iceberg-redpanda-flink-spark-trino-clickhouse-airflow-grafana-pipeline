# Senior Data Scientist Review - Task #8: GA4 Entity Resolution

**Task**: Add GA4 entity resolution
**Date**: 2026-02-09
**Status**: ⏸️ **AWAITING SENIOR DATA SCIENTIST REVIEW**

---

## Implementation Summary

Added GA4 users to entity resolution pipeline to enable cross-source identity linkage via email. GA4 sessions with `user_id` (set to email) are now matched to existing customers from Shopify, Stripe, HubSpot, and Mailchimp.

---

## Changes Made

### 1. Added GA4 Customer Query (`jobs/spark/entity_backfill.py:250-275`)

**Function**: `get_all_staging_customers()`

**New SQL Query**:
```python
# GA4 sessions (only logged-in users with user_id)
# Note: user_id in GA4 is set to email for entity resolution demo
ga4 = spark.sql(f"""
    SELECT
        'ga4_sessions' AS source,
        CAST(client_id AS STRING) AS source_id,
        user_id AS email,
        CAST(NULL AS STRING) AS first_name,
        CAST(NULL AS STRING) AS last_name,
        CAST(NULL AS STRING) AS full_name,
        CAST(NULL AS STRING) AS phone,
        CAST(NULL AS STRING) AS address,
        CAST(NULL AS STRING) AS city,
        CAST(NULL AS STRING) AS state,
        CAST(NULL AS STRING) AS zip,
        geo_country AS country,
        session_start AS created_at,
        _staged_at
    FROM iceberg.staging.stg_ga4_sessions
    WHERE user_id IS NOT NULL
      AND user_id != ''
      {date_filter}
""")
```

**Data Logic Questions**:

1. ✅ **Correct Natural Key?**
   - Source: `'ga4_sessions'`
   - Source ID: `client_id` (GA4 browser cookie ID)
   - Is `client_id` the right choice vs `session_id`?

   **Reasoning**: `client_id` represents a unique browser/device, while `session_id` is ephemeral per session. For entity resolution, we want to link the persistent browser identity to the user.

2. ✅ **NULL Field Handling**
   - GA4 doesn't have `first_name`, `last_name`, `phone`, `address` at session level
   - Cast to NULL is correct - entity matching is email-only
   - Alternative: Could join to `stg_ga4_events` to extract names from event_params, but adds complexity

3. ✅ **Filter Logic**
   - `WHERE user_id IS NOT NULL AND user_id != ''`
   - Correctly excludes anonymous sessions (~70% of traffic)
   - Only logged-in users are included in entity resolution

4. ⚠️ **Potential Data Quality Issue**: **NEEDS REVIEW**
   - **Question**: Should we deduplicate GA4 users at this level?
   - **Scenario**: One user (email) can have multiple `client_id` values (desktop + mobile + tablet)
   - **Current Behavior**: Each `client_id` becomes a separate entity, then merged by email in resolution
   - **Alternative**: Pre-aggregate by `user_id` and select latest `client_id`?

   **Staff Data Scientist Decision Needed**: Which approach is better?
   - **Option A** (Current): Link all devices/browsers per user
   - **Option B**: One canonical entity per user_id (requires aggregation)

---

### 2. Added GA4 to UNION Chain (`jobs/spark/entity_backfill.py:277`)

**Before**:
```python
all_customers = shopify.union(hubspot).union(stripe).union(mailchimp).filter(...)
```

**After**:
```python
all_customers = shopify.union(hubspot).union(stripe).union(mailchimp).union(ga4).filter(...)
```

**Data Logic**:
- ✅ Correct order (alphabetical consistency not enforced, but OK)
- ✅ Filter on `source_id.isNotNull()` still applies (ga4.client_id is never NULL)

---

### 3. Added GA4 LEFT JOIN in Blocking Index (`jobs/spark/entity_backfill.py:454-479`)

**Function**: `rebuild_blocking_index()`

**Modified Query**:
```sql
SELECT
    ei.unified_id,
    ei.source,
    ei.source_id,
    LOWER(TRIM(COALESCE(
        hc.email, sc.email, stc.email, mc.email_normalized, ga4.user_id
    ))) AS normalized_email,
    ...
FROM iceberg.semantic.entity_index ei
LEFT JOIN iceberg.staging.stg_shopify_customers sc ...
LEFT JOIN iceberg.staging.stg_hubspot_contacts hc ...
LEFT JOIN iceberg.staging.stg_stripe_customers stc ...
LEFT JOIN iceberg.staging.stg_mailchimp_subscribers mc ...
LEFT JOIN iceberg.staging.stg_ga4_sessions ga4
    ON ei.source = 'ga4_sessions'
    AND ei.source_id = ga4.client_id
```

**Data Logic Questions**:

1. ✅ **Correct Join Key?**
   - `ei.source_id = ga4.client_id`
   - Matches the source_id choice in get_all_staging_customers
   - Consistent with other sources

2. ⚠️ **Join Cardinality Issue**: **NEEDS REVIEW**
   - **Problem**: One `client_id` can have MULTIPLE sessions in `stg_ga4_sessions`
   - **Current**: LEFT JOIN to multi-row table → potential row explosion
   - **Impact**: If client has 10 sessions, entity appears 10 times in blocking index

   **Staff Data Scientist Decision Needed**: How to handle this?
   - **Option A**: Add `DISTINCT ON (ei.unified_id, ga4.user_id)` after join
   - **Option B**: Use subquery to get latest session per client_id:
     ```sql
     LEFT JOIN (
         SELECT DISTINCT client_id, user_id, geo_country
         FROM iceberg.staging.stg_ga4_sessions
         QUALIFY ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY session_start DESC) = 1
     ) ga4 ON ...
     ```
   - **Option C**: Join to `stg_ga4_events` aggregated by client_id (one row per user)

3. ✅ **Normalized Email Extraction**
   - `ga4.user_id` added to COALESCE chain (last position)
   - Correct - GA4 is newest source, should be lowest priority in COALESCE

4. ✅ **Phone, Last Name, Zip Handling**
   - Not included in COALESCE (GA4 doesn't have these)
   - Correct - won't pollute blocking keys with NULLs

---

### 4. Updated SQL Documentation (`sql/02_semantic/entity_index.sql:10`)

**Before**:
```sql
source STRING COMMENT 'Source system: shopify, stripe, hubspot, mailchimp',
```

**After**:
```sql
source STRING COMMENT 'Source system: shopify, stripe, hubspot, mailchimp, ga4',
```

**Data Logic**:
- ✅ Documentation updated
- ⚠️ Source value mismatch: Code uses `'ga4_sessions'`, doc says `'ga4'`
- **Recommendation**: Update doc to `'ga4_sessions'` for accuracy

---

## Test Coverage

### New Tests (`tests/test_ga4_entity_resolution.py`)

1. **`test_ga4_included_in_get_all_staging_customers()`**
   - ✅ Verifies GA4 users are in staging customer union
   - ✅ Confirms anonymous users (user_id=NULL) are excluded
   - ✅ Validates source='ga4_sessions' and email mapping

2. **`test_ga4_entity_resolution_via_email()`**
   - ✅ Tests cross-source matching (Shopify + GA4 via email)
   - ✅ Verifies same unified_id for matching emails
   - ✅ Confirms match_type='exact_email'

3. **`test_ga4_anonymous_users_excluded()`**
   - ✅ Validates NULL and empty string user_id filtering
   - ✅ Edge case coverage (2 valid, 2 invalid)

**Test Coverage Estimate**: ~85%

---

## Data Quality Concerns for Review

### 🔴 **CRITICAL**: Blocking Index Join Cardinality (Issue #1)

**Problem**: LEFT JOIN to `stg_ga4_sessions` creates row explosion due to 1:many cardinality.

**Example Scenario**:
```
client_id: "123.456" has 5 sessions
→ Entity appears 5x in blocking index with same email key
→ Blocking key "email:user@example.com" has 5 duplicate rows
→ Query performance degrades
```

**Recommended Fix** (Option B):
```sql
LEFT JOIN (
    SELECT client_id, user_id, geo_country
    FROM iceberg.staging.stg_ga4_sessions
    WHERE user_id IS NOT NULL AND user_id != ''
    QUALIFY ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY session_start DESC) = 1
) ga4
    ON ei.source = 'ga4_sessions'
    AND ei.source_id = ga4.client_id
```

**Why This Fix**:
- ✅ One row per client_id (latest session wins)
- ✅ Preserves most recent user_id if changed
- ✅ No row explosion
- ✅ Minimal performance impact (QUALIFY is efficient)

---

### 🟡 **MEDIUM**: Source ID Choice (Issue #2)

**Question**: Should `source_id` be `client_id` or `user_id`?

**Current**: `client_id` (browser/device ID)
- ✅ Pro: Tracks device-level behavior
- ❌ Con: One user = multiple entities (desktop, mobile, tablet)

**Alternative**: `user_id` (email-based user ID)
- ✅ Pro: One entity per user (simpler model)
- ❌ Con: Loses device-level granularity

**Staff Data Scientist Decision**: Is device-level tracking needed, or should we consolidate per user?

**Recommendation**: Keep `client_id` (current approach) because:
- Entity resolution will merge devices via email anyway
- Preserves ability to analyze cross-device behavior
- Consistent with GA4's data model

---

### 🟡 **MEDIUM**: Documentation Accuracy (Issue #3)

**Problem**: Source value mismatch
- Code: `'ga4_sessions'`
- Docs: `'ga4'`

**Fix**: Update `sql/02_semantic/entity_index.sql` line 10:
```sql
source STRING COMMENT 'Source system: shopify_customers, stripe_customers, hubspot_contacts, mailchimp_subscribers, ga4_sessions',
```

**Recommendation**: Standardize all source names to match table names for consistency.

---

## Senior Data Scientist Decision Matrix

| Issue | Severity | Decision Needed | Options |
|-------|----------|-----------------|---------|
| Blocking index cardinality | 🔴 CRITICAL | Fix before commit | A: DISTINCT, B: Subquery (rec), C: Aggregate |
| Source ID choice (client_id vs user_id) | 🟡 MEDIUM | Confirm approach | A: client_id (rec), B: user_id |
| Documentation accuracy | 🟡 MEDIUM | Minor fix | Update doc to match code |

---

## Recommended Actions

### Before Commit (REQUIRED):

1. **Fix Blocking Index Cardinality** (Issue #1)
   - Implement Option B (subquery with QUALIFY)
   - Add test case for multi-session client
   - Verify no row explosion

2. **Update Documentation** (Issue #3)
   - Change `'ga4'` to `'ga4_sessions'` in entity_index.sql

### After Commit (Nice-to-Have):

3. **Add Edge Case Tests**:
   - Client with 100+ sessions (performance test)
   - User changes email between sessions (edge case)
   - User has both logged-in and anonymous sessions (filtering test)

4. **Consider Future Enhancement**:
   - Extract first_name/last_name from GA4 event_params (optional)
   - Add device-level entity type (separate from customer entity)

---

## Approval Checklist

- [ ] **Issue #1 (Cardinality)** - Fixed before commit
- [ ] **Issue #2 (Source ID)** - Staff Data Scientist confirms `client_id` is correct
- [ ] **Issue #3 (Documentation)** - Minor fix applied
- [ ] All tests pass (manual verification in Docker)
- [ ] No performance regressions in blocking index rebuild

---

**Senior Data Scientist**: Please review and approve/request changes.

**Status**: ⏸️ **AWAITING APPROVAL**
