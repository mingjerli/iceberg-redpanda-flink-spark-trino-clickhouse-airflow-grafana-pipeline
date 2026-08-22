# PII Masking via Vault-Backed Tokenization

**Status:** Design approved and staff-reviewed, not yet implemented (9 review findings folded in)
**Date:** 2026-08-21
**Scope:** Direct identifiers across all five sources

---

## ⚠️ Demonstration Scope

This repository is a **demonstration pipeline, not a production system**. This
document describes a PII masking design that is architecturally honest — it
follows the shape a real deployment would take — but it makes deliberate
simplifications that a production or regulated deployment must not inherit.

Every such simplification is marked inline with a **Production note:** callout,
following the convention already used in `DESIGN_GA4.md`. The
[Production Gaps](#production-gaps) section collects them in one place.

Treat this design as a working reference implementation of the pattern. Do not
treat it as a compliance control. Nothing here has been reviewed against GDPR,
CCPA, HIPAA, or PCI-DSS requirements, and the data it protects is Faker-generated
throughout.

---

## 1. Problem

The pipeline currently implements no PII masking of any kind. Plaintext email
addresses, names, phone numbers, and street addresses flow unmodified from
webhook ingestion all the way to the serving layer.

Three findings motivate this work:

**PII reaches the serving layer in the clear.** `marts.customer_360` declares
`email`, `first_name`, `last_name`, `full_name`, `phone`, `address_line1`, and
`address_line2` as plain `STRING` columns
(`jobs/spark/marts_incremental.py:135-141`), populated directly from
`core.customers` (`:284-288`). `analytics.customer_metrics` carries `email` and
`full_name` (`jobs/spark/analytics_incremental.py:132-133`), and ClickHouse
mirrors both (`infrastructure/clickhouse/init-analytics.sql:17-18`).

**The semantic layer stores plaintext in two tables.**
`semantic.entity_index.match_reason` is written as
`concat("Matched via email: ", normalized_email)` (`entity_backfill.py:352`), so
every matched row carries the email address in a free-text column.
`semantic.blocking_index` stores blocking keys built as
`concat("email:", normalized_email)` (`jobs/spark/entity_backfill.py:373`) and
`concat("phone:", normalized_phone)` (`:391`). The table is, in effect, an index
of every email address and phone number the pipeline has seen.

**A dashboard displays raw PII today.**
`monitoring/dashboards/streaming_business.json` contains two panels that select
`email` and `receipt_email` directly from the raw layer. This leak is independent
of the rest of this design and is fixed as part of it.

The schemas under `schemas/*.json` carry `pii: true|false` annotations on 50
fields, but no code reads them. They are documentation with no consumer.

**Production note:** The `pii` annotations are also inconsistent. HubSpot contact
`phone` is marked `pii: true` while company `phone` is marked `pii: false`, and
`zip` differs the same way. No `schemas/ga4.json` exists at all. This design
therefore does not derive policy from those files; see
[Section 4](#4-the-pii-registry).

---

## 2. Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Strategy | Reversible tokenization with a vault | Supports authorized re-identification; keeps joins working |
| Boundary | Tokenize at the staging boundary | Only `raw.*` and the vault hold plaintext |
| Entity resolution | Operates entirely on tokens, no vault access | Exact-token matching reproduces current behavior exactly |
| Field scope | Direct identifiers only | Keeps `city`, `state`, `country`, and `zip` usable for geo analytics |
| Detokenization | Spark-only, audited | `detokenize()` is the only *audited* path; Trino and any Spark job can still read `semantic.pii_vault` directly (see Section 9) |

---

## 3. Threat Model and Guarantee

After implementation, plaintext direct identifiers exist in exactly two places:
the `raw.*` tables and `semantic.pii_vault`. Every layer from `staging.*`
downward — semantic, core, analytics, marts, ClickHouse, and Grafana — holds only
tokens.

This design defends against the realistic exposure path in this architecture:
analysts and dashboards querying the serving layer through Trino, ClickHouse, or
Grafana. A consumer with marts access can no longer read a customer's email
address.

It does **not** defend against an actor with MinIO bucket credentials or Spark
cluster access, both of which can read `raw.*` directly.

**Production note:** Raw-layer retention is currently unbounded, and
`docs/PROJECT_TRACKER.md:326` still lists a retention policy as an open decision.
Unbounded plaintext retention in the raw layer is the largest residual exposure in
this design. A production deployment must pair this work with a raw-layer
retention policy and, for a right-to-erasure obligation, a hard-delete path.
Neither is in scope here.

**Production note:** Deterministic tokenization is inherently vulnerable to
dictionary attack by anyone holding the pepper. Given the pepper, an attacker can
tokenize a candidate list of email addresses and match the results against the
marts layer. The pepper is therefore as sensitive as the plaintext itself. This is
a property of deterministic tokenization generally, not a defect in this design,
and it is the price paid for keeping joins working without vault access.

---

## 4. The PII Registry

`jobs/spark/pii/registry.py` maps each `(staging_table, column)` pair to a
semantic PII class. The registry is a hand-curated dictionary, not a read of
`schemas/*.json`.

Six classes cover the direct identifiers, as shipped in
`jobs/spark/pii/registry.py`:

| Class | Columns it covers | Normalizer |
|-------|-------------------|------------|
| `email` | Shopify `email` (customers) and `customer_email` (orders), HubSpot `email`, Stripe `email` (customers) and `billing_email` (charges), Mailchimp `email_address` and `email_normalized` (subscribers and events), GA4 `user_id` | `lower(trim(v))` |
| `phone` | Shopify `phone` (customers) and `customer_phone` (orders), Stripe `phone` (customers) and `billing_phone` (charges), HubSpot `phone` and `mobile_phone`, Mailchimp `phone` and `phone_normalized` | `regexp_replace(v, '[^0-9+]', '')`, then `NULL` if shorter than 7 characters |
| `name` | `first_name`, `last_name`, `full_name`, Stripe `name` (customers) and `shipping_name`, Stripe `billing_name` (charges) | `lower(trim(v))` |
| `address` | `address_line1`, `address_line2`, HubSpot `address`, Stripe `shipping_address_line1` | `lower(trim(v))` |
| `name_prefix` | Derived from `last_name` for blocking only | `lower(substring(v, 1, 3))` |
| `mailchimp_id` | Mailchimp `subscriber_id` (subscribers) and `email_id` (events) | `lower(trim(v))` |

`mailchimp_id` covers `subscriber_id`, which is `MD5(lower(email))` -- an
unsalted, publicly reproducible hash, so it is re-identifiable by dictionary
attack with no secret at all. It is weaker than the token that replaces it,
and it reaches `marts.customer_360.mailchimp_subscriber_id_token`.

The `email` and `phone` normalizers are lifted verbatim from
`entity_backfill.py:296` and `:299`. Token equality must reproduce today's match
equality exactly, so the normalizers cannot be rewritten from scratch.

Note that GA4 `user_id` maps to class `email`. `entity_backfill.py:251` sets
`user_id` to the customer's email address for entity resolution, so it must
tokenize identically to every other email column or cross-source matching breaks.
It is registered once, on `stg_ga4_events` -- `stg_ga4_sessions` has no entry of
its own, because `compute_ga4_sessions` reads `user_id_token` straight through
from `stg_ga4_events` without re-tokenizing it. Registering it a second time on
`stg_ga4_sessions` would hash an already-tokenized value -- `token(token(email))`
-- and silently break every GA4 cross-source match.

**Production note:** In a real system `user_id` is an opaque application
identifier rather than an email, and GA4 matching requires a separate
`semantic.ga4_user_mapping` lookup. `DESIGN_GA4.md:553` documents this
simplification.

The registry is hand-curated for two reasons. The `pii` flags in `schemas/*.json`
are internally inconsistent, and GA4 has no schema file. A curated registry is
also the artifact tests can enforce, which the annotations are not.

`zip`, `city`, `state`, `country`, and `country_code` stay in the clear. They
remain useful for geographic analysis, and they are weak identifiers in isolation.

**Production note:** Postal code combined with date of birth and gender is a
well-established re-identification vector. A production deployment handling a real
population should tokenize `zip` and emit a truncated `zip3` column for analytics
instead.

`stg_mailchimp_campaigns.from_name`, `from_email`, and `reply_to`
(`staging_batch.py:908-910`, `:959-961`) also stay in the clear, and are not in
the registry at all. These are not data-subject identifiers the way a
customer's own name or email is: they are the sending business's own contact
details, already published in the body of every campaign email by design, to
every recipient. Tokenizing them would not protect anyone's privacy -- the
values are not secret, Mailchimp's own dashboard shows them in plaintext to
anyone who can view a campaign -- and it would make campaign reporting
(`analytics.campaign_metrics`, the `batch_business` dashboard) unreadable for
no privacy gain, since an operator legitimately needs to know which sender
address a campaign went out under.

### Why `name_prefix` exists

`entity_backfill.py:403-413` builds a `name_zip` blocking key from
`lower(substring(last_name, 1, 3))` joined to `zip`. A prefix of a hash carries no
meaning, so the blocking key cannot be rebuilt from a `name` token.

The `name_prefix` class solves this by tokenizing the prefix itself as its own
value. Records sharing a surname prefix still share a token, so blocking behavior
is preserved exactly while the plaintext prefix never reaches the table.

**Production note:** A three-character prefix has roughly 17,000 possible values,
so a `name_prefix` token is trivially brute-forceable by anyone holding the pepper.
It reveals at most three letters of a surname, which is precisely the information
the existing blocking key already encodes, so this design accepts it. Low-entropy
classes are the weakest point of any deterministic tokenization scheme.

---

## 5. Token Scheme

A token derives deterministically from the normalized plaintext value and its PII
class:

```python
# jobs/spark/pii/tokenize.py
token = "tok_" + sha2(
    concat_ws("|", lit(PEPPER), lit(pii_class), normalized_value),
    256
).substr(1, 32)
```

Three properties make this work.

### Deterministic

The same input value always produces the same token. Equality joins and `GROUP BY`
continue to work throughout the downstream layers with no vault access at all.

### Keyed by semantic class, never by column name

This detail is load-bearing. Shopify `email`, HubSpot `email`, Stripe
`receipt_email`, and GA4 `user_id` all resolve to class `email`, so one address
produces one token across all four sources.

Keying the hash by column name instead would give the same address a different
token per source, and cross-source entity resolution would silently stop matching
anything. The failure is silent because unmatched records are not an error; they
simply become separate customers.

### Null-safe

`NULL` and empty string map to `NULL`, never to a token. Tokenizing `NULL` would
collapse every customer with a missing email into a single shared token and
fabricate matches between unrelated people.

The `phone` normalizer additionally returns `NULL` when the digit string is
shorter than seven characters. Today that check lives in the blocking filters as
`length(normalized_phone) >= 7` (`entity_backfill.py:388`, `:535`). Every token is
36 characters wide, so leaving the check downstream turns it into a no-op and
starts emitting blocking keys for the junk phone values it currently rejects.
Validity checks must therefore run on plaintext, inside the normalizer.

### Ordering constraint

Tokenization must run as the **final** step of each staging function, after every
derived column is built.

`staging_batch.py:346-350` constructs `full_name` by concatenating `first_name`
and `last_name`. Tokenizing the inputs first would concatenate two tokens into a
meaningless string, which would then be tokenized again. The rule is therefore:
build all derived plaintext columns, then tokenize the whole frame in one pass,
then write.

### Implementation choice

The implementation uses native `sha2(concat_ws(...))` rather than a Python UDF. The
computation stays on the JVM, and it matches the pattern
`jobs/spark/ga4_batch_ingest.py:74` already uses to build `_raw_id`.

**Production note:** A secret-prefix SHA-256 is not an HMAC. It is vulnerable to
length-extension, which allows an attacker who knows a token to forge additional
valid-looking tokens. It does not allow reversing a token, and a forged token
simply fails to resolve because the vault is the authority on which tokens exist.
This tradeoff is acceptable for a demonstration. A production deployment should use
a true HMAC and hold the key in a managed KMS rather than an environment variable.

---

## 6. Module Layout

```
jobs/spark/pii/
  __init__.py
  registry.py     # PII_FIELDS: (staging_table, column) -> pii_class
  tokenize.py     # Normalizers and tokenize_columns(df, table) -> df
  vault.py        # upsert_vault(), lookup()
  detokenize.py   # detokenize() and audit logging
```

Every file must begin with `from __future__ import annotations`. The Spark image
runs Python 3.8, where an evaluated `list[str]` annotation raises
`TypeError: 'type' object is not subscriptable` at import time, killing the job
before it starts.

---

## 7. The Vault

```sql
-- jobs/spark/pii/vault.py
CREATE TABLE IF NOT EXISTS iceberg.semantic.pii_vault (
    token          STRING NOT NULL COMMENT 'Deterministic token, primary key',
    pii_class      STRING NOT NULL COMMENT 'email | phone | name | address | name_prefix',
    plaintext      STRING NOT NULL COMMENT 'Original normalized value',
    key_version    INT            COMMENT 'Pepper version used to derive token',
    _first_seen_at TIMESTAMP      COMMENT 'When this token was first written',
    _first_source  STRING         COMMENT 'Staging table that first produced it'
)
USING iceberg
PARTITIONED BY (pii_class)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);
```

The vault is written with `MERGE ... WHEN NOT MATCHED THEN INSERT` keyed on
`token`. It is insert-only and never issues an `UPDATE`.

Two reasons support this. The repository's idempotency rules prohibit rewriting
watermark columns, and more fundamentally the plaintext for a given token is
immutable by construction. Because the token is a pure function of the plaintext, a
matching token can never require a different value.

`key_version` is carried from the start so that pepper rotation becomes possible
later without a schema migration.

> [!IMPORTANT]
> The MERGE source must be deduplicated on `token` before the merge runs.
> Multiple staging tables legitimately produce the same token for the same email
> address, which is the entire point of class-keyed tokens. Feeding a source with
> duplicate keys into a `MERGE` raises `MERGE_CARDINALITY_VIOLATION` — the same
> failure documented in `CLAUDE.md` from the GA4 sessions incident. Apply
> `dropDuplicates(["token"])` first.

**Production note:** This design ships no key rotation tooling. Rotating the pepper
requires re-tokenizing every table and rewriting the vault, which is a substantial
migration. A production deployment needs that tooling and a documented rotation
schedule before it stores real data.

### Idempotency

Tokenization is a pure function, and the vault write is a `MERGE` on the grain key.
Two consecutive runs therefore produce identical row counts, which satisfies the
verification rule in `CLAUDE.md`.

---

## 8. Pipeline Changes

### Staging

Each of the ten entries in `STAGING_FUNCTIONS`
(`jobs/spark/staging_batch.py:1325`) gains a single `tokenize_columns(df, table_key)`
call as the last step before the write, and the vault upsert happens in the same
job. See the [ordering constraint](#ordering-constraint) above.

Tokenized columns are renamed with an explicit `_token` suffix, so `email` becomes
`email_token`. The suffix is not cosmetic. It is what makes the enforcement test in
[Section 11](#11-testing) possible, because any bare `email` column appearing below
the staging boundary becomes a detectable violation.

`stg_mailchimp_subscribers._raw_id` is set from `subscriber_id_token`, not the
raw `subscriber_id` value. An earlier draft of this design implied `_raw_id`
stayed plaintext lineage, like every other table's `_raw_id`; it does not.
`subscriber_id` is itself PII (`pii/registry.py` class `mailchimp_id`) --
Mailchimp's own `MD5(lower(email))` convention, unsalted and publicly
reproducible -- so writing it into `_raw_id` unmodified would defeat the
tokenization on the very next column over. Lineage back to
`raw.mailchimp_subscribers` is not lost: `semantic.pii_vault` maps the token
back to that same MD5, and `raw`'s own `id` **is** that MD5, so a privileged
job can `detokenize()` and join on it.

### Entity resolution

`jobs/spark/entity_backfill.py` requires **no vault access at all**, and the job
becomes simpler rather than more complex:

- Exact email matching drops its `lower(trim(...))` wrapper and compares
  `email_token` directly, because values are pre-normalized by construction.
- Blocking keys become `email:<token>`, `phone:<token>`, and
  `name_zip:<name_prefix_token>_<zip>`. `semantic.blocking_index` therefore stops
  storing plaintext entirely, closing the leak at `:373` and `:391`.
- `zip` remains in the clear, so `name_zip` blocking continues to function.
- `match_reason` becomes `concat("Matched via email token: ", email_token)`, which
  removes the last plaintext column from `semantic.entity_index`.

> [!NOTE]
> An earlier draft of this design had entity resolution join the vault to recover
> plaintext for fuzzy name matching. The staff review established that **no fuzzy
> matching exists**. `match_type` only ever takes the values `exact_email` or
> `new_entity` (`entity_backfill.py:340-341`); records without an email each
> receive their own UUID (`:327-330`). The columns `fuzzy_name_matches`,
> `exact_phone_matches`, `ml_score_matches`, and `deterministic_rule_matches` are
> declared in the stats DDL at `:130-134` but are never populated by a real match.
>
> Removing the vault join is a meaningful simplification. No pipeline job reads
> plaintext, so no job can accidentally materialize it in a shuffle or spill.

> [!NOTE]
> `semantic.blocking_index` is written but never read by any pipeline job. It is
> referenced only in tests and as an Airflow task name. It is a prepared index
> awaiting a matching strategy that has not been built. This design preserves its
> behavior rather than removing it, because
> `tests/test_ga4_entity_resolution.py:159` asserts against it.

### Core, analytics, and marts

These changes are column renames rippling downstream:

| File | Change |
|------|--------|
| `jobs/spark/core_views.py:90-96` | `COALESCE(hc.email, sc.email) AS email` becomes `AS email_token`, and the same for name, phone, and address columns |
| `jobs/spark/core_views.py:165-166` | `core.orders` carries `so.customer_email_token AS customer_email_token` and `so.customer_phone_token AS customer_phone_token`, renamed from `customer_email`/`customer_phone` |
| `jobs/spark/marts_incremental.py:135-141` | `customer_360` drops seven plaintext columns and gains their `_token` equivalents |
| `jobs/spark/analytics_incremental.py:132-133` | `email` and `full_name` become `email_token` and `full_name_token` |
| `infrastructure/clickhouse/init-analytics.sql:17-18` | `email String` and `full_name String` become their `_token` equivalents |

### Grafana

`monitoring/dashboards/streaming_business.json` contains two panels that read
`email` and `receipt_email` from the raw layer. Because raw retains plaintext by
design, staging-level tokenization does not fix them. Both panels are repointed at
the tokenized staging tables.

---

## 9. Detokenization

```python
detokenize(spark, tokens, *, actor: str, reason: str) -> DataFrame
```

The function refuses to run without the vault credential, and every call appends a
row to an audit table:

```sql
-- jobs/spark/pii/detokenize.py
CREATE TABLE IF NOT EXISTS iceberg.semantic.pii_access_log (
    _access_id   STRING NOT NULL,
    actor        STRING NOT NULL COMMENT 'Who requested detokenization',
    reason       STRING NOT NULL COMMENT 'Stated purpose',
    pii_class    STRING,
    token_count  INT,
    tokens       ARRAY<STRING> COMMENT 'Tokens requested, never plaintext',
    accessed_at  TIMESTAMP NOT NULL
)
USING iceberg
PARTITIONED BY (pii_class);
```

The log records the tokens requested and never the plaintext returned. Logging the
returned values would turn the audit table into a second unguarded PII store, which
is a common way this pattern is implemented incorrectly.

`detokenize()` is the only **audited** path from a token back to plaintext --
every call it makes is logged to `semantic.pii_access_log`. It is not the only
path that can *reach* the vault. `infrastructure/trino/catalog/iceberg.properties`
mounts the entire Iceberg REST catalog with no schema filter and no access
control, so `SELECT plaintext FROM iceberg.semantic.pii_vault` also works from
the Trino CLI the README advertises, unaudited, bypassing `detokenize()`
entirely -- and any Spark job with catalog access can do the same by reading
the table directly. This is a documented gap, not an oversight: see
[Production Gaps](#production-gaps).

Separately, ClickHouse and Grafana have no route to the vault itself --
`infrastructure/clickhouse/iceberg_setup.sql` publishes no `semantic.*`
view -- but they do reach `raw.*` plaintext directly, through the
`iceberg.raw_*` views the same file publishes (Section 3).

**Production note:** `actor` and `reason` are self-reported by the calling job. A
production deployment must bind `actor` to an authenticated identity rather than
trusting a string argument, and should enforce approval workflows on bulk
detokenization.

---

## 10. Metrics

Two gauges are added and registered in `jobs/spark/metrics/registry.py`.
Registration is mandatory: `tests/test_metrics_registry.py` fails when an alert
references a metric with no declared producer.

| Metric | Labels | Purpose |
|--------|--------|---------|
| `pipeline_pii_vault_entries` | `pii_class` | Vault size by class |
| `pipeline_pii_tokenization_null_rate` | `table`, `column` | Canary for a broken normalizer |

The null-rate gauge matters more than it appears. A normalizer regression that
starts returning `NULL` shows up immediately as a null-rate spike, instead of
silently degrading match rates several layers downstream.

Both are gauges pushed through Pushgateway, consistent with the existing batch
metrics contract.

---

## 11. Testing

`tests/test_pii_registry.py` follows the structure of
`tests/test_metrics_registry.py` and uses the DDL helpers in
`tests/pipeline_tables.py`:

1. Every registry entry declares a valid class and has a defined normalizer.
2. **DDL scan:** no table at or below the staging boundary declares a bare PII
   column name.
3. **Determinism:** the same input produces the same token across calls.
4. **Null safety:** `NULL` and empty string produce `NULL`, not a token.
5. **Class scoping:** the same value in the same class produces an identical token
   across different sources; different classes produce different tokens.
6. **Vault idempotency:** running the upsert twice yields identical row counts,
   and a source containing duplicate tokens does not raise
   `MERGE_CARDINALITY_VIOLATION`.
7. **Derived column ordering:** `full_name_token` equals the token of the
   concatenated plaintext name, not a concatenation of tokens.
8. **Golden equivalence:** entity resolution over tokens produces the same
   `unified_id` groupings as entity resolution over plaintext, across a fixture set
   covering mixed-case emails and differently formatted phone numbers.

Test 8 is the one that matters most. It catches a normalizer that drifts from
`entity_backfill.py` and quietly degrades match quality, which is otherwise
invisible because unmatched records are not errors.

---

## 12. Migration

The order matters, and step 4 is the step most easily forgotten:

1. Deploy the code with tokenization enabled.
2. Drop every staging table with `DROP TABLE ... PURGE` (`PURGE`, not a plain
   `DROP TABLE` -- a plain drop removes the catalog entry but leaves the
   pre-migration plaintext Parquet files sitting in MinIO) and run
   `staging_batch.py --mode full` to recreate them, re-tokenize all data, and
   populate the vault.
3. Rebuild semantic, core, analytics, and marts in full.
4. Run `expire_snapshots.py --retention-days 0 --retain-last 1
   --remove-orphans --older-than "<now, UTC>"` against every rewritten table.
   Both `--retain-last 1` and `--older-than` must be passed explicitly:
   `RETAIN_LAST_N` defaults to 3, which keeps three pre-migration snapshots no
   matter what `--retention-days` says, and `remove_orphan_files` defaults
   `older_than` to three days ago, which leaves same-day orphan files (exactly
   what this migration just produced) on disk. `--retain-last 1` is the
   correct floor, not 0: Iceberg's `expire_snapshots` procedure requires
   `retain_last >= 1` and always keeps the current snapshot anyway, and after
   step 3's rebuild that current snapshot is the tokenized one, so `1` still
   purges every pre-migration, plaintext-bearing snapshot. See
   `docs/RUNBOOK.md`'s migration section for the exact commands.

Without step 4, Iceberg time travel continues to serve the pre-migration snapshots
containing plaintext, and the migration is cosmetic.

**Production note:** Snapshot expiry removes the metadata pointers, and by
default keeps 3 recent snapshots and skips orphan files less than three days
old -- both wrong for a same-day migration purge. `--retain-last 1` and
`--older-than` override those defaults; confirm with `--remove-orphans` that
the underlying data files are also removed from MinIO before considering
plaintext purged.

---

## 13. Implementation Phases

Each phase is test-first and committed only once its tests pass, per the multi-step
workflow in `CLAUDE.md`. Every phase leaves the pipeline in a working state.

| Phase | Deliverable |
|-------|-------------|
| 1 | `jobs/spark/pii/` module with unit tests, no pipeline wiring |
| 2 | Staging tokenization, vault population, **and** the `entity_backfill.py` rewire, in one commit |
| 3 | Core, marts, analytics, and ClickHouse column renames |
| 4 | `detokenize()` and the access log |
| 5 | Grafana fix, snapshot expiry migration, metrics registry, docs |

Phases 2 and 3 of the original draft are merged deliberately. Renaming staging
columns without simultaneously rewiring `entity_backfill.py` would leave the job
reading a column that no longer exists, so splitting them produces a commit whose
tests cannot pass.

---

## Production Gaps

This section collects every **Production note:** above. A production or regulated
deployment must address all of them.

| Gap | Section | Impact |
|-----|---------|--------|
| Secret-prefix SHA-256 instead of true HMAC | [5](#5-token-scheme) | Token forgery is possible, though reversal is not |
| Pepper stored in `.env`, not a managed KMS | [5](#5-token-scheme) | Pepper compromise enables dictionary attack on every token |
| Deterministic tokens are dictionary-attackable | [3](#3-threat-model-and-guarantee) | Inherent to the strategy; the pepper is as sensitive as plaintext |
| `name_prefix` tokens are low entropy | [4](#why-name_prefix-exists) | Roughly 17,000 possible values, brute-forceable given the pepper |
| No key rotation tooling | [7](#7-the-vault) | Rotation requires a full manual re-tokenization |
| Unbounded raw-layer plaintext retention | [3](#3-threat-model-and-guarantee) | Largest residual exposure in the design |
| No right-to-erasure or hard-delete path | [3](#3-threat-model-and-guarantee) | Cannot satisfy a data subject deletion request |
| `zip` left in the clear | [4](#4-the-pii-registry) | Re-identification vector when combined with other attributes |
| `actor` is self-reported, not authenticated | [9](#9-detokenization) | The audit trail is trust-based |
| GA4 `user_id` is an email rather than an opaque ID | [4](#4-the-pii-registry) | Demonstration simplification, documented in `DESIGN_GA4.md:553` |
| No Trino or ClickHouse access control | [3](#3-threat-model-and-guarantee) | `raw.*` plaintext (via ClickHouse's `iceberg.raw_*` views and Trino) and `semantic.pii_vault` plaintext (via Trino or any Spark job) are readable by any consumer with catalog access, unaudited -- it is the plaintext that is exposed, not the token columns, which are meant to be readable everywhere below staging |
| Not reviewed against any regulatory framework | Top | No compliance claim is made |

---

## Out of Scope

- Raw-layer retention policy and hard deletion
- Trino access control and ClickHouse RBAC
- Key rotation tooling
- Correcting the `pii` annotations in `schemas/*.json`
- Quasi-identifier treatment beyond direct identifiers
- Implementing the fuzzy, phone, and ML matching strategies whose stats columns
  already exist in `semantic.entity_resolution_stats` but are never populated

---

## Design Review Changes

The staff-engineer review required by `CLAUDE.md` changed six things in the
original draft. Recorded here as provenance.

| # | Finding | Change |
|---|---------|--------|
| 1 | No fuzzy name matching exists; `match_type` is only ever `exact_email` or `new_entity` | Entity resolution no longer reads the vault. No pipeline job touches plaintext |
| 2 | `name_zip` blocking uses a 3-character surname prefix, which a hash cannot preserve | Added the `name_prefix` PII class, tokenizing the prefix as its own value |
| 3 | `full_name` is built by concatenating `first_name` and `last_name` at staging | Added the ordering constraint: derive all plaintext columns first, tokenize last |
| 4 | Original phases 2 and 3 left `entity_backfill.py` reading a dropped column between commits | Merged into a single phase |
| 5 | Multiple staging tables produce identical tokens, risking `MERGE_CARDINALITY_VIOLATION` | Added the mandatory `dropDuplicates(["token"])` before the vault merge |
| 6 | `analytics.customer_metrics` carries `full_name` in addition to `email` | Corrected the rename table |
| 7 | Phone blocking guards on `length(normalized_phone) >= 7`, but every token is 36 characters, making the guard a no-op | Moved the length check into the `phone` normalizer, which now returns `NULL` for short values |
| 8 | `entity_index.match_reason` embeds the plaintext email (`:352`) | `match_reason` now carries the token; `entity_index` holds no plaintext |
| 9 | The `phone` class column list omitted Stripe `phone` and Mailchimp `phone_normalized` | Completed the column list |
