# PII Masking Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace plaintext direct identifiers with deterministic tokens from the staging layer downward, backed by a vault that supports audited re-identification.

**Architecture:** Each staging job tokenizes its PII columns as its final step before write, using `sha2(pepper | pii_class | normalized_value)`. Tokens are keyed by semantic class, not column name, so one email yields one token across all five sources and cross-source entity resolution keeps working unchanged. Plaintext survives only in `raw.*` and `semantic.pii_vault`.

**Tech Stack:** PySpark 3.5.3 (Python 3.8 in the Spark image), Apache Iceberg 1.5.0, pytest, ClickHouse, Grafana.

**Spec:** `docs/DESIGN_PII_MASKING.md`

## Global Constraints

- **This repository is a demonstration, not a production system.** Preserve the `**Production note:**` callout convention when touching design docs.
- **Every file under `jobs/spark/` and every test file must start with `from __future__ import annotations`.** The Spark image is Python 3.8; an evaluated `list[str]` annotation raises `TypeError: 'type' object is not subscriptable` at import time.
- **Run tests only via `./scripts/run_tests.sh`.** It executes inside the `infrastructure-spark-master` image. No other service needs to be up.
- **Never widen `EXTERNAL_METRIC_PREFIXES`.** New metrics need a producer registered in `jobs/spark/metrics/registry.py`.
- **All pipeline metrics are gauges.** Pushgateway replaces a group on push, so a pushed counter breaks `increase()`.
- **Idempotency:** a filtered read pairs with `append`; an unfiltered read pairs with `createOrReplace` or `MERGE` on the grain key. Verify with two consecutive runs producing identical row counts.
- **Shell scripts must never mask an exit code.** Use `run_spark_job()` or capture `${PIPESTATUS[0]}`.
- **Token format:** `tok_` + first 32 hex characters of the SHA-256 digest. 36 characters total.
- **Pepper:** read from the `PII_TOKEN_PEPPER` environment variable. Never commit a real value.

---

## File Structure

| File | Responsibility |
|------|----------------|
| `jobs/spark/pii/__init__.py` | Package marker |
| `jobs/spark/pii/registry.py` | Which `(table, column)` pairs are PII and of what class. The control plane. |
| `jobs/spark/pii/tokenize.py` | Normalizers and the token expression; produces the tokenized frame and vault rows |
| `jobs/spark/pii/vault.py` | Vault DDL and the insert-only MERGE |
| `jobs/spark/pii/detokenize.py` | Authorized reverse lookup and the access log |
| `tests/test_pii_tokenize.py` | Normalizer and token-property tests |
| `tests/test_pii_vault.py` | Vault MERGE and idempotency tests |
| `tests/test_pii_registry.py` | Enforcement ratchet: no bare PII column below staging |
| `tests/test_pii_entity_resolution.py` | Golden equivalence: token-based ER matches plaintext ER |
| `tests/test_pii_detokenize.py` | Detokenization and audit-log tests |

---

## Task 1: PII registry and tokenizer

**Files:**
- Create: `jobs/spark/pii/__init__.py`
- Create: `jobs/spark/pii/registry.py`
- Create: `jobs/spark/pii/tokenize.py`
- Test: `tests/test_pii_tokenize.py`

**Interfaces:**
- Consumes: nothing
- Produces:
  - `registry.PII_FIELDS: dict` — table to `{column: pii_class}`
  - `registry.PII_DERIVED: dict` — table to `{new_column: (source_column, pii_class)}`
  - `registry.PII_CLASSES: tuple`
  - `registry.pii_columns(table) -> dict`
  - `registry.derived_columns(table) -> dict`
  - `registry.token_column(column) -> str` returning `f"{column}_token"`
  - `tokenize.normalize(column, pii_class) -> Column`
  - `tokenize.token_expr(normalized, pii_class, pepper) -> Column`
  - `tokenize.tokenize_frame(df, table, pepper, key_version=1) -> (DataFrame, DataFrame)`

- [ ] **Step 1: Write the failing test**

Create `tests/test_pii_tokenize.py`:

```python
"""
Tests for PII normalizers and the token expression.

The properties asserted here are what downstream joins depend on. A token that
is not deterministic, or that differs per source for the same value, silently
breaks cross-source entity resolution -- silently, because unmatched records are
not an error, they just become separate customers.
"""
from __future__ import annotations

from pyspark.sql import Row
from pyspark.sql.functions import col

from pii.registry import ADDRESS, EMAIL, NAME, NAME_PREFIX, PHONE, token_column
from pii.tokenize import normalize, token_expr, tokenize_frame

PEPPER = "test-pepper-do-not-use-in-production"


def tokenize_one(spark, value, pii_class):
    """Return the token for a single scalar value, or None."""
    df = spark.createDataFrame([Row(v=value)])
    normalized = normalize("v", pii_class)
    return df.select(token_expr(normalized, pii_class, PEPPER).alias("t")).collect()[0]["t"]


def test_token_column_appends_suffix():
    assert token_column("email") == "email_token"


def test_email_tokens_are_deterministic(spark):
    first = tokenize_one(spark, "Ada@Example.COM", EMAIL)
    second = tokenize_one(spark, "Ada@Example.COM", EMAIL)
    assert first == second


def test_email_normalization_matches_entity_backfill(spark):
    """entity_backfill.py:296 matches on lower(trim(email)); tokens must agree."""
    assert tokenize_one(spark, "  Ada@Example.COM ", EMAIL) == tokenize_one(spark, "ada@example.com", EMAIL)


def test_token_has_expected_shape(spark):
    token = tokenize_one(spark, "ada@example.com", EMAIL)
    assert token.startswith("tok_")
    assert len(token) == 36


def test_null_and_blank_produce_null_not_a_token(spark):
    """concat_ws skips nulls, so without an explicit guard a NULL input yields a
    real token -- collapsing every customer with a missing email into one."""
    assert tokenize_one(spark, None, EMAIL) is None
    assert tokenize_one(spark, "   ", EMAIL) is None


def test_same_value_different_class_gives_different_token(spark):
    assert tokenize_one(spark, "ada", NAME) != tokenize_one(spark, "ada", ADDRESS)


def test_phone_normalizer_strips_formatting(spark):
    assert tokenize_one(spark, "+1 (555) 123-4567", PHONE) == tokenize_one(spark, "+15551234567", PHONE)


def test_phone_shorter_than_seven_chars_is_null(spark):
    """The length>=7 guard lives at entity_backfill.py:388 today. Tokens are all
    36 chars, so the guard must move into the normalizer or become a no-op."""
    assert tokenize_one(spark, "12345", PHONE) is None


def test_name_prefix_uses_first_three_characters(spark):
    assert tokenize_one(spark, "Lovelace", NAME_PREFIX) == tokenize_one(spark, "LOVeXXXX", NAME_PREFIX)


def test_tokenize_frame_drops_plaintext_and_emits_vault_rows(spark):
    df = spark.createDataFrame([
        Row(customer_id="1", email="Ada@Example.com", first_name="Ada",
            last_name="Lovelace", full_name="Ada Lovelace", phone="+15551234567",
            address_line1="1 Main St", address_line2=None),
    ])
    tokenized, vault = tokenize_frame(df, "stg_shopify_customers", PEPPER)

    for plaintext_column in ("email", "first_name", "last_name", "full_name", "phone", "address_line1"):
        assert plaintext_column not in tokenized.columns
        assert token_column(plaintext_column) in tokenized.columns

    assert "customer_id" in tokenized.columns
    assert "last_name_prefix_token" in tokenized.columns

    classes = {r["pii_class"] for r in vault.collect()}
    assert classes == {EMAIL, NAME, PHONE, ADDRESS, NAME_PREFIX}
    assert vault.filter(col("plaintext").isNull()).count() == 0


def test_full_name_token_is_the_token_of_the_joined_plaintext(spark):
    """
    full_name is built by concatenating first_name and last_name at
    staging_batch.py:346-350. Tokenizing the inputs first would concatenate two
    tokens into a meaningless string, so tokenization must run last.
    """
    df = spark.createDataFrame([
        Row(customer_id="1", first_name="Ada", last_name="Lovelace", full_name="Ada Lovelace"),
    ])
    tokenized, _ = tokenize_frame(df, "stg_shopify_customers", PEPPER)
    row = tokenized.collect()[0]

    assert row["full_name_token"] == tokenize_one(spark, "Ada Lovelace", NAME)
    assert row["full_name_token"] != row["first_name_token"]
    assert row["full_name_token"] != row["last_name_token"]


def test_tokenize_frame_gives_same_email_token_across_sources(spark):
    """Shopify email and GA4 user_id are the same person; class-keyed tokens
    must agree or cross-source entity resolution stops matching."""
    shopify = spark.createDataFrame([Row(customer_id="1", email="ada@example.com")])
    ga4 = spark.createDataFrame([Row(session_id="s1", user_id="ada@example.com")])

    shopify_token = tokenize_frame(shopify, "stg_shopify_customers", PEPPER)[0].collect()[0]["email_token"]
    ga4_token = tokenize_frame(ga4, "stg_ga4_sessions", PEPPER)[0].collect()[0]["user_id_token"]

    assert shopify_token == ga4_token
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `./scripts/run_tests.sh tests/test_pii_tokenize.py`
Expected: FAIL with `ModuleNotFoundError: No module named 'pii'`

- [ ] **Step 3: Create the package marker**

Create `jobs/spark/pii/__init__.py`:

```python
"""PII tokenization: registry, normalizers, vault, and audited detokenization."""
from __future__ import annotations
```

- [ ] **Step 4: Write the registry**

Create `jobs/spark/pii/registry.py`:

```python
"""
PII Field Registry
==================

Single source of truth for which staging columns hold direct identifiers and
what semantic class each belongs to.

Class matters more than column name. Shopify `email`, HubSpot `email`, Stripe
`billing_email` and GA4 `user_id` all carry class `email`, so the same address
produces the same token in all four. Keying tokens by column name instead would
give one person a different token per source, and cross-source entity resolution
would silently stop matching -- silently, because an unmatched record is not an
error, it just becomes a separate customer.

This registry is hand-curated rather than derived from schemas/*.json. Those
annotations are internally inconsistent (HubSpot contact `phone` is pii:true
while company `phone` is pii:false) and no schemas/ga4.json exists at all.

The __future__ import is load-bearing: the Spark image runs Python 3.8, where an
evaluated `dict[str, str]` annotation raises TypeError at import time.
"""
from __future__ import annotations

EMAIL = "email"
PHONE = "phone"
NAME = "name"
ADDRESS = "address"
NAME_PREFIX = "name_prefix"
MAILCHIMP_ID = "mailchimp_id"

PII_CLASSES = (EMAIL, PHONE, NAME, ADDRESS, NAME_PREFIX, MAILCHIMP_ID)

# Staging table -> {column: pii_class}. Column lists verified against the
# CREATE TABLE statements in jobs/spark/staging_batch.py.
PII_FIELDS = {
    "stg_shopify_customers": {
        "email": EMAIL,
        "first_name": NAME,
        "last_name": NAME,
        "full_name": NAME,
        "phone": PHONE,
        "address_line1": ADDRESS,
        "address_line2": ADDRESS,
    },
    "stg_stripe_customers": {
        "email": EMAIL,
        "name": NAME,
        "first_name": NAME,
        "last_name": NAME,
        "full_name": NAME,
        "phone": PHONE,
        "address_line1": ADDRESS,
        "address_line2": ADDRESS,
        "shipping_name": NAME,
        "shipping_address_line1": ADDRESS,
    },
    "stg_stripe_charges": {
        "billing_name": NAME,
        "billing_email": EMAIL,
        "billing_phone": PHONE,
    },
    "stg_hubspot_contacts": {
        "email": EMAIL,
        "first_name": NAME,
        "last_name": NAME,
        "full_name": NAME,
        "phone": PHONE,
        "mobile_phone": PHONE,
        "address": ADDRESS,
    },
    "stg_mailchimp_subscribers": {
        # subscriber_id is MD5(lower(email)) -- an unsalted, publicly
        # reproducible hash, so it is re-identifiable by dictionary attack with
        # no secret at all. It is weaker than the tokens replacing it, and it
        # reaches marts.customer_360.mailchimp_subscriber_id.
        "subscriber_id": MAILCHIMP_ID,
        "email_address": EMAIL,
        "email_normalized": EMAIL,
        "first_name": NAME,
        "last_name": NAME,
        "full_name": NAME,
        "phone": PHONE,
        "phone_normalized": PHONE,
    },
    "stg_mailchimp_events": {
        "email_id": MAILCHIMP_ID,
        "email_address": EMAIL,
        "email_normalized": EMAIL,
    },
    # GA4 user_id is set to the customer's email for the demo's entity
    # resolution (entity_backfill.py:251), so it carries class email.
    "stg_ga4_sessions": {
        "user_id": EMAIL,
    },
}

# Columns computed from another PII column before the source is dropped.
# rebuild_blocking_index needs a surname prefix, and a hash has no meaningful
# prefix -- so the prefix is tokenized as its own value.
PII_DERIVED = {
    table: {"last_name_prefix": ("last_name", NAME_PREFIX)}
    for table in (
        "stg_shopify_customers",
        "stg_stripe_customers",
        "stg_hubspot_contacts",
        "stg_mailchimp_subscribers",
    )
}


def pii_columns(table):
    """Return {column: pii_class} for a staging table, empty if it holds no PII."""
    return dict(PII_FIELDS.get(table, {}))


def derived_columns(table):
    """Return {new_column: (source_column, pii_class)} for a staging table."""
    return dict(PII_DERIVED.get(table, {}))


def token_column(column):
    """Return the tokenized column name for a plaintext column."""
    return f"{column}_token"


def all_token_columns():
    """Every token column name the pipeline can emit, across all tables."""
    names = set()
    for table, mapping in PII_FIELDS.items():
        names.update(token_column(c) for c in mapping)
        names.update(token_column(c) for c in derived_columns(table))
    return names
```

- [ ] **Step 5: Write the tokenizer**

Create `jobs/spark/pii/tokenize.py`:

```python
"""
PII Tokenization
================

Deterministic, class-keyed tokenization of direct identifiers.

    token = "tok_" + sha2(pepper | pii_class | normalized_value, 256)[:32]

Normalizers for `email` and `phone` are lifted verbatim from entity_backfill.py
(:296 and :299). Token equality must reproduce today's match equality exactly,
so they cannot be rewritten from scratch.

Validity checks belong inside the normalizer, never downstream. The phone
blocking filter guards on length(normalized_phone) >= 7; every token is 36
characters wide, so leaving that check downstream turns it into a no-op.

The __future__ import is load-bearing: the Spark image runs Python 3.8.
"""
from __future__ import annotations

import logging
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    concat,
    concat_ws,
    length,
    lit,
    lower,
    regexp_replace,
    sha2,
    substring,
    trim,
    when,
)

from pii.registry import (
    ADDRESS,
    EMAIL,
    MAILCHIMP_ID,
    NAME,
    NAME_PREFIX,
    PHONE,
    derived_columns,
    pii_columns,
    token_column,
)

logger = logging.getLogger(__name__)

MIN_PHONE_LENGTH = 7
TOKEN_PREFIX = "tok_"
TOKEN_HEX_LENGTH = 32


def _blank_to_null(column):
    """Map empty and whitespace-only strings to NULL."""
    return when(column.isNull() | (trim(column) == ""), lit(None).cast("string")).otherwise(column)


def normalize(column, pii_class):
    """
    Return the normalized form of a column for the given PII class.

    NULL and blank always normalize to NULL so they never receive a token.
    """
    source = col(column) if isinstance(column, str) else column

    if pii_class in (EMAIL, NAME, ADDRESS, MAILCHIMP_ID):
        normalized = lower(trim(source))
    elif pii_class == PHONE:
        digits = regexp_replace(source, "[^0-9+]", "")
        normalized = when(
            length(digits) < MIN_PHONE_LENGTH, lit(None).cast("string")
        ).otherwise(digits)
    elif pii_class == NAME_PREFIX:
        normalized = lower(substring(trim(source), 1, 3))
    else:
        raise ValueError(f"Unknown PII class: {pii_class}")

    return _blank_to_null(normalized)


def token_expr(normalized, pii_class, pepper):
    """
    Return the token expression for an already-normalized column.

    The outer NULL guard is required, not defensive. concat_ws skips NULL
    arguments, so a NULL value would otherwise hash "pepper|class" and produce a
    real token shared by every row with a missing value.
    """
    digest = sha2(concat_ws("|", lit(pepper), lit(pii_class), normalized), 256)
    return when(
        normalized.isNull(), lit(None).cast("string")
    ).otherwise(concat(lit(TOKEN_PREFIX), digest.substr(1, TOKEN_HEX_LENGTH)))


def _plan(table, available):
    """
    Return [(token_column, source_column, pii_class)] for the columns present.

    Derived entries come first so they read their source before it is dropped.
    """
    steps = [
        (token_column(new_column), source_column, pii_class)
        for new_column, (source_column, pii_class) in derived_columns(table).items()
        if source_column in available
    ]
    steps += [
        (token_column(column), column, pii_class)
        for column, pii_class in pii_columns(table).items()
        if column in available
    ]
    return steps


def tokenize_frame(df, table, pepper, key_version=1):
    """
    Tokenize a staging frame.

    Returns (tokenized_df, vault_df). The tokenized frame has every plaintext
    PII column replaced by its `_token` counterpart. The vault frame carries one
    deduplicated row per distinct token for semantic.pii_vault.

    Must run as the LAST step of a staging transform. full_name is built by
    concatenating first_name and last_name (staging_batch.py:346-350); tokenizing
    the inputs first would concatenate two tokens into a meaningless string.
    """
    if not pepper:
        raise ValueError("PII_TOKEN_PEPPER is empty; refusing to emit unsalted tokens")

    steps = _plan(table, set(df.columns))
    if not steps:
        logger.info(f"No PII columns registered for {table}")
        return df, None

    tokenized = df
    vault_parts = []

    for target, source_column, pii_class in steps:
        normalized = normalize(source_column, pii_class)
        token = token_expr(normalized, pii_class, pepper)

        tokenized = tokenized.withColumn(target, token)
        vault_parts.append(
            df.select(
                token.alias("token"),
                lit(pii_class).alias("pii_class"),
                normalized.alias("plaintext"),
                lit(key_version).alias("key_version"),
                lit(table).alias("_first_source"),
            ).where(token.isNotNull())
        )

    plaintext_columns = sorted({source for _, source, _ in steps})
    tokenized = tokenized.drop(*plaintext_columns)

    vault = reduce(DataFrame.unionByName, vault_parts).dropDuplicates(["token"])
    logger.info(f"Tokenized {len(steps)} columns on {table}")
    return tokenized, vault
```

- [ ] **Step 6: Run the test to verify it passes**

Run: `./scripts/run_tests.sh tests/test_pii_tokenize.py -v`
Expected: PASS, 12 tests

- [ ] **Step 7: Commit**

```bash
git add jobs/spark/pii/__init__.py jobs/spark/pii/registry.py jobs/spark/pii/tokenize.py tests/test_pii_tokenize.py
git commit -m "feat: add PII registry and deterministic tokenizer

Tokens are keyed by semantic class rather than column name so the same
email yields the same token across all five sources. NULL and blank
normalize to NULL rather than receiving a shared token, and the phone
length check moves into the normalizer because tokens are fixed-width."
```

---

## Task 2: The vault

**Files:**
- Create: `jobs/spark/pii/vault.py`
- Test: `tests/test_pii_vault.py`

**Interfaces:**
- Consumes: `tokenize.tokenize_frame` from Task 1
- Produces:
  - `vault.VAULT_TABLE` = `"iceberg.semantic.pii_vault"`
  - `vault.create_vault(spark) -> None`
  - `vault.upsert_vault(spark, vault_df) -> int` returning rows newly inserted
  - `vault.lookup(spark, tokens) -> DataFrame` with columns `token`, `pii_class`, `plaintext`

- [ ] **Step 1: Write the failing test**

Create `tests/test_pii_vault.py`:

```python
"""
Tests for semantic.pii_vault.

The dedup assertion is the important one. Several staging tables legitimately
produce the same token for the same email address -- that is the entire point of
class-keyed tokens -- and feeding a MERGE a source with duplicate keys raises
MERGE_CARDINALITY_VIOLATION. That failure already shipped once in this pipeline,
via marts GA4 sessions.
"""
from __future__ import annotations

from pyspark.sql import Row

from pii.tokenize import tokenize_frame
from pii.vault import VAULT_TABLE, create_vault, lookup, upsert_vault

PEPPER = "test-pepper-do-not-use-in-production"


def _shopify(spark, rows):
    return spark.createDataFrame([Row(**r) for r in rows])


def test_upsert_inserts_new_tokens(spark):
    create_vault(spark)
    spark.sql(f"DELETE FROM {VAULT_TABLE} WHERE 1=1")

    _, vault_df = tokenize_frame(
        _shopify(spark, [{"customer_id": "1", "email": "ada@example.com"}]),
        "stg_shopify_customers", PEPPER,
    )
    upsert_vault(spark, vault_df)

    stored = spark.table(VAULT_TABLE).collect()
    assert len(stored) == 1
    assert stored[0]["plaintext"] == "ada@example.com"
    assert stored[0]["pii_class"] == "email"


def test_upsert_is_idempotent(spark):
    create_vault(spark)
    spark.sql(f"DELETE FROM {VAULT_TABLE} WHERE 1=1")

    _, vault_df = tokenize_frame(
        _shopify(spark, [{"customer_id": "1", "email": "ada@example.com"}]),
        "stg_shopify_customers", PEPPER,
    )
    upsert_vault(spark, vault_df)
    first = spark.table(VAULT_TABLE).count()
    upsert_vault(spark, vault_df)
    second = spark.table(VAULT_TABLE).count()

    assert first == second


def test_duplicate_tokens_in_one_batch_do_not_raise(spark):
    """Two rows with the same email produce one token; MERGE must not see both."""
    create_vault(spark)
    spark.sql(f"DELETE FROM {VAULT_TABLE} WHERE 1=1")

    _, vault_df = tokenize_frame(
        _shopify(spark, [
            {"customer_id": "1", "email": "ada@example.com"},
            {"customer_id": "2", "email": "ADA@example.com"},
        ]),
        "stg_shopify_customers", PEPPER,
    )
    upsert_vault(spark, vault_df)

    assert spark.table(VAULT_TABLE).count() == 1


def test_lookup_returns_plaintext_for_known_tokens(spark):
    create_vault(spark)
    spark.sql(f"DELETE FROM {VAULT_TABLE} WHERE 1=1")

    tokenized, vault_df = tokenize_frame(
        _shopify(spark, [{"customer_id": "1", "email": "ada@example.com"}]),
        "stg_shopify_customers", PEPPER,
    )
    upsert_vault(spark, vault_df)
    token = tokenized.collect()[0]["email_token"]

    result = lookup(spark, [token]).collect()
    assert len(result) == 1
    assert result[0]["plaintext"] == "ada@example.com"


def test_lookup_of_unknown_token_returns_nothing(spark):
    create_vault(spark)
    assert lookup(spark, ["tok_" + "0" * 32]).count() == 0
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `./scripts/run_tests.sh tests/test_pii_vault.py`
Expected: FAIL with `ModuleNotFoundError: No module named 'pii.vault'`

- [ ] **Step 3: Write the vault**

Create `jobs/spark/pii/vault.py`:

```python
"""
PII Vault
=========

semantic.pii_vault maps a token back to its normalized plaintext. It is one of
only two places plaintext survives; the other is the raw layer.

The table is insert-only and never issues an UPDATE. Partly the watermark rule
in CLAUDE.md, but mainly because plaintext is immutable by construction: the
token is a pure function of the plaintext, so a matching token can never need a
different value.

The __future__ import is load-bearing: the Spark image runs Python 3.8.
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

VAULT_TABLE = "iceberg.semantic.pii_vault"

VAULT_DDL = f"""
    CREATE TABLE IF NOT EXISTS {VAULT_TABLE} (
        token          STRING NOT NULL COMMENT 'Deterministic token, primary key',
        pii_class      STRING NOT NULL COMMENT 'email | phone | name | address | name_prefix | mailchimp_id',
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
    )
"""


def create_vault(spark):
    """Create semantic.pii_vault if it does not already exist."""
    spark.sql(VAULT_DDL)


def upsert_vault(spark, vault_df):
    """
    Insert tokens not already present. Returns the number of rows added.

    The source MUST be deduplicated on token before the MERGE. Several staging
    tables legitimately produce the same token for one email address, and a
    MERGE whose source repeats a key raises MERGE_CARDINALITY_VIOLATION.
    """
    if vault_df is None:
        return 0

    create_vault(spark)
    before = spark.table(VAULT_TABLE).count()

    vault_df.dropDuplicates(["token"]).createOrReplaceTempView("pii_vault_updates")
    spark.sql(f"""
        MERGE INTO {VAULT_TABLE} AS target
        USING pii_vault_updates AS source
        ON target.token = source.token
        WHEN NOT MATCHED THEN INSERT (
            token, pii_class, plaintext, key_version, _first_seen_at, _first_source
        ) VALUES (
            source.token, source.pii_class, source.plaintext, source.key_version,
            current_timestamp(), source._first_source
        )
    """)

    added = spark.table(VAULT_TABLE).count() - before
    logger.info(f"Vault: {added} new tokens stored")
    return added


def lookup(spark, tokens):
    """Return vault rows for the given tokens. Unknown tokens yield no row."""
    create_vault(spark)
    if not tokens:
        return spark.table(VAULT_TABLE).limit(0)

    quoted = ", ".join("'" + t.replace("'", "''") + "'" for t in tokens)
    return spark.sql(f"""
        SELECT token, pii_class, plaintext
        FROM {VAULT_TABLE}
        WHERE token IN ({quoted})
    """)
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `./scripts/run_tests.sh tests/test_pii_vault.py -v`
Expected: PASS, 5 tests

- [ ] **Step 5: Commit**

```bash
git add jobs/spark/pii/vault.py tests/test_pii_vault.py
git commit -m "feat: add semantic.pii_vault with insert-only MERGE

The MERGE source is deduplicated on token because several staging tables
legitimately produce the same token for one email address, and a MERGE
whose source repeats a key raises MERGE_CARDINALITY_VIOLATION."
```

---

## Task 3: Wire staging and rewire entity resolution

Phases 2 and 3 of the design are deliberately one task here. Renaming staging columns without simultaneously rewiring `entity_backfill.py` leaves the job reading a column that no longer exists, so splitting them produces a commit whose tests cannot pass.

**Files:**
- Modify: `jobs/spark/staging_batch.py` — every function in `STAGING_FUNCTIONS` (`:1325`)
- Modify: `jobs/spark/entity_backfill.py:150-282` (`get_all_staging_customers`), `:283-440` (`perform_initial_resolution`), `:465-545` (`rebuild_blocking_index`)
- Modify: `tests/pipeline_tables.py:58-94`
- Test: `tests/test_pii_entity_resolution.py`

**Interfaces:**
- Consumes: `tokenize.tokenize_frame`, `vault.upsert_vault`
- Produces: `get_all_staging_customers` returning `source, source_id, email_token, first_name_token, last_name_token, full_name_token, phone_token, address_token, last_name_prefix_token, city, state, zip, country, created_at, _staged_at`

- [ ] **Step 1: Write the failing golden-equivalence test**

Create `tests/test_pii_entity_resolution.py`:

```python
"""
Golden equivalence for token-based entity resolution.

This is the test that matters most in the PII work. A normalizer that drifts
from entity_backfill.py degrades match quality invisibly, because an unmatched
record is not an error -- it just becomes a separate customer. Asserting that
token-based grouping equals plaintext-based grouping is what catches the drift.
"""
from __future__ import annotations

from datetime import datetime

from pyspark.sql.functions import lit

from pii.tokenize import normalize, token_expr
from tests.pipeline_tables import insert_rows

PEPPER = "test-pepper-do-not-use-in-production"
STAGED_AT = datetime(2026, 8, 21, 12, 0, 0)


def token_for(spark, value, pii_class):
    df = spark.range(1).withColumn("v", lit(value))
    return df.select(token_expr(normalize("v", pii_class), pii_class, PEPPER).alias("t")).collect()[0]["t"]


def _unified_groups(entity_index_df):
    """Return the set of (source, source_id) groups sharing a unified_id."""
    grouped = {}
    for r in entity_index_df.collect():
        grouped.setdefault(r["unified_id"], set()).add((r["source"], r["source_id"]))
    return {frozenset(v) for v in grouped.values()}


def test_mixed_case_emails_resolve_to_one_entity(spark, pipeline_tables):
    """Shopify and HubSpot rows for one person, differing only in case."""
    from jobs.spark.entity_backfill import get_all_staging_customers, perform_initial_resolution

    email_token = token_for(spark, "ada@example.com", "email")

    insert_rows(spark, "iceberg.staging.stg_shopify_customers", [
        {"customer_id": "S1", "email_token": email_token, "_staged_at": STAGED_AT},
    ])
    insert_rows(spark, "iceberg.staging.stg_hubspot_contacts", [
        {"contact_id": "H1", "email_token": email_token, "_staged_at": STAGED_AT},
    ])

    entity_index_df, _ = perform_initial_resolution(
        spark, get_all_staging_customers(spark), dry_run=True
    )

    assert _unified_groups(entity_index_df) == {
        frozenset({("shopify_customers", "S1"), ("hubspot_contacts", "H1")})
    }


def test_different_emails_stay_separate(spark, pipeline_tables):
    from jobs.spark.entity_backfill import get_all_staging_customers, perform_initial_resolution

    insert_rows(spark, "iceberg.staging.stg_shopify_customers", [
        {"customer_id": "S1", "email_token": token_for(spark, "ada@example.com", "email"),
         "_staged_at": STAGED_AT},
        {"customer_id": "S2", "email_token": token_for(spark, "grace@example.com", "email"),
         "_staged_at": STAGED_AT},
    ])

    entity_index_df, _ = perform_initial_resolution(
        spark, get_all_staging_customers(spark), dry_run=True
    )

    assert len(_unified_groups(entity_index_df)) == 2


def test_entity_index_match_reason_holds_no_plaintext(spark, pipeline_tables):
    """match_reason was concat('Matched via email: ', normalized_email) at :352."""
    from jobs.spark.entity_backfill import get_all_staging_customers, perform_initial_resolution

    email_token = token_for(spark, "ada@example.com", "email")
    insert_rows(spark, "iceberg.staging.stg_shopify_customers", [
        {"customer_id": "S1", "email_token": email_token, "_staged_at": STAGED_AT},
        {"customer_id": "S2", "email_token": email_token, "_staged_at": STAGED_AT},
    ])

    entity_index_df, _ = perform_initial_resolution(
        spark, get_all_staging_customers(spark), dry_run=True
    )

    reasons = [r["match_reason"] or "" for r in entity_index_df.collect()]
    assert not any("ada@example.com" in r for r in reasons)


def test_blocking_index_holds_no_plaintext(spark, pipeline_tables):
    from jobs.spark.entity_backfill import get_all_staging_customers, perform_initial_resolution

    email_token = token_for(spark, "ada@example.com", "email")
    insert_rows(spark, "iceberg.staging.stg_shopify_customers", [
        {"customer_id": "S1", "email_token": email_token, "_staged_at": STAGED_AT},
    ])

    _, blocking_df = perform_initial_resolution(
        spark, get_all_staging_customers(spark), dry_run=True
    )

    keys = [r["blocking_key"] for r in blocking_df.collect()]
    assert not any("ada@example.com" in k for k in keys)
    assert all(k.startswith(("email:tok_", "phone:tok_", "name_zip:tok_")) for k in keys)
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `./scripts/run_tests.sh tests/test_pii_entity_resolution.py`
Expected: FAIL — `stg_shopify_customers` has no column `email_token`

- [ ] **Step 3: Update the test DDL**

In `tests/pipeline_tables.py`, replace the PII column names in `STAGING_TABLE_DDL` (`:58-94`) with their token forms. `stg_shopify_customers` becomes:

```python
    "stg_shopify_customers": """
        customer_id STRING, email_token STRING, first_name_token STRING,
        last_name_token STRING, full_name_token STRING, phone_token STRING,
        last_name_prefix_token STRING, address_line1_token STRING,
        city STRING, province STRING, zip STRING, country STRING,
        created_at TIMESTAMP, _staged_at TIMESTAMP
    """,
```

Apply the same substitution to `stg_hubspot_contacts` (`email`, `first_name`, `last_name`, `full_name`, `phone`, `mobile_phone`, `address` become `*_token`, plus `last_name_prefix_token`), `stg_stripe_customers` (same set, plus `address_line1_token`), `stg_mailchimp_subscribers` (`email_normalized`, `first_name`, `last_name`, `full_name`, `phone_normalized`, `subscriber_id` become `*_token`, plus `last_name_prefix_token`), and `stg_ga4_sessions` (`user_id` becomes `user_id_token`). Leave `city`, `state`, `province`, `zip`, `postal_code`, `country`, and every non-PII column unchanged.

- [ ] **Step 4: Add tokenization to each staging function**

In `jobs/spark/staging_batch.py`, add near the existing imports:

```python
from pii.tokenize import tokenize_frame
from pii.vault import upsert_vault

PII_TOKEN_PEPPER = os.environ.get("PII_TOKEN_PEPPER", "")
```

In each function listed in `STAGING_FUNCTIONS` (`:1325`), immediately before the `writeTo(...)` call and after every derived column exists, insert:

```python
    transformed, vault_df = tokenize_frame(transformed, "stg_shopify_customers", PII_TOKEN_PEPPER)
    upsert_vault(spark, vault_df)
```

Substitute the table name per function. `stg_shopify_orders`, `stg_mailchimp_campaigns` and `stg_ga4_events` have no registered PII columns; `tokenize_frame` returns the frame unchanged and a `None` vault frame there, so the call is safe to add uniformly.

Then update each `CREATE TABLE IF NOT EXISTS iceberg.staging.*` DDL in the same file to declare the `_token` column names in place of the plaintext ones, matching the registry.

- [ ] **Step 5: Rewire `get_all_staging_customers`**

In `jobs/spark/entity_backfill.py:166-270`, change each source query to select token columns. The Shopify block becomes:

```python
    shopify = spark.sql(f"""
        SELECT
            'shopify_customers' AS source,
            CAST(customer_id AS STRING) AS source_id,
            email_token,
            first_name_token,
            last_name_token,
            full_name_token,
            phone_token,
            last_name_prefix_token,
            address_line1_token AS address_token,
            city,
            province AS state,
            zip,
            country,
            created_at,
            _staged_at
        FROM iceberg.staging.stg_shopify_customers
        WHERE 1=1 {date_filter}
    """)
```

Apply the same rename to the HubSpot, Stripe, Mailchimp and GA4 blocks, keeping their existing aliasing. Mailchimp uses `email_normalized_token AS email_token` and `phone_normalized_token AS phone_token`. GA4 uses `user_id_token AS email_token` and `CAST(NULL AS STRING) AS last_name_prefix_token`.

- [ ] **Step 6: Rewire `perform_initial_resolution`**

In `jobs/spark/entity_backfill.py:293-300`, delete the `normalized_email` and `normalized_phone` derivations. Values arrive pre-normalized by construction:

```python
    prepared = staging_data.withColumnRenamed("email_token", "normalized_email") \
                           .withColumnRenamed("phone_token", "normalized_phone")
```

Keeping the internal names `normalized_email` and `normalized_phone` means the windowing at `:305-333` needs no further change.

At `:352`, replace the plaintext match reason:

```python
            concat(lit("Matched via email token: "), col("normalized_email"))
```

At `:388`, delete the `length(col("normalized_phone")) >= 7` clause. The check now lives in the phone normalizer, and every token is 36 characters wide, so leaving it here makes it a no-op:

```python
    phone_blocking = with_unified_id.filter(
        (col("normalized_phone").isNotNull()) &
        (col("normalized_phone") != "")
    ).select(
```

At `:403-413`, build the name_zip key from the prefix token instead of a substring:

```python
    name_zip_blocking = with_unified_id.filter(
        (col("last_name_prefix_token").isNotNull()) &
        (col("zip").isNotNull())
    ).select(
        concat(
            lit("name_zip:"),
            col("last_name_prefix_token"),
            lit("_"),
            col("zip")
        ).alias("blocking_key"),
        lit("name_zip").alias("blocking_key_type"),
        col("unified_id"),
        lit("customer").alias("entity_type"),
        col("source"),
        col("source_id"),
        concat_ws("_", col("last_name_prefix_token"), col("zip")).alias("key_value"),
        lit(False).alias("is_primary"),
        current_timestamp().alias("created_at"),
        lit(None).cast("timestamp").alias("expires_at")
    )
```

- [ ] **Step 7: Rewire `rebuild_blocking_index`**

In `jobs/spark/entity_backfill.py:483-486`, replace the normalizing SELECT with token columns:

```sql
            COALESCE(hc.email_token, sc.email_token, stc.email_token,
                     mc.email_normalized_token, ga4_sub.user_id_token) AS normalized_email,
            COALESCE(hc.phone_token, hc.mobile_phone_token, sc.phone_token,
                     stc.phone_token, mc.phone_normalized_token) AS normalized_phone,
            COALESCE(hc.last_name_prefix_token, sc.last_name_prefix_token,
                     stc.last_name_prefix_token, mc.last_name_prefix_token) AS last_name_prefix_token,
            COALESCE(hc.zip, sc.zip, stc.postal_code) AS zip
```

Delete the `length(col("normalized_phone")) >= 7` clause at `:535` for the same reason as Step 6, and rebuild the name_zip key from `last_name_prefix_token` exactly as in Step 6.

- [ ] **Step 8: Run the full suite**

Run: `./scripts/run_tests.sh`
Expected: PASS. `tests/test_ga4_entity_resolution.py` and `tests/test_ga4_e2e.py` must stay green — they call the same functions the Airflow DAG invokes, so signature drift fails there rather than in a scheduled run.

- [ ] **Step 9: Commit**

```bash
git add jobs/spark/staging_batch.py jobs/spark/entity_backfill.py tests/pipeline_tables.py tests/test_pii_entity_resolution.py
git commit -m "feat: tokenize PII at the staging boundary

Staging now emits *_token columns and populates semantic.pii_vault.
Entity resolution reads tokens directly and needs no vault access: values
arrive pre-normalized, so exact-token equality reproduces the previous
lower(trim(email)) match exactly.

Two plaintext leaks close with this: entity_index.match_reason no longer
embeds the email address, and blocking_index keys are built from tokens.
The phone length>=7 guard moves into the normalizer, since every token is
36 characters and the downstream check had become a no-op."
```

---

## Task 4: Rename columns in core, analytics, marts, and ClickHouse

**Files:**
- Modify: `jobs/spark/core_views.py:90-96`
- Modify: `jobs/spark/analytics_incremental.py:132-133`, `:188`, `:264`
- Modify: `jobs/spark/marts_incremental.py:135-141`, `:284-288`
- Modify: `infrastructure/clickhouse/init-analytics.sql:17-18`
- Test: `tests/test_pii_registry.py`

**Interfaces:**
- Consumes: staging token columns from Task 3
- Produces: `marts.customer_360` and `analytics.customer_metrics` carrying `_token` columns only

- [ ] **Step 1: Write the failing enforcement test**

Create `tests/test_pii_registry.py`:

```python
"""
Enforcement ratchet for PII masking.

Modeled on tests/test_metrics_registry.py. That file exists because 13 of 15
alerts once referenced metrics nothing produced, so monitoring read as
configured while emitting nothing. The same failure mode applies here: a
plaintext column reintroduced downstream looks exactly like a masked pipeline
until someone reads the data.
"""
from __future__ import annotations

import re
from pathlib import Path

from pii.registry import PII_CLASSES, PII_FIELDS, derived_columns, pii_columns, token_column

ROOT = Path(__file__).resolve().parents[1]

# Files defining tables at or below the staging boundary.
GUARDED_SOURCES = (
    ROOT / "jobs" / "spark" / "staging_batch.py",
    ROOT / "jobs" / "spark" / "core_views.py",
    ROOT / "jobs" / "spark" / "analytics_incremental.py",
    ROOT / "jobs" / "spark" / "marts_incremental.py",
    ROOT / "infrastructure" / "clickhouse" / "init-analytics.sql",
)

# Bare column names that must never appear in a guarded DDL.
FORBIDDEN = {
    "email", "first_name", "last_name", "full_name", "phone", "mobile_phone",
    "address", "address_line1", "address_line2", "billing_email",
    "billing_name", "billing_phone", "shipping_name", "email_address",
    "email_normalized", "phone_normalized", "shipping_address_line1",
}

DDL_COLUMN = re.compile(
    r"^\s+(\w+)\s+(STRING|String|LowCardinality\(String\))\s*,?\s*(COMMENT|--|$)",
    re.MULTILINE,
)


def test_every_registry_entry_uses_a_known_class():
    for table, mapping in PII_FIELDS.items():
        for column, pii_class in mapping.items():
            assert pii_class in PII_CLASSES, f"{table}.{column} has unknown class {pii_class}"


def test_every_derived_entry_points_at_a_registered_source():
    for table in PII_FIELDS:
        for new_column, (source_column, pii_class) in derived_columns(table).items():
            assert source_column in pii_columns(table), \
                f"{table}.{new_column} derives from unregistered {source_column}"
            assert pii_class in PII_CLASSES


def test_token_column_naming_is_consistent():
    assert token_column("email") == "email_token"
    assert token_column("billing_email") == "billing_email_token"


def test_no_bare_pii_column_below_the_staging_boundary():
    """The ratchet. A plaintext column here means masking is cosmetic."""
    violations = []
    for path in GUARDED_SOURCES:
        text = path.read_text()
        for match in DDL_COLUMN.finditer(text):
            column = match.group(1)
            if column in FORBIDDEN:
                line = text[: match.start()].count("\n") + 1
                violations.append(f"{path.relative_to(ROOT)}:{line} declares `{column}`")
    assert not violations, "Plaintext PII columns below staging:\n" + "\n".join(violations)
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `./scripts/run_tests.sh tests/test_pii_registry.py::test_no_bare_pii_column_below_the_staging_boundary`
Expected: FAIL, listing `email`, `first_name`, `last_name`, `full_name`, `phone`, `address_line1`, `address_line2` in `marts_incremental.py`, and `email` / `full_name` in `analytics_incremental.py` and `init-analytics.sql`

- [ ] **Step 3: Rename in `core_views.py`**

At `jobs/spark/core_views.py:90-96`, replace the plaintext selects:

```sql
        COALESCE(hc.email_token, sc.email_token) AS email_token,
        COALESCE(hc.first_name_token, sc.first_name_token) AS first_name_token,
        COALESCE(hc.last_name_token, sc.last_name_token) AS last_name_token,
        COALESCE(hc.full_name_token, sc.full_name_token) AS full_name_token,
        COALESCE(hc.phone_token, hc.mobile_phone_token, sc.phone_token) AS phone_token,
        COALESCE(hc.address_token, sc.address_line1_token) AS address_line1_token,
        sc.address_line2_token AS address_line2_token,
```

- [ ] **Step 4: Rename in `analytics_incremental.py`**

At `:132-133` change the DDL columns to `email_token STRING,` and `full_name_token STRING,`. At `:188` and `:264` change `col("email")` to `col("email_token")` and any `col("full_name")` to `col("full_name_token")`.

- [ ] **Step 5: Rename in `marts_incremental.py`**

At `:135-141` replace the seven plaintext DDL columns:

```sql
            email_token STRING,
            first_name_token STRING,
            last_name_token STRING,
            full_name_token STRING,
            phone_token STRING,
            address_line1_token STRING,
            address_line2_token STRING,
```

At `:284-288` change the corresponding `col("c.email")` style selects to their `_token` names. Also rename `mailchimp_subscriber_id` to `mailchimp_subscriber_id_token`, since Task 1 registers `subscriber_id` as class `mailchimp_id`.

- [ ] **Step 6: Rename in the ClickHouse DDL**

At `infrastructure/clickhouse/init-analytics.sql:17-18`:

```sql
    email_token String,
    full_name_token String,
```

- [ ] **Step 7: Run the full suite**

Run: `./scripts/run_tests.sh`
Expected: PASS, including the ratchet

- [ ] **Step 8: Commit**

```bash
git add jobs/spark/core_views.py jobs/spark/analytics_incremental.py jobs/spark/marts_incremental.py infrastructure/clickhouse/init-analytics.sql tests/test_pii_registry.py
git commit -m "feat: carry tokens through core, analytics, marts and ClickHouse

Adds tests/test_pii_registry.py as a ratchet: it scans the DDL of every
file at or below the staging boundary and fails on any bare PII column
name. Without it a reintroduced plaintext column looks exactly like a
masked pipeline until someone reads the data."
```

---

## Task 5: Audited detokenization

**Files:**
- Create: `jobs/spark/pii/detokenize.py`
- Test: `tests/test_pii_detokenize.py`

**Interfaces:**
- Consumes: `vault.lookup`, `vault.create_vault`
- Produces:
  - `detokenize.ACCESS_LOG_TABLE` = `"iceberg.semantic.pii_access_log"`
  - `detokenize.detokenize(spark, tokens, actor, reason) -> DataFrame`

- [ ] **Step 1: Write the failing test**

Create `tests/test_pii_detokenize.py`:

```python
"""
Tests for audited detokenization.

The assertion that the access log stores tokens and never plaintext is the
important one. Logging returned values would turn the audit table into a second
unguarded PII store, which is the usual way this pattern is built wrong.
"""
from __future__ import annotations

import pytest
from pyspark.sql import Row

from pii.detokenize import ACCESS_LOG_TABLE, detokenize
from pii.tokenize import tokenize_frame
from pii.vault import VAULT_TABLE, create_vault, upsert_vault

PEPPER = "test-pepper-do-not-use-in-production"


@pytest.fixture
def seeded_token(spark):
    spark.sql(f"DROP TABLE IF EXISTS {ACCESS_LOG_TABLE}")
    create_vault(spark)
    spark.sql(f"DELETE FROM {VAULT_TABLE} WHERE 1=1")

    tokenized, vault_df = tokenize_frame(
        spark.createDataFrame([Row(customer_id="1", email="ada@example.com")]),
        "stg_shopify_customers", PEPPER,
    )
    upsert_vault(spark, vault_df)
    return tokenized.collect()[0]["email_token"]


def test_detokenize_returns_plaintext(spark, seeded_token):
    result = detokenize(spark, [seeded_token], actor="tester", reason="unit test")
    assert result.collect()[0]["plaintext"] == "ada@example.com"


def test_detokenize_writes_an_audit_row(spark, seeded_token):
    detokenize(spark, [seeded_token], actor="tester", reason="unit test")
    log = spark.table(ACCESS_LOG_TABLE).collect()
    assert len(log) == 1
    assert log[0]["actor"] == "tester"
    assert log[0]["reason"] == "unit test"
    assert log[0]["token_count"] == 1


def test_audit_log_stores_tokens_not_plaintext(spark, seeded_token):
    detokenize(spark, [seeded_token], actor="tester", reason="unit test")
    row = spark.table(ACCESS_LOG_TABLE).collect()[0]
    assert seeded_token in row["tokens"]
    assert "plaintext" not in spark.table(ACCESS_LOG_TABLE).columns


def test_detokenize_requires_actor_and_reason(spark, seeded_token):
    with pytest.raises(ValueError):
        detokenize(spark, [seeded_token], actor="", reason="unit test")
    with pytest.raises(ValueError):
        detokenize(spark, [seeded_token], actor="tester", reason="")
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `./scripts/run_tests.sh tests/test_pii_detokenize.py`
Expected: FAIL with `ModuleNotFoundError: No module named 'pii.detokenize'`

- [ ] **Step 3: Write the module**

Create `jobs/spark/pii/detokenize.py`:

```python
"""
Audited Detokenization
======================

The only path from a token back to plaintext. Spark-only: Trino, ClickHouse and
Grafana have no route to the vault.

The access log records which tokens were requested and never the values
returned. Logging the plaintext would turn the audit table into a second
unguarded PII store.

The __future__ import is load-bearing: the Spark image runs Python 3.8.
"""
from __future__ import annotations

import logging
import uuid

from pyspark.sql import Row
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from pii.vault import lookup

logger = logging.getLogger(__name__)

ACCESS_LOG_TABLE = "iceberg.semantic.pii_access_log"

ACCESS_LOG_DDL = f"""
    CREATE TABLE IF NOT EXISTS {ACCESS_LOG_TABLE} (
        _access_id  STRING NOT NULL COMMENT 'Unique id for one detokenize call',
        actor       STRING NOT NULL COMMENT 'Who requested detokenization',
        reason      STRING NOT NULL COMMENT 'Stated purpose',
        pii_class   STRING          COMMENT 'Class requested, NULL if mixed',
        token_count INT             COMMENT 'How many tokens were requested',
        tokens      ARRAY<STRING>   COMMENT 'Tokens requested, never plaintext',
        accessed_at TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (pii_class)
    TBLPROPERTIES (
        'format-version' = '2',
        'write.parquet.compression-codec' = 'zstd'
    )
"""

_PENDING_SCHEMA = StructType([
    StructField("_access_id", StringType()),
    StructField("actor", StringType()),
    StructField("reason", StringType()),
    StructField("pii_class", StringType()),
    StructField("token_count", IntegerType()),
    StructField("tokens", ArrayType(StringType())),
])


def _record_access(spark, tokens, actor, reason, pii_class):
    """Append one audit row. Stores the tokens requested, never the plaintext."""
    spark.sql(ACCESS_LOG_DDL)

    pending = spark.createDataFrame(
        [Row(
            _access_id=str(uuid.uuid4()),
            actor=actor,
            reason=reason,
            pii_class=pii_class,
            token_count=len(tokens),
            tokens=list(tokens),
        )],
        schema=_PENDING_SCHEMA,
    )

    pending.selectExpr(
        "_access_id", "actor", "reason", "pii_class",
        "token_count", "tokens", "current_timestamp() AS accessed_at",
    ).writeTo(ACCESS_LOG_TABLE).append()


def detokenize(spark, tokens, actor, reason):
    """
    Return vault rows for `tokens`, recording the access.

    `actor` and `reason` are mandatory. They are self-reported by the caller,
    which a production deployment must replace with an authenticated identity.
    """
    if not actor:
        raise ValueError("detokenize() requires a non-empty actor")
    if not reason:
        raise ValueError("detokenize() requires a non-empty reason")

    tokens = list(tokens)
    result = lookup(spark, tokens)

    classes = {r["pii_class"] for r in result.select("pii_class").distinct().collect()}
    pii_class = classes.pop() if len(classes) == 1 else None

    _record_access(spark, tokens, actor, reason, pii_class)
    logger.info(f"detokenize: {actor} resolved {len(tokens)} tokens for '{reason}'")
    return result
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `./scripts/run_tests.sh tests/test_pii_detokenize.py -v`
Expected: PASS, 4 tests

- [ ] **Step 5: Commit**

```bash
git add jobs/spark/pii/detokenize.py tests/test_pii_detokenize.py
git commit -m "feat: add audited detokenization

The access log stores the tokens requested and never the values returned;
logging plaintext would make the audit table a second unguarded PII store."
```

---

## Task 6: Metrics, Grafana, configuration, and migration

**Files:**
- Modify: `jobs/spark/metrics/registry.py` — add two `MetricDef` entries
- Modify: `monitoring/dashboards/streaming_business.json` — two panels
- Modify: `infrastructure/.env.example` — add `PII_TOKEN_PEPPER`
- Modify: `airflow/dags/iceberg_pipeline.py` — pass `PII_TOKEN_PEPPER` to spark-submit
- Modify: `scripts/reset_and_run.sh` — export `PII_TOKEN_PEPPER`
- Modify: `docs/RUNBOOK.md` — migration and detokenization procedures
- Modify: `README.md` — note the masking behavior

- [ ] **Step 1: Register the metrics**

In `jobs/spark/metrics/registry.py`, add to `PIPELINE_METRICS`:

```python
    MetricDef(
        name="pipeline_pii_vault_entries",
        kind="gauge",
        labels=("pii_class",),
        help="Distinct tokens stored in semantic.pii_vault, by PII class",
    ),
    MetricDef(
        name="pipeline_pii_tokenization_null_rate",
        kind="gauge",
        labels=("table", "column"),
        help="Fraction of rows whose PII column tokenized to NULL; a spike means a broken normalizer",
    ),
```

- [ ] **Step 2: Verify the metrics registry test still passes**

Run: `./scripts/run_tests.sh tests/test_metrics_registry.py`
Expected: PASS

- [ ] **Step 3: Fix the Grafana panels**

In `monitoring/dashboards/streaming_business.json`, replace the two raw-layer queries. They read `iceberg.raw_shopify_orders` and `iceberg.raw_stripe_charges`, which retain plaintext by design, so tokenizing staging does not fix them:

```sql
SELECT id, total_price, financial_status, created_at
FROM iceberg.staging.stg_shopify_orders
ORDER BY created_at DESC LIMIT 20
```

```sql
SELECT id, amount / 100.0 AS amount, currency, status, billing_email_token, created
FROM iceberg.staging.stg_stripe_charges
ORDER BY created DESC LIMIT 20
```

- [ ] **Step 4: Add the pepper to configuration**

In `infrastructure/.env.example`:

```bash
# PII tokenization pepper. Generate with: openssl rand -hex 32
# Losing this value permanently orphans every token from its vault entry.
# Demonstration only -- a production deployment holds this in a managed KMS.
PII_TOKEN_PEPPER=change-me-generate-with-openssl-rand-hex-32
```

Add `PII_TOKEN_PEPPER` to the spark-submit environment in `airflow/dags/iceberg_pipeline.py` alongside the existing environment variables, and export it in `scripts/reset_and_run.sh`.

- [ ] **Step 5: Document the migration**

Add to `docs/RUNBOOK.md`:

```markdown
### Migrating existing data to tokenized columns

Run in order. Step 4 is the one most easily forgotten.

1. Deploy the code with tokenization enabled.
2. `spark-submit /opt/spark/jobs/staging_batch.py --mode full`
3. Rebuild semantic, core, analytics, and marts in full.
4. `spark-submit /opt/spark/jobs/maintenance/expire_snapshots.py --retention-days 0 --remove-orphans`

Without step 4, Iceberg time travel keeps serving the pre-migration snapshots
containing plaintext and the migration is cosmetic. `--remove-orphans` confirms
the underlying data files leave MinIO, not just the metadata pointers.
```

- [ ] **Step 6: Run the full suite and verify idempotency**

Run: `./scripts/run_tests.sh`
Expected: PASS

Then verify the two-run rule from `CLAUDE.md`: run `staging_batch.py --mode full` twice against a live stack and confirm every row count, including `semantic.pii_vault`, is identical.

- [ ] **Step 7: Commit**

```bash
git add jobs/spark/metrics/registry.py monitoring/dashboards/streaming_business.json infrastructure/.env.example airflow/dags/iceberg_pipeline.py scripts/reset_and_run.sh docs/RUNBOOK.md README.md
git commit -m "feat: register PII metrics, fix the raw-layer dashboard leak

streaming_business.json selected email and receipt_email straight from
the raw layer, which retains plaintext by design, so staging-level
tokenization did not fix those panels. Both now read staging."
```

---

## Deferred

Recorded so they are not mistaken for oversights. Each appears in the design's Production Gaps table.

- Raw-layer retention policy and hard deletion for right-to-erasure
- Trino access control and ClickHouse RBAC
- Pepper rotation tooling (the `key_version` column exists to make it possible later)
- Correcting the `pii` annotations in `schemas/*.json`
- Tokenizing `zip` and emitting a truncated `zip3` for analytics
- Binding `actor` to an authenticated identity rather than a self-reported string
