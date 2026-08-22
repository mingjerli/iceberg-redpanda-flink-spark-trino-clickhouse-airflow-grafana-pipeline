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
        # No trim here, deliberately: entity_backfill.py's pre-tokenization
        # blocking key was lower(substring(last_name, 1, 3)) with no trim
        # (see git history prior to d544d0f), and design doc Section 4's
        # normalizer table matches that. Adding a trim would change which
        # token a whitespace-padded surname produces post-migration.
        normalized = lower(substring(source, 1, 3))
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
