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

# Single ordered definition ensures schema and row projection cannot drift.
# pii/vault.py landed on this shape after Task 2 lost two fix rounds to a
# Row(**kwargs) built against a separately declared StructType: PySpark binds
# Row fields to an explicit schema positionally, not by name, so the keyword
# order in the Row() call and the field order in the schema were two lists
# that had to be kept in sync by hand -- and didn't stay in sync. Here the
# pending row is built as a tuple pulled from a name-keyed dict in
# ACCESS_LOG_COLUMN_NAMES order, and the schema is built from the same tuple,
# so there is exactly one place that decides field order.
ACCESS_LOG_COLUMNS = (
    ("_access_id",  StringType(),            False),
    ("actor",       StringType(),            False),
    ("reason",      StringType(),            False),
    ("pii_class",   StringType(),            True),
    ("token_count", IntegerType(),           True),
    ("tokens",      ArrayType(StringType()), True),
)

ACCESS_LOG_COLUMN_NAMES = tuple(name for name, _, _ in ACCESS_LOG_COLUMNS)

PENDING_SCHEMA = StructType([
    StructField(name, dtype, nullable) for name, dtype, nullable in ACCESS_LOG_COLUMNS
])

ACCESS_LOG_DDL = f"""
    CREATE TABLE IF NOT EXISTS {ACCESS_LOG_TABLE} (
        _access_id  STRING NOT NULL COMMENT 'Unique id for one detokenize call',
        actor       STRING NOT NULL COMMENT 'Who requested detokenization',
        reason      STRING NOT NULL COMMENT 'Stated purpose',
        pii_class   STRING          COMMENT 'Class requested, NULL if mixed',
        token_count INT             COMMENT 'How many tokens were requested',
        tokens      ARRAY<STRING>   COMMENT 'Tokens requested, never plaintext',
        accessed_at TIMESTAMP       COMMENT 'When this access was recorded'
    )
    USING iceberg
    PARTITIONED BY (pii_class)
    TBLPROPERTIES (
        'format-version' = '2',
        'write.parquet.compression-codec' = 'zstd'
    )
"""


def _record_access(spark, tokens, actor, reason, pii_class):
    """Append one audit row. Stores the tokens requested, never the plaintext."""
    spark.sql(ACCESS_LOG_DDL)

    values = {
        "_access_id": str(uuid.uuid4()),
        "actor": actor,
        "reason": reason,
        "pii_class": pii_class,
        "token_count": len(tokens),
        "tokens": list(tokens),
    }
    pending_row = tuple(values[name] for name in ACCESS_LOG_COLUMN_NAMES)

    pending = spark.createDataFrame([pending_row], schema=PENDING_SCHEMA)

    pending.selectExpr(
        *ACCESS_LOG_COLUMN_NAMES,
        "current_timestamp() AS accessed_at",
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
