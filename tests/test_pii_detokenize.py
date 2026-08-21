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
