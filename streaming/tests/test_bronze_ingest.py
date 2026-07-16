"""Tests for the bronze parse logic, on static DataFrames.

Structured Streaming's readStream/writeStream cannot be unit-tested without a broker
and a running query, but parse_topic is a pure DataFrame function, so the schema
handling — the part where bugs actually live — is testable on a hand-built Kafka-
shaped DataFrame. The exactly-once machinery (checkpoint + Delta commit) is a
property of Spark and Delta, not of this code, so it is not re-tested here; what is
tested is that this code parses correctly and preserves the audit columns.

Run:  pytest streaming/tests -v   (needs Java + pyspark)
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

pytest.importorskip("pyspark")

from pyspark.sql import Row  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

import bronze_ingest as bi  # noqa: E402


def _kafka_df(spark, values: list[str], keys=None, partition=0, base_offset=0):
    """Build a DataFrame shaped like Spark's Kafka source: key/value are binary."""
    keys = keys or [f"STORE-{i}" for i in range(len(values))]
    rows = [
        Row(
            key=keys[i].encode(),
            value=values[i].encode(),
            partition=partition,
            offset=base_offset + i,
            timestamp=None,
        )
        for i in range(len(values))
    ]
    return spark.createDataFrame(rows)


def test_valid_order_parses_all_fields(spark):
    order = (
        '{"event_id":"E1","event_type":"order","order_id":"ORD-1",'
        '"event_time":"2026-06-20T09:00:00","store_id":"BLR-KOR","city":"Bengaluru",'
        '"zone":"Koramangala","order_value":249.5,"line_count":2,"status":"placed",'
        '"payment_method":"UPI","promised_minutes":10}'
    )
    df = bi.parse_topic(_kafka_df(spark, [order]), bi.ORDER_SCHEMA)
    row = df.collect()[0]

    assert row["event_id"] == "E1"
    assert row["order_id"] == "ORD-1"
    assert row["store_id"] == "BLR-KOR"
    assert abs(row["order_value"] - 249.5) < 1e-6
    assert row["line_count"] == 2


def test_kafka_metadata_is_preserved(spark):
    """_kafka_partition/_kafka_offset are the physical address of the event — the
    audit trail that lets you prove which offset produced which bronze row."""
    order = '{"event_id":"E1","store_id":"BLR-KOR","event_time":"2026-06-20T09:00:00"}'
    df = bi.parse_topic(_kafka_df(spark, [order], partition=3, base_offset=100), bi.ORDER_SCHEMA)
    row = df.collect()[0]

    assert row["_kafka_partition"] == 3
    assert row["_kafka_offset"] == 100
    assert row["_kafka_key"] == "STORE-0"
    assert row["_ingested_at"] is not None


def test_malformed_json_becomes_null_payload_not_dropped(spark):
    """Bronze captures everything; silver decides dead-letter. A row where from_json
    failed is kept with null fields, not discarded — dropping here would lose the
    evidence that a malformed event arrived."""
    df = bi.parse_topic(_kafka_df(spark, ["{not valid json"]), bi.ORDER_SCHEMA)

    assert df.count() == 1  # not dropped
    row = df.collect()[0]
    assert row["event_id"] is None       # unparseable -> null
    assert row["_kafka_offset"] == 0     # but the Kafka metadata survives


def test_off_schema_fields_are_ignored_not_errored(spark):
    """An event with an extra unknown field parses fine — from_json keeps the schema
    fields and ignores the rest. Additive upstream changes must not break bronze."""
    order = ('{"event_id":"E1","store_id":"BLR-KOR","event_time":"2026-06-20T09:00:00",'
             '"brand_new_field":"surprise"}')
    df = bi.parse_topic(_kafka_df(spark, [order]), bi.ORDER_SCHEMA)
    assert df.collect()[0]["event_id"] == "E1"
    assert "brand_new_field" not in df.columns


def test_wrong_type_becomes_null_for_that_field(spark):
    """order_value as a string cannot cast to double under from_json -> that field is
    null, but the rest of the row parses. Silver's range checks catch the null."""
    order = ('{"event_id":"E1","store_id":"BLR-KOR","event_time":"2026-06-20T09:00:00",'
             '"order_value":"not-a-number"}')
    df = bi.parse_topic(_kafka_df(spark, [order]), bi.ORDER_SCHEMA)
    row = df.collect()[0]
    assert row["event_id"] == "E1"
    assert row["order_value"] is None


def test_nested_line_items_parse(spark):
    order = (
        '{"event_id":"E1","store_id":"BLR-KOR","event_time":"2026-06-20T09:00:00",'
        '"lines":[{"sku":"SKU-MILK-1L","name":"Milk 1L","category":"dairy","qty":2,"unit_price":60.0}]}'
    )
    df = bi.parse_topic(_kafka_df(spark, [order]), bi.ORDER_SCHEMA)
    lines = df.collect()[0]["lines"]
    assert len(lines) == 1
    assert lines[0]["sku"] == "SKU-MILK-1L"
    assert lines[0]["qty"] == 2


def test_inventory_schema_parses(spark):
    inv = ('{"event_id":"I1","event_type":"inventory_update","store_id":"BLR-KOR",'
           '"event_time":"2026-06-20T09:00:00","sku":"SKU-MILK-1L","previous_level":50,'
           '"new_level":40,"delta":-10,"reason":"restock"}')
    df = bi.parse_topic(_kafka_df(spark, [inv]), bi.INVENTORY_SCHEMA)
    row = df.collect()[0]
    assert row["new_level"] == 40
    assert row["delta"] == -10


def test_all_three_topic_schemas_are_registered():
    assert set(bi.TOPIC_SCHEMAS) == {"orders", "inventory_updates", "delivery_status"}
    for schema in bi.TOPIC_SCHEMAS.values():
        names = [f.name for f in schema.fields]
        assert "event_id" in names   # dedup key
        assert "event_time" in names  # watermark key
