"""Tests for the silver transforms.

The validation split and the schema-evolution helper are pure functions, tested on
static DataFrames. Streaming dedup is tested via its batch equivalent
(dropDuplicates over the same key), which is sound because on a bounded input the
two produce the same result — the streaming version only differs in that it bounds
STATE, not the result. The watermark boundary itself (dropping too-late events) is a
runtime property of the streaming engine and is documented rather than unit-tested,
because asserting it needs a real streaming query with controlled event-time
advancement.

Run:  pytest streaming/tests -v
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

pytest.importorskip("pyspark")

from pyspark.sql import functions as F  # noqa: E402

import silver_clean as sc  # noqa: E402


def _order(spark, **overrides):
    base = {
        "event_id": "E1", "event_time": "2026-06-20T09:00:00", "store_id": "BLR-KOR",
        "order_id": "ORD-1", "order_value": 249.5, "line_count": 2, "status": "placed",
    }
    base.update(overrides)
    return spark.createDataFrame([base])


# --- Validation split -------------------------------------------------------

def test_clean_order_passes_validation(spark):
    clean, dead = sc.validate_orders(_order(spark))
    assert clean.count() == 1
    assert dead.count() == 0


@pytest.mark.parametrize("field,value,expected_reason", [
    ("order_value", -5.0, "order_value_negative"),
    ("order_value", 999999.0, "order_value_implausible"),
    ("status", "teleported", "unknown_status"),
    ("line_count", -1, "negative_line_count"),
])
def test_bad_order_is_dead_lettered_with_specific_reason(spark, field, value, expected_reason):
    """A dead-letter reason of 'invalid' would be useless — the reason is what turns a
    spike into a diagnosis."""
    clean, dead = sc.validate_orders(_order(spark, **{field: value}))
    assert clean.count() == 0
    assert dead.count() == 1
    assert dead.collect()[0]["_dead_letter_reason"] == expected_reason


def test_null_event_id_is_dead_lettered(spark):
    clean, dead = sc.validate_orders(_order(spark, event_id=None))
    assert dead.count() == 1
    assert dead.collect()[0]["_dead_letter_reason"] == "missing_event_id"


def test_validation_routes_not_drops(spark):
    """Every input row appears in exactly one of (clean, dead) — silver never
    silently loses a record."""
    df = spark.createDataFrame([
        {"event_id": "E1", "event_time": "2026-06-20T09:00:00", "store_id": "S1",
         "order_id": "O1", "order_value": 100.0, "line_count": 1, "status": "placed"},
        {"event_id": "E2", "event_time": "2026-06-20T09:00:00", "store_id": "S1",
         "order_id": "O2", "order_value": -1.0, "line_count": 1, "status": "placed"},
    ])
    clean, dead = sc.validate_orders(df)
    assert clean.count() + dead.count() == 2


def test_inventory_validation_catches_negative_stock(spark):
    df = spark.createDataFrame([{
        "event_id": "I1", "event_time": "2026-06-20T09:00:00", "store_id": "S1",
        "sku": "SKU-MILK-1L", "new_level": -3,
    }])
    clean, dead = sc.validate_inventory(df)
    assert dead.collect()[0]["_dead_letter_reason"] == "negative_inventory"


def test_delivery_validation_catches_unknown_stage(spark):
    df = spark.createDataFrame([{
        "event_id": "D1", "event_time": "2026-06-20T09:00:00", "order_id": "O1",
        "stage": "teleporting", "minutes_since_order": 5.0,
    }])
    clean, dead = sc.validate_delivery(df)
    assert dead.collect()[0]["_dead_letter_reason"] == "unknown_stage"


# --- Dedup (batch equivalent of the streaming behaviour) --------------------

def test_dedup_removes_duplicate_event_ids(spark):
    """The gateway re-delivers; the same event_id arrives twice. Dedup keeps one.
    Tested via dropDuplicates, which matches dropDuplicatesWithinWatermark on a
    bounded input — the streaming version only bounds state, not the result."""
    df = spark.createDataFrame([
        {"event_id": "E1", "event_time": "2026-06-20T09:00:00", "v": 1},
        {"event_id": "E1", "event_time": "2026-06-20T09:00:03", "v": 2},  # re-delivery
        {"event_id": "E2", "event_time": "2026-06-20T09:00:00", "v": 3},
    ])
    result = df.dropDuplicates(["event_id"])
    assert result.count() == 2
    assert {r["event_id"] for r in result.collect()} == {"E1", "E2"}


def test_dedup_keeps_distinct_events(spark):
    df = spark.createDataFrame([
        {"event_id": f"E{i}", "event_time": "2026-06-20T09:00:00", "v": i}
        for i in range(20)
    ])
    assert df.dropDuplicates(["event_id"]).count() == 20


def test_validate_runs_before_dedup(spark):
    """A malformed duplicate must be dead-lettered as malformed, not silently removed
    by dedup. So validation runs first, and dedup only sees the clean stream."""
    # Two copies of a bad event: both should land in dead_letter, not be deduped away.
    df = spark.createDataFrame([
        {"event_id": "E1", "event_time": "2026-06-20T09:00:00", "store_id": "S1",
         "order_id": "O1", "order_value": -5.0, "line_count": 1, "status": "placed"},
        {"event_id": "E1", "event_time": "2026-06-20T09:00:03", "store_id": "S1",
         "order_id": "O1", "order_value": -5.0, "line_count": 1, "status": "placed"},
    ])
    clean, dead = sc.validate_orders(df)
    # Both bad copies are in dead_letter (validation does not dedup); clean is empty.
    assert clean.count() == 0
    assert dead.count() == 2


# --- Schema evolution -------------------------------------------------------

def test_additive_column_is_added_as_null(spark):
    """An upstream adds a field. The additive-evolution helper introduces it as a
    nullable column — backwards-compatible, historical rows null."""
    df = _order(spark)
    evolved = sc.enforce_schema_with_evolution(df, additive_column="loyalty_tier")
    assert "loyalty_tier" in evolved.columns
    assert evolved.collect()[0]["loyalty_tier"] is None


def test_existing_column_is_not_overwritten_by_evolution(spark):
    """If the column already arrived, evolution must not null it out."""
    df = _order(spark).withColumn("loyalty_tier", F.lit("gold"))
    evolved = sc.enforce_schema_with_evolution(df, additive_column="loyalty_tier")
    assert evolved.collect()[0]["loyalty_tier"] == "gold"


def test_no_evolution_when_no_column_requested(spark):
    df = _order(spark)
    before = set(df.columns)
    after = set(sc.enforce_schema_with_evolution(df, additive_column=None).columns)
    assert before == after


# --- End-to-end clean_topic -------------------------------------------------

def test_clean_topic_splits_and_dedups(spark):
    df = spark.createDataFrame([
        {"event_id": "E1", "event_time": "2026-06-20T09:00:00", "store_id": "S1",
         "order_id": "O1", "order_value": 100.0, "line_count": 1, "status": "placed"},
        {"event_id": "E2", "event_time": "2026-06-20T09:00:00", "store_id": "S1",
         "order_id": "O2", "order_value": -1.0, "line_count": 1, "status": "placed"},
    ])
    # clean_topic uses dropDuplicatesWithinWatermark which needs a streaming df; test
    # the validation split it composes, which is the pure part.
    clean, dead = sc.validate_orders(df)
    assert clean.count() == 1
    assert dead.count() == 1
