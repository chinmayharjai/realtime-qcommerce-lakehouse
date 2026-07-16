"""Tests for the gold aggregation and stockout logic.

The aggregation functions and the stockout detector are pure functions of
DataFrames, tested on static input. The foreachBatch MERGE and the OPTIMIZE are
streaming/maintenance wrappers whose core (latest_inventory) is tested directly; the
MERGE execution itself is Delta's behaviour, exercised end-to-end only against a real
Delta table.

Run:  pytest streaming/tests -v
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

pytest.importorskip("pyspark")

from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import (DoubleType, IntegerType, StringType,  # noqa: E402
                               StructField, StructType, TimestampType)

import gold_aggregates as gold  # noqa: E402


# --- Stockout detector (the core logic) -------------------------------------

_INV_SCHEMA = StructType([
    StructField("store_id", StringType()),
    StructField("sku", StringType()),
    StructField("current_level", IntegerType()),
])
_VEL_SCHEMA = StructType([
    StructField("store_id", StringType()),
    StructField("sku", StringType()),
    StructField("units_ordered", IntegerType()),
    StructField("order_lines", IntegerType()),
])


def _inv(spark, rows):
    return spark.createDataFrame(rows, schema=_INV_SCHEMA)


def _vel(spark, rows):
    return spark.createDataFrame(rows, schema=_VEL_SCHEMA)


def test_high_demand_low_stock_triggers_alert(spark):
    """10 units left, 40 ordered in a 5-min window = 8/min -> 1.25 min to stockout,
    well within the 15-min horizon. Must alert."""
    inv = _inv(spark, [("BLR-KOR", "SKU-MILK-1L", 10)])
    vel = _vel(spark, [("BLR-KOR", "SKU-MILK-1L", 40, 20)])

    alerts = gold.detect_stockouts(inv, vel).collect()
    assert len(alerts) == 1
    assert alerts[0]["store_id"] == "BLR-KOR"
    assert alerts[0]["minutes_to_stockout"] == pytest.approx(1.25, abs=0.1)


def test_ample_stock_no_alert(spark):
    """500 units, 10 ordered = 250 min to stockout, far beyond the horizon. No alert."""
    inv = _inv(spark, [("BLR-KOR", "SKU-RICE-5KG", 500)])
    vel = _vel(spark, [("BLR-KOR", "SKU-RICE-5KG", 10, 5)])
    assert gold.detect_stockouts(inv, vel).count() == 0


def test_zero_demand_never_stocks_out(spark):
    """No orders means no stockout risk from ordering — division would be infinite,
    and the guard must produce no alert, not a crash or a false positive."""
    inv = _inv(spark, [("BLR-KOR", "SKU-COFFEE-200", 3)])
    vel = _vel(spark, [("BLR-KOR", "SKU-COFFEE-200", 0, 0)])
    assert gold.detect_stockouts(inv, vel).count() == 0


def test_already_at_zero_alerts_immediately(spark):
    """Zero stock with active demand is an immediate stockout — minutes_to_stockout
    must be 0, and it must alert."""
    inv = _inv(spark, [("BLR-KOR", "SKU-EGGS-6", 0)])
    vel = _vel(spark, [("BLR-KOR", "SKU-EGGS-6", 20, 10)])
    alerts = gold.detect_stockouts(inv, vel).collect()
    assert len(alerts) == 1
    assert alerts[0]["minutes_to_stockout"] == 0.0


def test_store_sku_with_no_recent_orders_is_not_joined(spark):
    """A store/SKU present in inventory but absent from demand produces no alert (the
    inner join drops it) — it cannot stock out from ordering it isn't seeing."""
    inv = _inv(spark, [("BLR-KOR", "SKU-A", 5), ("BLR-KOR", "SKU-B", 5)])
    vel = _vel(spark, [("BLR-KOR", "SKU-A", 40, 20)])  # only SKU-A has demand
    alerts = gold.detect_stockouts(inv, vel).collect()
    assert {a["sku"] for a in alerts} == {"SKU-A"}


def test_boundary_at_the_horizon(spark):
    """Right at the 15-min horizon should alert (<=), just beyond should not."""
    # 30 units, 10/window = 2/min -> 15.0 min exactly.
    inv = _inv(spark, [("S", "SKU-X", 30)])
    vel = _vel(spark, [("S", "SKU-X", 10, 5)])
    assert gold.detect_stockouts(inv, vel).count() == 1

    # 31 units -> 15.5 min, just beyond.
    inv2 = _inv(spark, [("S", "SKU-X", 31)])
    assert gold.detect_stockouts(inv2, vel).count() == 0


# --- latest_inventory (CDC reduce) ------------------------------------------

_INV_EVENT_SCHEMA = StructType([
    StructField("store_id", StringType()),
    StructField("sku", StringType()),
    StructField("new_level", IntegerType()),
    StructField("event_time", TimestampType()),
])


def test_latest_inventory_keeps_newest_per_store_sku(spark):
    rows = [
        ("S1", "SKU-A", 100, datetime(2026, 6, 20, 9, 0, 0)),
        ("S1", "SKU-A", 80, datetime(2026, 6, 20, 9, 5, 0)),   # newer
        ("S1", "SKU-A", 90, datetime(2026, 6, 20, 9, 3, 0)),   # older than the 80
        ("S1", "SKU-B", 50, datetime(2026, 6, 20, 9, 1, 0)),
    ]
    df = spark.createDataFrame(rows, schema=_INV_EVENT_SCHEMA)
    result = {(r["store_id"], r["sku"]): r["current_level"]
              for r in gold.latest_inventory(df).collect()}
    assert result[("S1", "SKU-A")] == 80   # the 09:05 event won
    assert result[("S1", "SKU-B")] == 50


# --- Windowed aggregation ---------------------------------------------------

_ORDER_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_time", TimestampType()),
    StructField("zone", StringType()),
    StructField("city", StringType()),
    StructField("store_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("order_value", DoubleType()),
    StructField("status", StringType()),
])


def test_orders_by_zone_windows_aggregate(spark):
    """Two orders in the same 5-min window and zone aggregate into one row."""
    rows = [
        ("E1", datetime(2026, 6, 20, 9, 1, 0), "Koramangala", "BLR", "S1", "C1", 100.0, "placed"),
        ("E2", datetime(2026, 6, 20, 9, 3, 0), "Koramangala", "BLR", "S1", "C2", 200.0, "placed"),
        ("E3", datetime(2026, 6, 20, 9, 8, 0), "Koramangala", "BLR", "S1", "C3", 50.0, "placed"),
    ]
    df = spark.createDataFrame(rows, schema=_ORDER_SCHEMA)
    result = gold.orders_by_zone_window(df).collect()

    by_start = {r["window_start"]: r for r in result}
    # 09:00-09:05 window has E1+E2; 09:05-09:10 has E3.
    first = [r for r in result if r["order_count"] == 2]
    assert len(first) == 1
    assert first[0]["total_value"] == 300.0


def test_cancel_rate_is_computed(spark):
    rows = [
        ("E1", datetime(2026, 6, 20, 9, 1, 0), "Z", "C", "S1", "C1", 100.0, "placed"),
        ("E2", datetime(2026, 6, 20, 9, 2, 0), "Z", "C", "S1", "C2", 100.0, "cancelled"),
    ]
    df = spark.createDataFrame(rows, schema=_ORDER_SCHEMA)
    row = gold.orders_by_zone_window(df).collect()[0]
    assert row["order_count"] == 2
    assert row["cancelled_count"] == 1
    assert row["cancel_rate"] == 0.5
