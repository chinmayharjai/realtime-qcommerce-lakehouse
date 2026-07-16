"""Tests for the feature table — the leakage guard above all.

Point-in-time correctness is the one property that, if broken, produces a model
that looks brilliant in training and falls apart in production. It is also
invisible in any single row — you can only see it by asserting that a feature for
day D does not change when day D's own data changes. That is what the leakage test
does, and it is the reason this suite exists.

Run:  pytest ml_handoff/tests -v
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

pytest.importorskip("pyspark")

from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import (ArrayType, DoubleType, IntegerType, StringType,  # noqa: E402
                               StructField, StructType, TimestampType)

import feature_table as ft  # noqa: E402

_ORDER_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_time", TimestampType()),
    StructField("store_id", StringType()),
    StructField("status", StringType()),
    StructField("lines", ArrayType(StructType([
        StructField("sku", StringType()),
        StructField("qty", IntegerType()),
        StructField("unit_price", DoubleType()),
    ]))),
])


def _order(day: int, store="S1", sku="SKU-A", qty=5, price=10.0, status="placed",
           hour=10):
    return (
        f"E-{store}-{sku}-{day}-{hour}",
        datetime(2026, 6, day, hour, 0, 0),
        store,
        status,
        [{"sku": sku, "qty": qty, "unit_price": price}],
    )


def _df(spark, rows):
    return spark.createDataFrame(rows, schema=_ORDER_SCHEMA)


# --- Base daily series --------------------------------------------------------

def test_daily_demand_aggregates_to_store_sku_day(spark):
    df = _df(spark, [
        _order(10, qty=3, hour=9),
        _order(10, qty=4, hour=15),   # same store/sku/day, later hour
        _order(11, qty=2),
    ])
    daily = {(str(r["feature_date"]), r["units"]) for r in
             ft.daily_store_sku_demand(df).collect()}
    assert ("2026-06-10", 7) in daily
    assert ("2026-06-11", 2) in daily


def test_cancelled_orders_are_not_demand(spark):
    df = _df(spark, [
        _order(10, qty=3),
        _order(10, qty=100, status="cancelled", hour=12),
    ])
    row = ft.daily_store_sku_demand(df).collect()[0]
    assert row["units"] == 3  # the cancelled 100 units never count


# --- THE leakage test ---------------------------------------------------------

def test_features_for_day_d_do_not_see_day_d(spark):
    """The point-in-time guard. The 7-day average for day 15 must be identical
    whether day 15 sold 5 units or 500 — if changing day 15's own sales changes
    day 15's features, the features leak the label."""
    base_days = [_order(d, qty=10) for d in range(8, 15)]  # days 8..14, qty 10

    quiet_15 = _df(spark, base_days + [_order(15, qty=5)])
    crazy_15 = _df(spark, base_days + [_order(15, qty=500)])

    def features_for_day_15(df):
        feats = ft.build_feature_table(df)
        row = feats.filter(F.col("feature_date") == "2026-06-15").collect()[0]
        return (row["units_avg_7d"], row["units_lag_1d"], row["units_max_7d"])

    assert features_for_day_15(quiet_15) == features_for_day_15(crazy_15), \
        "day 15's features changed when day 15's own sales changed — leakage"


def test_lag_1d_is_yesterday(spark):
    df = _df(spark, [_order(10, qty=7), _order(11, qty=3)])
    feats = ft.build_feature_table(df)
    day11 = feats.filter(F.col("feature_date") == "2026-06-11").collect()[0]
    assert day11["units_lag_1d"] == 7


def test_avg_7d_averages_prior_days_only(spark):
    # Days 10, 11, 12 with qty 10, 20, 30. For day 12, avg_7d = (10+20)/2 = 15.
    df = _df(spark, [_order(10, qty=10), _order(11, qty=20), _order(12, qty=30)])
    feats = ft.build_feature_table(df)
    day12 = feats.filter(F.col("feature_date") == "2026-06-12").collect()[0]
    assert day12["units_avg_7d"] == 15.0


# --- Null semantics -------------------------------------------------------------

def test_first_day_has_null_lags_not_zero(spark):
    """No history = null, not 0. Zero is an observed 'nobody bought'; null is 'no
    data'. Encoding unknown as 0 teaches a model that new SKUs have zero demand —
    exactly backwards for a product launch."""
    df = _df(spark, [_order(10, qty=5)])
    row = ft.build_feature_table(df).collect()[0]
    assert row["units_lag_1d"] is None
    assert row["units_avg_7d"] is None


def test_trend_is_null_when_base_week_is_empty(spark):
    df = _df(spark, [_order(14, qty=5), _order(15, qty=6)])
    feats = ft.build_feature_table(df)
    day15 = feats.filter(F.col("feature_date") == "2026-06-15").collect()[0]
    # 14-day window has one prior day; 7-day window has the same one day. Trend is
    # defined here (both windows non-empty), so just assert it does not blow up and
    # the first day's trend is null.
    day14 = feats.filter(F.col("feature_date") == "2026-06-14").collect()[0]
    assert day14["demand_trend_7d"] is None


# --- Calendar -------------------------------------------------------------------

def test_calendar_features_are_deterministic(spark):
    df = _df(spark, [_order(13, qty=5)])  # 2026-06-13 is a Saturday
    row = ft.build_feature_table(df).collect()[0]
    assert row["is_weekend"] is True
    assert row["day_of_week"] == 7  # Spark: 1=Sunday, 7=Saturday


def test_grain_is_store_sku_date(spark):
    df = _df(spark, [
        _order(10, store="S1", sku="SKU-A"),
        _order(10, store="S1", sku="SKU-B", hour=11),
        _order(10, store="S2", sku="SKU-A", hour=12),
    ])
    feats = ft.build_feature_table(df)
    keys = [(r["store_id"], r["sku"], str(r["feature_date"])) for r in feats.collect()]
    assert len(keys) == len(set(keys)), "duplicate (store, sku, date) rows — grain broken"
    assert len(keys) == 3
