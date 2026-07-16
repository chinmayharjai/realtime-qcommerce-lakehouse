"""PySpark backfill tests — the SAME invariants as
batch/scala/.../NightlyBackfillSpec.scala.

Keeping these two suites in lockstep is what makes the "same job in two languages"
claim verifiable rather than aspirational: they build the same inputs and assert the
same outputs, so if the Python and Scala jobs ever diverge, one suite fails.

Run:  pytest batch/tests -v
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

pytest.importorskip("pyspark")

from pyspark.sql.types import (DoubleType, StringType, StructField,  # noqa: E402
                               StructType)

import nightly_backfill as nb  # noqa: E402

_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_time", StringType()),
    StructField("ingest_time", StringType()),
    StructField("store_id", StringType()),
    StructField("value", DoubleType()),
])


def _df(spark, rows):
    return spark.createDataFrame(rows, schema=_SCHEMA)


def test_dedupe_full_keeps_latest_by_ingest_time(spark):
    df = _df(spark, [
        ("E1", "2026-06-20T09:00:00", "2026-06-20T09:00:05", "S1", 1.0),
        ("E1", "2026-06-20T09:00:00", "2026-06-20T09:03:00", "S1", 2.0),  # later
        ("E2", "2026-06-20T09:00:00", "2026-06-20T09:00:06", "S1", 3.0),
    ])
    result = nb.dedupe_full(df).collect()
    assert len(result) == 2
    e1 = [r for r in result if r["event_id"] == "E1"][0]
    assert e1["value"] == 2.0


def test_dedupe_full_is_deterministic_on_ties(spark):
    df = _df(spark, [
        ("E1", "2026-06-20T09:00:00", "2026-06-20T09:00:05", "SA", 1.0),
        ("E1", "2026-06-20T09:00:00", "2026-06-20T09:00:05", "SB", 2.0),
    ])
    first = [r["store_id"] for r in nb.dedupe_full(df).collect()]
    second = [r["store_id"] for r in nb.dedupe_full(df).collect()]
    assert first == second


def test_dedupe_full_keeps_distinct_events(spark):
    rows = [(f"E{i}", "2026-06-20T09:00:00", "2026-06-20T09:00:05", "S1", float(i))
            for i in range(20)]
    assert nb.dedupe_full(_df(spark, rows)).count() == 20


def test_apply_corrections_filters_to_process_date(spark):
    df = _df(spark, [
        ("E1", "2026-06-20T09:00:00", "2026-06-20T09:00:05", "S1", 1.0),
        ("E2", "2026-06-21T09:00:00", "2026-06-21T09:00:05", "S1", 2.0),  # different day
        ("E3", "2026-06-20T23:30:00", "2026-06-22T03:00:00", "S1", 3.0),  # late, event on the 20th
    ])
    ids = {r["event_id"] for r in nb.apply_corrections(df, "2026-06-20").collect()}
    assert ids == {"E1", "E3"}


def test_apply_corrections_includes_late_event_by_event_date(spark):
    df = _df(spark, [
        ("LATE", "2026-06-20T23:55:00", "2026-06-22T04:00:00", "S1", 9.0),
    ])
    result = nb.apply_corrections(df, "2026-06-20").collect()
    assert len(result) == 1
    assert result[0]["event_id"] == "LATE"


def test_apply_corrections_dedupes_within_the_day(spark):
    df = _df(spark, [
        ("E1", "2026-06-20T09:00:00", "2026-06-20T09:00:05", "S1", 1.0),
        ("E1", "2026-06-20T09:00:00", "2026-06-20T09:20:00", "S1", 2.0),  # 20 min later
    ])
    result = nb.apply_corrections(df, "2026-06-20").collect()
    assert len(result) == 1
    assert result[0]["value"] == 2.0


def test_backfill_is_deterministic(spark):
    rows = [
        ("E1", "2026-06-20T09:00:00", "2026-06-20T09:00:05", "S1", 1.0),
        ("E1", "2026-06-20T09:00:00", "2026-06-20T09:10:00", "S1", 2.0),
        ("E2", "2026-06-20T10:00:00", "2026-06-20T10:00:05", "S2", 3.0),
    ]
    a = {(r["event_id"], r["value"]) for r in nb.apply_corrections(_df(spark, rows), "2026-06-20").collect()}
    b = {(r["event_id"], r["value"]) for r in nb.apply_corrections(_df(spark, rows), "2026-06-20").collect()}
    assert a == b
