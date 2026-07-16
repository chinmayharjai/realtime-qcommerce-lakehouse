"""Spark session for the batch backfill tests. Mirrors streaming/tests/conftest.py."""

from __future__ import annotations

import pytest

try:
    from pyspark.sql import SparkSession
    _SPARK = True
except ImportError:
    _SPARK = False


@pytest.fixture(scope="session")
def spark():
    if not _SPARK:
        pytest.skip("pyspark not installed")
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("backfill-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()
