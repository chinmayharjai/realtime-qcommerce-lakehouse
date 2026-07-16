"""Shared Spark session with Delta enabled, for the streaming and batch tests.

Session-scoped: JVM startup is the slow part, and paying it per test would make the
suite too slow to run on every commit. Delta extensions are configured because the
jobs read and write Delta tables, so a session without them fails the moment a test
touches `.format("delta")`.
"""

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

    try:
        from delta import configure_spark_with_delta_pip
        builder = (
            SparkSession.builder
            .master("local[2]")
            .appName("qcommerce-tests")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
        )
        session = configure_spark_with_delta_pip(builder).getOrCreate()
    except Exception:
        # delta-spark not installed, or the pip-based jar fetch failed. The parse
        # tests do not actually need Delta (only the batch write tests do), so fall
        # back to a plain session rather than skipping the whole suite.
        session = (
            SparkSession.builder
            .master("local[2]")
            .appName("qcommerce-tests")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )

    yield session
    session.stop()
