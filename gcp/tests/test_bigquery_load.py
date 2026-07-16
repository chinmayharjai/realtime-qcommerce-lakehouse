"""Tests for the BigQuery load config logic that does NOT need a live BigQuery.

The partition-decorator logic is the part where an idempotent reload can silently
become a full-table wipe, so it is worth pinning even though the actual load needs
GCP. build_load_config is tested only when google-cloud-bigquery is installed
(importorskip), since it constructs real SDK objects.

Run:  pytest gcp/tests -v
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import bigquery_load as bq  # noqa: E402


# --- partition decorator (pure, no SDK) -------------------------------------

def test_no_decorator_when_not_replacing():
    assert bq.partition_decorator("orders_by_zone_5min", None) == "orders_by_zone_5min"


def test_decorator_appends_compact_date():
    """'2026-06-20' -> table$20260620. This scopes WRITE_TRUNCATE to one partition;
    getting it wrong turns an idempotent reload into a full-table wipe."""
    assert bq.partition_decorator("orders_by_zone_5min", "2026-06-20") == "orders_by_zone_5min$20260620"


def test_decorator_strips_dashes():
    assert "$" in bq.partition_decorator("t", "2026-01-05")
    assert bq.partition_decorator("t", "2026-01-05").endswith("$20260105")


# --- load config (needs the SDK) --------------------------------------------
# Gated per-test rather than at module level, so the pure decorator tests above
# always run even where google-cloud-bigquery is absent.

def _bq():
    return pytest.importorskip("google.cloud.bigquery")


def test_replace_partition_uses_write_truncate():
    bigquery = _bq()
    """An idempotent reload must TRUNCATE its partition, not APPEND — appending would
    double the dashboard's counts on a rerun, the exact failure the batch job's
    partition overwrite avoids."""
    config = bq.build_load_config("t", "window_start", ["zone"], replace_partition="2026-06-20")
    assert config.write_disposition == bigquery.WriteDisposition.WRITE_TRUNCATE


def test_no_replace_uses_write_append():
    bigquery = _bq()
    config = bq.build_load_config("t", "window_start", ["zone"], replace_partition=None)
    assert config.write_disposition == bigquery.WriteDisposition.WRITE_APPEND


def test_config_is_partitioned_and_clustered():
    bigquery = _bq()
    config = bq.build_load_config("t", "window_start", ["zone", "city"], None)
    assert config.time_partitioning.field == "window_start"
    assert config.time_partitioning.type_ == bigquery.TimePartitioningType.DAY
    assert config.clustering_fields == ["zone", "city"]


def test_source_format_is_parquet():
    """Parquet carries its own schema, so a gold-job schema drift surfaces as a load
    error rather than a silent column mismatch."""
    bigquery = _bq()
    config = bq.build_load_config("t", "window_start", ["zone"], None)
    assert config.source_format == bigquery.SourceFormat.PARQUET
