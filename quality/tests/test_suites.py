"""Tests for the data-quality expectations.

The expectation logic is pure functions over metrics, so these run without Great
Expectations, Spark, or any data — which is the point of extracting the logic. The
GE framework's job is orchestration and reporting; the DECISIONS (is this volume
anomalous, is this rate plausible) are here and are what's worth testing.

Run:  pytest quality/tests -v
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "expectations"))

import suites  # noqa: E402


# --- Volume z-score ---------------------------------------------------------

def test_normal_volume_passes():
    trailing = [100_000, 105_000, 98_000, 102_000, 99_000, 101_000, 103_000]
    result = suites.check_volume_anomaly(100_500, trailing)
    assert result.passed


def test_volume_spike_is_flagged():
    """A spike can be a duplicate-injection bug or a stuck producer — and it's the
    more dangerous direction because it inflates downstream metrics silently."""
    trailing = [100_000, 105_000, 98_000, 102_000, 99_000, 101_000, 103_000]
    result = suites.check_volume_anomaly(300_000, trailing)  # 3x normal
    assert not result.passed
    assert "spike" in result.detail


def test_volume_drop_is_flagged():
    trailing = [100_000, 105_000, 98_000, 102_000, 99_000, 101_000, 103_000]
    result = suites.check_volume_anomaly(10_000, trailing)  # broken upstream
    assert not result.passed
    assert "drop" in result.detail


def test_zscore_of_zero_variance_history_is_zero():
    """A flat history means 'no basis to call anything anomalous', not 'everything is
    infinitely anomalous' — dividing by a zero stdev would give the latter."""
    assert suites.compute_volume_zscore(999, [100, 100, 100, 100]) == 0.0


def test_zscore_with_too_little_history_is_zero():
    """Two points minimum before the check has any authority."""
    assert suites.compute_volume_zscore(500, [100]) == 0.0


def test_zscore_magnitude_is_correct():
    # [90, 90, 110, 110]: mean 100, pstdev exactly 10 -> 130 is +3 sigma.
    z = suites.compute_volume_zscore(130, [90, 90, 110, 110])
    assert abs(z - 3.0) < 1e-9


def test_busy_but_not_anomalous_day_passes():
    """A day 2 sigma up is busy, not broken — the threshold is 3 sigma, so a real
    Saturday surge does not page anyone."""
    trailing = [100_000, 100_000, 100_000, 100_000]  # mean 100k, but add spread
    trailing = [90_000, 100_000, 110_000, 95_000, 105_000]
    result = suites.check_volume_anomaly(120_000, trailing, z_threshold=3.0)
    assert result.passed


# --- Freshness --------------------------------------------------------------

def test_fresh_data_passes():
    assert suites.check_freshness(30).passed


def test_stale_data_fails():
    assert not suites.check_freshness(200).passed


# --- Range / null -----------------------------------------------------------

def test_negative_count_fails():
    assert not suites.check_no_negative_counts(-1, "order_count").passed


def test_zero_count_passes():
    assert suites.check_no_negative_counts(0, "order_count").passed


def test_null_key_fails():
    assert not suites.check_null_rate(5, 1000, "zone", max_null_rate=0.0).passed


def test_no_nulls_passes():
    assert suites.check_null_rate(0, 1000, "zone").passed


def test_cancel_rate_above_one_fails():
    """A ratio above 1 means numerator/denominator got crossed — an aggregation bug,
    not a business event."""
    assert not suites.check_cancel_rate_plausible(1.5).passed


def test_cancel_rate_in_bounds_passes():
    assert suites.check_cancel_rate_plausible(0.08).passed


# --- Suite summary / severity split -----------------------------------------

def test_summary_blocks_only_on_error_severity():
    results = [
        suites.ExpectationResult("a", passed=True, detail=""),
        suites.ExpectationResult("b", passed=False, detail="", severity="warn"),  # non-blocking
        suites.ExpectationResult("c", passed=False, detail="", severity="error"),  # blocking
    ]
    ok, failures = suites.summarize(results)
    assert not ok
    assert len(failures) == 1
    assert failures[0].check == "c"


def test_summary_passes_when_only_warns_fail():
    results = [
        suites.ExpectationResult("a", passed=True, detail=""),
        suites.ExpectationResult("b", passed=False, detail="", severity="warn"),
    ]
    ok, failures = suites.summarize(results)
    assert ok
    assert failures == []


def test_full_gold_suite_on_clean_metrics():
    metrics = {
        "latest_age_minutes": 20,
        "today_count": 101_000,
        "trailing_counts": [100_000, 99_000, 102_000, 98_000, 101_000],
        "min_order_count": 0,
        "min_total_value": 0.0,
        "null_zone_count": 0,
        "max_cancel_rate": 0.07,
    }
    results = suites.build_gold_suite_results(metrics)
    ok, failures = suites.summarize(results)
    assert ok, [f.detail for f in failures]


def test_full_gold_suite_catches_a_volume_spike():
    metrics = {
        "latest_age_minutes": 20,
        "today_count": 500_000,   # spike
        "trailing_counts": [100_000, 99_000, 102_000, 98_000, 101_000],
        "min_order_count": 0,
        "min_total_value": 0.0,
        "null_zone_count": 0,
        "max_cancel_rate": 0.07,
    }
    ok, failures = suites.summarize(suites.build_gold_suite_results(metrics))
    assert not ok
    assert any("volume" in f.check for f in failures)
