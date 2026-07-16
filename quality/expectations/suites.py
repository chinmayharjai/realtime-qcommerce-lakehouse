"""Great Expectations suites for the gold tables, wired into the batch DAG.

Three families of check, each catching a different class of failure:

  freshness   the data is recent enough to be worth serving
  volume      today's row count is within a z-score band of the trailing history
  null/range  the columns hold values that make sense (no negative counts, etc.)

The volume check is the interesting one and the reason this is code, not a static
YAML suite: "is today's volume anomalous" cannot be a fixed threshold, because
quick-commerce volume has a strong weekly and weather-driven pattern — a fixed
"expect > 100k rows" fires every quiet Tuesday and misses a half-empty Saturday.
The z-score approach compares today to the trailing distribution, so the band moves
with the baseline. That logic (compute_volume_zscore) is a pure function, unit-
tested without Great Expectations or Spark.
"""

from __future__ import annotations

import statistics
from dataclasses import dataclass


@dataclass
class ExpectationResult:
    check: str
    passed: bool
    detail: str
    severity: str = "error"  # error fails the DAG; warn logs and continues


def compute_volume_zscore(today_count: int, trailing_counts: list[int]) -> float:
    """Z-score of today's row count against the trailing window.

    Pure function so the anomaly logic is testable without GE. A z-score, not a
    fixed threshold, because quick-commerce volume swings ~3-4x between a quiet
    weekday trough and a rain-surge Saturday — a static bound cannot tell an
    anomaly from a busy day. The z-score asks "how many standard deviations from the
    recent norm", which moves with the baseline.

    Returns 0.0 when the trailing window has no spread (all identical), because a
    zero-variance history means "we have no basis to call anything anomalous yet",
    not "everything is infinitely anomalous" — dividing by a zero stdev would be the
    latter, which is the wrong and noisy answer.
    """
    if len(trailing_counts) < 2:
        return 0.0
    mean = statistics.mean(trailing_counts)
    stdev = statistics.pstdev(trailing_counts)
    if stdev == 0:
        return 0.0
    return (today_count - mean) / stdev


def check_volume_anomaly(today_count: int, trailing_counts: list[int],
                         z_threshold: float = 3.0) -> ExpectationResult:
    """Flag a volume anomaly when |z| exceeds the threshold.

    Both directions matter and this is why the check is two-sided. A volume SPIKE
    can be a duplicate-injection bug or a producer stuck in a loop; a volume DROP
    can be a broken upstream or a stuck consumer. A one-sided "too few rows" check
    misses the spike, which is the more dangerous failure because it inflates every
    downstream metric silently.
    """
    z = compute_volume_zscore(today_count, trailing_counts)
    passed = abs(z) <= z_threshold
    direction = "spike" if z > 0 else "drop"
    return ExpectationResult(
        check="volume_zscore",
        passed=passed,
        detail=(f"today={today_count:,} z={z:.2f} threshold=±{z_threshold} "
                f"({'ok' if passed else direction + ' anomaly'})"),
        severity="error" if not passed else "info",
    )


def check_freshness(latest_event_age_minutes: float, max_age_minutes: float = 120) -> ExpectationResult:
    """Fail if the newest gold row is older than max_age.

    120 minutes for a BATCH gold table (the DAG runs periodically). The STREAMING
    gold tables have their own, tighter freshness expectation elsewhere — a
    streaming table that is 2 hours stale is broken, but a nightly batch table
    legitimately is. The threshold has to match the cadence of the thing it
    measures, or it either never fires or always does.
    """
    passed = latest_event_age_minutes <= max_age_minutes
    return ExpectationResult(
        check="freshness",
        passed=passed,
        detail=f"newest row is {latest_event_age_minutes:.0f} min old (max {max_age_minutes})",
    )


def check_no_negative_counts(min_value: int, column: str) -> ExpectationResult:
    """A count column must never be negative. Cheap, and catches a whole class of
    aggregation sign bugs."""
    passed = min_value >= 0
    return ExpectationResult(
        check=f"{column}_non_negative",
        passed=passed,
        detail=f"min({column})={min_value}",
    )


def check_null_rate(null_count: int, total: int, column: str,
                    max_null_rate: float = 0.0) -> ExpectationResult:
    """Null rate on a required column. Default 0 for keys; a caller can loosen it for
    genuinely optional columns."""
    rate = (null_count / total) if total else 0.0
    passed = rate <= max_null_rate
    return ExpectationResult(
        check=f"{column}_null_rate",
        passed=passed,
        detail=f"{column} null rate {rate:.4f} (max {max_null_rate})",
    )


def check_cancel_rate_plausible(max_cancel_rate: float) -> ExpectationResult:
    """cancel_rate is a ratio and must be in [0, 1]. A value above 1 means the
    numerator and denominator got crossed — a real aggregation bug, not a business
    event."""
    passed = 0.0 <= max_cancel_rate <= 1.0
    return ExpectationResult(
        check="cancel_rate_bounds",
        passed=passed,
        detail=f"max cancel_rate {max_cancel_rate} (must be in [0,1])",
    )


def summarize(results: list[ExpectationResult]) -> tuple[bool, list[ExpectationResult]]:
    """Return (all_error_checks_passed, failed_checks).

    Only error-severity failures block the DAG; info/warn failures are surfaced but
    do not stop the pipeline. This split is what lets the volume check be strict
    (block on a genuine anomaly) while a softer check (say, a mild freshness lag)
    can warn without halting a pipeline that is otherwise fine.
    """
    blocking_failures = [r for r in results if not r.passed and r.severity == "error"]
    return (len(blocking_failures) == 0, blocking_failures)


def build_gold_suite_results(metrics: dict) -> list[ExpectationResult]:
    """Run the full gold suite from a metrics dict (computed by the DAG task from the
    gold tables). Kept as a dict-in so the suite is testable without Spark: the DAG
    computes the aggregates, this evaluates the expectations over them.
    """
    return [
        check_freshness(metrics["latest_age_minutes"]),
        check_volume_anomaly(metrics["today_count"], metrics["trailing_counts"]),
        check_no_negative_counts(metrics["min_order_count"], "order_count"),
        check_no_negative_counts(metrics["min_total_value"], "total_value"),
        check_null_rate(metrics["null_zone_count"], metrics["today_count"], "zone"),
        check_cancel_rate_plausible(metrics["max_cancel_rate"]),
    ]
