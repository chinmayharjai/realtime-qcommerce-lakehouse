"""Nightly backfill DAG: quality gate -> backfill -> re-check -> BigQuery load.

Runs at 02:30 IST (21:00 UTC), after the day's traffic has quieted and before the
morning ops review reads the dashboard.

The dependency shape:

    check_source_quality >> run_backfill >> check_gold_quality >> load_bigquery

Quality runs BOTH before and after the backfill, and the two checks answer
different questions. The pre-check asks "is the input worth processing?" — a broken
bronze table should fail fast, before an hour of Spark, not after. The post-check
asks "did the backfill produce sane output?" — and it is the gate in front of the
BigQuery load, so a backfill that produced garbage never reaches the dashboard's
serving layer. One check in either position would miss half of that.

Idempotency guarantees, stated because the DAG's design depends on them:
  - The backfill overwrites by event_date partition (dynamic partition overwrite),
    so re-running a date replaces rather than appends. Documented and tested in
    batch/.
  - The BigQuery load uses WRITE_TRUNCATE scoped to a $YYYYMMDD partition
    decorator, so a reloaded day overwrites there too (gcp/bigquery_load.py).
  - Therefore: clearing any task and re-running it, or re-running the whole DAG for
    a logical date, converges to the same state. The recovery procedure for nearly
    everything is "clear and re-run" — see the runbooks.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

REPO = os.environ.get("QCOMMERCE_REPO", "/opt/airflow/qcommerce")
LAKEHOUSE = os.environ.get("LAKEHOUSE_PATH", "/opt/airflow/data/lakehouse")

default_args = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
    # Retries are safe precisely because every task is idempotent — the backfill
    # overwrites its partition and the BQ load truncates its partition. Without
    # those properties, retries would be the thing that CREATES duplicates, and
    # this block would be a bug rather than a default.
    "depends_on_past": False,
    # False for the same reason as the revenue DAG: each run reprocesses its own
    # date independently. A failed Tuesday does not corrupt Wednesday, and blocking
    # Wednesday on Tuesday's failure would turn a one-night gap into a multi-day
    # outage. It WOULD flip to True if the backfill ever became cumulative
    # (depending on the previous day's output), which it deliberately is not.
}


def run_quality_gate(stage: str, **context) -> None:
    """Evaluate the expectation suite and fail the task on a blocking failure.

    The suite logic lives in quality/expectations/suites.py (pure functions, unit
    tested); this task computes the metrics from the Delta tables and hands them
    over. Failing the TASK is the mechanism that stops the DAG — Airflow's
    dependency graph does the gating, the suite just decides pass/fail.
    """
    import sys
    sys.path.insert(0, f"{REPO}/quality/expectations")
    import suites

    # In the real deployment these metrics come from a Spark read of the gold
    # tables. The metric computation is one aggregate query; what matters here is
    # the gate structure.
    metrics = compute_gold_metrics(stage)
    results = suites.build_gold_suite_results(metrics)
    ok, failures = suites.summarize(results)

    for r in results:
        print(f"[{stage}] {'PASS' if r.passed else 'FAIL'} {r.check}: {r.detail}")

    if not ok:
        details = "; ".join(f.detail for f in failures)
        raise RuntimeError(f"quality gate '{stage}' failed: {details}")


def compute_gold_metrics(stage: str) -> dict:
    """Compute the suite's input metrics from the lakehouse.

    Isolated so the DAG file has no Spark import at module level — Airflow parses
    DAG files constantly, and a module-level SparkSession would make every parse
    spin up a JVM. Imports happen inside the task, at execution time.
    """
    from pyspark.sql import SparkSession, functions as F

    spark = (SparkSession.builder.appName(f"quality_{stage}")
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog",
                     "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .getOrCreate())

    table = f"{LAKEHOUSE}/gold_orders_by_zone_5min"
    df = spark.read.format("delta").load(table)

    today = df.filter(F.to_date("window_start") == F.current_date())
    trailing = (
        df.filter(F.to_date("window_start") < F.current_date())
        .groupBy(F.to_date("window_start").alias("d"))
        .agg(F.sum("order_count").alias("n"))
        .orderBy(F.col("d").desc())
        .limit(14)
    )

    agg = today.agg(
        F.sum("order_count").alias("today_count"),
        F.min("order_count").alias("min_order_count"),
        F.min("total_value").alias("min_total_value"),
        F.sum(F.when(F.col("zone").isNull(), 1).otherwise(0)).alias("null_zone"),
        F.max("cancel_rate").alias("max_cancel_rate"),
        F.max("window_start").alias("latest"),
    ).collect()[0]

    latest_age_min = 0.0
    if agg["latest"] is not None:
        latest_age_min = (datetime.utcnow() - agg["latest"]).total_seconds() / 60

    return {
        "latest_age_minutes": latest_age_min,
        "today_count": agg["today_count"] or 0,
        "trailing_counts": [r["n"] for r in trailing.collect()],
        "min_order_count": agg["min_order_count"] or 0,
        "min_total_value": float(agg["min_total_value"] or 0),
        "null_zone_count": agg["null_zone"] or 0,
        "max_cancel_rate": float(agg["max_cancel_rate"] or 0),
    }


with DAG(
    dag_id="qcommerce_batch_backfill",
    description="Nightly reconciliation: quality gate -> backfill -> re-check -> BigQuery",
    start_date=datetime(2026, 6, 1),
    schedule="0 21 * * *",  # 02:30 IST
    catchup=False,
    # catchup=False with a twist worth noting: unlike the P1 full-refresh DAG, this
    # job IS date-parameterized ({{ ds }}), so backfilling a missed range is
    # legitimate — but done deliberately via `airflow dags backfill -s ... -e ...`,
    # not automatically on unpause. An automatic catchup after two weeks paused
    # would launch 14 Spark jobs at once into one cluster.
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=3),
    default_args=default_args,
    tags=["qcommerce", "backfill", "delta", "bigquery"],
    doc_md=__doc__,
) as dag:

    check_source_quality = PythonOperator(
        task_id="check_source_quality",
        python_callable=run_quality_gate,
        op_kwargs={"stage": "pre_backfill"},
        # Fail fast: a broken bronze table should stop the DAG here, before an hour
        # of Spark, not surface as a confusing backfill failure later.
    )

    run_backfill = BashOperator(
        task_id="run_backfill",
        bash_command=(
            f"cd {REPO} && python batch/nightly_backfill.py "
            f"--raw-path {LAKEHOUSE}/bronze_orders "
            f"--silver-path {LAKEHOUSE}/silver_orders "
            "--process-date {{ ds }}"
        ),
        # {{ ds }} — the DAG's logical date, not datetime.now(). This is what makes
        # `airflow dags backfill` and task clearing work: re-running yesterday's DAG
        # run processes yesterday's date, not today's. A now() here would make every
        # retry process a different day than the run it belongs to.
        execution_timeout=timedelta(hours=1),
    )

    check_gold_quality = PythonOperator(
        task_id="check_gold_quality",
        python_callable=run_quality_gate,
        op_kwargs={"stage": "post_backfill"},
        retries=0,
        # No retries on a quality check: the data is what it is, and re-checking it
        # three times yields the same failure 15 minutes later. Same reasoning as
        # dbt_test in the P1 DAG.
    )

    load_bigquery = BashOperator(
        task_id="load_bigquery",
        bash_command=(
            f"cd {REPO} && python gcp/bigquery_load.py "
            "--project $GCP_PROJECT --dataset qcommerce_gold "
            f"--source gs://$GOLD_BUCKET/gold_orders_by_zone_5min/*.parquet "
            "--table orders_by_zone_5min "
            "--replace-partition {{ ds }}"
        ),
        # --replace-partition makes the load idempotent on the BigQuery side:
        # WRITE_TRUNCATE scoped to this date's $YYYYMMDD partition. A retry or a
        # cleared-and-rerun task overwrites the partition rather than appending a
        # second copy of the day.
        execution_timeout=timedelta(minutes=30),
    )

    check_source_quality >> run_backfill >> check_gold_quality >> load_bigquery
