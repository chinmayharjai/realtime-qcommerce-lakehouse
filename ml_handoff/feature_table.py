"""Build per-store demand features and register them as a Delta feature table.

The handoff point between this data platform and the ML side (the companion
quickcommerce-demand-intelligence repo, which trains demand forecasting and
replenishment models on exactly these features:
https://github.com/chinmayharjai/quickcommerce-demand-intelligence).

The contract this table makes with its ML consumers, stated because a feature
table's value is the contract, not the SQL:

  Grain      one row per (store_id, sku, feature_date)
  Freshness  rebuilt nightly by the batch DAG after the backfill, so features are
             computed on the CORRECTED data, not the stream's approximation
  Point-in-time correctness
             every feature for feature_date D uses data from dates < D only. No
             same-day data. This is the property that prevents leakage: a model
             trained on "demand_avg_7d including today" learns to predict today
             from today and falls apart in production, where today is not
             available at prediction time. The asymmetry is deliberate and the
             tests pin it.
  Nulls      a store/SKU with no history has null lag features (not zero — zero is
             a real observed demand; null is "no data"), and the consumer decides
             imputation. Encoding "unknown" as 0 would teach a model that new SKUs
             have zero demand, which is exactly backwards for a new product launch.

Transformations are pure functions over DataFrames (chispa-tested); main() does I/O.
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def daily_store_sku_demand(orders: DataFrame) -> DataFrame:
    """Collapse order lines to daily units per store/SKU — the base series.

    Only non-cancelled orders count as demand: a cancelled order is not a unit the
    store needed to stock. (Arguable — cancellations can signal unmet demand if
    driven by stockouts — but that is a second feature, not a redefinition of the
    first.)
    """
    return (
        orders
        .filter(F.col("status") != "cancelled")
        .withColumn("line", F.explode("lines"))
        .groupBy(
            F.to_date("event_time").alias("feature_date"),
            F.col("store_id"),
            F.col("line.sku").alias("sku"),
        )
        .agg(
            F.sum("line.qty").alias("units"),
            F.count("*").alias("order_lines"),
            F.sum(F.col("line.qty") * F.col("line.unit_price")).alias("revenue"),
        )
    )


def add_lag_features(daily: DataFrame) -> DataFrame:
    """Rolling and lag features, each shifted one day so feature_date D sees < D only.

    Every window ends at -1 (yesterday), never 0 (today). That single offset is the
    leakage guard: with rows at day grain, a window ending at the current row would
    include the very quantity the model is asked to predict. The tests assert a
    feature row for day D is computable from days < D alone.
    """
    day_window = (
        Window.partitionBy("store_id", "sku")
        .orderBy(F.col("feature_date").cast("timestamp").cast("long"))
        .rangeBetween(-7 * 86400, -1 * 86400)
    )
    day_window_14 = (
        Window.partitionBy("store_id", "sku")
        .orderBy(F.col("feature_date").cast("timestamp").cast("long"))
        .rangeBetween(-14 * 86400, -1 * 86400)
    )
    lag_1 = Window.partitionBy("store_id", "sku").orderBy("feature_date")

    return (
        daily
        .withColumn("units_lag_1d", F.lag("units", 1).over(lag_1))
        .withColumn("units_lag_7d", F.lag("units", 7).over(lag_1))
        .withColumn("units_avg_7d", F.avg("units").over(day_window))
        .withColumn("units_max_7d", F.max("units").over(day_window))
        .withColumn("units_avg_14d", F.avg("units").over(day_window_14))
        .withColumn("revenue_avg_7d", F.avg("revenue").over(day_window))
        # Demand trend: last week's average vs the week before. A ratio, guarded
        # against a zero denominator (a SKU that sold nothing in the base week has
        # no defined trend — null, and the consumer decides).
        .withColumn(
            "demand_trend_7d",
            F.when(F.col("units_avg_14d") > 0,
                   F.col("units_avg_7d") / F.col("units_avg_14d"))
        )
    )


def add_calendar_features(df: DataFrame) -> DataFrame:
    """Calendar features the demand models always want. Deterministic from the date,
    so they never leak — the day of week is known in advance by definition."""
    return (
        df
        .withColumn("day_of_week", F.dayofweek("feature_date"))
        .withColumn("is_weekend", F.dayofweek("feature_date").isin(1, 7))
        .withColumn("day_of_month", F.dayofmonth("feature_date"))
    )


def build_feature_table(orders: DataFrame) -> DataFrame:
    """The full feature build: daily grain -> lags -> calendar."""
    daily = daily_store_sku_demand(orders)
    with_lags = add_lag_features(daily)
    return add_calendar_features(with_lags).withColumn(
        "_features_built_at", F.current_timestamp()
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--silver-orders", required=True)
    ap.add_argument("--feature-path", required=True)
    args = ap.parse_args()

    spark = (
        SparkSession.builder.appName("feature_table")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )

    orders = spark.read.format("delta").load(args.silver_orders)
    features = build_feature_table(orders)

    # Partitioned by feature_date and dynamically overwritten — the same idempotency
    # as everything else: the nightly rebuild replaces the dates it recomputes.
    (features.write.format("delta").mode("overwrite")
        .partitionBy("feature_date")
        .save(args.feature_path))

    print(f"feature table written to {args.feature_path}")
    spark.stop()


if __name__ == "__main__":
    main()
