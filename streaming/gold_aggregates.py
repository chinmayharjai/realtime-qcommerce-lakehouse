"""Gold: 5-minute windowed aggregates + stockout detector.

Reads silver as a stream and produces the ops-floor gold tables:

  orders_by_zone_5min   windowed order counts and value per zone (the dashboard)
  store_sku_inventory   latest inventory per store/SKU via MERGE (CDC-style upsert)
  stockout_alerts       alerts where projected inventory falls below threshold

Three techniques the milestone asks for live here: a windowed streaming
aggregation, a MERGE INTO upsert (foreachBatch, because a streaming aggregation
cannot write to Delta with a merge directly), and OPTIMIZE + Z-ORDER as a
maintenance step. Pure aggregation functions are chispa-testable; the foreachBatch
merge and the OPTIMIZE are documented as the streaming/maintenance wrappers around
them.
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

WINDOW_DURATION = "5 minutes"
WATERMARK = "10 minutes"

# Stockout threshold: alert when projected inventory would run out within this many
# minutes at the current order rate. 15 minutes because a dark store's promise is
# ~10-20 minutes and a restock takes time — an alert that fires only at zero stock
# is an alert that fires too late to act on. The point is to warn while there is
# still time to reroute or restock.
STOCKOUT_HORIZON_MINUTES = 15


def orders_by_zone_window(orders: DataFrame) -> DataFrame:
    """5-minute tumbling windows of order activity per zone.

    Tumbling, not sliding: the dashboard shows discrete 5-minute buckets, and
    overlapping windows would make one order contribute to several buckets and the
    totals stop summing to the period total. The watermark bounds the window state
    so a window is finalized once its watermark passes — late orders beyond the
    watermark are dropped rather than reopening a closed window.
    """
    return (
        orders
        .withWatermark("event_time", WATERMARK)
        .groupBy(
            F.window(F.col("event_time"), WINDOW_DURATION),
            F.col("zone"),
            F.col("city"),
        )
        .agg(
            F.count("*").alias("order_count"),
            F.sum("order_value").alias("total_value"),
            F.avg("order_value").alias("avg_order_value"),
            F.countDistinct("store_id").alias("active_stores"),
            F.countDistinct("customer_id").alias("distinct_customers"),
            F.sum(F.when(F.col("status") == "cancelled", 1).otherwise(0)).alias("cancelled_count"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "zone", "city",
            "order_count", "total_value", "avg_order_value",
            "active_stores", "distinct_customers", "cancelled_count",
        )
        .withColumn("cancel_rate",
                    F.round(F.col("cancelled_count") / F.col("order_count"), 4))
    )


def order_velocity_per_store_sku(orders: DataFrame) -> DataFrame:
    """Units ordered per store/SKU in the recent window — the demand rate the
    stockout detector projects forward.

    Explodes the order line items (each order has several) to reach SKU grain, then
    sums quantity per store/SKU/window. This is the numerator of the "how fast is
    this selling" question that the stockout projection needs.
    """
    return (
        orders
        .withWatermark("event_time", WATERMARK)
        .withColumn("line", F.explode("lines"))
        .groupBy(
            F.window(F.col("event_time"), WINDOW_DURATION),
            F.col("store_id"),
            F.col("line.sku").alias("sku"),
        )
        .agg(
            F.sum("line.qty").alias("units_ordered"),
            F.count("*").alias("order_lines"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "store_id", "sku", "units_ordered", "order_lines",
        )
    )


def detect_stockouts(inventory: DataFrame, velocity: DataFrame) -> DataFrame:
    """Join current inventory to recent demand and project time-to-stockout.

    The core detector, and a pure function of two DataFrames so it is directly
    testable. For each store/SKU:

        demand_per_minute = units_ordered_in_window / window_minutes
        minutes_to_stockout = current_level / demand_per_minute

    An alert fires when minutes_to_stockout is below the horizon. Guards:
      - zero demand -> no stockout (division would be infinite); no alert, correctly
      - already at zero -> immediate alert (minutes_to_stockout = 0)

    The projection is deliberately simple (linear extrapolation of the recent rate).
    A real system would use a smoothed or forecast demand; the point here is the
    detection mechanism and its edge cases, not the forecasting model.
    """
    window_minutes = 5.0

    joined = (
        inventory.alias("inv")
        .join(velocity.alias("vel"),
              on=["store_id", "sku"],
              how="inner")
        # inner join: a store/SKU with no recent orders has no demand to project, so
        # it cannot stock out from ordering and correctly produces no alert. A left
        # join would emit rows with null demand that the filter would have to special-
        # case anyway.
    )

    return (
        joined
        .withColumn("demand_per_minute",
                    F.col("units_ordered") / F.lit(window_minutes))
        .withColumn(
            "minutes_to_stockout",
            F.when(F.col("demand_per_minute") <= 0, F.lit(None))
            .when(F.col("current_level") <= 0, F.lit(0.0))
            .otherwise(F.col("current_level") / F.col("demand_per_minute")),
        )
        .withColumn(
            "is_stockout_risk",
            (F.col("minutes_to_stockout").isNotNull())
            & (F.col("minutes_to_stockout") <= F.lit(STOCKOUT_HORIZON_MINUTES)),
        )
        .filter(F.col("is_stockout_risk"))
        .select(
            "store_id", "sku", "current_level", "units_ordered",
            "demand_per_minute",
            F.round("minutes_to_stockout", 1).alias("minutes_to_stockout"),
            F.lit(STOCKOUT_HORIZON_MINUTES).alias("horizon_minutes"),
            F.current_timestamp().alias("detected_at"),
        )
    )


def latest_inventory(inventory_events: DataFrame) -> DataFrame:
    """Reduce the inventory event stream to the current level per store/SKU.

    Keeps the row with the max event_time per (store_id, sku). This is what the
    MERGE upsert writes into store_sku_inventory — the CDC-style "current state"
    table derived from an event log.
    """
    from pyspark.sql import Window
    w = Window.partitionBy("store_id", "sku").orderBy(F.col("event_time").desc())
    return (
        inventory_events
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .select(
            "store_id", "sku",
            F.col("new_level").alias("current_level"),
            F.col("event_time").alias("as_of"),
        )
    )


def upsert_inventory_batch(batch_df: DataFrame, batch_id: int, spark: SparkSession,
                           target_path: str) -> None:
    """foreachBatch sink: MERGE each micro-batch into the inventory state table.

    A streaming aggregation cannot write to Delta with MERGE directly — outputMode
    complete/update writes replace or append, they do not upsert. foreachBatch is
    the escape hatch: each micro-batch is a bounded DataFrame, so inside it a normal
    Delta MERGE works. This is the standard pattern for CDC-style streaming upserts,
    and the reason the merge is here rather than in the streaming writer.

    The MERGE updates an existing store/SKU only when the incoming event is newer,
    so an out-of-order micro-batch cannot overwrite a fresher level with a staler
    one — the same guard the batch dedup uses, applied at merge time.
    """
    from delta.tables import DeltaTable

    reduced = latest_inventory(batch_df)

    if not DeltaTable.isDeltaTable(spark, target_path):
        reduced.write.format("delta").save(target_path)
        return

    target = DeltaTable.forPath(spark, target_path)
    (
        target.alias("t")
        .merge(reduced.alias("s"), "t.store_id = s.store_id AND t.sku = s.sku")
        .whenMatchedUpdate(
            condition="s.as_of > t.as_of",   # only if the incoming event is newer
            set={"current_level": "s.current_level", "as_of": "s.as_of"},
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


def optimize_and_zorder(spark: SparkSession, path: str, zorder_cols: list[str]) -> None:
    """Compact small files and Z-ORDER by the query columns.

    Streaming writes produce many small files (one or more per micro-batch), and a
    table read with many tiny files is slow regardless of its size — the read spends
    its time opening files. OPTIMIZE compacts them. Z-ORDER co-locates rows that
    share values in the given columns into the same files, so a query filtering on
    those columns reads fewer files (data skipping). Z-ORDER on the columns the
    dashboard actually filters by (store_id, sku) — Z-ordering on a column nothing
    filters by costs the rewrite and buys nothing.

    Run as a periodic maintenance job, NOT inline in the stream: OPTIMIZE rewrites
    files and would contend with the streaming writer's commits if run continuously.
    """
    spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY ({', '.join(zorder_cols)})")


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("gold_aggregates")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-path", default="data/lakehouse")
    ap.add_argument("--checkpoint-base", default="data/checkpoints")
    args = ap.parse_args()

    spark = build_spark()
    orders = spark.readStream.format("delta").load(f"{args.base_path}/silver_orders")
    inventory = spark.readStream.format("delta").load(f"{args.base_path}/silver_inventory_updates")

    # 1. Windowed orders per zone -> gold table
    zone_windows = orders_by_zone_window(orders)
    q1 = (zone_windows.writeStream.format("delta").outputMode("append")
          .option("checkpointLocation", f"{args.checkpoint_base}/gold_orders_by_zone")
          .partitionBy("zone")
          .trigger(processingTime="30 seconds")
          .start(f"{args.base_path}/gold_orders_by_zone_5min"))

    # 2. Inventory state via foreachBatch MERGE
    inv_target = f"{args.base_path}/gold_store_sku_inventory"
    q2 = (inventory.writeStream
          .foreachBatch(lambda df, bid: upsert_inventory_batch(df, bid, spark, inv_target))
          .option("checkpointLocation", f"{args.checkpoint_base}/gold_inventory_merge")
          .trigger(processingTime="30 seconds")
          .start())

    print("started gold streams (zone windows + inventory merge)")
    print("run stockout detection and OPTIMIZE as scheduled batch jobs over the gold tables")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
