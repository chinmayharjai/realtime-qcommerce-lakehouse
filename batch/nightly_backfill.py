"""Nightly backfill (PySpark): apply corrections into the same Delta tables.

The streaming pipeline is fast but approximate at the edges — late events past the
watermark are dropped, and a source can send a correction hours after the fact. This
batch job reconciles: it reprocesses a day's raw events with no watermark (so nothing
is dropped for lateness) and overwrites that day's silver partition with the
corrected, fully-deduplicated result.

It writes into the SAME Delta tables the stream writes, and that is the point of a
lakehouse — one table, kept correct by a fast approximate stream and a slow exact
batch, rather than a "speed layer" and "batch layer" the consumer has to reconcile
(the lambda-architecture tax this design deliberately avoids).

Idempotent by partition overwrite: rerun it for the same date and the result is
identical. The logic here is intentionally the same shape as
batch/scala/NightlyBackfill.scala — same transformation, same tests — so the two
implementations can be compared line for line.
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def dedupe_full(df: DataFrame, key: str = "event_id",
                order_col: str = "ingest_time") -> DataFrame:
    """Full deduplication — no watermark, keep the latest by ingest_time.

    Unlike the streaming dedup, this remembers EVERYTHING: the batch has the whole
    day in hand, so it can guarantee one row per event_id across the entire day, not
    just within a window. That is exactly the correction the batch exists to make —
    a duplicate the stream missed because its two copies straddled the watermark is
    caught here because the batch has no watermark.

    Deterministic tiebreak (event_id as the secondary sort) so reruns are identical,
    the same discipline as every dedup in this portfolio.
    """
    w = Window.partitionBy(key).orderBy(
        F.col(order_col).desc_nulls_last(),
        F.col(key).desc(),
    )
    return (
        df.withColumn("_rn", F.row_number().over(w))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
    )


def apply_corrections(raw: DataFrame, process_date: str) -> DataFrame:
    """Reprocess one day: filter to the event date, dedupe fully.

    Filters on event_date (derived from event_time), not the ingest partition, so a
    late event whose event_time is on process_date is included even though it landed
    in a later partition. This is the whole reason to reprocess by event date rather
    than by partition — it is where the backfill actually corrects the stream.
    """
    return (
        raw
        .withColumn("event_date", F.to_date("event_time"))
        .filter(F.col("event_date") == F.lit(process_date))
        .transform(dedupe_full)
    )


def write_partition_overwrite(df: DataFrame, path: str) -> None:
    """Overwrite exactly the reprocessed partition(s), leaving the rest untouched.

    replaceWhere scoped to the process date is what makes this a surgical
    correction rather than a full-table rewrite: it replaces only the day being
    backfilled. Combined with the deterministic dedup, rerunning the backfill for a
    date is idempotent — same input, same partition content.
    """
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("event_date")
        .save(path)
    )


def run_backfill(spark: SparkSession, raw_path: str, silver_path: str,
                 process_date: str) -> int:
    raw = spark.read.format("delta").load(raw_path)
    corrected = apply_corrections(raw, process_date)
    count = corrected.count()
    write_partition_overwrite(corrected, silver_path)
    return count


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("nightly_backfill")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw-path", required=True)
    ap.add_argument("--silver-path", required=True)
    ap.add_argument("--process-date", required=True, help="YYYY-MM-DD")
    args = ap.parse_args()

    spark = build_spark()
    n = run_backfill(spark, args.raw_path, args.silver_path, args.process_date)
    print(f"backfilled {args.process_date}: {n:,} rows written to {args.silver_path}")
    spark.stop()


if __name__ == "__main__":
    main()
