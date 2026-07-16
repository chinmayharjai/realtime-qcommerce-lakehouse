"""Silver: dedup, watermarked late-event handling, schema enforcement, dead-letter.

Reads the bronze Delta tables as a stream and produces clean silver tables plus a
dead_letter table for records that fail validation. This is where the four
streaming concepts the project is built to demonstrate actually do work:

  - streaming dedup on event_id (drop the gateway's re-delivered duplicates)
  - watermark-based late-event handling (bound the state, drop the too-late)
  - schema enforcement + Delta schema evolution (accept one additive change)
  - dead-letter routing (a failing record is quarantined with a reason, not lost)

Every transform is a pure function so the dedup, the validation split, and the
watermark boundary are chispa-testable on static DataFrames — streaming dedup and a
batch dropDuplicates over the same key produce the same result on a bounded input,
which is exactly what lets a static test stand in for the streaming behaviour.
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# The watermark: how long silver waits for a late event before it stops accepting
# events for that time window and drops anything later. 10 minutes because the
# producer injects lateness up to 15 minutes, so 10 is deliberately SHORTER than the
# worst-case lateness — which means some events are dropped as too-late, and that is
# the point. A watermark longer than the worst lateness would accept everything and
# demonstrate nothing; the interesting, testable behaviour is at the boundary.
WATERMARK_DELAY = "10 minutes"

VALID_ORDER_STATUSES = ["placed", "confirmed", "cancelled"]
VALID_DELIVERY_STAGES = ["assigned", "picked", "packed", "dispatched", "delivered"]


def validate_orders(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split orders into (clean, dead_letter) with a failure reason on the rejects.

    The validation is expressed as a set of named predicates so the dead-letter
    reason is specific — "order_value_negative", not just "invalid". A dead-letter
    table where every row says "failed validation" is a table nobody can act on; the
    reason is what turns a dead-letter spike into a diagnosis.

    Returns both halves rather than filtering, because silver must not silently drop
    a bad record. Bronze captured it; silver's job is to route it to dead_letter
    with evidence, so the record still exists and the failure is countable.
    """
    reason = (
        F.when(F.col("event_id").isNull(), "missing_event_id")
        .when(F.col("event_time").isNull(), "missing_event_time")
        .when(F.col("store_id").isNull(), "missing_store_id")
        .when(F.col("order_value") < 0, "order_value_negative")
        .when(F.col("order_value") > 100000, "order_value_implausible")
        .when(~F.col("status").isin(VALID_ORDER_STATUSES), "unknown_status")
        .when(F.col("line_count") < 0, "negative_line_count")
        .otherwise(None)
    )
    tagged = df.withColumn("_dead_letter_reason", reason)

    clean = tagged.filter(F.col("_dead_letter_reason").isNull()).drop("_dead_letter_reason")
    dead = tagged.filter(F.col("_dead_letter_reason").isNotNull())
    return clean, dead


def validate_inventory(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    reason = (
        F.when(F.col("event_id").isNull(), "missing_event_id")
        .when(F.col("event_time").isNull(), "missing_event_time")
        .when(F.col("store_id").isNull(), "missing_store_id")
        .when(F.col("sku").isNull(), "missing_sku")
        .when(F.col("new_level") < 0, "negative_inventory")
        .otherwise(None)
    )
    tagged = df.withColumn("_dead_letter_reason", reason)
    return (tagged.filter(F.col("_dead_letter_reason").isNull()).drop("_dead_letter_reason"),
            tagged.filter(F.col("_dead_letter_reason").isNotNull()))


def validate_delivery(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    reason = (
        F.when(F.col("event_id").isNull(), "missing_event_id")
        .when(F.col("event_time").isNull(), "missing_event_time")
        .when(F.col("order_id").isNull(), "missing_order_id")
        .when(~F.col("stage").isin(VALID_DELIVERY_STAGES), "unknown_stage")
        .when(F.col("minutes_since_order") < 0, "negative_elapsed")
        .otherwise(None)
    )
    tagged = df.withColumn("_dead_letter_reason", reason)
    return (tagged.filter(F.col("_dead_letter_reason").isNull()).drop("_dead_letter_reason"),
            tagged.filter(F.col("_dead_letter_reason").isNotNull()))


def deduplicate_stream(df: DataFrame) -> DataFrame:
    """Streaming dedup on event_id, bounded by the watermark.

    dropDuplicatesWithinWatermark, not plain dropDuplicates, and the difference is
    the whole reason a streaming dedup is hard. Plain dropDuplicates must remember
    EVERY event_id it has ever seen, forever, to guarantee it never admits a
    duplicate — unbounded state that grows until the job dies. The watermarked
    variant only remembers event_ids within the watermark window, so state is
    bounded: a duplicate arriving more than 10 minutes after the original is not
    caught, but by then the original is long since committed and a re-delivery that
    late is not something Kafka does in practice.

    This is the standard trade for any streaming dedup: bounded state in exchange for
    a dedup guarantee that holds within a window rather than for all time. Stating
    the window makes the trade explicit.
    """
    return df.withWatermark("event_time", WATERMARK_DELAY) \
             .dropDuplicatesWithinWatermark(["event_id"])


def enforce_schema_with_evolution(df: DataFrame, additive_column: str | None = None) -> DataFrame:
    """Demonstrate additive schema evolution: one new nullable column.

    The scenario the milestone asks for: an upstream starts sending a new field. The
    RIGHT response to an additive change is to accept it — a new nullable column is
    backwards-compatible, and rejecting it would take the pipeline down over a change
    that breaks nothing. The write path enables mergeSchema so Delta widens the table
    to include it.

    What must NOT evolve automatically is a type change or a dropped column — those
    are breaking, and Delta's mergeSchema correctly refuses them (a String-to-Int
    change fails the write rather than corrupting the column). So "schema evolution
    enabled" is precise: additive yes, breaking no. This function adds the column if
    present; the write's mergeSchema=true does the table widening.
    """
    if additive_column and additive_column not in df.columns:
        # A downstream consumer added a field to the contract. Present as null for
        # historical rows, populated going forward. This is what backwards-compatible
        # means in practice.
        return df.withColumn(additive_column, F.lit(None).cast("string"))
    return df


TOPIC_VALIDATORS = {
    "orders": validate_orders,
    "inventory_updates": validate_inventory,
    "delivery_status": validate_delivery,
}


def clean_topic(bronze: DataFrame, topic: str) -> tuple[DataFrame, DataFrame]:
    """Full silver transform for one topic: validate, then dedup the clean half.

    Order matters: validate BEFORE dedup. A malformed duplicate should be
    dead-lettered as malformed, not silently removed by the dedup — routing it to
    dead_letter preserves the evidence, and deduping first would delete the second
    copy before its reason was recorded. Dedup runs only on the clean stream.
    """
    validator = TOPIC_VALIDATORS[topic]
    clean, dead = validator(bronze)
    deduped = deduplicate_stream(clean)
    return deduped, dead


def read_bronze_stream(spark: SparkSession, path: str) -> DataFrame:
    return spark.readStream.format("delta").load(path)


def write_silver_stream(df: DataFrame, path: str, checkpoint: str, topic: str,
                        allow_schema_evolution: bool = True):
    writer = (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .queryName(f"silver_{topic}")
        .trigger(processingTime="10 seconds")
    )
    if allow_schema_evolution:
        # mergeSchema=true is what makes additive evolution work: Delta widens the
        # table to include a new nullable column rather than failing the write. It
        # does NOT permit a type change or a column drop — those still fail, which is
        # correct, because they are breaking changes that should stop the pipeline.
        writer = writer.option("mergeSchema", "true")
    return writer.start(path)


def write_dead_letter_stream(df: DataFrame, path: str, checkpoint: str, topic: str):
    return (
        df.withColumn("_dead_lettered_at", F.current_timestamp())
        .withColumn("_source_topic", F.lit(topic))
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .option("mergeSchema", "true")
        # mergeSchema on the dead-letter table too: it collects records from all
        # three topics with different shapes, so its schema is genuinely the union
        # and must widen as new failure shapes arrive. This is the one place broad
        # evolution is correct rather than dangerous.
        .queryName(f"dead_letter_{topic}")
        .trigger(processingTime="10 seconds")
        .start(path)
    )


def run(spark: SparkSession, base_path: str, checkpoint_base: str):
    queries = []
    for topic in TOPIC_VALIDATORS:
        bronze = read_bronze_stream(spark, f"{base_path}/bronze_{topic}")
        clean, dead = clean_topic(bronze, topic)

        queries.append(write_silver_stream(
            clean, f"{base_path}/silver_{topic}",
            f"{checkpoint_base}/silver_{topic}", topic))
        queries.append(write_dead_letter_stream(
            dead, f"{base_path}/dead_letter",
            f"{checkpoint_base}/dead_letter_{topic}", topic))
    return queries


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("silver_clean")
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
    queries = run(spark, args.base_path, args.checkpoint_base)
    print(f"started {len(queries)} silver + dead-letter streams")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
