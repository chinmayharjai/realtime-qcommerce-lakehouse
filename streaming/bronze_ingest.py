"""Bronze: Kafka -> Delta, exactly-once.

Spark Structured Streaming job that consumes all three topics and lands them in
Delta bronze tables. This is the ingestion boundary, so its one job is to capture
every event exactly once, with full fidelity, and let the silver layer clean it.

Exactly-once here is not a slogan — it is a specific contract between three things:

  1. Kafka offsets as the source of truth for "what have we read".
  2. A Delta sink that commits atomically.
  3. A checkpoint that ties the two together transactionally.

Structured Streaming records, in the checkpoint, exactly which Kafka offsets went
into each Delta commit. If the job dies mid-batch, the Delta write for that batch
either committed (and the offset advanced) or it did not (and the batch replays from
the same offset). There is no "committed the data but lost the offset" state, which
is the state that produces duplicates. This is why the sink must be Delta (atomic
commits) and not, say, a plain parquet append (no atomicity — a crash mid-write
leaves a partial file the next run cannot reconcile).

The parsing logic is a pure function (parse_topic) so the schema handling is
chispa-testable without a running stream.
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (ArrayType, DoubleType, IntegerType, StringType,
                               StructField, StructType, TimestampType)

# One explicit schema per topic. NOT schema inference.
#
# from_json with an explicit schema is the difference between a bronze layer that is
# a contract and one that is a guess. Inference on a streaming source is impossible
# anyway (there is no bounded sample), but even where it is possible it means the
# columns depend on whatever happened to be in the first micro-batch — so a field
# absent early silently never appears, and a type is whatever the first value looked
# like. Stating the schema makes bronze's shape a decision, and makes a
# contract-violating event land as a null the silver layer can route to dead-letter
# rather than as a parse that quietly reshapes the table.

ORDER_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("order_id", StringType()),
    StructField("event_time", TimestampType()),
    StructField("ingest_time", TimestampType()),
    StructField("is_late", StringType()),
    StructField("store_id", StringType()),
    StructField("city", StringType()),
    StructField("zone", StringType()),
    StructField("customer_id", StringType()),
    StructField("order_value", DoubleType()),
    StructField("line_count", IntegerType()),
    StructField("lines", ArrayType(StructType([
        StructField("sku", StringType()),
        StructField("name", StringType()),
        StructField("category", StringType()),
        StructField("qty", IntegerType()),
        StructField("unit_price", DoubleType()),
    ]))),
    StructField("payment_method", StringType()),
    StructField("status", StringType()),
    StructField("promised_minutes", IntegerType()),
])

INVENTORY_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_time", TimestampType()),
    StructField("ingest_time", TimestampType()),
    StructField("is_late", StringType()),
    StructField("store_id", StringType()),
    StructField("sku", StringType()),
    StructField("sku_name", StringType()),
    StructField("category", StringType()),
    StructField("previous_level", IntegerType()),
    StructField("new_level", IntegerType()),
    StructField("delta", IntegerType()),
    StructField("reason", StringType()),
])

DELIVERY_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_time", TimestampType()),
    StructField("ingest_time", TimestampType()),
    StructField("is_late", StringType()),
    StructField("order_id", StringType()),
    StructField("store_id", StringType()),
    StructField("stage", StringType()),
    StructField("minutes_since_order", DoubleType()),
    StructField("rider_id", StringType()),
])

TOPIC_SCHEMAS = {
    "orders": ORDER_SCHEMA,
    "inventory_updates": INVENTORY_SCHEMA,
    "delivery_status": DELIVERY_SCHEMA,
}


def parse_topic(kafka_df: DataFrame, schema: StructType) -> DataFrame:
    """Parse the Kafka value JSON against a schema, keeping the Kafka metadata.

    Pure function of a DataFrame, so it runs the same over a real Kafka stream and
    over a hand-built DataFrame in a chispa test.

    The Kafka metadata columns are kept, not discarded, and that is deliberate:
    _kafka_partition and _kafka_offset are the physical address of the event, and
    keeping them in bronze means that if the silver dedup ever disagrees with the
    exactly-once claim, you can prove which offset produced which row. They are the
    audit trail of the ingestion itself.
    """
    return (
        kafka_df
        .select(
            F.col("key").cast("string").alias("_kafka_key"),
            F.col("partition").alias("_kafka_partition"),
            F.col("offset").alias("_kafka_offset"),
            F.col("timestamp").alias("_kafka_timestamp"),
            F.from_json(F.col("value").cast("string"), schema).alias("payload"),
        )
        .select("_kafka_key", "_kafka_partition", "_kafka_offset", "_kafka_timestamp", "payload.*")
        # A row where payload is entirely null means from_json could not parse the
        # value at all (not valid JSON, or wildly off-schema). It is kept, not
        # dropped: bronze captures everything, and silver decides what is
        # dead-letter. Dropping here would lose the evidence that a malformed event
        # arrived.
        .withColumn("_ingested_at", F.current_timestamp())
    )


def read_topic_stream(spark: SparkSession, bootstrap: str, topic: str,
                      starting_offsets: str = "earliest") -> DataFrame:
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        # earliest on first run (backfill the topic), then the checkpoint takes over
        # and this option is ignored. maxOffsetsPerTrigger caps how much a single
        # micro-batch pulls, so a job restarting after downtime processes the
        # backlog in bounded chunks rather than one enormous batch that OOMs.
        .option("maxOffsetsPerTrigger", 50000)
        .option("failOnDataLoss", "true")
        # true, not false. If Kafka has aged out an offset the checkpoint still wants
        # (retention expired during downtime), that is real data loss, and the job
        # should FAIL loudly rather than silently skip to the next available offset.
        # false is the setting that turns a retention misconfiguration into missing
        # rows nobody notices.
        .load()
    )


def write_bronze_stream(parsed: DataFrame, path: str, checkpoint: str, topic: str):
    return (
        parsed.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        # The checkpoint is per-stream and must be stable across restarts — it holds
        # the Kafka offsets committed with each Delta batch. Point two streams at the
        # same checkpoint and they corrupt each other's offset tracking; change a
        # stream's checkpoint and it forgets what it read and reprocesses everything.
        # It is the single most important path in the job.
        .option("mergeSchema", "false")
        # false in bronze. Bronze's schema is the explicit contract above; a new
        # field appearing should be a deliberate schema change (M3 demonstrates
        # additive evolution in silver), not something bronze absorbs silently on a
        # random Tuesday.
        .partitionBy("_ingest_date")
        .trigger(processingTime="10 seconds")
        # Micro-batch every 10s rather than continuous or as-fast-as-possible. 10s
        # is the latency the ops dashboard needs (stockouts in minutes, not
        # seconds), and a larger batch amortises the Delta commit overhead — a
        # commit per second would spend more time committing than processing.
        .queryName(f"bronze_{topic}")
        .start(path)
    )


def ingest(spark: SparkSession, bootstrap: str, base_path: str, checkpoint_base: str):
    """Start one stream per topic. Returns the streaming queries."""
    queries = []
    for topic, schema in TOPIC_SCHEMAS.items():
        raw = read_topic_stream(spark, bootstrap, topic)
        parsed = parse_topic(raw, schema).withColumn(
            "_ingest_date", F.to_date("_ingested_at")
        )
        query = write_bronze_stream(
            parsed,
            path=f"{base_path}/bronze_{topic}",
            checkpoint=f"{checkpoint_base}/bronze_{topic}",
            topic=topic,
        )
        queries.append(query)
    return queries


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("bronze_ingest")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.streaming.stateStore.stateSchemaCheck", "true")
        .getOrCreate()
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:19092")
    ap.add_argument("--base-path", default="data/lakehouse")
    ap.add_argument("--checkpoint-base", default="data/checkpoints")
    args = ap.parse_args()

    spark = build_spark()
    queries = ingest(spark, args.bootstrap, args.base_path, args.checkpoint_base)
    print(f"started {len(queries)} bronze streams: {[q.name for q in queries]}")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
