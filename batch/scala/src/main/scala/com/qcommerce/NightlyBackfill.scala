package com.qcommerce

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/** Nightly backfill (Scala): apply corrections into the same Delta tables.
  *
  * The exact counterpart of batch/nightly_backfill.py — same transformation, same
  * tests — so the two implementations can be read side by side. The README's M5
  * section compares them.
  *
  * Why maintain both: the streaming and gold jobs are Python (Databricks notebooks,
  * PySpark), but many data platforms are Scala/JVM shops, and a batch job that has
  * to run on a Scala-first cluster (or as a JAR on Dataproc, which M6 does) needs a
  * JVM implementation. Keeping the logic identical means the correctness argument is
  * made once and inherited by both — the transformation functions are pure, take a
  * DataFrame, return a DataFrame, and the tests assert the same invariants in both
  * languages.
  */
object NightlyBackfill {

  /** Full deduplication — no watermark, keep the latest by ingestTime.
    *
    * The batch has the whole day in hand, so unlike the streaming dedup it can
    * guarantee one row per eventId across the entire day rather than within a
    * window. A deterministic tiebreak (eventId as the secondary sort) makes reruns
    * identical — the same discipline as the Python version's dedupe_full.
    */
  def dedupeFull(df: DataFrame,
                 key: String = "event_id",
                 orderCol: String = "ingest_time"): DataFrame = {
    val w = Window
      .partitionBy(key)
      .orderBy(col(orderCol).desc_nulls_last, col(key).desc)

    df.withColumn("_rn", row_number().over(w))
      .filter(col("_rn") === 1)
      .drop("_rn")
  }

  /** Reprocess one day: filter to the event date, dedupe fully.
    *
    * Filters on eventDate derived from eventTime, not the ingest partition, so a
    * late event whose eventTime is on processDate is included even though it landed
    * in a later partition. This is where the backfill corrects the stream —
    * identical to apply_corrections in the Python job.
    */
  def applyCorrections(raw: DataFrame, processDate: String): DataFrame = {
    val withDate = raw.withColumn("event_date", to_date(col("event_time")))
    dedupeFull(withDate.filter(col("event_date") === lit(processDate)))
  }

  /** Overwrite exactly the reprocessed partition(s), leaving the rest untouched.
    *
    * Dynamic partition overwrite scoped by event_date — a surgical correction, not a
    * full-table rewrite. Combined with the deterministic dedup, rerunning for a date
    * is idempotent.
    */
  def writePartitionOverwrite(df: DataFrame, path: String): Unit = {
    df.write
      .format("delta")
      .mode("overwrite")
      .option("partitionOverwriteMode", "dynamic")
      .partitionBy("event_date")
      .save(path)
  }

  def runBackfill(spark: SparkSession,
                  rawPath: String,
                  silverPath: String,
                  processDate: String): Long = {
    val raw = spark.read.format("delta").load(rawPath)
    val corrected = applyCorrections(raw, processDate)
    val count = corrected.count()
    writePartitionOverwrite(corrected, silverPath)
    count
  }

  def buildSpark(): SparkSession =
    SparkSession
      .builder()
      .appName("nightly_backfill")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog",
              "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()

  def main(args: Array[String]): Unit = {
    val argMap = args
      .sliding(2, 2)
      .collect { case Array(k, v) => k.stripPrefix("--") -> v }
      .toMap

    val rawPath = argMap("raw-path")
    val silverPath = argMap("silver-path")
    val processDate = argMap("process-date")

    val spark = buildSpark()
    val n = runBackfill(spark, rawPath, silverPath, processDate)
    // scalastyle:off
    println(s"backfilled $processDate: $n rows written to $silverPath")
    // scalastyle:on
    spark.stop()
  }
}
