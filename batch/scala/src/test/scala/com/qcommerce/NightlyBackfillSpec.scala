package com.qcommerce

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/** The Scala counterpart of batch/tests/test_nightly_backfill.py — the SAME
  * invariants, asserted in Scala. If the two jobs are truly the same logic, these
  * tests and the Python ones pass on the same inputs, which is the check that the
  * "same job in two languages" claim is real rather than aspirational.
  */
class NightlyBackfillSpec extends AnyFunSuite with BeforeAndAfterAll {

  @transient private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("backfill-tests")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  // A row: (event_id, event_time, ingest_time, store_id, value)
  private def df(rows: (String, String, String, String, Double)*) = {
    // Bind a local val: `import spark.implicits._` needs a STABLE identifier, and
    // `spark` is a var (reassigned in beforeAll), so importing off it directly is a
    // compile error. The local val is stable, which is the idiomatic fix and what
    // brings the .toDF implicit into scope.
    val ss = spark
    import ss.implicits._
    rows.toDF("event_id", "event_time", "ingest_time", "store_id", "value")
  }

  test("dedupeFull keeps the latest by ingest_time") {
    val input = df(
      ("E1", "2026-06-20T09:00:00", "2026-06-20T09:00:05", "S1", 1.0),
      ("E1", "2026-06-20T09:00:00", "2026-06-20T09:03:00", "S1", 2.0), // re-delivery, later
      ("E2", "2026-06-20T09:00:00", "2026-06-20T09:00:06", "S1", 3.0)
    )
    val result = NightlyBackfill.dedupeFull(input).collect()
    assert(result.length == 2)

    val e1 = result.find(_.getAs[String]("event_id") == "E1").get
    assert(e1.getAs[Double]("value") == 2.0) // the later ingest won
  }

  test("dedupeFull is deterministic on ingest_time ties") {
    // Two rows, same event_id, same ingest_time — the secondary sort on event_id
    // must make the survivor deterministic across runs.
    val input = df(
      ("E1", "2026-06-20T09:00:00", "2026-06-20T09:00:05", "SA", 1.0),
      ("E1", "2026-06-20T09:00:00", "2026-06-20T09:00:05", "SB", 2.0)
    )
    val first = NightlyBackfill.dedupeFull(input).collect().map(_.getAs[String]("store_id"))
    val second = NightlyBackfill.dedupeFull(input).collect().map(_.getAs[String]("store_id"))
    assert(first.sameElements(second))
  }

  test("dedupeFull keeps distinct events") {
    val rows = (0 until 20).map(i =>
      (s"E$i", "2026-06-20T09:00:00", "2026-06-20T09:00:05", "S1", i.toDouble))
    assert(NightlyBackfill.dedupeFull(df(rows: _*)).count() == 20)
  }

  test("applyCorrections filters to the process date by event_time") {
    val input = df(
      ("E1", "2026-06-20T09:00:00", "2026-06-20T09:00:05", "S1", 1.0),
      ("E2", "2026-06-21T09:00:00", "2026-06-21T09:00:05", "S1", 2.0), // different day
      ("E3", "2026-06-20T23:30:00", "2026-06-22T03:00:00", "S1", 3.0)  // late: event on the 20th, ingested the 22nd
    )
    val result = NightlyBackfill.applyCorrections(input, "2026-06-20").collect()
    val ids = result.map(_.getAs[String]("event_id")).toSet
    // E1 and the late E3 (event_time on the 20th) are included; E2 is not.
    assert(ids == Set("E1", "E3"))
  }

  test("applyCorrections includes a late event by its event date, not ingest date") {
    // The key correction the backfill makes: an event whose event_time is on
    // processDate but which arrived a day late is still attributed to processDate.
    val input = df(
      ("LATE", "2026-06-20T23:55:00", "2026-06-22T04:00:00", "S1", 9.0)
    )
    val result = NightlyBackfill.applyCorrections(input, "2026-06-20").collect()
    assert(result.length == 1)
    assert(result(0).getAs[String]("event_id") == "LATE")
  }

  test("applyCorrections dedupes within the day") {
    // A duplicate whose copies the stream missed (straddling its watermark) is
    // caught here because the batch has no watermark.
    val input = df(
      ("E1", "2026-06-20T09:00:00", "2026-06-20T09:00:05", "S1", 1.0),
      ("E1", "2026-06-20T09:00:00", "2026-06-20T09:20:00", "S1", 2.0) // 20 min later than the first
    )
    val result = NightlyBackfill.applyCorrections(input, "2026-06-20").collect()
    assert(result.length == 1)
    assert(result(0).getAs[Double]("value") == 2.0)
  }

  test("backfill is deterministic — same input, same output") {
    val rows = Seq(
      ("E1", "2026-06-20T09:00:00", "2026-06-20T09:00:05", "S1", 1.0),
      ("E1", "2026-06-20T09:00:00", "2026-06-20T09:10:00", "S1", 2.0),
      ("E2", "2026-06-20T10:00:00", "2026-06-20T10:00:05", "S2", 3.0)
    )
    val a = NightlyBackfill.applyCorrections(df(rows: _*), "2026-06-20")
      .collect().map(r => (r.getAs[String]("event_id"), r.getAs[Double]("value"))).toSet
    val b = NightlyBackfill.applyCorrections(df(rows: _*), "2026-06-20")
      .collect().map(r => (r.getAs[String]("event_id"), r.getAs[Double]("value"))).toSet
    assert(a == b)
  }
}
