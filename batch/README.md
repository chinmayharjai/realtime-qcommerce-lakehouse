# Batch backfill — PySpark and Scala, the same job twice

The nightly backfill reconciles what the streaming pipeline got approximately right:
it reprocesses a day's raw events with **no watermark**, so nothing is dropped for
lateness, and overwrites that day's silver partition with the corrected, fully
deduplicated result. It writes into the **same Delta tables** the stream writes —
one table kept correct by a fast approximate stream and a slow exact batch, not a
"speed layer" and "batch layer" the consumer has to reconcile.

It exists in two implementations, and they are deliberately identical in logic:

| | PySpark | Scala |
|---|---|---|
| File | [`nightly_backfill.py`](nightly_backfill.py) | [`scala/.../NightlyBackfill.scala`](scala/src/main/scala/com/qcommerce/NightlyBackfill.scala) |
| Tests | [`tests/test_nightly_backfill.py`](tests/test_nightly_backfill.py) | [`.../NightlyBackfillSpec.scala`](scala/src/test/scala/com/qcommerce/NightlyBackfillSpec.scala) |
| Runs on | Databricks, Dataproc (PySpark) | Dataproc (JAR), any JVM Spark |
| Verified | chispa in CI | `sbt test` in CI |

## Why maintain both

The streaming and gold jobs are Python — Databricks notebooks, PySpark. But plenty
of data platforms are Scala/JVM shops, and a batch job that has to run as a JAR on a
Scala-first cluster (which the GCP Dataproc path in M6 does) needs a JVM
implementation. Rather than have the two drift, the logic is kept identical: the
transformation functions are pure (`DataFrame -> DataFrame`), and **both test suites
assert the same invariants on the same inputs**. If the two ever diverge, one suite
fails.

## The three functions, side by side

Both implementations have exactly these, same names, same behaviour:

**`dedupe_full` / `dedupeFull`** — full deduplication, no watermark, keep the latest
by `ingest_time`. The batch has the whole day in hand, so it guarantees one row per
`event_id` across the entire day, catching a duplicate the stream missed because its
two copies straddled the watermark. Deterministic tiebreak on `event_id`.

**`apply_corrections` / `applyCorrections`** — filter to the process date **by
`event_time`, not the ingest partition**, then dedupe. This is where the backfill
actually corrects the stream: a late event whose `event_time` is on the process date
is included even though it landed in a later partition.

**`write_partition_overwrite` / `writePartitionOverwrite`** — dynamic partition
overwrite scoped to `event_date`. Surgical, not a full-table rewrite; combined with
the deterministic dedup, rerunning for a date is idempotent.

## What differs between the languages (and what doesn't)

The **logic** doesn't differ — that's the point. What differs is idiom:

- Python's `Window.partitionBy(...).orderBy(F.col(...).desc_nulls_last())` vs Scala's
  `Window.partitionBy(...).orderBy(col(...).desc_nulls_last)` — the same operators,
  spelled per language.
- Python's `.transform(dedupe_full)` (a method taking a function) vs Scala's plain
  `dedupeFull(df)` call — Scala has no `.transform` chaining idiom here, so it reads
  as nested calls.
- Scala's `build.sbt` carries the Java 17 `--add-opens` flags Spark needs under the
  module system; PySpark inherits those from the cluster config. This is the one
  place the JVM implementation has ceremony the Python one doesn't.

## Running

```bash
# PySpark
python batch/nightly_backfill.py \
  --raw-path data/lakehouse/bronze_orders \
  --silver-path data/lakehouse/silver_orders \
  --process-date 2026-06-20

# Scala — build the JAR, then spark-submit it
cd batch/scala && sbt package
spark-submit --class com.qcommerce.NightlyBackfill \
  target/scala-2.12/nightly-backfill_2.12-1.0.0.jar \
  --raw-path ... --silver-path ... --process-date 2026-06-20
```

The Scala JAR is what M6's Dataproc path submits.
