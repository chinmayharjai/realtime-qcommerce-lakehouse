# Runbook — backfill OOM

**Symptom:** `qcommerce_batch_backfill`'s `run_backfill` task fails with
`OutOfMemoryError`, an executor lost to the OOM killer, or a driver that stopped
heartbeating mid-job.

> The reflex is "give it more memory". Sometimes that is right; more often the OOM
> is a *shape* problem — one huge partition, a skewed key, or an accidental
> unbounded read — and more memory just moves the failure later while doubling the
> cluster bill. Diagnose which before touching the cluster size.

## 0. What actually ran out?

The error tells you which side died, and they mean different things:

| Error | Which side | Usual cause |
|---|---|---|
| `java.lang.OutOfMemoryError: Java heap space` in an **executor** | Executor heap | One task's partition too large: skew, or a huge day |
| Executor lost / `ExecutorLostFailure` with exit code 137 | The OS OOM-killer, not the JVM | Off-heap overhead (shuffle, Arrow) exceeded the container — heap tuning will NOT fix this; the overhead fraction will |
| Driver OOM | Driver heap | A `collect()`/`toPandas()` on something big, or too many small tasks' metadata. The backfill has one `count()` — if the driver died, look for a recent code change collecting data |

## 1. Is the input abnormally large?

The backfill reads one event_date. Check whether that date is an outlier:

```bash
# Rows per event_date in bronze (a quick Delta query):
# SELECT event_date, COUNT(*) FROM bronze_orders GROUP BY 1 ORDER BY 1 DESC LIMIT 7
```

- **A rain-surge day is legitimately 3x normal.** If the failing date is a surge
  day, the job may genuinely need more memory *for this one run* — that is the one
  case where "bigger cluster" is the right answer, applied once, not permanently.
- If the date's volume is normal, the OOM is a shape problem. Continue.

## 2. Skew — the usual culprit

The dedup windows by `event_id` (fine — unique keys never skew) but the shuffle
before it partitions by whatever Spark chose. Check the Spark UI's failed stage:
if the task-duration distribution shows 199 tasks at seconds and 1 task at
minutes-then-dead, that one task got a hot partition.

Fixes in order of preference:

1. **AQE (adaptive query execution) skew handling** — on by default in Spark 3.5;
   confirm nobody disabled it (`spark.sql.adaptive.skewJoin.enabled`).
2. **More shuffle partitions** (`spark.sql.shuffle.partitions`) — splits the load
   finer, costs nothing at this scale.
3. **Salting** the hot key — a real code change; only if 1-2 do not clear it.

## 3. The unbounded-read trap

The backfill's read is `raw` filtered to one `event_date` — but the filter is on a
**derived** column (`to_date(event_time)`), which cannot prune partitions if bronze
is partitioned by `_ingest_date`. That means the job may be reading far more than
one day and filtering after the read.

This is a known, deliberate trade in the current design (late events force reading
beyond the target date), but if bronze has grown large, the fix is bounding the read
by ingest partition too:

```python
# read only ingest partitions that could contain the target event date
raw = raw.filter(F.col("_ingest_date").between(process_date, date_add(process_date, 3)))
```

Late events arrive at most ~48h behind, so ingest partitions beyond D+3 cannot
contain events for day D. This bounds the read without losing correctness, and it is
the first change to make when the backfill's input outgrows the cluster.

## 4. If it really is memory

Only after 1-3 are excluded:

- Executor heap OOM: fewer cores per executor (more memory per task) beats a bigger
  executor at the same total spend.
- Exit code 137: raise `spark.executor.memoryOverhead` (or `memoryOverheadFactor`),
  NOT the heap — the heap was fine, the container ceiling was not.
- Driver OOM: find and remove the collect. The backfill does not need to bring data
  to the driver.

## Recovery — always the same, because the job is idempotent

Whatever the fix, recovery is: **clear the failed task and re-run it.** The backfill
overwrites its event_date partition (dynamic partition overwrite), so the partial
output of the OOM'd attempt — if any files were written at all — is replaced
entirely. There is no cleanup step, no partial state to reason about. A failed
attempt followed by a successful one is indistinguishable from a single successful
run. That property is tested (`batch/tests`, both languages) and is the entire
reason this runbook has one recovery line instead of a page.

## What NOT to do

- **Do not delete the failing date's silver partition "to start clean".** The
  overwrite does that atomically; a manual delete followed by a failed run leaves
  the date missing entirely, which is worse than stale.
- **Do not permanently double the cluster for a one-day surge.** Size for the
  normal case; handle the outlier day as a one-off with a temporarily larger
  cluster.
- **Do not disable AQE to "make plans predictable".** AQE's skew splitting is the
  thing saving this job on skewed days.
