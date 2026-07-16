# Runbook — consumer lag high

**Symptom:** the streaming bronze/silver queries are falling behind the topics —
dashboard freshness degrades, `window_start` on the newest gold rows drifts minutes
behind wall clock.

> Lag has exactly three causes, and they need different fixes: **the stream is slow**
> (processing < arrival rate), **the stream is stopped** (crashed or wedged), or
> **the input spiked** (rain surge — arrival rate went up and the lag is temporary
> and self-healing). The first job is telling those apart; two of the three fix
> themselves or need no code change.

## 0. First two minutes — which of the three is it?

```bash
# Is the query even running? (Spark UI -> Structured Streaming tab, or:)
# For each query: is batchDuration < triggerInterval, and is inputRate > processedRate?
```

Check the streaming query's recent progress (Spark UI → Structured Streaming → the
query → "Recent Progress", or `query.recentProgress` in the notebook):

| Signal | Meaning | Go to |
|---|---|---|
| `numInputRows` spiked, `processedRowsPerSecond` steady | **Input surge** (rain mode) — the stream is healthy, the backlog will drain | §1 |
| `batchDuration` > trigger interval, input steady | **Stream is slow** — each batch takes longer than the trigger | §2 |
| No new progress entries at all | **Stream is stopped** | §3 |

Also check Redpanda's own view of the lag:

```bash
docker compose exec redpanda rpk group describe <consumer-group> --brokers localhost:9092
# LAG column per partition. Uniform lag across partitions = throughput problem.
# One partition lagging alone = a hot key (one store dominating traffic).
```

## 1. Input surge (rain mode) — usually do nothing

The producer's rain-surge mode (and real weather) triples order volume in affected
zones. Lag rises during the surge and drains after it. This is the system working.

- Confirm the surge is real: the gold `orders_by_zone_5min` table shows which zones
  spiked. If two zones tripled and the rest are flat, it is weather, not a bug.
- The stream processes the backlog at `maxOffsetsPerTrigger` per batch (50K), so the
  drain rate is bounded and predictable: `backlog / 50K ≈ batches to catch up`, at
  ~10s per batch.
- **Escalate only if** the backlog is still growing an hour after the surge ended —
  then it is actually §2 wearing a surge costume.

**Do not raise `maxOffsetsPerTrigger` mid-surge to "catch up faster".** The cap is
what keeps a recovering stream from pulling one enormous batch that OOMs — the exact
failure the backfill-OOM runbook deals with. A bounded drain is slow and reliable;
an unbounded one is fast until it dies.

## 2. Stream is slow — find the stage, not the knob

`batchDuration` exceeding the trigger interval means each micro-batch takes longer
than 10s. Look at the query's SQL tab in the Spark UI for the slow stage:

| Slow stage | Cause | Fix |
|---|---|---|
| State store ops (dedup) | Watermark state grew — usually event_time skew pushing the watermark far back | Check for events with wildly wrong event_time (a device clock in 1970 holds the watermark back forever, so state never expires). Dead-letter them at silver |
| Shuffle in the windowed agg | Data skew — one zone/store dominating | Confirm with the per-partition lag in §0; a hot key needs salting or a bigger shuffle partition count |
| Delta commit | Too many small files in the target | Run `OPTIMIZE` on the table (the maintenance job may be behind) |
| The sink write itself | Cloud storage throttling | Check the storage account/bucket metrics; usually transient |

The watermark-stuck-in-the-past case is the classic one: **one event with an absurd
event_time keeps the dedup state from ever expiring**, state grows monotonically,
and every batch gets slower until the job dies. The silver validators bound
plausible event times for exactly this reason — if this happened, an implausible
timestamp got past them, and the fix is tightening that check, not restarting the
job (a restart inherits the same state via the checkpoint).

## 3. Stream is stopped

```bash
# The query terminated. Find out how it ended:
# - notebook/driver log: the exception from query.exception() if it crashed
# - or someone stopped it (Databricks cluster auto-terminated?)
```

| Cause | Recovery |
|---|---|
| Cluster auto-terminated (idle policy) | Restart the cluster and the streams. The checkpoint resumes from the committed offsets — nothing is lost and nothing duplicates (that is what exactly-once bought) |
| `failOnDataLoss` fired — Kafka aged out an offset the checkpoint wants | **Real data loss.** The stream was down longer than the topic retention (7 days). Restarting with a fresh checkpoint skips the gap; the nightly backfill CANNOT recover it (bronze never got the events). Document the gap window and its size before restarting — see below |
| An exception in the stream (schema, storage) | Fix the cause; restart. The checkpoint resumes exactly |

**Restart is always safe** because of the checkpoint contract (see
`streaming/bronze_ingest.py`): offsets and Delta commits advance together, so a
restart never re-emits committed data and never skips uncommitted data. The one
thing you must NOT do is delete or change the checkpoint directory "to get it
unstuck" — that discards what has been read, and the stream will reprocess the whole
retained topic into the bronze table. The dedup in silver would absorb it, but you
would pay a full-retention reprocess for no reason.

## Escalation

| Situation | Action |
|---|---|
| Lag from a confirmed surge, draining | None. Note it in the channel |
| Lag growing steadily with flat input | Engineering — a stage got slower; find which one before restarting |
| `failOnDataLoss` fired | **Incident.** There is a real gap in bronze; downstream metrics for that window are permanently incomplete and consumers must be told |
| Same query stopped twice in a week | Engineering — the second stop is a pattern, not an accident |
