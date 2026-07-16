# Runbook — stockout alert flood

**Symptom:** the `stockout_alerts` table is filling far faster than normal — the ops
channel is getting dozens of alerts a minute, and people have started ignoring them.

> An alert flood has two failure modes and the second is worse: either the alerts
> are **true** (a demand surge is genuinely draining many stores at once) or they
> are **false** (a data problem is making healthy inventory look empty). The first
> is an ops problem the system is correctly reporting. The second is the detector
> lying — and every minute it floods, real alerts drown in it. The worst response
> to either is muting the table, because the day you mute it is the day a real
> stockout goes unseen. Diagnose in minutes, then act on the cause.

## 0. True or false? — three queries, two minutes

**1. Is the demand real?** Check the zone windows for a surge:

```sql
SELECT window_start, zone, order_count
FROM gold_orders_by_zone_5min
WHERE window_start > now() - INTERVAL 1 HOUR
ORDER BY order_count DESC LIMIT 20;
```
A rain surge shows as 2-3 specific zones at 3x volume. If the alert flood's stores
are in those zones: **the alerts are probably true.** Go to §1.

**2. Is the inventory data fresh?** The detector projects from
`gold_store_sku_inventory`. If inventory *updates* stopped flowing (stuck stream,
lagging consumer) while orders kept coming, the detector sees stock draining with no
restocks — and floods with alerts for stores that restocked an hour ago:

```sql
SELECT max(as_of) FROM gold_store_sku_inventory;
-- if this is > 15 min old, the flood is FALSE: stale inventory, live demand
```
Stale inventory + live orders = false flood. Go to §2.

**3. Did the alert threshold or the detector change?** `git log` on
`streaming/gold_aggregates.py`. A threshold change (say horizon 15 → 30 minutes)
legitimately multiplies alert volume overnight. If the flood started at a deploy,
it is the deploy.

## 1. True flood — a real multi-store surge

The system is working; the problem is operational. What the data can contribute:

- **Rank the alerts by `minutes_to_stockout` ascending** and give ops the top of the
  list — during a flood, the scarce resource is attention, and 4-minutes-out is not
  the same as 14-minutes-out:

```sql
SELECT store_id, sku, minutes_to_stockout, current_level
FROM stockout_alerts
WHERE detected_at > now() - INTERVAL 15 MINUTE
ORDER BY minutes_to_stockout ASC LIMIT 25;
```

- Alerts on the same store/SKU repeat every window while the condition holds. For
  the ops channel, deduplicate to first-seen (the table keeps every firing for
  analysis; the channel should not).

## 2. False flood — stale inventory

The inventory consumer is behind or stopped while orders flow. This is
[consumer_lag_high.md](consumer_lag_high.md) for the `inventory_updates` stream —
follow that runbook to restore the stream. Meanwhile:

- **Tell ops the alerts are unreliable and why**, with the `max(as_of)` timestamp as
  evidence. An explicit "ignore until further notice, inventory feed is 40 minutes
  stale" preserves trust in a way silence does not.
- **Do not delete the false alerts.** They are evidence of the detector's behaviour
  during a data outage, and the post-incident review will want them. They are also
  timestamped, so consumers can exclude the window.
- When the stream catches up, the detector self-corrects on the next window — fresh
  inventory levels arrive and the projections go sane. No detector restart needed.

## 3. Structural fixes if floods recur

Each of these is a code change with a trade-off, not a config toggle:

| Fix | Trade |
|---|---|
| Alert only on first-crossing (state: alert once per store/SKU episode, not per window) | Needs state in the detector — more complexity, better signal |
| A freshness guard: suppress alerts when `max(as_of)` is older than N minutes | The detector goes silent during inventory outages — arguably correct, but silence must itself alert (the P2 "suspiciously zero" pattern) |
| Per-zone alert budget with overflow to a summary ("Koramangala: 34 SKUs at risk") | Ops sees the shape of a surge instead of 34 messages |

The freshness guard is the one this incident usually motivates, and its silence
alarm is not optional — a detector that quietly suppresses during the exact windows
when things are most uncertain needs its own watchdog, or the suppression becomes
the next silent failure.

## What NOT to do

- **Do not mute the alerts table or the channel.** Muting converts a noisy day into
  a blind month. The mute never gets lifted; the next real stockout arrives unseen.
- **Do not raise the stockout threshold during a flood** to quiet it. Threshold
  changes during an incident are how detectors get permanently miscalibrated —
  change thresholds from post-incident analysis, not mid-panic.
- **Do not truncate `stockout_alerts`.** It is append-only evidence. The dashboard
  should filter to recent windows; deleting history to fix a display problem
  destroys the record.
