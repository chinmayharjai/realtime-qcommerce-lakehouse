"""Stream simulated quick-commerce events into Kafka/Redpanda.

Three topics, one dark-store network:

    orders            an order placed at a dark store (the high-volume topic)
    inventory_updates stock level changes (restocks, picks, adjustments)
    delivery_status   an order's journey (assigned -> picked -> dispatched -> delivered)

Event *generation* is separated from Kafka *publishing* on purpose. `EventFactory`
is a pure, seedable generator with no Kafka dependency, so the event shapes, the
rain-surge multiplier, and the lateness injection are all unit-testable without a
broker (tests/test_order_stream.py). Only `stream()` touches Kafka.

Design choices that the streaming jobs downstream depend on:
  - every event has an event_id (for dedup) and an event_time (for watermarking)
  - events are keyed by store_id, so one store's events stay ordered in a partition
  - a configurable fraction of events are emitted LATE (event_time well behind now),
    which is what makes the silver layer's watermark do observable work
  - a rain-surge mode multiplies order volume in affected zones, so the stockout
    detector has a realistic demand spike to catch

Usage:
    python producer/order_stream.py --duration 300 --events-per-sec 50
    python producer/order_stream.py --rain-surge --duration 120
"""

from __future__ import annotations

import argparse
import json
import os
import random
import signal
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

# Dark stores across a few cities. Small enough that a store recurs often (so
# per-store velocity and stockouts are observable), large enough that zone-level
# aggregation is meaningful.
ZONES = [
    ("Bengaluru", "Koramangala", "BLR-KOR"),
    ("Bengaluru", "Indiranagar", "BLR-IND"),
    ("Bengaluru", "Whitefield", "BLR-WHF"),
    ("Mumbai", "Andheri", "MUM-AND"),
    ("Mumbai", "Bandra", "MUM-BAN"),
    ("Delhi", "Saket", "DEL-SAK"),
    ("Delhi", "Dwarka", "DEL-DWK"),
    ("Hyderabad", "Gachibowli", "HYD-GAC"),
]

# SKUs by category, with a base price band. Perishables and staples, because a
# quick-commerce catalogue is groceries, not electronics.
SKU_CATALOG = [
    ("SKU-MILK-1L", "Milk 1L", "dairy", 60, 72),
    ("SKU-BREAD-400", "Bread 400g", "bakery", 35, 45),
    ("SKU-EGGS-6", "Eggs (6)", "dairy", 42, 55),
    ("SKU-BANANA-1KG", "Banana 1kg", "produce", 40, 60),
    ("SKU-TOMATO-1KG", "Tomato 1kg", "produce", 25, 80),
    ("SKU-ONION-1KG", "Onion 1kg", "produce", 30, 70),
    ("SKU-RICE-5KG", "Rice 5kg", "staples", 320, 420),
    ("SKU-OIL-1L", "Cooking Oil 1L", "staples", 140, 200),
    ("SKU-MAGGI-4", "Maggi (4 pack)", "packaged", 48, 60),
    ("SKU-CHIPS-L", "Chips (large)", "snacks", 40, 50),
    ("SKU-COLA-2L", "Cola 2L", "beverages", 90, 110),
    ("SKU-COFFEE-200", "Coffee 200g", "beverages", 250, 340),
    ("SKU-DETERGENT-1KG", "Detergent 1kg", "household", 180, 260),
    ("SKU-SOAP-4", "Soap (4 pack)", "household", 120, 160),
    ("SKU-CHOCO-L", "Chocolate (large)", "snacks", 80, 150),
]

DELIVERY_STAGES = ["assigned", "picked", "packed", "dispatched", "delivered"]
PAYMENT_METHODS = ["UPI", "CARD", "WALLET", "COD"]
ORDER_STATUS = ["placed", "confirmed", "cancelled"]


@dataclass
class EventFactory:
    """Pure, seedable event generator. No Kafka.

    Holds a little state — open orders awaiting delivery updates, per-store
    inventory — so the streams are coherent: a delivery_status event refers to an
    order that was actually placed, and inventory falls as orders consume it. That
    coherence is what makes the stockout detector have something real to detect
    rather than random noise.
    """

    seed: int = 42
    late_fraction: float = 0.05
    rain_surge: bool = False
    rng: random.Random = field(init=False)
    _open_orders: list[dict] = field(default_factory=list, init=False)
    _inventory: dict[tuple[str, str], int] = field(default_factory=dict, init=False)
    _surge_zones: set[str] = field(default_factory=set, init=False)

    def __post_init__(self):
        self.rng = random.Random(self.seed)
        # Seed inventory: each store stocks each SKU at a random level.
        for _, _, store_id in ZONES:
            for sku in SKU_CATALOG:
                self._inventory[(store_id, sku[0])] = self.rng.randint(20, 200)
        if self.rain_surge:
            # Rain hits two zones; their order volume multiplies (see next_order).
            self._surge_zones = {z[2] for z in self.rng.sample(ZONES, 2)}

    def _event_time(self, now: datetime) -> tuple[str, bool]:
        """Return (iso_event_time, is_late).

        A late event's event_time is 1-15 minutes behind `now`, simulating a device
        that buffered offline or a delayed gateway. The stream still emits it at
        `now`, so its event_time trails its arrival — exactly the condition the
        silver watermark exists to handle. Everything else is a few seconds behind
        now, normal processing latency.
        """
        if self.rng.random() < self.late_fraction:
            lag = timedelta(minutes=self.rng.randint(1, 15))
            return (now - lag).isoformat(), True
        return (now - timedelta(seconds=self.rng.randint(0, 5))).isoformat(), False

    def next_order(self, now: datetime) -> dict:
        city, zone_name, store_id = self.rng.choice(ZONES)
        n_lines = self.rng.randint(1, 6)
        lines = []
        order_value = 0.0
        for _ in range(n_lines):
            sku = self.rng.choice(SKU_CATALOG)
            qty = self.rng.randint(1, 4)
            price = round(self.rng.uniform(sku[3], sku[4]), 2)
            lines.append({"sku": sku[0], "name": sku[1], "category": sku[2],
                          "qty": qty, "unit_price": price})
            order_value += qty * price
            # Consume inventory. This is what a run of orders on one SKU drains
            # toward a stockout.
            key = (store_id, sku[0])
            self._inventory[key] = max(0, self._inventory.get(key, 0) - qty)

        event_time, is_late = self._event_time(now)
        order_id = f"ORD-{uuid.UUID(int=self.rng.getrandbits(128)).hex[:16]}"

        order = {
            "event_id": str(uuid.UUID(int=self.rng.getrandbits(128))),
            "event_type": "order",
            "order_id": order_id,
            "event_time": event_time,
            "ingest_time": now.isoformat(),
            "is_late": is_late,
            "store_id": store_id,
            "city": city,
            "zone": zone_name,
            "customer_id": f"CUST-{self.rng.randint(1, 50000):06d}",
            "order_value": round(order_value, 2),
            "line_count": n_lines,
            "lines": lines,
            "payment_method": self.rng.choice(PAYMENT_METHODS),
            "status": self.rng.choices(ORDER_STATUS, weights=[0.88, 0.09, 0.03])[0],
            "promised_minutes": self.rng.choice([10, 15, 20]),
        }
        # Remember it so a delivery_status event can reference a real order.
        if order["status"] != "cancelled":
            self._open_orders.append({"order_id": order_id, "store_id": store_id,
                                      "stage_idx": 0, "placed_at": now})
        return order

    def next_inventory_update(self, now: datetime) -> dict:
        _, _, store_id = self.rng.choice(ZONES)
        sku = self.rng.choice(SKU_CATALOG)
        key = (store_id, sku[0])
        current = self._inventory.get(key, 0)

        # A mix of restocks (level jumps up) and adjustments (small corrections).
        if self.rng.random() < 0.4:
            delta = self.rng.randint(50, 150)  # restock
            reason = "restock"
        else:
            delta = self.rng.randint(-5, 5)    # cycle-count adjustment
            reason = "adjustment"
        new_level = max(0, current + delta)
        self._inventory[key] = new_level

        event_time, is_late = self._event_time(now)
        return {
            "event_id": str(uuid.UUID(int=self.rng.getrandbits(128))),
            "event_type": "inventory_update",
            "event_time": event_time,
            "ingest_time": now.isoformat(),
            "is_late": is_late,
            "store_id": store_id,
            "sku": sku[0],
            "sku_name": sku[1],
            "category": sku[2],
            "previous_level": current,
            "new_level": new_level,
            "delta": new_level - current,
            "reason": reason,
        }

    def next_delivery_status(self, now: datetime) -> dict | None:
        """Advance an open order to its next delivery stage.

        Returns None if no order is waiting for an update — the caller falls back to
        producing another order, so the stream never stalls. Keeps deliveries tied
        to real orders rather than inventing order_ids.
        """
        if not self._open_orders:
            return None
        idx = self.rng.randrange(len(self._open_orders))
        order = self._open_orders[idx]
        order["stage_idx"] += 1
        stage = DELIVERY_STAGES[min(order["stage_idx"], len(DELIVERY_STAGES) - 1)]

        elapsed = (now - order["placed_at"]).total_seconds() / 60
        if stage == "delivered" or order["stage_idx"] >= len(DELIVERY_STAGES) - 1:
            self._open_orders.pop(idx)  # done; stop tracking

        event_time, is_late = self._event_time(now)
        return {
            "event_id": str(uuid.UUID(int=self.rng.getrandbits(128))),
            "event_type": "delivery_status",
            "event_time": event_time,
            "ingest_time": now.isoformat(),
            "is_late": is_late,
            "order_id": order["order_id"],
            "store_id": order["store_id"],
            "stage": stage,
            "minutes_since_order": round(elapsed, 2),
            "rider_id": f"RIDER-{self.rng.randint(1, 2000):05d}",
        }

    def surge_multiplier(self, store_id: str) -> int:
        """How many orders this tick a surge zone produces vs a normal one."""
        return 3 if store_id in self._surge_zones else 1


def iter_events(factory: EventFactory, now: datetime) -> list[tuple[str, str, dict]]:
    """One 'tick' of events as (topic, key, event).

    Emits a realistic mix: mostly orders, some inventory, some delivery updates.
    Returned rather than produced so tests can assert on the mix without Kafka.
    """
    out: list[tuple[str, str, dict]] = []

    order = factory.next_order(now)
    reps = factory.surge_multiplier(order["store_id"])
    out.append(("orders", order["store_id"], order))
    for _ in range(reps - 1):
        extra = factory.next_order(now)
        out.append(("orders", extra["store_id"], extra))

    if factory.rng.random() < 0.4:
        inv = factory.next_inventory_update(now)
        out.append(("inventory_updates", inv["store_id"], inv))

    if factory.rng.random() < 0.5:
        delivery = factory.next_delivery_status(now)
        if delivery is not None:
            out.append(("delivery_status", delivery["store_id"], delivery))

    return out


def stream(bootstrap: str, events_per_sec: int, duration: int, factory: EventFactory) -> dict:
    """Produce to Kafka for `duration` seconds. The only Kafka-touching function."""
    from confluent_kafka import Producer

    producer = Producer({
        "bootstrap.servers": bootstrap,
        "linger.ms": 20,           # small batching window: trade a few ms latency for throughput
        "compression.type": "lz4", # cheap on CPU, meaningful on network
        "acks": "all",             # durability: the leader waits for replicas. At r=1 locally this is just the leader, but the setting is what a real cluster needs
        "enable.idempotence": True, # the producer half of exactly-once: no duplicate on retry
    })

    counts: dict[str, int] = {}
    stop = {"flag": False}
    signal.signal(signal.SIGINT, lambda *_: stop.__setitem__("flag", True))

    interval = 1.0 / events_per_sec
    end_at = time.monotonic() + duration
    produced = 0

    while time.monotonic() < end_at and not stop["flag"]:
        now = datetime.now(timezone.utc)
        for topic, key, event in iter_events(factory, now):
            producer.produce(
                topic=topic,
                key=key.encode("utf-8"),
                value=json.dumps(event, separators=(",", ":")).encode("utf-8"),
                # Keyed by store_id so a store's events land in one partition and
                # stay ordered — the streaming dedup and the per-store aggregation
                # both assume within-store ordering.
                on_delivery=lambda err, msg: None if err is None else print(f"delivery failed: {err}"),
            )
            counts[topic] = counts.get(topic, 0) + 1
            produced += 1
        producer.poll(0)  # serve delivery callbacks without blocking
        time.sleep(interval)

    producer.flush(30)
    return counts


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092"))
    ap.add_argument("--events-per-sec", type=int, default=int(os.environ.get("EVENTS_PER_SEC", "50")))
    ap.add_argument("--duration", type=int, default=300)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--late-fraction", type=float, default=0.05)
    ap.add_argument("--rain-surge", action="store_true",
                    help="multiply order volume in two zones, to exercise the stockout detector")
    args = ap.parse_args()

    factory = EventFactory(seed=args.seed, late_fraction=args.late_fraction,
                           rain_surge=args.rain_surge)

    print(f"producing to {args.bootstrap} at ~{args.events_per_sec}/sec for {args.duration}s")
    if args.rain_surge:
        print(f"rain surge active in: {sorted(factory._surge_zones)}")

    counts = stream(args.bootstrap, args.events_per_sec, args.duration, factory)

    total = sum(counts.values())
    print(f"\nproduced {total:,} events:")
    for topic, n in sorted(counts.items()):
        print(f"  {topic:<20} {n:>8,}")


if __name__ == "__main__":
    main()
