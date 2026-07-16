"""Tests for the producer's event generation.

The EventFactory / iter_events split keeps Kafka out of these entirely: they build
events and assert on their shape, coherence and the injected properties (lateness,
rain surge) that the streaming jobs downstream depend on. No broker, no docker.

Run:  pytest producer/tests -v
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from order_stream import DELIVERY_STAGES, EventFactory, iter_events  # noqa: E402

NOW = datetime(2026, 6, 20, 9, 0, 0, tzinfo=timezone.utc)


def test_every_event_has_id_and_time():
    """The streaming layer dedups on event_id and watermarks on event_time. An event
    missing either is invisible to the pipeline's core mechanisms."""
    factory = EventFactory(seed=1)
    for _ in range(50):
        for _topic, _key, event in iter_events(factory, NOW):
            assert event.get("event_id"), event
            assert event.get("event_time"), event
            assert event.get("store_id"), event


def test_event_ids_are_unique():
    factory = EventFactory(seed=1)
    ids = []
    for _ in range(200):
        for _t, _k, event in iter_events(factory, NOW):
            ids.append(event["event_id"])
    assert len(ids) == len(set(ids)), "duplicate event_id — dedup tests would be meaningless"


def test_events_are_keyed_by_store():
    """Keying by store_id is what keeps a store's events ordered within a partition,
    which the dedup and per-store aggregation both assume."""
    factory = EventFactory(seed=1)
    for _ in range(50):
        for _topic, key, event in iter_events(factory, NOW):
            assert key == event["store_id"]


def test_three_topics_are_produced():
    factory = EventFactory(seed=2)
    topics = set()
    for _ in range(300):
        for topic, _k, _e in iter_events(factory, NOW):
            topics.add(topic)
    assert topics == {"orders", "inventory_updates", "delivery_status"}


def test_orders_dominate_the_stream():
    """orders is the high-volume topic (6 partitions vs 3). If the mix ever flips,
    the partition sizing in docker-compose is wrong."""
    factory = EventFactory(seed=3)
    counts = {"orders": 0, "inventory_updates": 0, "delivery_status": 0}
    for _ in range(500):
        for topic, _k, _e in iter_events(factory, NOW):
            counts[topic] += 1
    assert counts["orders"] > counts["inventory_updates"]
    assert counts["orders"] > counts["delivery_status"]


# --- Lateness injection -----------------------------------------------------

def test_some_events_are_late():
    """Late events (event_time behind arrival) are what make the silver watermark do
    observable work. Without them the watermark is untested plumbing."""
    factory = EventFactory(seed=4, late_fraction=0.2)
    late = 0
    total = 0
    for _ in range(500):
        for _t, _k, event in iter_events(factory, NOW):
            total += 1
            if event.get("is_late"):
                late += 1
    assert late > 0, "no late events injected"
    # Roughly the configured fraction (loose bound — the flag is per-event).
    assert 0.10 < late / total < 0.30


def test_late_event_time_is_actually_behind_arrival():
    factory = EventFactory(seed=5, late_fraction=1.0)  # everything late
    for _ in range(20):
        for _t, _k, event in iter_events(factory, NOW):
            et = datetime.fromisoformat(event["event_time"])
            it = datetime.fromisoformat(event["ingest_time"])
            if event["is_late"]:
                assert et < it, "a 'late' event's event_time is not behind its ingest_time"
                assert (it - et).total_seconds() >= 60


def test_no_lateness_when_fraction_is_zero():
    factory = EventFactory(seed=6, late_fraction=0.0)
    for _ in range(200):
        for _t, _k, event in iter_events(factory, NOW):
            assert not event["is_late"]


# --- Coherence: streams reference each other --------------------------------

def test_delivery_status_references_a_real_order():
    """A delivery_status for an order that was never placed would make the join in
    gold meaningless. Deliveries advance real open orders."""
    factory = EventFactory(seed=7)
    placed_order_ids = set()
    delivery_order_ids = set()

    for _ in range(400):
        for topic, _k, event in iter_events(factory, NOW):
            if topic == "orders" and event["status"] != "cancelled":
                placed_order_ids.add(event["order_id"])
            elif topic == "delivery_status":
                delivery_order_ids.add(event["order_id"])

    assert delivery_order_ids, "no delivery events produced"
    assert delivery_order_ids <= placed_order_ids, \
        "a delivery references an order that was never placed"


def test_delivery_stages_advance_in_order():
    factory = EventFactory(seed=8)
    per_order_stages: dict[str, list[str]] = {}
    for _ in range(600):
        for topic, _k, event in iter_events(factory, NOW):
            if topic == "delivery_status":
                per_order_stages.setdefault(event["order_id"], []).append(event["stage"])

    stage_rank = {s: i for i, s in enumerate(DELIVERY_STAGES)}
    for order_id, stages in per_order_stages.items():
        ranks = [stage_rank[s] for s in stages]
        assert ranks == sorted(ranks), f"{order_id} stages went backwards: {stages}"


def test_inventory_falls_as_orders_consume_it():
    """The stockout detector needs inventory to actually decline under order load.
    If orders did not consume stock, there would be nothing to detect."""
    factory = EventFactory(seed=9)
    # Drive a lot of orders and watch a store/SKU's inventory.
    start_levels = dict(factory._inventory)
    for _ in range(1000):
        iter_events(factory, NOW)
    # At least some store/SKU pairs should have dropped.
    dropped = sum(1 for k, v in factory._inventory.items() if v < start_levels[k])
    assert dropped > 0, "orders consumed no inventory — stockouts would never occur"


# --- Rain surge -------------------------------------------------------------

def test_rain_surge_multiplies_order_volume_in_affected_zones():
    """The surge is the demand spike the stockout detector exists to catch."""
    surge = EventFactory(seed=10, rain_surge=True)
    assert surge._surge_zones, "rain surge selected no zones"

    normal = EventFactory(seed=10, rain_surge=False)

    def order_count(factory, ticks=500):
        n = 0
        for _ in range(ticks):
            for topic, _k, _e in iter_events(factory, NOW):
                if topic == "orders":
                    n += 1
        return n

    assert order_count(surge) > order_count(normal), \
        "rain surge did not raise order volume"


def test_surge_multiplier_is_higher_for_affected_zones():
    factory = EventFactory(seed=11, rain_surge=True)
    surge_zone = next(iter(factory._surge_zones))
    normal_zone = next(z[2] for z in __import__("order_stream").ZONES
                       if z[2] not in factory._surge_zones)
    assert factory.surge_multiplier(surge_zone) > factory.surge_multiplier(normal_zone)


# --- Determinism ------------------------------------------------------------

def test_generation_is_deterministic_for_a_seed():
    a = EventFactory(seed=99)
    b = EventFactory(seed=99)
    for _ in range(100):
        ea = iter_events(a, NOW)
        eb = iter_events(b, NOW)
        assert [e for _, _, e in ea] == [e for _, _, e in eb]
