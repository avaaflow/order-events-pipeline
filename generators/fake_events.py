"""Synthetic food-delivery order event generator."""

from __future__ import annotations

import random
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from typing import Iterator, Optional

from faker import Faker

fake = Faker("fa_IR")

CITIES = ["تهران", "مشهد", "اصفهان", "شیراز", "تبریز", "کرج"]
EVENT_TYPES = [
    "order_placed",
    "restaurant_accepted",
    "preparing",
    "picked_up",
    "delivered",
    "cancelled",
]

# Lunch & dinner peaks (hour in Asia/Tehran context, simplified as UTC+3:30 offset in timestamp)
PEAK_HOURS = {12, 13, 14, 19, 20, 21, 22}


@dataclass
class OrderEvent:
    event_id: str
    order_id: str
    user_id: int
    restaurant_id: int
    event_type: str
    timestamp: str
    amount: int
    city: str
    delivery_time_sec: Optional[int]

    def to_dict(self) -> dict:
        return asdict(self)


def _random_timestamp(days_back: int = 30) -> datetime:
    now = datetime.now()
    start = now - timedelta(days=days_back)
    base = fake.date_time_between(start_date=start, end_date=now)

    # Bias toward peak hours
    if random.random() < 0.6:
        hour = random.choice(list(PEAK_HOURS))
        base = base.replace(hour=hour, minute=random.randint(0, 59))

    return base


def _maybe_null_delivery_time(event_type: str) -> Optional[int]:
    """~5% null rate for data-quality testing."""
    if random.random() < 0.05:
        return None
    if event_type in ("delivered", "picked_up"):
        return random.randint(600, 5400)
    return None


def generate_event(
    *,
    order_id: str | None = None,
    force_duplicate: bool = False,
    seed_event: OrderEvent | None = None,
) -> OrderEvent:
    """Generate a single synthetic order lifecycle event."""
    if seed_event and force_duplicate:
        dup = OrderEvent(**seed_event.to_dict())
        dup.event_id = str(uuid.uuid4())
        return dup

    event_type = random.choices(
        EVENT_TYPES,
        weights=[30, 20, 15, 10, 20, 5],
        k=1,
    )[0]

    ts = _random_timestamp()
    return OrderEvent(
        event_id=str(uuid.uuid4()),
        order_id=order_id or str(uuid.uuid4()),
        user_id=random.randint(1, 500_000),
        restaurant_id=random.randint(1, 10_000),
        event_type=event_type,
        timestamp=ts.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3],
        amount=random.randint(50_000, 500_000),
        city=random.choice(CITIES),
        delivery_time_sec=_maybe_null_delivery_time(event_type),
    )


def generate_events(count: int) -> Iterator[OrderEvent]:
    """Yield *count* events with ~0.1% intentional duplicates."""
    duplicate_pool: list[OrderEvent] = []
    duplicate_every = max(1, count // 1000)

    for i in range(count):
        if i > 0 and i % duplicate_every == 0 and duplicate_pool:
            yield generate_event(force_duplicate=True, seed_event=random.choice(duplicate_pool))
            continue

        event = generate_event()
        duplicate_pool.append(event)
        if len(duplicate_pool) > 200:
            duplicate_pool.pop(0)
        yield event
