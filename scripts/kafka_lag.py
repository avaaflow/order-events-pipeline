#!/usr/bin/env python3
"""Show Kafka consumer lag per partition (without running full consumer)."""

from __future__ import annotations

import os
import sys

from dotenv import load_dotenv
from kafka import KafkaConsumer, TopicPartition

load_dotenv()

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "order_events")
GROUP = os.getenv("CONSUMER_GROUP", "clickhouse-sink")


def main() -> int:
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=BOOTSTRAP,
            group_id=GROUP,
            enable_auto_commit=False,
        )
    except Exception as exc:
        print(f"Kafka not reachable: {exc}")
        return 1

    parts = consumer.partitions_for_topic(TOPIC)
    if not parts:
        print(f"Topic '{TOPIC}' not found or empty")
        return 1

    tps = [TopicPartition(TOPIC, p) for p in sorted(parts)]
    end_offsets = consumer.end_offsets(tps)

    print(f"=== Lag for group '{GROUP}' on topic '{TOPIC}' ===")
    total_lag = 0
    for tp in tps:
        committed = consumer.committed(tp)
        end = end_offsets[tp]
        if committed is None:
            lag = end
            status = "no committed offset yet"
        else:
            lag = max(0, end - committed)
            status = f"committed={committed}"
        total_lag += lag
        print(f"  partition {tp.partition}: end={end}  lag={lag}  ({status})")

    print(f"\n  TOTAL LAG: {total_lag}")
    if total_lag > 1000:
        print("  ⚠ HIGH LAG — consumer is behind or dead")
    elif total_lag > 0:
        print("  → consumer catching up")
    else:
        print("  ✓ no lag")

    consumer.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
