#!/usr/bin/env python3
"""Publish a few invalid events to trigger the DLQ path in the consumer."""

from __future__ import annotations

import json
import sys

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from settings import get_kafka_config


def main() -> int:
    kafka_cfg = get_kafka_config()
    try:
        producer = KafkaProducer(
            bootstrap_servers=kafka_cfg.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
        )
    except NoBrokersAvailable:
        print("Kafka not available — run: make up", file=sys.stderr)
        return 1

    bad_events = [
        {"order_id": "missing-fields"},
        {"event_id": "not-a-uuid", "order_id": "also-bad", "amount": "nan"},
        {"event_id": "123", "order_id": "456", "user_id": 1, "restaurant_id": 2,
         "event_type": "placed", "timestamp": "bad-date", "amount": 100, "city": "Tehran"},
    ]
    for payload in bad_events:
        producer.send(kafka_cfg.topic, value=payload)
    producer.flush()
    producer.close()
    print(f"Sent {len(bad_events)} invalid events to '{kafka_cfg.topic}'")
    print(f"Watch DLQ panel — messages land in '{kafka_cfg.dlq_topic}'")
    return 0


if __name__ == "__main__":
    sys.exit(main())
