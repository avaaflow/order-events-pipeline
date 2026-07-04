#!/usr/bin/env python3
"""Stream synthetic order events to Kafka/Redpanda."""

from __future__ import annotations

import json
import logging
import sys
import time

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from settings import get_kafka_config, get_producer_config
from generators.fake_events import generate_events

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def create_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=5,
        linger_ms=10,
    )


def main() -> int:
    kafka_cfg = get_kafka_config()
    producer_cfg = get_producer_config()

    try:
        producer = create_producer(kafka_cfg.bootstrap_servers)
    except NoBrokersAvailable:
        logger.error(
            "Kafka broker not available at %s — run: make up",
            kafka_cfg.bootstrap_servers,
        )
        return 1

    logger.info(
        "Streaming %d events to topic '%s' at ~%d events/sec",
        producer_cfg.total_events,
        kafka_cfg.topic,
        producer_cfg.rate,
    )

    sent = 0
    interval = 1.0 / max(producer_cfg.rate, 1)
    start = time.perf_counter()

    for event in generate_events(producer_cfg.total_events):
        payload = event.to_dict()
        producer.send(
            kafka_cfg.topic,
            key=event.order_id,
            value=payload,
        )
        sent += 1

        if sent % 5000 == 0:
            elapsed = time.perf_counter() - start
            logger.info("Sent %d events (%.0f events/sec)", sent, sent / elapsed)

        time.sleep(interval)

    producer.flush()
    elapsed = time.perf_counter() - start
    logger.info("Done. Sent %d events in %.1fs (%.0f events/sec)", sent, elapsed, sent / elapsed)
    return 0


if __name__ == "__main__":
    sys.exit(main())
