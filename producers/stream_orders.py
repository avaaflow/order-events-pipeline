#!/usr/bin/env python3
"""Stream synthetic order events to Kafka/Redpanda."""

from __future__ import annotations

import json
import logging
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from prometheus_client import CONTENT_TYPE_LATEST, Counter, generate_latest

from settings import get_kafka_config, get_producer_config
from generators.fake_events import generate_events

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

ORDERS_PRODUCED = Counter("oep_orders_produced_total", "Total order events produced to Kafka")


class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path == "/metrics":
            self.send_response(200)
            self.send_header("Content-Type", CONTENT_TYPE_LATEST)
            self.end_headers()
            self.wfile.write(generate_latest())
        elif self.path == "/health":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format: str, *args: Any) -> None:
        return


def start_metrics_server(port: int) -> HTTPServer:
    server = HTTPServer(("127.0.0.1", port), MetricsHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logger.info("Metrics → http://127.0.0.1:%d/metrics", port)
    return server


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
    metrics_server = start_metrics_server(producer_cfg.metrics_port)

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
        ORDERS_PRODUCED.inc()
        sent += 1

        if sent % 5000 == 0:
            elapsed = time.perf_counter() - start
            logger.info("Sent %d events (%.0f events/sec)", sent, sent / elapsed)

        time.sleep(interval)

    producer.flush()
    producer.close()
    elapsed = time.perf_counter() - start
    logger.info("Done. Sent %d events in %.1fs (%.0f events/sec)", sent, elapsed, sent / elapsed)
    metrics_server.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
