#!/usr/bin/env python3
"""Consume order events from Kafka and sink into ClickHouse."""

from __future__ import annotations

import json
import logging
import signal
import sys
import threading
import uuid
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

import clickhouse_connect
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, generate_latest

from settings import get_clickhouse_config, get_consumer_config, get_kafka_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

EVENTS_CONSUMED = Counter("oep_events_consumed_total", "Total events consumed from Kafka")
EVENTS_INSERTED = Counter("oep_events_inserted_total", "Total events inserted into ClickHouse")
EVENTS_FAILED = Counter("oep_events_failed_total", "Total failed event inserts")
BATCH_SIZE_GAUGE = Gauge("oep_last_batch_size", "Size of the last inserted batch")
CONSUMER_LAG = Gauge("oep_consumer_lag", "Approximate consumer lag (messages behind)")

_shutdown = threading.Event()


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
    logger.info("Metrics → http://127.0.0.1:%d/metrics (while consumer is running)", port)
    return server


def _parse_timestamp(value: str) -> datetime:
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    raise ValueError(f"Unsupported timestamp format: {value}")


def parse_event(raw: dict[str, Any]) -> list[Any]:
    return [
        uuid.UUID(raw["event_id"]),
        uuid.UUID(raw["order_id"]),
        int(raw["user_id"]),
        int(raw["restaurant_id"]),
        raw["event_type"],
        _parse_timestamp(raw["timestamp"]),
        int(raw["amount"]),
        raw["city"],
        raw.get("delivery_time_sec"),
    ]


def main() -> int:
    kafka_cfg = get_kafka_config()
    ch_cfg = get_clickhouse_config()
    consumer_cfg = get_consumer_config()

    metrics_server = start_metrics_server(consumer_cfg.metrics_port)

    def handle_signal(signum: int, frame: Any) -> None:
        logger.info("Shutdown signal received")
        _shutdown.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    try:
        consumer = KafkaConsumer(
            kafka_cfg.topic,
            bootstrap_servers=kafka_cfg.bootstrap_servers,
            group_id=consumer_cfg.group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            consumer_timeout_ms=1000,
        )
    except NoBrokersAvailable:
        logger.error("Kafka broker not available — run: make up")
        return 1

    client = clickhouse_connect.get_client(
        host=ch_cfg.host,
        port=ch_cfg.port,
        database=ch_cfg.database,
    )

    columns = [
        "event_id",
        "order_id",
        "user_id",
        "restaurant_id",
        "event_type",
        "timestamp",
        "amount",
        "city",
        "delivery_time_sec",
    ]
    table = f"{ch_cfg.database}.{ch_cfg.table}"
    batch: list[list[Any]] = []

    logger.info("Consuming topic '%s' → %s", kafka_cfg.topic, table)

    while not _shutdown.is_set():
        polled = consumer.poll(timeout_ms=1000, max_records=consumer_cfg.batch_size)
        if not polled:
            if batch:
                _flush_batch(client, table, columns, batch)
                batch = []
            continue

        for tp, messages in polled.items():
            CONSUMER_LAG.set(sum(consumer.end_offsets([tp]).values()) - messages[-1].offset)

            for message in messages:
                EVENTS_CONSUMED.inc()
                try:
                    batch.append(parse_event(message.value))
                except (KeyError, TypeError, ValueError) as exc:
                    EVENTS_FAILED.inc()
                    logger.warning("Invalid event skipped: %s", exc)

                if len(batch) >= consumer_cfg.batch_size:
                    _flush_batch(client, table, columns, batch)
                    batch = []

    if batch:
        _flush_batch(client, table, columns, batch)

    consumer.close()
    metrics_server.shutdown()
    logger.info("Consumer stopped cleanly")
    return 0


def _flush_batch(client: Any, table: str, columns: list[str], batch: list[list[Any]]) -> None:
    try:
        client.insert(table, batch, column_names=columns)
        EVENTS_INSERTED.inc(len(batch))
        BATCH_SIZE_GAUGE.set(len(batch))
        logger.info("Inserted batch of %d rows into %s", len(batch), table)
    except Exception as exc:
        EVENTS_FAILED.inc(len(batch))
        logger.error("ClickHouse insert failed (%d rows): %s", len(batch), exc)


if __name__ == "__main__":
    sys.exit(main())
