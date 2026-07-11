#!/usr/bin/env python3
"""Consume order events from Kafka and sink into ClickHouse."""

from __future__ import annotations

import json
import logging
import signal
import sys
import threading
import time
import uuid
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

import clickhouse_connect
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.consumer.subscription_state import ConsumerRebalanceListener
from kafka.errors import NoBrokersAvailable
from kafka.structs import OffsetAndMetadata
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

from settings import get_clickhouse_config, get_consumer_config, get_kafka_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

EVENTS_CONSUMED = Counter("oep_events_consumed_total", "Total events consumed from Kafka")
EVENTS_INSERTED = Counter("oep_events_inserted_total", "Total events inserted into ClickHouse")
EVENTS_FAILED = Counter("oep_events_failed_total", "Total failed inserts or commits")
EVENTS_DUPLICATE_SKIPPED = Counter("oep_events_duplicate_skipped_total", "Rows skipped (dedup)")
DLQ_EVENTS = Counter("oep_dlq_events_total", "Events sent to the dead-letter topic")
BATCH_SIZE_GAUGE = Gauge("oep_last_batch_size", "Size of the last inserted batch")
CONSUMER_LAG = Gauge("oep_consumer_lag", "Total lag across assigned partitions")
REBALANCE_COUNT = Counter("oep_rebalance_total", "Partition assignment changes")
SIMULATED_DELAY = Gauge("oep_simulated_slow_ms", "Artificial processing delay per batch")
CLICKHOUSE_INSERT_DURATION = Histogram(
    "oep_clickhouse_insert_duration_seconds",
    "ClickHouse insert latency per batch",
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)
CLICKHOUSE_INSERT_LAST = Gauge(
    "oep_clickhouse_insert_last_seconds",
    "Duration of the most recent ClickHouse insert",
)
BATCH_DURATION = Histogram(
    "oep_batch_duration_seconds",
    "End-to-end batch duration (slow simulation + insert + commit)",
    buckets=(0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0),
)
BATCH_DURATION_LAST = Gauge(
    "oep_batch_duration_last_seconds",
    "Duration of the most recent batch (insert + commit)",
)

_shutdown = threading.Event()
_assigned_partitions: list[TopicPartition] = []


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


def _on_assign(partitions: list[TopicPartition]) -> None:
    global _assigned_partitions
    REBALANCE_COUNT.inc()
    _assigned_partitions = list(partitions)
    names = [f"{p.topic}:{p.partition}" for p in partitions]
    logger.warning("REBALANCE → assigned partitions: %s", names or "(none)")


def _on_revoke(partitions: list[TopicPartition]) -> None:
    names = [f"{p.topic}:{p.partition}" for p in partitions]
    logger.warning("REBALANCE → revoked partitions: %s", names or "(none)")


class _RebalanceListener(ConsumerRebalanceListener):
    def on_partitions_assigned(self, assigned: list[TopicPartition]) -> None:
        _on_assign(assigned)

    def on_partitions_revoked(self, revoked: list[TopicPartition]) -> None:
        _on_revoke(revoked)


def _update_lag(consumer: KafkaConsumer) -> None:
    total_lag = 0
    assignment = consumer.assignment()
    if not assignment:
        CONSUMER_LAG.set(0)
        return
    end_offsets = consumer.end_offsets(list(assignment))
    for tp in assignment:
        end = end_offsets.get(tp, 0)
        pos = consumer.position(tp)
        total_lag += max(0, end - pos - 1)
    CONSUMER_LAG.set(total_lag)


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


def _send_dlq(
    dlq_producer: KafkaProducer,
    dlq_topic: str,
    message: Any,
    reason: str,
) -> None:
    envelope = {
        "reason": reason,
        "topic": message.topic,
        "partition": message.partition,
        "offset": message.offset,
        "value": message.value,
    }
    dlq_producer.send(
        dlq_topic,
        key=message.key,
        value=json.dumps(envelope, ensure_ascii=False).encode("utf-8"),
    )
    dlq_producer.flush(timeout=5)
    DLQ_EVENTS.inc()
    logger.warning(
        "DLQ → %s (partition=%d offset=%d): %s",
        dlq_topic,
        message.partition,
        message.offset,
        reason,
    )


def main() -> int:
    kafka_cfg = get_kafka_config()
    ch_cfg = get_clickhouse_config()
    consumer_cfg = get_consumer_config()

    metrics_server = start_metrics_server(consumer_cfg.metrics_port)
    SIMULATED_DELAY.set(consumer_cfg.simulate_slow_ms)

    def handle_signal(signum: int, frame: Any) -> None:
        logger.info("Shutdown signal received")
        _shutdown.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    try:
        consumer = KafkaConsumer(
            bootstrap_servers=kafka_cfg.bootstrap_servers,
            group_id=consumer_cfg.group_id,
            client_id=consumer_cfg.consumer_id,
            auto_offset_reset="earliest",
            enable_auto_commit=consumer_cfg.auto_commit,
            session_timeout_ms=consumer_cfg.session_timeout_ms,
            max_poll_interval_ms=consumer_cfg.max_poll_interval_ms,
            max_poll_records=consumer_cfg.max_poll_records,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            consumer_timeout_ms=1000,
        )
        consumer.subscribe([kafka_cfg.topic], listener=_RebalanceListener())
    except NoBrokersAvailable:
        logger.error("Kafka broker not available — run: make up")
        return 1

    dlq_producer = KafkaProducer(
        bootstrap_servers=kafka_cfg.bootstrap_servers,
        value_serializer=lambda v: v,
        key_serializer=lambda k: k if k is None else k,
        acks="all",
        retries=3,
    )

    try:
        client = clickhouse_connect.get_client(
            host=ch_cfg.host,
            port=ch_cfg.port,
            database=ch_cfg.database,
        )
        client.command("SELECT 1")
    except Exception as exc:
        logger.error(
            "ClickHouse not reachable at %s:%d — run: make up",
            ch_cfg.host,
            ch_cfg.port,
        )
        logger.error("Detail: %s", exc)
        logger.error("Tip: use http://127.0.0.1:8123 (NOT localhost — proxy blocks it)")
        return 1

    columns = [
        "event_id", "order_id", "user_id", "restaurant_id",
        "event_type", "timestamp", "amount", "city", "delivery_time_sec",
    ]
    table = f"{ch_cfg.database}.{ch_cfg.table}"
    batch: list[list[Any]] = []
    pending_commit: dict[TopicPartition, int] = {}

    logger.info(
        "Consumer '%s' | group=%s | auto_commit=%s | slow_ms=%d",
        consumer_cfg.consumer_id,
        consumer_cfg.group_id,
        consumer_cfg.auto_commit,
        consumer_cfg.simulate_slow_ms,
    )
    logger.info("Consuming '%s' → %s (DLQ: %s)", kafka_cfg.topic, table, kafka_cfg.dlq_topic)

    while not _shutdown.is_set():
        polled = consumer.poll(timeout_ms=1000, max_records=consumer_cfg.batch_size)
        if not polled:
            if batch:
                _flush_batch(client, table, columns, batch, consumer, pending_commit, consumer_cfg)
                batch = []
                pending_commit = {}
            _update_lag(consumer)
            continue

        for tp, messages in polled.items():
            for message in messages:
                EVENTS_CONSUMED.inc()
                pending_commit[tp] = message.offset
                try:
                    batch.append(parse_event(message.value))
                except (KeyError, TypeError, ValueError) as exc:
                    try:
                        _send_dlq(dlq_producer, kafka_cfg.dlq_topic, message, str(exc))
                    except Exception as dlq_exc:
                        EVENTS_FAILED.inc()
                        logger.error("DLQ publish failed: %s", dlq_exc)

                if len(batch) >= consumer_cfg.batch_size:
                    _flush_batch(client, table, columns, batch, consumer, pending_commit, consumer_cfg)
                    batch = []
                    pending_commit = {}

        _update_lag(consumer)

    if batch:
        _flush_batch(client, table, columns, batch, consumer, pending_commit, consumer_cfg)

    consumer.close()
    dlq_producer.close()
    metrics_server.shutdown()
    logger.info("Consumer stopped cleanly")
    return 0


def _flush_batch(
    client: Any,
    table: str,
    columns: list[str],
    batch: list[list[Any]],
    consumer: KafkaConsumer,
    pending_commit: dict[TopicPartition, int],
    consumer_cfg: Any,
) -> None:
    if not batch:
        return

    batch_start = time.perf_counter()

    if consumer_cfg.simulate_slow_ms > 0:
        logger.warning("SIMULATE_SLOW: sleeping %dms before insert", consumer_cfg.simulate_slow_ms)
        time.sleep(consumer_cfg.simulate_slow_ms / 1000)

    try:
        insert_start = time.perf_counter()
        client.insert(table, batch, column_names=columns)
        insert_elapsed = time.perf_counter() - insert_start
        CLICKHOUSE_INSERT_DURATION.observe(insert_elapsed)
        CLICKHOUSE_INSERT_LAST.set(insert_elapsed)
        EVENTS_INSERTED.inc(len(batch))
        BATCH_SIZE_GAUGE.set(len(batch))
        logger.info("Inserted batch of %d rows", len(batch))
    except Exception as exc:
        EVENTS_FAILED.inc(len(batch))
        logger.error("ClickHouse insert failed (%d rows): %s", len(batch), exc)
        return

    if not consumer_cfg.auto_commit and pending_commit:
        try:
            offsets = {
                tp: OffsetAndMetadata(offset + 1, "")
                for tp, offset in pending_commit.items()
            }
            consumer.commit(offsets=offsets)
            logger.info(
                "Manual commit → offsets %s",
                {f"p{tp.partition}": meta.offset for tp, meta in offsets.items()},
            )
        except Exception as exc:
            EVENTS_FAILED.inc(len(batch))
            logger.error("Kafka offset commit failed (%d rows): %s", len(batch), exc)
            return

    batch_elapsed = time.perf_counter() - batch_start
    BATCH_DURATION.observe(batch_elapsed)
    BATCH_DURATION_LAST.set(batch_elapsed)


if __name__ == "__main__":
    sys.exit(main())
