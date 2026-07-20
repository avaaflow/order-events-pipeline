"""Shared configuration loaded from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()

# Corporate proxy (Squid) breaks localhost — bypass for local services
_LOCAL_NO_PROXY = "127.0.0.1,localhost"
for _key in ("NO_PROXY", "no_proxy"):
    _existing = os.environ.get(_key, "")
    if _LOCAL_NO_PROXY not in _existing:
        os.environ[_key] = f"{_existing},{_LOCAL_NO_PROXY}".strip(",")


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str
    topic: str
    dlq_topic: str


@dataclass(frozen=True)
class ClickHouseConfig:
    host: str
    port: int
    database: str
    table: str


@dataclass(frozen=True)
class ProducerConfig:
    rate: int
    total_events: int
    metrics_port: int


@dataclass(frozen=True)
class ConsumerConfig:
    group_id: str
    batch_size: int
    metrics_port: int
    consumer_id: str
    auto_commit: bool
    simulate_slow_ms: int
    session_timeout_ms: int
    max_poll_interval_ms: int
    max_poll_records: int
    insert_max_retries: int
    insert_retry_base_ms: int


def get_kafka_config() -> KafkaConfig:
    return KafkaConfig(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
        topic=os.getenv("KAFKA_TOPIC", "order_events"),
        dlq_topic=os.getenv("KAFKA_DLQ_TOPIC", "order_events_dlq"),
    )


def get_clickhouse_config() -> ClickHouseConfig:
    return ClickHouseConfig(
        host=os.getenv("CLICKHOUSE_HOST", "127.0.0.1"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        database=os.getenv("CLICKHOUSE_DATABASE", "orders"),
        table=os.getenv("CLICKHOUSE_TABLE", "events_raw"),
    )


def get_producer_config() -> ProducerConfig:
    return ProducerConfig(
        rate=int(os.getenv("PRODUCER_RATE", "500")),
        total_events=int(os.getenv("PRODUCER_TOTAL_EVENTS", "100000")),
        metrics_port=int(os.getenv("PRODUCER_METRICS_PORT", "8010")),
    )


def get_consumer_config() -> ConsumerConfig:
    return ConsumerConfig(
        group_id=os.getenv("CONSUMER_GROUP", "clickhouse-sink"),
        batch_size=int(os.getenv("CONSUMER_BATCH_SIZE", "1000")),
        metrics_port=int(os.getenv("METRICS_PORT", "8000")),
        consumer_id=os.getenv("CONSUMER_ID", "consumer-1"),
        auto_commit=os.getenv("CONSUMER_AUTO_COMMIT", "false").lower() == "true",
        simulate_slow_ms=int(os.getenv("SIMULATE_SLOW_MS", "0")),
        session_timeout_ms=int(os.getenv("SESSION_TIMEOUT_MS", "45000")),
        max_poll_interval_ms=int(os.getenv("MAX_POLL_INTERVAL_MS", "300000")),
        max_poll_records=int(os.getenv("MAX_POLL_RECORDS", "500")),
        insert_max_retries=int(os.getenv("INSERT_MAX_RETRIES", "3")),
        insert_retry_base_ms=int(os.getenv("INSERT_RETRY_BASE_MS", "500")),
    )
