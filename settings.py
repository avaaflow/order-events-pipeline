"""Shared configuration loaded from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str
    topic: str


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


@dataclass(frozen=True)
class ConsumerConfig:
    group_id: str
    batch_size: int
    metrics_port: int


def get_kafka_config() -> KafkaConfig:
    return KafkaConfig(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
        topic=os.getenv("KAFKA_TOPIC", "order_events"),
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
    )


def get_consumer_config() -> ConsumerConfig:
    return ConsumerConfig(
        group_id=os.getenv("CONSUMER_GROUP", "clickhouse-sink"),
        batch_size=int(os.getenv("CONSUMER_BATCH_SIZE", "1000")),
        metrics_port=int(os.getenv("METRICS_PORT", "8000")),
    )
