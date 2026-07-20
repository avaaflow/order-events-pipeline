"""Reliability tests for ClickHouse sink: retry, DLQ, safe offset commit."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

import pytest
from kafka import TopicPartition

from consumers.clickhouse_sink import (
    _commit_offsets,
    _insert_with_retry,
    _send_dlq,
    flush_batch,
)


def _consumer_cfg(**overrides):
    base = dict(
        auto_commit=False,
        simulate_slow_ms=0,
        insert_max_retries=3,
        insert_retry_base_ms=10,
    )
    base.update(overrides)
    return SimpleNamespace(**base)


def _tp(partition: int = 0) -> TopicPartition:
    return TopicPartition("order_events", partition)


class TestInsertRetry:
    def test_temporary_failure_then_success(self):
        client = MagicMock()
        client.insert.side_effect = [
            ConnectionError("connection refused"),
            ConnectionError("connection refused"),
            None,
        ]

        with patch("consumers.clickhouse_sink.time.sleep") as sleep_mock:
            ok, attempts, err = _insert_with_retry(
                client,
                "orders.events_raw",
                ["event_id"],
                [["row1"]],
                max_retries=3,
                retry_base_ms=100,
            )

        assert ok is True
        assert attempts == 3
        assert err is None
        assert client.insert.call_count == 3
        # exponential: 100ms, 200ms
        assert sleep_mock.call_count == 2
        assert sleep_mock.call_args_list == [call(0.1), call(0.2)]

    def test_permanent_failure_exhausts_retries(self):
        client = MagicMock()
        client.insert.side_effect = RuntimeError("disk full")

        with patch("consumers.clickhouse_sink.time.sleep"):
            ok, attempts, err = _insert_with_retry(
                client,
                "orders.events_raw",
                ["event_id"],
                [["row1"]],
                max_retries=3,
                retry_base_ms=10,
            )

        assert ok is False
        assert attempts == 3
        assert "disk full" in (err or "")
        assert client.insert.call_count == 3


class TestFlushBatch:
    def test_temporary_failure_then_success_commits_once(self):
        client = MagicMock()
        client.insert.side_effect = [ConnectionError("blip"), None]
        consumer = MagicMock()
        dlq_producer = MagicMock()
        pending = {_tp(): 10}
        originals = [
            {
                "original_event": {"event_id": "a"},
                "key": b"k",
                "topic": "order_events",
                "partition": 0,
                "offset": 10,
            }
        ]
        cfg = _consumer_cfg()

        with patch("consumers.clickhouse_sink.time.sleep"):
            handled = flush_batch(
                client,
                "orders.events_raw",
                ["event_id"],
                [["row"]],
                originals,
                consumer,
                pending,
                cfg,
                dlq_producer,
                "order_events_dlq",
            )

        assert handled is True
        assert client.insert.call_count == 2
        consumer.commit.assert_called_once()
        dlq_producer.send.assert_not_called()

    def test_permanent_failure_goes_to_dlq_then_commits(self):
        client = MagicMock()
        client.insert.side_effect = RuntimeError("permanent")
        consumer = MagicMock()
        future = MagicMock()
        future.get.return_value = None
        dlq_producer = MagicMock()
        dlq_producer.send.return_value = future

        pending = {_tp(): 5}
        originals = [
            {
                "original_event": {"event_id": "bad-1", "amount": 1},
                "key": b"k1",
                "topic": "order_events",
                "partition": 0,
                "offset": 4,
            },
            {
                "original_event": {"event_id": "bad-2", "amount": 2},
                "key": b"k2",
                "topic": "order_events",
                "partition": 0,
                "offset": 5,
            },
        ]
        cfg = _consumer_cfg()

        with patch("consumers.clickhouse_sink.time.sleep"):
            handled = flush_batch(
                client,
                "orders.events_raw",
                ["event_id"],
                [["r1"], ["r2"]],
                originals,
                consumer,
                pending,
                cfg,
                dlq_producer,
                "order_events_dlq",
            )

        assert handled is True
        assert client.insert.call_count == 3
        assert dlq_producer.send.call_count == 2
        consumer.commit.assert_called_once()

        # DLQ payload shape
        import json

        payload = json.loads(dlq_producer.send.call_args_list[0].kwargs["value"].decode())
        assert payload["original_event"]["event_id"] == "bad-1"
        assert "permanent" in payload["error"]
        assert payload["retry_count"] == 3
        assert "timestamp" in payload

    def test_offset_not_committed_before_successful_handling(self):
        client = MagicMock()
        client.insert.side_effect = RuntimeError("down")
        consumer = MagicMock()
        dlq_producer = MagicMock()
        dlq_producer.send.side_effect = RuntimeError("kafka dlq down")

        pending = {_tp(): 7}
        originals = [
            {
                "original_event": {"event_id": "x"},
                "key": None,
                "topic": "order_events",
                "partition": 0,
                "offset": 7,
            }
        ]
        cfg = _consumer_cfg()

        with patch("consumers.clickhouse_sink.time.sleep"):
            handled = flush_batch(
                client,
                "orders.events_raw",
                ["event_id"],
                [["row"]],
                originals,
                consumer,
                pending,
                cfg,
                dlq_producer,
                "order_events_dlq",
            )

        assert handled is False
        consumer.commit.assert_not_called()
        # batch/pending left to caller — we only assert commit blocked
        assert pending[_tp()] == 7

    def test_insert_ok_but_commit_fails_keeps_batch(self):
        client = MagicMock()
        consumer = MagicMock()
        consumer.commit.side_effect = RuntimeError("coordinator down")
        dlq_producer = MagicMock()
        pending = {_tp(): 1}
        cfg = _consumer_cfg()

        handled = flush_batch(
            client,
            "orders.events_raw",
            ["event_id"],
            [["row"]],
            [{"original_event": {}, "offset": 1, "partition": 0, "topic": "order_events"}],
            consumer,
            pending,
            cfg,
            dlq_producer,
            "order_events_dlq",
        )

        assert handled is False
        consumer.commit.assert_called_once()
        dlq_producer.send.assert_not_called()


class TestCommitAndDlqHelpers:
    def test_commit_offsets_manual(self):
        consumer = MagicMock()
        pending = {_tp(0): 9, _tp(1): 3}
        assert _commit_offsets(consumer, pending, auto_commit=False) is True
        consumer.commit.assert_called_once()
        offsets = consumer.commit.call_args.kwargs["offsets"]
        assert offsets[_tp(0)].offset == 10
        assert offsets[_tp(1)].offset == 4

    def test_commit_skipped_when_auto_commit(self):
        consumer = MagicMock()
        assert _commit_offsets(consumer, {_tp(): 1}, auto_commit=True) is True
        consumer.commit.assert_not_called()

    def test_send_dlq_failure_returns_false(self):
        producer = MagicMock()
        producer.send.side_effect = RuntimeError("broker down")
        ok = _send_dlq(
            producer,
            "order_events_dlq",
            original_event={"a": 1},
            error="parse",
            retry_count=0,
        )
        assert ok is False
