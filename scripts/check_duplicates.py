#!/usr/bin/env python3
"""Check duplicate events in ClickHouse (at-least-once symptom)."""

from __future__ import annotations

import sys
import urllib.request


def query(sql: str) -> str:
    url = f"http://127.0.0.1:8123/?query={urllib.request.quote(sql)}"
    with urllib.request.urlopen(url, timeout=30) as resp:
        return resp.read().decode("utf-8").strip()


def main() -> int:
    try:
        total = int(query("SELECT count() FROM orders.events_raw"))
        unique = int(query("SELECT uniqExact(event_id) FROM orders.events_raw"))
    except Exception as exc:
        print(f"ClickHouse not reachable: {exc}")
        print("Run: make up")
        return 1

    duplicates = total - unique
    rate = (duplicates / total * 100) if total else 0

    print("=== Duplicate check (at-least-once) ===")
    print(f"  Total rows:     {total}")
    print(f"  Unique events:  {unique}")
    print(f"  Duplicates:     {duplicates} ({rate:.2f}%)")

    if duplicates > 0:
        print("\n  → at-least-once delivery detected (normal after consumer crash/rebalance)")
        print("  → production fix: manual commit + dedup on event_id")
    else:
        print("\n  → no duplicates (manual commit working or clean run)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
