"""ClickHouse HTTP client — stdlib only (no pip install in Airflow)."""

from __future__ import annotations

import json
import urllib.error
import urllib.request

CLICKHOUSE_URL = "http://127.0.0.1:8123"


def query_json(sql: str) -> list[dict]:
    """Run a SELECT and return rows as list of dicts."""
    full_sql = sql.strip().rstrip(";") + " FORMAT JSON"
    req = urllib.request.Request(
        CLICKHOUSE_URL,
        data=full_sql.encode("utf-8"),
        method="POST",
        headers={"Content-Type": "text/plain"},
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except urllib.error.URLError as exc:
        raise RuntimeError(f"ClickHouse query failed: {exc}") from exc

    return payload.get("data", [])


def query_scalar(sql: str) -> float:
    rows = query_json(sql)
    if not rows:
        return 0.0
    return float(next(iter(rows[0].values())))
