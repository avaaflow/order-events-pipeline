#!/usr/bin/env bash
# Health check for all services
set -e

CURL="curl --noproxy '*' -sf"

echo "=== Docker containers ==="
docker compose ps -a 2>/dev/null || true

echo ""
echo "=== Kafka ==="
if (echo >/dev/tcp/127.0.0.1/9092) 2>/dev/null || nc -z 127.0.0.1 9092 2>/dev/null; then
  echo "  OK — 127.0.0.1:9092"
else
  echo "  FAIL — kafka not reachable"
fi

echo ""
echo "=== ClickHouse ==="
if $CURL http://127.0.0.1:8123/ping >/dev/null; then
  COUNT=$(curl --noproxy '*' -s "http://127.0.0.1:8123/?query=SELECT%20count()%20FROM%20orders.events_raw")
  echo "  OK — 127.0.0.1:8123 (rows: ${COUNT})"
else
  echo "  FAIL — clickhouse not reachable"
fi

echo ""
echo "=== Consumer metrics ==="
if curl -sf http://127.0.0.1:8000/health >/dev/null 2>&1; then
  echo "  OK — 127.0.0.1:8000"
else
  echo "  SKIP — consumer not running (make consume)"
fi

echo ""
echo "=== Prometheus targets ==="
if curl -sf http://127.0.0.1:9090/-/healthy >/dev/null 2>&1; then
  echo "  Prometheus OK"
  if curl -sf http://127.0.0.1:9090/api/v1/targets 2>/dev/null | grep -q '"health":"up".*clickhouse_sink\|clickhouse_sink.*"health":"up"'; then
    echo "  clickhouse_sink target: UP"
  else
    echo "  clickhouse_sink target: DOWN or no data — run: make consume"
  fi
else
  echo "  SKIP — run: make monitoring"
fi
