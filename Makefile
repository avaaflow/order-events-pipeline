.PHONY: help setup up down logs producer consume consume-docker demo test lint clean query \
        monitoring airflow airflow-logs status health labs lag duplicates lab-dlq \
        consume-slow consume-2 consume-fast-commit lab-producer-burst

help:
	@echo "Order Events Pipeline"
	@echo ""
	@echo "  Core:  make up | consume | producer | query | monitoring"
	@echo "  Labs:  make labs | lag | duplicates | consume-slow | consume-2"

# Bypass corporate proxy for local Docker services
export NO_PROXY := 127.0.0.1,localhost
export no_proxy := 127.0.0.1,localhost

CURL := curl --noproxy '*' -s
CH_URL := http://127.0.0.1:8123

setup:
	python3 -m venv .venv
	.venv/bin/pip install -r requirements.txt
	cp -n .env.example .env 2>/dev/null || true

# Step-by-step start (like ticketing — avoids proxy spike)
up:
	@echo "Starting kafka..."
	docker compose up -d kafka
	@echo "Waiting for kafka..."
	@sleep 20
	@echo "Starting clickhouse..."
	docker compose up -d clickhouse
	@sleep 5
	@$(MAKE) status
	@echo ""
	@echo "  Kafka       → 127.0.0.1:9092"
	@echo "  ClickHouse  → http://127.0.0.1:8123"
	@echo ""
	@echo "  Terminal 2: make consume"
	@echo "  Terminal 3: make producer"

monitoring:
	docker compose --profile monitoring up -d
	@echo ""
	@echo "  Grafana    → http://127.0.0.1:3000 (admin/admin)"
	@echo "  Prometheus → http://127.0.0.1:9090"
	@echo ""
	@echo "  IMPORTANT: make consume must be running in another terminal!"
	@echo "  Then run make producer to generate metrics."
	@echo "  Check Prometheus targets: http://127.0.0.1:9090/targets"

monitoring-reload:
	docker compose --profile monitoring restart prometheus grafana
	@echo "Reloaded. Restart consumer too: Ctrl+C then make consume"

airflow:
	docker compose --profile airflow down 2>/dev/null || true
	rm -rf airflow/logs
	docker compose --profile airflow up -d
	@echo ""
	@echo "  Airflow → http://127.0.0.1:8080 (admin/admin)"
	@echo "  Wait ~90s, then: make airflow-logs"

consume-docker:
	docker compose --profile app up -d --build consumer

down:
	docker compose --profile monitoring --profile airflow --profile app down

status:
	@docker compose ps -a

health:
	@bash scripts/check_health.sh

airflow-logs:
	docker compose logs -f airflow-webserver airflow-scheduler

logs:
	docker compose logs -f kafka clickhouse

producer:
	PYTHONPATH=. .venv/bin/python producers/stream_orders.py

consume:
	PYTHONPATH=. .venv/bin/python consumers/clickhouse_sink.py

consume-slow:
	SIMULATE_SLOW_MS=3000 CONSUMER_BATCH_SIZE=500 PYTHONPATH=. .venv/bin/python consumers/clickhouse_sink.py

consume-2:
	CONSUMER_ID=consumer-2 METRICS_PORT=8001 PYTHONPATH=. .venv/bin/python consumers/clickhouse_sink.py

consume-fast-commit:
	CONSUMER_AUTO_COMMIT=true PYTHONPATH=. .venv/bin/python consumers/clickhouse_sink.py

lab-producer-burst:
	PRODUCER_TOTAL_EVENTS=50000 PRODUCER_RATE=8000 PYTHONPATH=. .venv/bin/python producers/stream_orders.py

lab-dlq:
	PYTHONPATH=. .venv/bin/python scripts/lab_dlq.py

lag:
	PYTHONPATH=. .venv/bin/python scripts/kafka_lag.py

duplicates:
	PYTHONPATH=. .venv/bin/python scripts/check_duplicates.py

labs:
	@echo ""
	@echo "=== Kafka Challenge Labs ==="
	@echo ""
	@echo "Lab 1 — Consumer crash + lag spike"
	@echo "  T1: make monitoring && make consume"
	@echo "  T2: make lab-producer-burst"
	@echo "  T1: Ctrl+C (kill consumer)"
	@echo "  T2: make lab-producer-burst"
	@echo "  T3: make lag          → lag بالا"
	@echo "  T1: make consume      → lag پایین"
	@echo "  Grafana: Consumer Lag panel"
	@echo ""
	@echo "Lab 2 — Slow consumer (producer faster than consumer)"
	@echo "  T1: make consume-slow"
	@echo "  T2: make lab-producer-burst"
	@echo "  Grafana: lag بالا می‌رود"
	@echo ""
	@echo "Lab 3 — Rebalance (two consumers)"
	@echo "  T1: make consume"
	@echo "  T2: make consume-2"
	@echo "  Log: REBALANCE → assigned partitions"
	@echo ""
	@echo "Lab 4 — At-least-once duplicates"
	@echo "  T1: make consume-fast-commit"
	@echo "  T2: make lab-producer-burst"
	@echo "  T1: Ctrl+C وسط producer"
	@echo "  T1: make consume-fast-commit"
	@echo "  make duplicates"
	@echo ""
	@echo "Docs: docs/KAFKA_LABS.md"
	@echo ""
	$(MAKE) up
	@echo "=== Starting consumer (background) ==="
	@PYTHONPATH=. .venv/bin/python consumers/clickhouse_sink.py & echo $$! > .consumer.pid
	@sleep 8
	@echo "=== Streaming 10000 events ==="
	PRODUCER_TOTAL_EVENTS=10000 PRODUCER_RATE=2000 PYTHONPATH=. .venv/bin/python producers/stream_orders.py
	@sleep 10
	@kill `cat .consumer.pid` 2>/dev/null || true
	@rm -f .consumer.pid
	@echo "=== Row count ==="
	$(CURL) "$(CH_URL)/?query=SELECT%20count()%20FROM%20orders.events_raw"
	@echo ""
	@echo "=== Sample ==="
	$(CURL) "$(CH_URL)/?query=SELECT%20event_type,city,amount%20FROM%20orders.events_raw%20LIMIT%205%20FORMAT%20Pretty"

query:
	@echo "--- Total ---"
	$(CURL) "$(CH_URL)/?query=SELECT%20count()%20FROM%20orders.events_raw"
	@echo ""
	@echo "--- By type ---"
	$(CURL) "$(CH_URL)/?query=SELECT%20event_type,count()%20c%20FROM%20orders.events_raw%20GROUP%20BY%20event_type%20ORDER%20BY%20c%20DESC%20FORMAT%20Pretty"

test:
	.venv/bin/pytest tests/ -v

lint:
	.venv/bin/ruff check .

clean:
	docker compose --profile monitoring --profile airflow --profile app down -v
	rm -rf .venv airflow/logs __pycache__ generators/__pycache__ .consumer.pid
