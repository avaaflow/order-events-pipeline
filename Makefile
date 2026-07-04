.PHONY: help setup up down logs producer consume consume-docker demo test lint clean query \
        monitoring airflow airflow-logs status health

help:
	@echo "Order Events Pipeline (Docker — event-ticketing style)"
	@echo ""
	@echo "  make setup           - venv + pip install"
	@echo "  make up              - kafka + clickhouse only (2 containers)"
	@echo "  make consume         - consumer on host (terminal 2)"
	@echo "  make producer        - send fake events (terminal 3)"
	@echo "  make demo            - full end-to-end test"
	@echo "  make monitoring      - add Grafana + Prometheus"
	@echo "  make airflow         - add Airflow (heavy — run separately)"
	@echo "  make status          - container status"
	@echo "  make down            - stop everything"

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

demo:
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
	curl -s "http://localhost:8123/?query=SELECT%20count()%20FROM%20orders.events_raw"
	@echo ""
	@echo "=== Sample ==="
	curl -s "http://localhost:8123/?query=SELECT%20event_type,city,amount%20FROM%20orders.events_raw%20LIMIT%205%20FORMAT%20Pretty"

query:
	@echo "--- Total ---"
	curl -s "http://localhost:8123/?query=SELECT%20count()%20FROM%20orders.events_raw"
	@echo ""
	@echo "--- By type ---"
	curl -s "http://localhost:8123/?query=SELECT%20event_type,count()%20c%20FROM%20orders.events_raw%20GROUP%20BY%20event_type%20ORDER%20BY%20c%20DESC%20FORMAT%20Pretty"

test:
	.venv/bin/pytest tests/ -v

lint:
	.venv/bin/ruff check .

clean:
	docker compose --profile monitoring --profile airflow --profile app down -v
	rm -rf .venv airflow/logs __pycache__ generators/__pycache__ .consumer.pid
