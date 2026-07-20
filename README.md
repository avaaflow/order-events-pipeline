# Order Events Pipeline

A DataOps portfolio project that streams synthetic order events through **Kafka** into **ClickHouse**, with a reliable consumer, observability, and batch orchestration.

```
Generator → Producer → Kafka → Consumer → ClickHouse
                              ↓
                         Prometheus → Grafana
                              ↓
                           Airflow
```

## What was built

| Area | Implementation |
|------|----------------|
| **Streaming** | Kafka producer (keyed by `order_id`, `acks=all`) and consumer group sink |
| **OLAP sink** | Batch inserts into ClickHouse (MergeTree) |
| **Reliability** | Manual offset commit, insert retries with backoff, dead-letter queue (DLQ), commit only after success |
| **Observability** | Prometheus metrics + Grafana dashboard (throughput, lag, latency, failures, DLQ) |
| **Orchestration** | Airflow DAGs for daily summaries and data-quality checks |
| **Platform** | Docker Compose for local stack; Kubernetes manifests for consumer + Kafka (KRaft) |

## Architecture

1. **Generator** creates food-delivery–style order events (placed, paid, delivered, …).  
2. **Producer** publishes to Kafka topic `order_events`.  
3. **Consumer** polls Kafka, validates events, batches rows, inserts into ClickHouse.  
4. On transient ClickHouse errors → **retry** (up to 3, exponential backoff).  
5. On permanent failure or bad payload → event goes to **DLQ**; offsets advance only after insert or DLQ ack.  
6. **Prometheus** scrapes consumer metrics; **Grafana** visualizes pipeline health.  
7. **Airflow** runs scheduled jobs against ClickHouse for aggregates and quality checks.

## Reliability design

- **At-least-once** delivery via manual Kafka commits (auto-commit off by default).  
- **Retries** around ClickHouse batch inserts.  
- **DLQ** for poison messages and exhausted insert failures.  
- **Safe commit rule:** never commit an offset unless the event was written to ClickHouse or successfully published to the DLQ.

## Observability

Exposed metrics include:

- Events consumed / inserted / failed  
- Consumer lag  
- ClickHouse insert latency and batch duration  
- DLQ volume  
- Rebalance count  

Grafana dashboard: *Order Events Pipeline*.

## Kubernetes

Manifests under `k8s/`:

- **Deployment** — ClickHouse sink consumer (scalable replicas, metrics Service)  
- **StatefulSet** — single-node Kafka in KRaft mode (headless Service for stable DNS)  
- **ConfigMaps** — runtime configuration without hardcoded env in Pods  

Demonstrates Deployment vs StatefulSet, headless Services, and config externalization.

## Project layout

```text
generators/     Synthetic event generator
producers/      Kafka producer
consumers/      ClickHouse sink (retry, DLQ, metrics)
clickhouse/     Schema / init SQL
airflow/dags/   Batch & data-quality DAGs
monitoring/     Prometheus + Grafana provisioning
k8s/            Kubernetes manifests
scripts/        Lab helpers (lag, DLQ, health)
tests/          Unit tests (including sink reliability)
```

## How to run (summary)

```bash
make setup && make up
make consume    # separate terminal
make producer   # separate terminal
make monitoring # optional
make test
```

Defaults: Kafka `127.0.0.1:9092`, ClickHouse `http://127.0.0.1:8123`, metrics `:8000`, Grafana `:3000`.

## Skills demonstrated

- Designing a Kafka → OLAP streaming pipeline  
- Consumer groups, lag, and rebalancing  
- At-least-once semantics, retries, and dead-letter queues  
- Prometheus metric types (counter / gauge / histogram)  
- Packaging workloads for Kubernetes (Deployments, StatefulSets, Services, ConfigMaps)
