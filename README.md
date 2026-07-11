<div dir="rtl">

# Order Events Pipeline


## معماری

```
دیتای فیک → Producer → Kafka → Consumer → ClickHouse → Airflow → Grafana
```

## شروع سریع

```bash
make setup          # یکبار
make up             # kafka + clickhouse (host network)
make consume        # ترمینال ۲
make producer       # ترمینال ۳
make query          # چک داده
```

## شبکه — host mode (بدون bridge)

همه سرویس‌ها با `network_mode: host` اجرا می‌شوند:

| سرویس | آدرس |
|--------|------|
| Kafka | `127.0.0.1:9092` |
| ClickHouse | `http://127.0.0.1:8123` |
| Grafana | `http://127.0.0.1:3000` |
| Airflow | `http://127.0.0.1:8080` |
| Prometheus | `http://127.0.0.1:9090` |
| Metrics | `http://127.0.0.1:8000/metrics` |

**مزیت:** شبکه `data_ops_default` ساخته نمی‌شود → پروکسی/VPN قطع نمی‌شود.

## دستورات

| دستور | کار |
|--------|-----|
| `make up` | kafka + clickhouse |
| `make monitoring` | + grafana + prometheus |
| `make airflow` | + airflow + postgres |
| `make demo` | تست end-to-end |
| `make down` | خاموش کردن |

## استک

| لایه | ابزار |
|------|--------|
| Streaming | Apache Kafka 3.8 |
| OLAP | ClickHouse 24 |
| Orchestration | Apache Airflow 2.8 |
| Monitoring | Prometheus + Grafana |
| Language | Python 3.11 |

## ساختار

```
├── generators/       # دیتای فیک
├── producers/        # Kafka producer
├── consumers/        # ClickHouse sink
├── clickhouse/       # schema SQL
├── airflow/dags/     # DAGs
├── monitoring/       # Grafana + Prometheus
└── docker-compose.yml
```

</div>
