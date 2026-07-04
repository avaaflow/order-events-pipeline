<div dir="rtl">

# راهنمای کامل — Order Events Pipeline

پروژه DataOps برای آماده‌سازی مصاحبه Senior DataOps در اسنپ‌فود.

---

## معماری کلی

```
generators/fake_events.py     ← دیتای فیک (سفارش غذا)
        ↓
producers/stream_orders.py    ← Producer (Python)
        ↓
Kafka (apache/kafka)          ← Message Broker
        ↓
consumers/clickhouse_sink.py  ← Consumer (Python)
        ↓
ClickHouse (OLAP)             ← ذخیره analytics
        ↓
airflow/dags/                 ← aggregate + quality check
        ↓
Prometheus + Grafana          ← monitoring (اختیاری)
```

---

# بخش ۱ — چرا `network_mode: host`؟

## قبل (مشکل‌دار)

```
Docker ساخت:  Network data_ops_default (bridge)
                    ↓
         قوانین iptables عوض شد
                    ↓
         پروکسی/VPN قطع شد ❌
```

هر container داخل یک شبکه مجازی جدا بود:
- Kafka آدرس داخلی: `kafka:9092`
- ClickHouse آدرس داخلی: `clickhouse:8123`
- Producer روی host باید از `localhost:19092` وصل می‌شد

## بعد (fix فعلی)

```yaml
kafka:
  network_mode: host    # ← کلید اصلی

clickhouse:
  network_mode: host
```

**یعنی چی؟**
- Container دیگر شبکه مجازی ندارد
- مستقیم از شبکه لپ‌تاپ استفاده می‌کند
- `data_ops_default` ساخته **نمی‌شود**
- پروکسی/VPN دست نخورده می‌ماند ✅

## چی حذف / عوض شد؟

| قبل | بعد | دلیل |
|-----|-----|------|
| Redpanda (`docker.redpanda.com`) | Apache Kafka (`docker.io`) | registry مشکل‌دار |
| پورت `19092` | پورت `9092` | استاندارد Kafka |
| `ports: "9092:9092"` | بدون `ports` | host mode — مستقیم bind |
| آدرس `clickhouse:8123` | `127.0.0.1:8123` | بدون DNS داخلی Docker |
| `_PIP_ADDITIONAL_REQUIREMENTS` | حذف شد | pip هر بار اینترنت می‌گرفت |
| همه سرویس با `make up` | فقط kafka + clickhouse | سبک‌تر مثل event-ticketing |
| `docker-compose.core.yml` | حذف — یک فایل | ساده‌تر |
| Grafana analytics | خاموش | درخواست خارجی نمی‌زند |
| Image از `docker.io/python` | Nexus شرکت | مثل event-ticketing |

## سرویس‌های اختیاری (profile)

| Profile | سرویس‌ها | دستور |
|---------|----------|--------|
| پیش‌فرض | kafka + clickhouse | `make up` |
| `monitoring` | + prometheus + grafana | `make monitoring` |
| `airflow` | + postgres + airflow | `make airflow` |
| `app` | consumer در Docker | `make consume-docker` |

**توصیه:** consumer را روی host اجرا کن (`make consume`) — مثل `manage.py runserver` در event-ticketing.

---

# بخش ۲ — راه‌اندازی مرحله‌به‌مرحله

## مرحله ۰ — پیش‌نیاز

```bash
cd /home/dev/PycharmProjects/Data_Ops
make setup
```

چک:
```bash
.venv/bin/python --version   # 3.11+
docker compose version
```

---

## مرحله ۱ — بالا آوردن زیرساخت

```bash
make down
docker network rm data_ops_default 2>/dev/null || true
make up
```

**چی می‌شود:**
1. Kafka بالا می‌آید → `127.0.0.1:9092`
2. ۲۰ ثانیه صبر
3. ClickHouse بالا می‌آید → `127.0.0.1:8123`

**چک:**
```bash
make status
curl http://127.0.0.1:8123/ping          # → Ok.
docker network ls | grep data_ops         # → نباید چیزی ببینی
```

**باید بفهمی:**
- چرا مرحله‌ای start می‌کنیم → جلوگیری از spike شبکه
- host mode یعنی container = process روی لپ‌تاپ

---

## مرحله ۲ — Consumer (ترمینال ۲)

```bash
make consume
```

**چی می‌کند:**
- به topic `order_events` گوش می‌دهد
- batch ۱۰۰۰ تایی در ClickHouse insert می‌کند
- metrics روی http://127.0.0.1:8000/metrics

**لاگ نمونه:**
```
Consuming topic 'order_events' → orders.events_raw
Inserted batch of 1000 rows
```

**باید بفهمی:**
- consumer group چیست
- چرا batch insert → سریع‌تر
- offset و lag

---

## مرحله ۳ — Producer (ترمینال ۳)

```bash
make producer
```

یا با حجم بیشتر:
```bash
PRODUCER_TOTAL_EVENTS=50000 PRODUCER_RATE=2000 make producer
```

**چک در ترمینال consumer:** batch insert می‌بینی

**چک داده:**
```bash
make query
```

---

## مرحله ۴ — دمو یک‌خطی

```bash
make demo
```

همه مراحل ۱–۳ خودکار — برای تست سریع.

---

## مرحله ۵ — Monitoring (وقتی core stable است)

```bash
make monitoring
```

| سرویس | آدرس | ورود |
|--------|------|------|
| Grafana | http://127.0.0.1:3000 | admin / admin |
| Prometheus | http://127.0.0.1:9090 | — |
| Metrics | http://127.0.0.1:8000/metrics | — |

**در Grafana:** Dashboard → Order Events Pipeline

**تمرین incident:**
```bash
# consumer را kill کن (Ctrl+C در ترمینال ۲)
make producer   # event بفرست
# دوباره make consume
# در Grafana lag را ببین
```

---

## مرحله ۶ — Airflow (روز ۳+)

```bash
make airflow
# ۹۰ ثانیه صبر
make airflow-logs
```

http://127.0.0.1:8080 — admin / admin

**DAGها:**
| DAG | کار |
|-----|-----|
| `daily_summary` | aggregate دیروز → PostgreSQL |
| `data_quality` | null rate + duplicate check |

**دستی trigger کن** (دکمه ▶ در UI)

---

## مرحله ۷ — یادگیری کد

| فایل | بخوان برای |
|------|-----------|
| `generators/fake_events.py` | schema دیتا، null/duplicate عمدی |
| `producers/stream_orders.py` | Kafka producer، partitioning |
| `consumers/clickhouse_sink.py` | consumer، batch، prometheus |
| `clickhouse/init.sql` | MergeTree، materialized view |
| `airflow/dags/data_quality.py` | data quality logic |

---

## مرحله ۸ — GitHub + مصاحبه

```bash
git add -A
git commit -m "Order events pipeline: Kafka, ClickHouse, Airflow, Grafana (host network)"
git push -u origin main
```

**در مصاحبه بگو:**
> Pipeline event-driven ساختم: fake order events → Kafka → ClickHouse.
> consumer lag و data quality را monitor کردم.
> برای dev از host network استفاده کردم؛ در production روی K8s deploy می‌شود.

---

# بخش ۳ — دستورات سریع

```bash
make help        # لیست دستورات
make up          # kafka + clickhouse
make consume     # terminal 2
make producer    # terminal 3
make query       # آمار ClickHouse
make monitoring  # grafana + prometheus
make airflow     # airflow
make down        # خاموش کردن همه
make demo        # تست end-to-end
make test        # unit tests
```

---

# بخش ۴ — چک‌لیست مصاحبه

| # | سوال | جواب تو |
|---|------|---------|
| 1 | داده کجا به کجا می‌رود؟ | fake → producer → kafka → consumer → clickhouse |
| 2 | چرا host network؟ | bridge Docker با VPN/پروکسی تداخل داشت |
| 3 | partition key چیست؟ | order_id |
| 4 | چرا ClickHouse نه PostgreSQL؟ | OLAP — query روی میلیون‌ها رکورد سریع |
| 5 | consumer lag یعنی چه؟ | consumer از producer عقب افتاده |
| 6 | data quality چی چک می‌کند؟ | null rate + duplicate rate |
| 7 | throughput چقدر بود؟ | عدد واقعی از demo |

</div>
