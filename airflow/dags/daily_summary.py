"""Daily aggregation from ClickHouse into PostgreSQL."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from ch_http import query_json

POSTGRES_CONN_ID = "postgres_default"


def aggregate_daily_summary(**_: object) -> None:
    target_date = (datetime.now() - timedelta(days=1)).date()

    rows = query_json(
        f"""
        SELECT
            toDate(timestamp) AS summary_date,
            city,
            event_type,
            count() AS event_count,
            sum(amount) AS total_amount,
            avg(delivery_time_sec) AS avg_delivery_sec
        FROM orders.events_raw
        WHERE toDate(timestamp) = '{target_date}'
        GROUP BY summary_date, city, event_type
        ORDER BY city, event_type
        """
    )
    if not rows:
        return

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            for row in rows:
                cur.execute(
                    """
                    INSERT INTO daily_order_summary
                        (summary_date, city, event_type, event_count, total_amount, avg_delivery_sec)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (summary_date, city, event_type) DO UPDATE SET
                        event_count = EXCLUDED.event_count,
                        total_amount = EXCLUDED.total_amount,
                        avg_delivery_sec = EXCLUDED.avg_delivery_sec
                    """,
                    (
                        row["summary_date"],
                        row["city"],
                        row["event_type"],
                        row["event_count"],
                        row["total_amount"],
                        row["avg_delivery_sec"],
                    ),
                )
        conn.commit()
    finally:
        conn.close()


default_args = {
    "owner": "dataops",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="daily_summary",
    default_args=default_args,
    description="Aggregate yesterday's order events from ClickHouse to PostgreSQL",
    schedule_interval="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["orders", "aggregation"],
) as dag:
    PythonOperator(
        task_id="aggregate_daily_summary",
        python_callable=aggregate_daily_summary,
    )
