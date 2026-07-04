"""Data quality checks on order events pipeline."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from ch_http import query_scalar

POSTGRES_CONN_ID = "postgres_default"
NULL_RATE_THRESHOLD = 0.10
DUPLICATE_THRESHOLD = 0.02


def _save_check(
    check_date: datetime.date,
    name: str,
    status: str,
    metric: float,
    threshold: float,
    details: str,
) -> None:
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO data_quality_checks
                    (check_date, check_name, status, metric_value, threshold, details)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (check_date, name, status, metric, threshold, details),
            )
        conn.commit()
    finally:
        conn.close()


def check_null_delivery_time(**_: object) -> None:
    today = datetime.now().date()
    null_rate = query_scalar(
        """
        SELECT countIf(delivery_time_sec IS NULL) / count() AS null_rate
        FROM orders.events_raw
        WHERE toDate(timestamp) = today()
        """
    )
    status = "PASS" if null_rate <= NULL_RATE_THRESHOLD else "FAIL"
    _save_check(
        today,
        "null_delivery_time_rate",
        status,
        null_rate,
        NULL_RATE_THRESHOLD,
        f"Null rate for delivery_time_sec: {null_rate:.2%}",
    )
    if status == "FAIL":
        raise ValueError(f"Null rate {null_rate:.2%} exceeds threshold {NULL_RATE_THRESHOLD:.0%}")


def check_duplicate_events(**_: object) -> None:
    today = datetime.now().date()
    duplicate_rate = query_scalar(
        """
        SELECT (count() - uniqExact(event_id)) / count() AS duplicate_rate
        FROM orders.events_raw
        WHERE toDate(timestamp) = today()
        """
    )
    status = "PASS" if duplicate_rate <= DUPLICATE_THRESHOLD else "FAIL"
    _save_check(
        today,
        "duplicate_event_rate",
        status,
        duplicate_rate,
        DUPLICATE_THRESHOLD,
        f"Duplicate rate by event_id: {duplicate_rate:.2%}",
    )
    if status == "FAIL":
        raise ValueError(
            f"Duplicate rate {duplicate_rate:.2%} exceeds threshold {DUPLICATE_THRESHOLD:.0%}"
        )


default_args = {
    "owner": "dataops",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="data_quality",
    default_args=default_args,
    description="Validate order events data quality in ClickHouse",
    schedule_interval="0 7 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["orders", "quality"],
) as dag:
    PythonOperator(
        task_id="check_null_delivery_time",
        python_callable=check_null_delivery_time,
    ) >> PythonOperator(
        task_id="check_duplicate_events",
        python_callable=check_duplicate_events,
    )
