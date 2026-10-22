"""Scheduled Tiki extract -> transform -> load pipeline for local Airflow."""

from __future__ import annotations

import calendar
import os
import subprocess
import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.sensors.python import PythonSensor

PROJECT_DIR = Path(os.environ.get("TIKI_PROJECT_DIR", "/opt/tiki/project"))
RUNNER = PROJECT_DIR / "04_orchestration" / "airflow" / "scripts" / "minio_task.py"
LOCAL_TZ = pendulum.timezone("Asia/Bangkok")
DEFAULT_ARGS = {
    "owner": "tiki-data",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(hours=1),
}


def local_run_time(**context):
    # Cron-triggered Airflow runs expose the scheduled boundary as data_interval_end.
    return context["data_interval_end"].in_timezone("Asia/Bangkok")


def requested_manual_task(**context):
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None) or {}
    return conf.get("task")


def should_crawl_price(**context) -> bool:
    manual_task = requested_manual_task(**context)
    if manual_task:
        return manual_task == "daily_price"
    return local_run_time(**context).hour == 21


def should_crawl_product(**context) -> bool:
    manual_task = requested_manual_task(**context)
    if manual_task:
        return manual_task == "product"
    run_at = local_run_time(**context)
    last_day = calendar.monthrange(run_at.year, run_at.month)[1]
    return run_at.day == last_day and run_at.hour in {2, 10}


def should_crawl_review(**context) -> bool:
    manual_task = requested_manual_task(**context)
    if manual_task:
        return manual_task == "review"
    run_at = local_run_time(**context)
    return run_at.day == 1 and run_at.hour in {2, 10}


def should_run_transform(**context) -> bool:
    manual_task = requested_manual_task(**context)
    if manual_task in {"daily_price", "product", "review", "preprocess", "push"}:
        return True
    return local_run_time(**context).hour == 21


def is_manual_data_run(**context) -> bool:
    return requested_manual_task(**context) in {"preprocess", "push"}


def time_is_at_or_after(hour: int) -> bool:
    return pendulum.now("Asia/Bangkok").hour >= hour


def wait_for_22(**context) -> bool:
    if requested_manual_task(**context):
        return True
    return time_is_at_or_after(22)


def wait_for_23(**context) -> bool:
    if requested_manual_task(**context):
        return True
    return time_is_at_or_after(23)


def invoke_task(task_name: str) -> None:
    subprocess.run(
        [sys.executable, str(RUNNER), task_name],
        cwd=PROJECT_DIR,
        check=True,
    )


def run_task(task_name: str, task_id: str) -> PythonOperator:
    return PythonOperator(
        task_id=task_id,
        python_callable=invoke_task,
        op_kwargs={"task_name": task_name},
        execution_timeout=timedelta(hours=6),
    )


with DAG(
    dag_id="tiki_extract_transform_load",
    description=(
        "Crawl theo lịch local; lượt giá 21:00 chạy transform sau 22:00 và chỉ load "
        "Supabase sau 23:00 nếu mọi bước trước thành công."
    ),
    schedule="0 2,10,21 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz=LOCAL_TZ),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["tiki", "extract", "transform", "load", "minio"],
) as tiki_pipeline:
    start = EmptyOperator(task_id="start")

    price_gate = ShortCircuitOperator(
        task_id="is_daily_price_time",
        python_callable=should_crawl_price,
        ignore_downstream_trigger_rules=False,
    )
    price = run_task("daily_price", "crawl_daily_price")
    price_gate >> price

    product_gate = ShortCircuitOperator(
        task_id="is_product_month_end_time",
        python_callable=should_crawl_product,
        ignore_downstream_trigger_rules=False,
    )
    product = run_task("product", "crawl_monthly_product")
    product_gate >> product

    review_gate = ShortCircuitOperator(
        task_id="is_review_first_day_time",
        python_callable=should_crawl_review,
        ignore_downstream_trigger_rules=False,
    )
    review = run_task("review", "crawl_new_reviews")
    review_gate >> review

    # At a given scheduled time only one extractor is active. If that extractor
    # fails after retries, this join and all downstream tasks are blocked.
    extract_complete = EmptyOperator(
        task_id="extract_complete",
        trigger_rule="none_failed_min_one_success",
    )
    [price, product, review] >> extract_complete

    manual_marker = ShortCircuitOperator(
        task_id="manual_preprocess_or_push_mode",
        python_callable=is_manual_data_run,
        ignore_downstream_trigger_rules=False,
    )
    start >> manual_marker >> extract_complete

    evening_gate = ShortCircuitOperator(
        task_id="is_evening_transform_run",
        python_callable=should_run_transform,
    )
    wait_until_22 = PythonSensor(
        task_id="wait_until_22_local_time",
        python_callable=wait_for_22,
        mode="reschedule",
        poke_interval=60,
        timeout=timedelta(hours=4),
    )
    transform = run_task("preprocess", "run_preprocessing")
    wait_until_23 = PythonSensor(
        task_id="wait_until_23_local_time",
        python_callable=wait_for_23,
        mode="reschedule",
        poke_interval=60,
        timeout=timedelta(hours=4),
    )
    load = run_task("push", "load_new_rows_to_supabase")

    start >> [price_gate, product_gate, review_gate]
    extract_complete >> evening_gate >> wait_until_22 >> transform >> wait_until_23 >> load
