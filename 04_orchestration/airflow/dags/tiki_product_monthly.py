"""Monthly full product extraction, transformation, and loading DAG."""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

PROJECT_DIR = Path(os.environ.get("TIKI_PROJECT_DIR", "/opt/tiki/project"))
RUNNER = PROJECT_DIR / "04_orchestration" / "airflow" / "scripts" / "minio_task.py"
LOCAL_TZ = pendulum.timezone("Asia/Ho_Chi_Minh")
SYNC_POOL = "tiki_sync_pool"

DEFAULT_ARGS = {
    "owner": "tiki-data",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(hours=2),
}


def invoke_task(task_name: str, **context) -> None:
    run_id = context.get("run_id")
    run_at = context.get("data_interval_end")
    command = [sys.executable, str(RUNNER), task_name]
    if run_id:
        command.append(str(run_id))
    if run_at:
        command.append(run_at.in_timezone("Asia/Ho_Chi_Minh").format("YYYY-MM-DD"))
    subprocess.run(
        command,
        cwd=PROJECT_DIR,
        check=True,
    )


def run_task(
    task_name: str,
    task_id: str,
    pool: str | None = None,
    execution_timeout: timedelta | None = None,
) -> PythonOperator:
    return PythonOperator(
        task_id=task_id,
        python_callable=invoke_task,
        op_kwargs={"task_name": task_name},
        pool=pool,
        execution_timeout=execution_timeout or timedelta(hours=6),
    )


with DAG(
    dag_id="tiki_product_monthly",
    description=(
        "Chạy full crawl danh mục và chi tiết sản phẩm vào ngày 1 hàng tháng. "
        "Sau đó transform, validate Silver và load vào Supabase qua sync pool."
    ),
    schedule="0 2 1 * *",
    start_date=pendulum.datetime(2026, 1, 1, tz=LOCAL_TZ),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["tiki", "monthly", "product", "extract", "transform", "load"],
) as monthly_dag:
    start = EmptyOperator(task_id="start")

    # Full monthly product crawl can take 96h - 120h; set 144h timeout.
    crawl_product = run_task(
        "product",
        "crawl_monthly_product_full",
        execution_timeout=timedelta(hours=144),
    )

    transform = run_task("preprocess", "run_preprocessing", pool=SYNC_POOL)
    validate_silver = run_task("validate_silver", "validate_silver_data", pool=SYNC_POOL)
    load = run_task("push", "load_new_rows_to_supabase", pool=SYNC_POOL)

    start >> crawl_product >> transform >> validate_silver >> load
