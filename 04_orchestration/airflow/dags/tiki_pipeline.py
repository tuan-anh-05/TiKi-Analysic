"""Scheduled Tiki extract -> transform -> load pipeline for local Airflow."""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.trigger_rule import TriggerRule

PROJECT_DIR = Path(os.environ.get("TIKI_PROJECT_DIR", "/opt/tiki/project"))
RUNNER = PROJECT_DIR / "04_orchestration" / "airflow" / "scripts" / "minio_task.py"
LOCAL_TZ = pendulum.timezone("Asia/Ho_Chi_Minh")
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
    return context["data_interval_end"].in_timezone("Asia/Ho_Chi_Minh")


def requested_manual_task(**context):
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None) or {}
    return conf.get("task")


def should_crawl_price(**context) -> bool:
    manual_task = requested_manual_task(**context)
    if manual_task:
        return manual_task == "daily_price"
    return local_run_time(**context).hour in {18, 21}


def should_crawl_product(**context) -> bool:
    manual_task = requested_manual_task(**context)
    if manual_task:
        return manual_task == "product"
    run_at = local_run_time(**context)
    return run_at.hour in {2, 10} and run_at.day % 2 == 1


def should_crawl_review(**context) -> bool:
    manual_task = requested_manual_task(**context)
    if manual_task:
        return manual_task == "review"
    run_at = local_run_time(**context)
    return run_at.hour in {2, 10} and run_at.day % 2 == 0


def is_manual_data_run(**context) -> bool:
    return requested_manual_task(**context) in {"preprocess", "push"}


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
        "Mỗi lượt crawl đồng bộ Bronze lên MinIO, sau đó transform, kiểm tra Silver "
        "và load Gold vào Supabase nếu mọi bước trước thành công."
    ),
    schedule="0 2,10,18,21 * * *",
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
        task_id="is_product_crawl_time",
        python_callable=should_crawl_product,
        ignore_downstream_trigger_rules=False,
    )
    product = run_task("product", "crawl_monthly_product")
    product_gate >> product

    review_gate = ShortCircuitOperator(
        task_id="is_review_crawl_time",
        python_callable=should_crawl_review,
        ignore_downstream_trigger_rules=False,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )
    review = run_task("review", "crawl_new_reviews")
    review_gate >> review

    # Join selected extraction branches only after all have finished. If a
    # selected extractor fails after retries, downstream processing is blocked.
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
    # Keep the manual-only short circuit off the join itself. Otherwise a
    # non-manual scheduled run would skip extract_complete even after a crawl
    # task succeeded.
    manual_run_complete = EmptyOperator(task_id="manual_run_complete")
    start >> manual_marker >> manual_run_complete
    manual_run_complete >> extract_complete

    transform = run_task("preprocess", "run_preprocessing")
    validate_silver = run_task("validate_silver", "validate_silver_data")
    load = run_task("push", "load_new_rows_to_supabase")

    start >> [price_gate, product_gate]
    # Product refreshes the listing/detail/seller inputs used by the review
    # crawler, so run review after product when both are scheduled. At other
    # times `start` still lets the review gate evaluate its own condition.
    [start, product] >> review_gate
    extract_complete >> transform >> validate_silver >> load
