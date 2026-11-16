"""Pure schedule rules shared by the Airflow DAG and unit tests."""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

LOCAL_TZ = ZoneInfo("Asia/Ho_Chi_Minh")
CRAWL_TASKS = frozenset({"daily_price", "product", "review"})


def selected_tasks(run_at: datetime, manual_task: str | None = None) -> frozenset[str]:
    """Return the crawler task(s) selected at a scheduled/manual run time."""
    if manual_task is not None:
        return frozenset({manual_task}) if manual_task in CRAWL_TASKS else frozenset()

    local_time = run_at.astimezone(LOCAL_TZ)
    selected: set[str] = set()
    if local_time.hour in {14, 21}:
        selected.add("daily_price")
    if local_time.hour == 9:
        selected.add("review")
    if local_time.hour == 3:
        selected.add("product")
    return frozenset(selected)
