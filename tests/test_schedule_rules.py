import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "04_orchestration/airflow/dags"))
from schedule_rules import selected_tasks


class ScheduleRulesTests(unittest.TestCase):
    def test_daily_product_runs_at_three_am_local(self):
        # 20:00 UTC is 03:00 the following day in Vietnam.
        run_at = datetime(2026, 10, 22, 20, tzinfo=timezone.utc)
        self.assertEqual(selected_tasks(run_at), {"product"})

    def test_review_runs_at_nine_am_local(self):
        # 02:00 UTC is 09:00 in Vietnam.
        run_at = datetime(2026, 10, 23, 2, tzinfo=timezone.utc)
        self.assertEqual(selected_tasks(run_at), {"review"})

    def test_price_runs_at_two_and_nine_pm_local(self):
        # 07:00 UTC is 14:00 in Vietnam.
        self.assertEqual(selected_tasks(datetime(2026, 10, 23, 7, tzinfo=timezone.utc)), {"daily_price"})
        # 14:00 UTC is 21:00 in Vietnam.
        self.assertEqual(selected_tasks(datetime(2026, 10, 23, 14, tzinfo=timezone.utc)), {"daily_price"})

    def test_manual_crawl_overrides_calendar_rule(self):
        run_at = datetime(2026, 10, 23, 7, tzinfo=timezone.utc)
        self.assertEqual(selected_tasks(run_at, "review"), {"review"})
        self.assertEqual(selected_tasks(run_at, "preprocess"), set())

    def test_unscheduled_hour_selects_no_crawler(self):
        # 11:00 UTC is 18:00 in Vietnam (no task scheduled).
        self.assertEqual(selected_tasks(datetime(2026, 10, 23, 11, tzinfo=timezone.utc)), set())

    def test_minio_task_commands_exist_on_filesystem(self):
        project_root = Path(__file__).resolve().parents[1]
        sys.path.insert(0, str(project_root / "04_orchestration/airflow/scripts"))
        import minio_task

        for task_name, command in minio_task.TASK_COMMANDS.items():
            script_path = project_root / command[0]
            self.assertTrue(
                script_path.is_file(),
                f"Task {task_name} points to missing script: {script_path}",
            )


if __name__ == "__main__":
    unittest.main()
