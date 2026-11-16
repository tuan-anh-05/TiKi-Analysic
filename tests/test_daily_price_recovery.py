import csv
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "01_craw_data/daily_price"))
from recovery import load_completed_keys, load_failed_targets, pending_rows, validate_run_date


class DailyPriceRecoveryTests(unittest.TestCase):
    def test_pending_rows_reconcile_success_file_and_keep_unlogged_inflight_rows(self):
        rows = [
            {"product_id": "10", "seller_product_id": "100"},
            {"product_id": "20", "seller_product_id": "200"},
            {"product_id": "30", "seller_product_id": "300"},
        ]
        pending = pending_rows(
            rows,
            completed_keys={("10", "100")},
            failed_keys={("20", "200")},
            failed_product_ids={"20"},
        )
        self.assertEqual(
            [(row["product_id"], row["seller_product_id"]) for row in pending],
            [("20", "200"), ("30", "300")],
        )

    def test_reads_structured_log_and_legacy_error_ids_by_logical_date(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as tmp:
            path = Path(tmp) / "crawl_daily_log.csv"
            with path.open("w", encoding="utf-8", newline="") as file:
                writer = csv.DictWriter(
                    file,
                    fieldnames=["log_time", "crawl_date", "product_id", "seller_product_id", "error"],
                )
                writer.writeheader()
                writer.writerow({"log_time": "2026-10-24 01:00:00", "crawl_date": "2026-10-23", "product_id": "20", "seller_product_id": "200", "error": "HTTP 500"})
                writer.writerow({"log_time": "2026-10-23 19:00:00", "crawl_date": "", "product_id": "", "seller_product_id": "", "error": "30: HTTP 429"})
                writer.writerow({"log_time": "2026-10-22 19:00:00", "crawl_date": "2026-10-22", "product_id": "40", "seller_product_id": "400", "error": "HTTP 500"})

            keys, product_ids = load_failed_targets(path, "2026-10-23")
            self.assertEqual(keys, {("20", "200")})
            self.assertEqual(product_ids, {"20", "30"})

    def test_load_completed_keys_uses_run_date_and_normalizes_numeric_ids(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as tmp:
            path = Path(tmp) / "prices.csv"
            with path.open("w", encoding="utf-8", newline="") as file:
                writer = csv.DictWriter(file, fieldnames=["product_id", "seller_product_id", "crawl_date"])
                writer.writeheader()
                writer.writerow({"product_id": "10.0", "seller_product_id": "100.0", "crawl_date": "2026-10-23"})
                writer.writerow({"product_id": "20", "seller_product_id": "200", "crawl_date": "2026-10-22"})
            self.assertEqual(load_completed_keys(path, "2026-10-23"), {("10", "100")})

    def test_run_date_validates_or_uses_fallback(self):
        self.assertEqual(validate_run_date("2026-10-23", "2026-10-24"), "2026-10-23")
        self.assertEqual(validate_run_date(None, "2026-10-24"), "2026-10-24")
        with self.assertRaises(ValueError):
            validate_run_date("not-a-date", "2026-10-24")


if __name__ == "__main__":
    unittest.main()
