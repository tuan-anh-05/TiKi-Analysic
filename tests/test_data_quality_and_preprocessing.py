import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

PROJECT_ROOT = Path(__file__).resolve().parents[1]

sys.path.insert(0, str(PROJECT_ROOT / "02_process_raw_data"))
import run_all_preprocessing

validator_path = PROJECT_ROOT / "02_process_raw_data/validate_silver.py"
spec = importlib.util.spec_from_file_location("silver_validator", validator_path)
silver_validator = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = silver_validator
spec.loader.exec_module(silver_validator)


class DataQualityAndPreprocessingTests(unittest.TestCase):
    def test_preprocessing_module_skips_only_when_input_and_output_fingerprints_match(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as tmp:
            root = Path(tmp)
            source = root / "raw.csv"
            output = root / "silver.csv"
            source.write_text("raw-v1", encoding="utf-8")
            output.write_text("silver-v1", encoding="utf-8")
            input_hashes = {str(source): run_all_preprocessing.file_sha256(source)}
            state = {
                "daily_price": {
                    "inputs": input_hashes,
                    "outputs": {str(output): run_all_preprocessing.file_sha256(output)},
                }
            }
            self.assertTrue(run_all_preprocessing.should_skip_module("daily_price", input_hashes, [output], state))

            output.write_text("tampered", encoding="utf-8")
            self.assertFalse(run_all_preprocessing.should_skip_module("daily_price", input_hashes, [output], state))
            source.write_text("raw-v2", encoding="utf-8")
            self.assertFalse(run_all_preprocessing.should_skip_module("daily_price", input_hashes, [output], state))

    def test_validator_accepts_contract_and_reports_duplicate_keys(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as tmp:
            root = Path(tmp)
            folder = root / "category"
            folder.mkdir()
            path = folder / "category.csv"
            path.write_text(
                "category_id,name\n1,Home\n1,Duplicate\n",
                encoding="utf-8",
            )
            contract = silver_validator.CsvContract(
                "category/category.csv",
                ("category_id", "name"),
                ("category_id",),
                ("category_id",),
            )
            with mock.patch.object(silver_validator, "SILVER_ROOT", root):
                issues = silver_validator.validate_csv(contract)
            self.assertTrue(any("duplicate key" in issue for issue in issues))

    def test_validator_rejects_invalid_date_and_numeric_range(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parent) as tmp:
            root = Path(tmp)
            folder = root / "daily_price"
            folder.mkdir()
            (folder / "prices.csv").write_text(
                "product_id,seller_product_id,crawl_date,price\n1,2,not-a-date,-5\n",
                encoding="utf-8",
            )
            contract = silver_validator.CsvContract(
                "daily_price/prices.csv",
                ("product_id", "seller_product_id", "crawl_date", "price"),
                ("product_id", "seller_product_id", "crawl_date"),
                ("product_id", "seller_product_id", "crawl_date"),
                (("crawl_date", "%Y-%m-%d"),),
                (("price", 0, float("inf")),),
            )
            with mock.patch.object(silver_validator, "SILVER_ROOT", root):
                issues = silver_validator.validate_csv(contract)
            self.assertTrue(any("invalid crawl_date" in issue for issue in issues))
            self.assertTrue(any("invalid price" in issue for issue in issues))


if __name__ == "__main__":
    unittest.main()
