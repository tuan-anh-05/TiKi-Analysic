"""Validate Silver CSV contracts before data is loaded into the Gold database."""

from __future__ import annotations

import csv
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


PROJECT_DIR = Path(__file__).resolve().parents[1]
DATA_ROOT = Path(os.environ.get("TIKI_DATA_DIR", PROJECT_DIR)).resolve()
SILVER_ROOT = DATA_ROOT / "02_process_raw_data"


@dataclass(frozen=True)
class CsvContract:
    relative_path: str
    required_columns: tuple[str, ...]
    unique_key: tuple[str, ...] = ()
    required_key: tuple[str, ...] = ()
    date_columns: tuple[tuple[str, str], ...] = ()
    numeric_ranges: tuple[tuple[str, float, float], ...] = ()


CONTRACTS = (
    CsvContract("category/category.csv", ("category_id", "name"), ("category_id",), ("category_id",)),
    CsvContract("category/sub_category.csv", ("category_id", "category_name"), required_key=("category_id",)),
    CsvContract(
        "daily_price/daily_price_history.csv",
        ("product_id", "seller_product_id", "crawl_date", "price", "discount_rate", "rating_average"),
        ("product_id", "seller_product_id", "crawl_date"),
        ("product_id", "seller_product_id", "crawl_date"),
        (("crawl_date", "%Y-%m-%d"),),
        (("price", 0, float("inf")), ("discount_rate", 0, 100), ("rating_average", 0, 5)),
    ),
    CsvContract(
        "product/product.csv",
        ("product_id", "seller_product_id", "category_id", "crawl_date"),
        ("product_id", "seller_product_id", "category_id", "crawl_date"),
        ("product_id", "seller_product_id", "category_id", "crawl_date"),
        (("crawl_date", "%Y-%m"),),
    ),
    CsvContract(
        "product/product_detail.csv",
        ("product_id", "crawl_date", "rating_average"),
        ("product_id", "crawl_date"),
        ("product_id", "crawl_date"),
        (("crawl_date", "%Y-%m"),),
        (("rating_average", 0, 5),),
    ),
    CsvContract(
        "product/product_seller.csv",
        ("product_id", "seller_product_id", "seller_id", "crawl_date"),
        ("product_id", "seller_product_id", "seller_id", "crawl_date"),
        ("product_id", "seller_product_id", "crawl_date"),
        (("crawl_date", "%Y-%m"),),
    ),
    CsvContract(
        "product/product_category.csv",
        ("product_id", "category_id", "crawl_date"),
        ("product_id", "category_id", "crawl_date"),
        ("product_id", "category_id", "crawl_date"),
        (("crawl_date", "%Y-%m"),),
    ),
    CsvContract(
        "review/product_review.csv",
        ("review_id", "product_id", "seller_product_id", "rating"),
        ("review_id", "product_id", "seller_product_id"),
        ("review_id", "product_id", "seller_product_id"),
        numeric_ranges=(("rating", 1, 5),),
    ),
)


def validate_csv(contract: CsvContract) -> list[str]:
    path = SILVER_ROOT / contract.relative_path
    issues: list[str] = []
    if not path.is_file() or path.stat().st_size == 0:
        return [f"{contract.relative_path}: file missing or empty"]

    seen: set[tuple[str, ...]] = set()
    duplicate_count = 0
    row_count = 0
    with path.open("r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)
        if not reader.fieldnames:
            return [f"{contract.relative_path}: missing CSV header"]
        missing = sorted(set(contract.required_columns) - set(reader.fieldnames))
        if missing:
            return [f"{contract.relative_path}: missing columns {', '.join(missing)}"]

        for line_number, row in enumerate(reader, start=2):
            row_count += 1
            if contract.required_key:
                blank = [column for column in contract.required_key if not (row.get(column) or "").strip()]
                if blank and len(issues) < 20:
                    issues.append(f"{contract.relative_path}:{line_number}: blank key column(s) {', '.join(blank)}")

            if contract.unique_key:
                key = tuple((row.get(column) or "").strip() for column in contract.unique_key)
                if key in seen:
                    duplicate_count += 1
                else:
                    seen.add(key)

            for column, pattern in contract.date_columns:
                value = (row.get(column) or "").strip()
                try:
                    datetime.strptime(value, pattern)
                except ValueError:
                    if len(issues) < 20:
                        issues.append(f"{contract.relative_path}:{line_number}: invalid {column}={value!r}")

            for column, minimum, maximum in contract.numeric_ranges:
                value = (row.get(column) or "").strip()
                try:
                    number = float(value)
                    valid = minimum <= number <= maximum
                except ValueError:
                    valid = False
                if not valid and len(issues) < 20:
                    issues.append(f"{contract.relative_path}:{line_number}: invalid {column}={value!r}; expected {minimum}..{maximum}")

    if row_count == 0:
        issues.append(f"{contract.relative_path}: has header but no data rows")
    if duplicate_count:
        issues.append(f"{contract.relative_path}: {duplicate_count:,} duplicate key row(s) for {contract.unique_key}")
    print(f"Silver check: {contract.relative_path}: {row_count:,} rows; {duplicate_count:,} duplicate keys")
    return issues


def main() -> None:
    issues = [issue for contract in CONTRACTS for issue in validate_csv(contract)]
    if issues:
        print("Silver data validation failed:")
        for issue in issues:
            print(f"- {issue}")
        raise SystemExit(1)
    print(f"Silver data validation passed ({len(CONTRACTS)} CSV contracts).")


if __name__ == "__main__":
    main()
