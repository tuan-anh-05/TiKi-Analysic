"""
Master Pipeline: Chạy toàn bộ tiến trình tiền xử lý dữ liệu Tiki
Thực thi tuần tự các module trong thư mục 02_process_raw_data:
1. Category
2. Daily Price
3. Product
4. Review
"""

from pathlib import Path
import hashlib
import json
import os
import sys
import time

PROJECT_DIR = Path(__file__).resolve().parent.parent
DATA_ROOT = Path(os.environ.get("TIKI_DATA_DIR", PROJECT_DIR)).resolve()
STATE_FILE = DATA_ROOT / "02_process_raw_data" / ".preprocessing_state.json"
sys.path.insert(0, str(PROJECT_DIR))

from category.process_category import main as run_category
from daily_price.process_daily_price import main as run_daily_price
from product.process_product import main as run_product
from review.process_review import main as run_review


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for block in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()

def load_state() -> dict:
    if not STATE_FILE.is_file():
        return {}
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        print("[WARN] Preprocessing state unreadable; modules will be reprocessed.")
        return {}


def save_state(state: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    temporary = STATE_FILE.with_suffix(".tmp")
    temporary.write_text(json.dumps(state, indent=2), encoding="utf-8")
    os.replace(temporary, STATE_FILE)


def module_definitions():
    raw = DATA_ROOT / "01_craw_data"
    silver = DATA_ROOT / "02_process_raw_data"
    return [
        ("category", [raw / "category/category.csv", raw / "category/sub_category.csv"],
         [silver / "category/category.csv", silver / "category/sub_category.csv"], run_category),
        ("daily_price", [raw / "daily_price/daily_price_history.csv"],
         [silver / "daily_price/daily_price_history.csv"], run_daily_price),
        ("product", [raw / "product/product.csv", raw / "product/product_detail.csv",
                      raw / "product/product_seller.csv", raw / "product/product_category.csv"],
         [silver / "product/product.csv", silver / "product/product_detail.csv",
          silver / "product/product_seller.csv", silver / "product/product_category.csv"], run_product),
        ("review", [raw / "review/product_review.csv"],
         [silver / "review/product_review.csv"], run_review),
    ]


def should_skip_module(name: str, input_hashes: dict, output_files: list[Path], state: dict) -> bool:
    previous = state.get(name, {})
    if previous.get("inputs") != input_hashes:
        return False
    expected_outputs = previous.get("outputs", {})
    if set(expected_outputs) != {str(path) for path in output_files}:
        return False
    for path in output_files:
        if not path.is_file() or path.stat().st_size == 0:
            return False
        if file_sha256(path) != expected_outputs[str(path)]:
            return False
    return True


def run_pipeline():
    start_all = time.time()
    print("=" * 70)
    print("KHỞI ĐỘNG PIPELINE TIỀN XỬ LÝ TOÀN BỘ DỮ LIỆU TIKI")
    print(f"Project directory: {PROJECT_DIR}")
    print("=" * 70)

    state = load_state()
    run_date = os.environ.get("TIKI_RUN_DATE")
    for name, input_files, output_files, runner in module_definitions():
        input_hashes = {str(path): file_sha256(path) for path in input_files}
        if should_skip_module(name, input_hashes, output_files, state):
            print(f">> {name}: input/output không đổi, bỏ qua transform.\n")
            continue

        t0 = time.time()
        runner()
        missing = [str(path) for path in output_files if not path.is_file() or path.stat().st_size == 0]
        if missing:
            raise RuntimeError(f"{name} completed without producing required output(s): {missing}")
        state[name] = {
            "inputs": input_hashes,
            "outputs": {str(path): file_sha256(path) for path in output_files},
            "run_date": run_date,
        }
        save_state(state)
        print(f">> {name} hoàn thành sau: {time.time() - t0:.2f}s\n")

    total_time = time.time() - start_all
    print("=" * 70)
    print(f"TẤT CẢ DỮ LIỆU ĐÃ ĐƯỢC TIỀN XỬ LÝ THÀNH CÔNG VÀ LƯU VÀO 02_process_raw_data")
    print(f"Tổng thời gian thực thi: {total_time:.2f} giây")
    print("=" * 70)


if __name__ == "__main__":
    run_pipeline()

