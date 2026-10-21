"""
Master Pipeline: Chạy toàn bộ tiến trình tiền xử lý dữ liệu Tiki
Thực thi tuần tự các module trong thư mục 02_process_raw_data:
1. Category
2. Daily Price
3. Product
4. Review
"""

from pathlib import Path
import sys
import time

PROJECT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_DIR))

from category.process_category import main as run_category
from daily_price.process_daily_price import main as run_daily_price
from product.process_product import main as run_product
from review.process_review import main as run_review


def run_pipeline():
    start_all = time.time()
    print("=" * 70)
    print("KHỞI ĐỘNG PIPELINE TIỀN XỬ LÝ TOÀN BỘ DỮ LIỆU TIKI")
    print(f"Project directory: {PROJECT_DIR}")
    print("=" * 70)

    # 1. Danh mục
    t0 = time.time()
    run_category()
    print(f">> Category hoàn thành sau: {time.time() - t0:.2f}s\n")

    # 2. Daily Price
    t0 = time.time()
    run_daily_price()
    print(f">> Daily Price hoàn thành sau: {time.time() - t0:.2f}s\n")

    # 3. Product
    t0 = time.time()
    run_product()
    print(f">> Product hoàn thành sau: {time.time() - t0:.2f}s\n")

    # 4. Review
    t0 = time.time()
    run_review()
    print(f">> Review hoàn thành sau: {time.time() - t0:.2f}s\n")

    total_time = time.time() - start_all
    print("=" * 70)
    print(f"TẤT CẢ DỮ LIỆU ĐÃ ĐƯỢC TIỀN XỬ LÝ THÀNH CÔNG VÀ LƯU VÀO 02_process_raw_data")
    print(f"Tổng thời gian thực thi: {total_time:.2f} giây")
    print("=" * 70)


if __name__ == "__main__":
    run_pipeline()

