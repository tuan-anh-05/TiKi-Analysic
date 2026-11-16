"""
Crawl dữ liệu giá hằng ngày từ Tiki.
Được chuyển từ notebook: crawl_daily_price.ipynb
Chạy tối đa MAX_STAGES lượt (1 lượt chính + các lượt thử lại ID lỗi).
"""

# %% Cell 1
from pathlib import Path
import os
from datetime import datetime
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import threading
import requests
import random
import time
from zoneinfo import ZoneInfo
from recovery import load_completed_keys, load_failed_targets, pending_rows, validate_run_date

PROJECT_DIR = Path.cwd().resolve()
if not (PROJECT_DIR / "01_craw_data").is_dir():
    for parent in PROJECT_DIR.parents:
        if (parent / "01_craw_data").is_dir():
            PROJECT_DIR = parent
            break

DATA_ROOT = Path(os.environ.get("TIKI_DATA_DIR", PROJECT_DIR)).resolve()
PRICE_DIR = DATA_ROOT / "01_craw_data" / "daily_price"
PRICE_DIR.mkdir(parents=True, exist_ok=True)

INPUT_FILE = PRICE_DIR / "product_pareto.csv"
SAVE_FILE = PRICE_DIR / "daily_price_history.csv"
LOG_FILE = PRICE_DIR / "crawl_daily_log.csv"


# %% Cell 2
# ==================================================
# 1. CẤU HÌNH
# ==================================================
MAX_WORKERS = 4
MAX_RETRIES = 2
SAVE_EVERY = 30
BATCH_SIZE = 120
TIMEOUT = 20
REQUEST_DELAY = (0.5, 1.5)

MAX_STAGES = 5          # tổng số lượt crawl (1 lượt chính + 4 lượt thử lại)
RETRY_SLEEP = 5         # giây nghỉ cơ bản giữa các lượt (nhân theo số lượt)

COLUMNS = [
    "product_id", "seller_product_id", "seller_id",
    "category_id", "crawl_date", "price", "list_price",
    "original_price", "discount", "discount_rate", "rating_average",
    "review_count", "all_time_quantity_sold", "inventory_status"
]

# ==================================================
# 2. ĐỌC INPUT
# ==================================================
if not INPUT_FILE.exists():
    raise FileNotFoundError(f"Không tìm thấy: {INPUT_FILE}")

df = pd.read_csv(
    INPUT_FILE,
    dtype={
        "product_id": "string",
        "seller_product_id": "string",
        "category_id": "string"
    }
)

required = {"product_id", "seller_product_id"}
if not required.issubset(df.columns):
    raise ValueError(f"File cần có các cột: {required}")

df = (
    df.dropna(subset=["product_id", "seller_product_id"])
      .drop_duplicates(["product_id", "seller_product_id"])
      .reset_index(drop=True)
)
for col in ["product_id", "seller_product_id", "category_id"]:
    if col in df.columns:
        df[col] = df[col].str.replace("\ufeff", "", regex=False).str.strip()

# ==================================================
# 3. BỎ QUA SẢN PHẨM ĐÃ CRAWL HÔM NAY
# ==================================================
today = validate_run_date(
    os.environ.get("TIKI_RUN_DATE"),
    datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).strftime("%Y-%m-%d"),
)
LOG_COLUMNS = ["log_time", "crawl_date", "product_id", "seller_product_id", "error"]

if LOG_FILE.exists() and LOG_FILE.stat().st_size > 0:
    try:
        logs = pd.read_csv(LOG_FILE, encoding="utf-8-sig", low_memory=False)

        if not {"log_time", "error"}.issubset(logs.columns):
            raise ValueError("Log không có cột log_time/error")
        # Tương thích log cũ dạng (log_time, error). Không có seller ID thì
        # lượt retry sẽ chọn mọi biến thể của product_id lỗi.
        for col in LOG_COLUMNS:
            if col not in logs.columns:
                logs[col] = pd.NA
        logs = logs[LOG_COLUMNS]
        # Migrate the previous two-column log before appending structured retries.
        logs.to_csv(LOG_FILE, index=False, encoding="utf-8-sig")
        print(f"Log: giữ lại lịch sử {len(logs):,} dòng.")
    except (pd.errors.EmptyDataError, pd.errors.ParserError, UnicodeDecodeError):
        pd.DataFrame(columns=LOG_COLUMNS).to_csv(
            LOG_FILE, index=False, encoding="utf-8-sig"
        )
        print("Log: không đọc được file cũ, đã khởi tạo lại.")
    except ValueError as exc:
        raise ValueError(f"File log crawl không hợp lệ: {exc}") from exc
elif not LOG_FILE.exists():
    # Chưa có file log; tạo file rỗng với header chuẩn.
    pd.DataFrame(columns=LOG_COLUMNS).to_csv(
        LOG_FILE, index=False, encoding="utf-8-sig"
    )

done = load_completed_keys(SAVE_FILE, today)
failed_keys, failed_product_ids = load_failed_targets(LOG_FILE, today)
pending = pending_rows(
    df.to_dict("records"), done, failed_keys, failed_product_ids
)
df = pd.DataFrame(pending, columns=df.columns)

if done:
    key = pd.MultiIndex.from_frame(df[["product_id", "seller_product_id"]])
    df = df.loc[~key.isin(done)]

df = df.reset_index(drop=True)
total = len(df)

print("Input:", INPUT_FILE)
print("Output:", SAVE_FILE)
print("Ngày crawl:", today)
print("ID lỗi đọc từ log hôm nay:", len(failed_product_ids))
print("Key thành công đã có trong file:", len(done))
print("Số sản phẩm cần crawl:", total)

# ==================================================
# 4. SESSION RIÊNG CHO MỖI THREAD
# ==================================================
thread_local = threading.local()

def get_session():
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
    return thread_local.session

# ==================================================
# 5. CRAWL MỘT SẢN PHẨM
# Trả về (record, error, gone)
#   gone=True: sản phẩm không còn tồn tại (404/410), không cần thử lại
# ==================================================
def crawl_product(row):
    pid = str(row["product_id"])
    spid = str(row["seller_product_id"])

    url = f"https://tiki.vn/api/v2/products/{pid}"
    params = {"platform": "web", "spid": spid, "version": 3}
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/130.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Referer": f"https://tiki.vn/-p{pid}.html"
    }

    for attempt in range(1, MAX_RETRIES + 1):
        time.sleep(random.uniform(*REQUEST_DELAY))
        try:
            response = get_session().get(
                url, params=params, headers=headers, timeout=TIMEOUT
            )
            status = response.status_code

            if status in (404, 410):
                return None, f"{pid}: HTTP {status}", True

            if status == 429:
                if attempt < MAX_RETRIES:
                    time.sleep(10 * attempt)
                    continue
                return None, f"{pid}: HTTP 429", False

            if 400 <= status < 500:
                return None, f"{pid}: HTTP {status}", False

            if status >= 500:
                if attempt < MAX_RETRIES:
                    time.sleep(3 * attempt)
                    continue
                return None, f"{pid}: HTTP {status}", False

            response.raise_for_status()

            content_type = response.headers.get("Content-Type", "").lower()
            if "json" not in content_type:
                if attempt < MAX_RETRIES:
                    time.sleep(5 * attempt)
                    continue
                return None, (
                    f"{pid}: Non-JSON; status={status}; "
                    f"type={content_type}; body={response.text[:150]}"
                ), False

            data = response.json()
            if not isinstance(data, dict) or not data.get("id"):
                return None, f"{pid}: JSON không có dữ liệu sản phẩm", False

            seller = data.get("current_seller") or {}

            record = {
                "product_id": pid,
                "seller_product_id": spid,
                "seller_id": seller.get("id"),
                "category_id": row.get("category_id"),
                "crawl_date": today,
                "price": data.get("price"),
                "list_price": data.get("list_price"),
                "original_price": data.get("original_price"),
                "discount": data.get("discount"),
                "discount_rate": data.get("discount_rate"),
                "rating_average": data.get("rating_average"),
                "review_count": data.get("review_count"),
                "all_time_quantity_sold": data.get("all_time_quantity_sold"),
                "inventory_status": data.get("inventory_status")
            }
            return record, None, False

        except Exception as e:
            if attempt == MAX_RETRIES:
                return None, f"{pid}: {str(e)[:200]}", False
            time.sleep(2 * attempt)

# ==================================================
# 6. HÀM LƯU DỮ LIỆU
# ==================================================
def append_csv(path, data, columns):
    has_data = path.exists() and path.stat().st_size > 0
    pd.DataFrame(data, columns=columns).to_csv(
        path,
        mode="a",
        header=not has_data,
        index=False,
        encoding="utf-8" if has_data else "utf-8-sig"
    )

def save_records(records):
    if records:
        append_csv(SAVE_FILE, records, COLUMNS)
        records.clear()

def save_errors(errors):
    if errors:
        append_csv(LOG_FILE, errors, LOG_COLUMNS)
        errors.clear()

# ==================================================
# 7. CRAWL THEO BATCH
# stage < MAX_STAGES: lỗi chưa ghi log, để lượt sau thử lại
# stage == MAX_STAGES: lượt cuối, ghi log các sản phẩm vẫn lỗi
# ==================================================
def crawl_rows(rows, stage):
    if not rows:
        return [], 0, 0

    failed_rows = []
    completed = success = gone_count = 0
    total_rows = len(rows)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for start in range(0, total_rows, BATCH_SIZE):
            batch = rows[start:start + BATCH_SIZE]
            future_to_row = {executor.submit(crawl_product, r): r for r in batch}

            results, errors = [], []

            for future in as_completed(future_to_row):
                row = future_to_row[future]
                completed += 1
                now_str = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )

                try:
                    record, error, is_gone = future.result()
                except Exception as e:  # lưới an toàn
                    record, error, is_gone = None, f"{row['product_id']}: {str(e)[:200]}", False

                if record is not None:
                    results.append(record)
                    success += 1
                elif is_gone:
                    # 404/410: chỉ đếm, không thử lại và không lưu file
                    gone_count += 1
                else:
                    failed_rows.append(row)
                    # chỉ ghi log khi vẫn lỗi sau lượt cuối cùng
                    if stage == MAX_STAGES:
                        errors.append({
                            "log_time": now_str,
                            "crawl_date": today,
                            "product_id": str(row["product_id"]),
                            "seller_product_id": str(row["seller_product_id"]),
                            "error": error or f"{row['product_id']}: Lỗi không xác định"
                        })

                if len(results) >= SAVE_EVERY:
                    save_records(results)

            # Cuối batch: lưu mọi thứ còn lại
            save_records(results)
            save_errors(errors)

            print(
                f"Lượt {stage}: {completed}/{total_rows} "
                f"({completed / total_rows * 100:.1f}%)"
                f" | Thành công: {success}"
                f" | Đã xóa: {gone_count}"
                f" | Lỗi: {len(failed_rows)}"
            )

    return failed_rows, success, gone_count

# ==================================================
# 8. CHẠY NHIỀU LƯỢT (tối đa MAX_STAGES)
# ==================================================
rows_to_crawl = [row for _, row in df.iterrows()]
stage_stats = []   # (lượt, thành công, đã xóa, còn lỗi)

for stage in range(1, MAX_STAGES + 1):
    if not rows_to_crawl:
        print(f"\nKhông còn sản phẩm lỗi, dừng ở lượt {stage - 1}.")
        break

    if stage == 1:
        print("\n========== LƯỢT 1: CRAWL CHÍNH ==========")
    else:
        print(
            f"\n========== LƯỢT {stage}/{MAX_STAGES}: "
            f"THỬ LẠI {len(rows_to_crawl)} ID LỖI =========="
        )
        time.sleep(RETRY_SLEEP * (stage - 1))   # nghỉ lâu dần để tránh bị chặn 429

    rows_to_crawl, ok, gone_n = crawl_rows(rows_to_crawl, stage=stage)
    stage_stats.append((stage, ok, gone_n, len(rows_to_crawl)))

still_failed = rows_to_crawl

# ==================================================
# 9. KẾT QUẢ
# ==================================================
print("\n========== HOÀN TẤT ==========")
print("Tổng sản phẩm cần crawl:", total)
for stage, ok, gone_n, remain in stage_stats:
    print(f"Lượt {stage}: thành công {ok} | đã xóa {gone_n} | còn lỗi {remain}")
print("Tổng thành công:", sum(s[1] for s in stage_stats))
print("Sản phẩm đã bị xóa (404/410):", sum(s[2] for s in stage_stats))
print("Số sản phẩm vẫn lỗi:", len(still_failed))
print("File giá:", SAVE_FILE.resolve())
print("File log:", LOG_FILE.resolve())