"""
Crawl dữ liệu giá hằng ngày từ Tiki.
Được chuyển từ notebook: crawl_daily_price.ipynb
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
import time
from zoneinfo import ZoneInfo

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

PRICE_DIR.mkdir(parents=True, exist_ok=True)


# %% Cell 2
# ==================================================
# 1. CẤU HÌNH
# ==================================================
MAX_WORKERS = 4
MAX_RETRIES = 3
SAVE_EVERY = 30
BATCH_SIZE = 120
TIMEOUT = 20

GONE_FILE = SAVE_FILE.with_name("deleted_products.csv")  # sản phẩm 404/410

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
# 3. BỎ QUA SẢN PHẨM ĐÃ CRAWL HÔM NAY VÀ SẢN PHẨM ĐÃ BỊ XÓA
# ==================================================
today = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).strftime("%Y-%m-%d")
LOG_COLUMNS = ["log_time", "product_id", "seller_product_id", "error"]

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

done = set()
if SAVE_FILE.exists() and SAVE_FILE.stat().st_size > 0:
    old = pd.read_csv(
        SAVE_FILE,
        dtype={
            "product_id": "string",
            "seller_product_id": "string",
            "crawl_date": "string"
        },
        usecols=lambda c: c in {"product_id", "seller_product_id", "crawl_date"}
    )
    if {"product_id", "seller_product_id", "crawl_date"}.issubset(old.columns):
        for col in ["product_id", "seller_product_id", "crawl_date"]:
            old[col] = old[col].str.replace("\ufeff", "", regex=False).str.strip()
        old_today = old.loc[old["crawl_date"].str[:10] == today]
        done = set(zip(old_today["product_id"], old_today["seller_product_id"]))

# Lấy các biến thể lỗi từ log hôm nay để lượt 21:00 retry phần crawl 18:00
# chưa lấy được. Các key chưa có trong file kết quả cũng được đưa vào để có
# thể phục hồi nếu tiến trình trước bị dừng trước lúc kịp ghi log.
failed_keys = set()
failed_product_ids = set()
if LOG_FILE.exists() and LOG_FILE.stat().st_size > 0:
    logs = pd.read_csv(LOG_FILE, encoding="utf-8-sig", low_memory=False)
    if {"log_time", "error"}.issubset(logs.columns):
        logs["log_time"] = pd.to_datetime(logs["log_time"], errors="coerce")
        logs_today = logs.loc[logs["log_time"].dt.strftime("%Y-%m-%d") == today]
        if "product_id" in logs_today.columns:
            pids = logs_today["product_id"].dropna().astype("string").str.strip()
            pids = pids.str.replace(r"\.0$", "", regex=True)
            failed_product_ids.update(pids[pids.ne("")].tolist())
        # Log đời cũ không có product_id riêng, chỉ ghi "<id>: <lỗi>".
        legacy_pids = logs_today["error"].astype("string").str.extract(
            r"^\s*(\d+)\s*:", expand=False
        )
        failed_product_ids.update(legacy_pids.dropna().tolist())
        if "seller_product_id" in logs_today.columns:
            for row in logs_today.dropna(
                subset=["product_id", "seller_product_id"]
            ).itertuples(index=False):
                pid = str(row.product_id).strip()
                spid = str(row.seller_product_id).strip()
                pid = pid.removesuffix(".0")
                spid = spid.removesuffix(".0")
                if pid and spid:
                    failed_keys.add((pid, spid))

gone_ids = set()
if GONE_FILE.exists() and GONE_FILE.stat().st_size > 0:
    gone_history = pd.read_csv(
        GONE_FILE, dtype={"product_id": "string", "log_time": "string"}, low_memory=False
    )
    if {"product_id", "log_time"}.issubset(gone_history.columns):
        gone_history["log_time"] = pd.to_datetime(gone_history["log_time"], errors="coerce")
        retry_after = pd.Timestamp.now() - pd.Timedelta(days=30)
        recent_gone = gone_history.loc[gone_history["log_time"] >= retry_after]
        gone_ids = set(
            recent_gone["product_id"].dropna().astype("string")
            .str.replace("\ufeff", "", regex=False).str.strip()
        )

if done:
    key = pd.MultiIndex.from_frame(df[["product_id", "seller_product_id"]])
    df = df.loc[~key.isin(done)]

if failed_keys or failed_product_ids:
    key = pd.MultiIndex.from_frame(df[["product_id", "seller_product_id"]])
    retry_mask = key.isin(failed_keys)
    if failed_product_ids:
        retry_mask |= df["product_id"].isin(failed_product_ids).to_numpy()
    # Nếu log có lỗi hôm nay, lượt tiếp chỉ lấy đúng những biến thể lỗi đó.
    # Nếu không có log lỗi, giữ các key còn thiếu để phục hồi run bị ngắt đột ngột.
    if retry_mask.any():
        df = df.loc[retry_mask]

if gone_ids:
    df = df.loc[~df["product_id"].isin(gone_ids)]

df = df.reset_index(drop=True)
total = len(df)

print("Input:", INPUT_FILE)
print("Output:", SAVE_FILE)
print("Ngày crawl:", today)
print("Đã bỏ qua (đã bị xóa từ trước):", len(gone_ids))
print("Sản phẩm lỗi đọc từ log hôm nay:", len(failed_product_ids | {pid for pid, _ in failed_keys}))
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

def save_gone(gone):
    if gone:
        append_csv(GONE_FILE, gone, ["product_id", "seller_product_id", "log_time"])
        gone.clear()

# ==================================================
# 7. CRAWL THEO BATCH
# stage=1: crawl chính; stage=2: thử lại các sản phẩm lỗi ở lượt 1
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

            results, errors, gone = [], [], []

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
                    gone_count += 1
                    gone.append({
                        "product_id": row["product_id"],
                        "seller_product_id": row["seller_product_id"],
                        "log_time": now_str
                    })
                else:
                    failed_rows.append(row)
                    # chỉ ghi log khi vẫn lỗi sau lượt thử lại cuối
                    if stage == 2:
                        errors.append({
                            "log_time": now_str,
                            "product_id": str(row["product_id"]),
                            "seller_product_id": str(row["seller_product_id"]),
                            "error": error or f"{row['product_id']}: Lỗi không xác định"
                        })

                if len(results) >= SAVE_EVERY:
                    save_records(results)

            # Cuối batch: lưu mọi thứ còn lại (log lỗi được ghi ngay, không chờ hết lượt 2)
            save_records(results)
            save_errors(errors)
            save_gone(gone)

            print(
                f"Lượt {stage}: {completed}/{total_rows} "
                f"({completed / total_rows * 100:.1f}%)"
                f" | Thành công: {success}"
                f" | Đã xóa: {gone_count}"
                f" | Lỗi: {len(failed_rows)}"
            )

    return failed_rows, success, gone_count

# ==================================================
# 8. LƯỢT 1: CRAWL CHÍNH
# ==================================================
print("\n========== LƯỢT 1: CRAWL CHÍNH ==========")

main_rows = [row for _, row in df.iterrows()]
failed_rows, success_1, gone_1 = crawl_rows(main_rows, stage=1)

print("\nKết thúc crawl chính.")
print("Thành công lượt 1:", success_1)
print("Sản phẩm đã bị xóa:", gone_1)
print("Sản phẩm cần thử lại:", len(failed_rows))

# ==================================================
# 9. LƯỢT 2: THỬ LẠI ID LỖI
# ==================================================
success_2 = gone_2 = 0
still_failed = []

if failed_rows:
    print("\n========== LƯỢT 2: THỬ LẠI ID LỖI ==========")
    time.sleep(5)
    still_failed, success_2, gone_2 = crawl_rows(failed_rows, stage=2)
    print("\nKết thúc lượt thử lại.")
else:
    print("\nKhông có sản phẩm lỗi cần thử lại.")

# ==================================================
# 10. KẾT QUẢ
# ==================================================
print("\n========== HOÀN TẤT ==========")
print("Tổng sản phẩm cần crawl:", total)
print("Thành công lượt 1:", success_1)
print("Thành công lượt 2:", success_2)
print("Tổng thành công:", success_1 + success_2)
print("Sản phẩm đã bị xóa (404/410):", gone_1 + gone_2)
print("Số sản phẩm vẫn lỗi:", len(still_failed))
print("File giá:", SAVE_FILE.resolve())
print("File log:", LOG_FILE.resolve())
print("File sản phẩm đã xóa:", GONE_FILE.resolve())
