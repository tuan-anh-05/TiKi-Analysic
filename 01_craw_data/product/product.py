from pathlib import Path
import os
import json
import time
import random
import requests
import pandas as pd

# ---------- Đường dẫn ----------
PROJECT_DIR = Path.cwd().resolve()
if not (PROJECT_DIR / "01_craw_data").is_dir():
    for parent in PROJECT_DIR.parents:
        if (parent / "01_craw_data").is_dir():
            PROJECT_DIR = parent
            break
    else:
        raise FileNotFoundError("Không tìm thấy thư mục 01_craw_data")

DATA_ROOT = Path(os.environ.get("TIKI_DATA_DIR", PROJECT_DIR)).resolve()
DATA_DIR = DATA_ROOT / "01_craw_data" / "product"
DATA_DIR.mkdir(parents=True, exist_ok=True)

CATEGORIES_FILE = DATA_ROOT / "01_craw_data" / "category" / "category.csv"
INPUT_FILE = DATA_DIR / "product.csv"              
LISTING_CHECKPOINT_FILE = DATA_DIR / "listing_checkpoint.json"
PRODUCT_FILE = DATA_DIR / "product_detail.csv"
SELLER_FILE = DATA_DIR / "product_seller.csv"
CATEGORY_FILE = DATA_DIR / "product_category.csv"

# ---------- Tham số chung ----------
LISTING_URL = "https://tiki.vn/api/personalish/v1/blocks/listings"
API_URL = "https://tiki.vn/api/v2/products/{}"
SAVE_EVERY = 100
REQUEST_DELAY = (3, 5)
MAX_RETRIES = 3
CRAWL_DATE = pd.Timestamp.now(tz="Asia/Bangkok").strftime("%Y-%m")   # Chỉ lấy tháng năm

session = requests.Session()
session.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/130.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://tiki.vn/",
})


# # 1. Crawl product.csv


LISTING_COLUMNS = ["product_id", "seller_product_id", "category_id", "crawl_date"]


def save_listing(rows):
    if rows:
        has_data = INPUT_FILE.exists() and INPUT_FILE.stat().st_size > 0
        pd.DataFrame(rows).reindex(columns=LISTING_COLUMNS).to_csv(
            INPUT_FILE, mode="a", index=False,
            header=not has_data, encoding="utf-8" if has_data else "utf-8-sig")
        rows.clear()


def fetch_listing(params, retries=3):
    for attempt in range(retries):
        r = session.get(LISTING_URL, params=params, timeout=30)
        if r.status_code == 429 or r.status_code >= 500:
            time.sleep(10 * (attempt + 1))
            continue
        r.raise_for_status()
        return r.json().get("data") or []
    raise RuntimeError(f"Failed after {retries} retries: {params}")


def crawl_product_ids():
    categories = pd.read_csv(CATEGORIES_FILE).drop_duplicates("category_id")

    # Dùng checkpoint riêng để chỉ đánh dấu danh mục hoàn tất sau khi crawl đủ trang.
    done = set()
    if LISTING_CHECKPOINT_FILE.exists():
        try:
            checkpoint = json.loads(LISTING_CHECKPOINT_FILE.read_text(encoding="utf-8"))
            if checkpoint.get("crawl_month") == CRAWL_DATE:
                done = {int(category_id) for category_id in checkpoint.get("completed_category_ids", [])}
        except (OSError, ValueError, TypeError):
            print("Cảnh báo: không đọc được listing checkpoint; sẽ crawl lại danh mục.")

    categories = categories[~categories["category_id"].astype(int).isin(done)]
    print(f"Danh mục cần crawl: {len(categories)} (đã xong tháng {CRAWL_DATE}: {len(done)})")

    for _, cat in categories.iterrows():
        category_id = int(cat["category_id"])
        params = {"category": category_id, "src": cat["src"], "urlKey": cat["urlKey"]}
        rows, seen, ok = [], set(), True

        try:
            for page in range(1, 101):
                params["page"] = page
                try:
                    records = fetch_listing(params)
                except (requests.RequestException, ValueError, RuntimeError) as e:
                    print(f"[LỖI] {cat['name']} trang {page}: {e}")
                    ok = False
                    break

                new = [r for r in records if r.get("id") is not None and r["id"] not in seen]
                if not new:
                    break
                for r in new:
                    seen.add(r["id"])
                    rows.append({
                        "product_id": r["id"],
                        "seller_product_id": r.get("seller_product_id"),
                        "category_id": category_id,
                        "crawl_date": CRAWL_DATE,
                    })
                if len(rows) >= SAVE_EVERY:
                    save_listing(rows)
                time.sleep(random.uniform(3, 6))
            else:
                # Không đánh dấu xong nếu chạm ngưỡng trang mà chưa thấy trang kết thúc.
                ok = False
                print(f"[CẢNH BÁO] {cat['name']} đã chạm giới hạn 100 trang; sẽ tiếp tục ở lần chạy sau.")
        finally:
            save_listing(rows)

        if ok:
            done.add(category_id)
            LISTING_CHECKPOINT_FILE.write_text(
                json.dumps({
                    "crawl_month": CRAWL_DATE,
                    "completed_category_ids": sorted(done),
                }, indent=2),
                encoding="utf-8",
            )

        print(f"Crawl: {cat['name']} - {len(seen)} product_id" + ("" if ok else " (CHƯA ĐẦY ĐỦ)"))

    # Dedupe cuối file
    if INPUT_FILE.exists():
        df = pd.read_csv(INPUT_FILE, dtype=str).drop_duplicates()
        df.to_csv(INPUT_FILE, index=False, encoding="utf-8-sig")

    print("\nHoàn tất crawl danh sách product_id!")


# # 2. Crawl chi tiết


PRODUCT_COLUMNS = [
    "product_id", "master_id", "sku", "product_name", "product_type", "url_key",
    "created_date", "favourite_count", "brand_id", "brand_name",
    "rating_average", "review_count", "all_time_quantity_sold", "crawl_date",
]
SELLER_COLUMNS = [
    "product_id", "seller_product_id", "seller_id", "seller_name", "seller_link",
    "store_id", "seller_price", "is_current_seller", "is_best_store", "crawl_date",
]
CATEGORY_COLUMNS = [
    "product_id", "category_id", "category_name", "category_level",
    "category_url", "is_leaf", "is_primary", "crawl_date",
]

PRODUCT_KEYS = ["product_id", "crawl_date"]
SELLER_KEYS = ["product_id", "seller_product_id", "seller_id", "crawl_date"]
CATEGORY_KEYS = ["product_id", "category_id", "crawl_date"]


# ---------- Đọc input ----------
def read_product_ids():
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Không tìm thấy: {INPUT_FILE}")

    df = pd.read_csv(
        INPUT_FILE,
        dtype={"product_id": "string", "seller_product_id": "string", "crawl_date": "string"},
        low_memory=False,
    )
    required = {"product_id", "seller_product_id"}
    if not required.issubset(df.columns):
        raise ValueError(f"File đầu vào phải có các cột: {required}")

    if "crawl_date" not in df.columns:
        raise ValueError("product.csv cần có cột crawl_date để chọn snapshot tháng hiện tại")
    df["crawl_date"] = df["crawl_date"].str.replace("\ufeff", "", regex=False).str.strip()
    df = df.loc[df["crawl_date"].str[:7] == CRAWL_DATE, ["product_id", "seller_product_id"]].dropna()
    for col in ["product_id", "seller_product_id"]:
        df[col] = df[col].str.replace("\ufeff", "", regex=False).str.strip()

    df = df[df["product_id"].ne("") & df["seller_product_id"].ne("")]
    # Mỗi product_id chỉ gọi API một lần
    return df.drop_duplicates("product_id").reset_index(drop=True)


def read_done_product_ids():
    """Các product_id đã crawl trong tháng hiện tại (đã có trong product_detail)."""
    if not PRODUCT_FILE.exists():
        return set()

    df = pd.read_csv(PRODUCT_FILE, dtype=str, usecols=lambda c: c in {"product_id", "crawl_date"})
    if "crawl_date" not in df.columns:
        return set()

    df["product_id"] = df["product_id"].str.replace("\ufeff", "", regex=False).str.strip()
    df["crawl_date"] = df["crawl_date"].str.replace("\ufeff", "", regex=False).str.strip()
    df = df[df["crawl_date"] == CRAWL_DATE].dropna(subset=["product_id"])
    return set(df["product_id"])


# ---------- Gọi API ----------
def get_product_detail(product_id, seller_product_id):
    url = API_URL.format(product_id)
    params = {"platform": "web", "spid": seller_product_id, "version": 3}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(url, params=params, timeout=30)

            if response.status_code in (401, 403, 429):
                print(f"API trả HTTP {response.status_code}. Dừng crawl để tránh gửi thêm request.")
                return None, "stop"

            response.raise_for_status()
            data = response.json()

            if not isinstance(data, dict) or data.get("id") is None:
                return None, "error"

            return data, "ok"

        except (requests.RequestException, ValueError) as e:
            if attempt == MAX_RETRIES:
                print(f"Lỗi {product_id}/{seller_product_id}: {e}")
                return None, "error"
            time.sleep(attempt * 2)

    return None, "error"


# ---------- Parse ----------
def created_date(day_ago):
    """Ngày bán đầu tiên = ngày crawl - day_ago_created. Trả về dạng YYYY-MM-DD."""
    if day_ago is None:
        return None
    try:
        return (
            pd.Timestamp.now().normalize() - pd.Timedelta(days=float(day_ago))
        ).strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return None


def get_category_id(category):
    """Hỗ trợ JSON dùng category_id hoặc id."""
    value = category.get("category_id")
    if value is None:
        value = category.get("id")
    return str(value) if value is not None else ""


def full_url(url):
    """Ghép domain nếu URL là đường dẫn tương đối."""
    if url and str(url).startswith("/"):
        return "https://tiki.vn" + str(url)
    return url


def parse_product(data, product_id):
    brand = data.get("brand")
    if not isinstance(brand, dict):
        brand = {}

    quantity_sold = data.get("quantity_sold")
    all_time_quantity = data.get("all_time_quantity_sold")
    if all_time_quantity is None and isinstance(quantity_sold, dict):
        all_time_quantity = quantity_sold.get("value")

    return {
        "product_id": str(product_id),
        "master_id": data.get("master_id"),
        "sku": data.get("sku"),
        "product_name": data.get("name"),
        "product_type": data.get("product_type"),
        "url_key": data.get("url_key"),
        "created_date": created_date(data.get("day_ago_created")),
        "favourite_count": data.get("favourite_count"),
        "brand_id": brand.get("id"),
        "brand_name": brand.get("name"),
        "rating_average": data.get("rating_average"),
        "review_count": data.get("review_count"),
        "all_time_quantity_sold": all_time_quantity,
        "crawl_date": CRAWL_DATE,
    }


def parse_categories(data, product_id):
    rows = []

    breadcrumbs = data.get("breadcrumbs") or []
    if not isinstance(breadcrumbs, list):
        breadcrumbs = []

    category_info = data.get("categories") or {}
    if not isinstance(category_info, dict):
        category_info = {}

    primary_id = category_info.get("id")
    primary_id = str(primary_id) if primary_id is not None else None

    # Breadcrumb hợp lệ: bỏ category_id = 0 (chính là sản phẩm) và trùng lặp
    valid, seen_ids = [], set()
    for item in breadcrumbs:
        if not isinstance(item, dict):
            continue
        cid = get_category_id(item)
        if not cid or cid == "0" or cid in seen_ids:
            continue
        seen_ids.add(cid)
        valid.append(item)

    # Danh mục chính chưa có trong breadcrumbs thì bổ sung
    if primary_id and primary_id != "0" and primary_id not in seen_ids:
        valid.append({
            "id": primary_id,
            "name": category_info.get("name"),
            "url": category_info.get("url"),
        })

    for level, item in enumerate(valid, start=1):
        cid = get_category_id(item)
        is_primary = (cid == primary_id) if primary_id else None

        url = item.get("url") or item.get("url_key") or item.get("url_path")
        if not url and is_primary:
            url = category_info.get("url")

        rows.append({
            "product_id": str(product_id),
            "category_id": cid,
            "category_name": item.get("name"),
            "category_level": level,
            "category_url": full_url(url),
            "is_leaf": category_info.get("is_leaf") if is_primary else None,
            "is_primary": is_primary,
            "crawl_date": CRAWL_DATE,
        })

    return rows


def parse_sellers(data, product_id, requested_spid):
    rows, sellers = [], []

    current = data.get("current_seller")
    if isinstance(current, dict) and current:
        sellers.append((current, True))

    other_sellers = data.get("other_sellers") or []
    if isinstance(other_sellers, list):
        for seller in other_sellers:
            if isinstance(seller, dict):
                sellers.append((seller, False))

    seen = set()
    for seller, is_current in sellers:
        # seller.product_id là ID sản phẩm của chính người bán đó
        spid = seller.get("product_id")
        if spid is None and is_current:
            spid = requested_spid

        seller_id = seller.get("id", seller.get("seller_id"))
        spid = str(spid) if spid is not None else None
        seller_id = str(seller_id) if seller_id is not None else None

        key = (spid, seller_id) if seller_id is not None else (spid, seller.get("name"), seller.get("link"))
        if key in seen:
            continue
        seen.add(key)

        rows.append({
            "product_id": str(product_id),
            "seller_product_id": spid,
            "seller_id": seller_id,
            "seller_name": seller.get("name"),
            "seller_link": seller.get("link"),
            "store_id": seller.get("store_id"),
            "seller_price": seller.get("price"),
            "is_current_seller": is_current,
            "is_best_store": seller.get("is_best_store"),
            "crawl_date": CRAWL_DATE,
        })

    if not rows:
        rows.append({
            "product_id": str(product_id),
            "seller_product_id": str(requested_spid),
            "seller_id": None,
            "seller_name": None,
            "seller_link": None,
            "store_id": None,
            "seller_price": data.get("price"),
            "is_current_seller": None,
            "is_best_store": None,
            "crawl_date": CRAWL_DATE,
        })

    return rows


# ---------- Lưu file ----------
def append_rows(rows, file_path, columns):
    """Ghi nối vào cuối file (nhanh, không đọc lại file cũ)."""
    if not rows:
        return
    has_data = file_path.exists() and file_path.stat().st_size > 0
    df = pd.DataFrame(rows).reindex(columns=columns)
    df.to_csv(
        file_path, mode="a", index=False,
        header=not has_data, encoding="utf-8" if has_data else "utf-8-sig",
    )


def flush(buffers):
    # Ghi product CUỐI CÙNG: nó là mốc "đã crawl xong" dùng cho resume
    append_rows(buffers["seller"], SELLER_FILE, SELLER_COLUMNS)
    append_rows(buffers["category"], CATEGORY_FILE, CATEGORY_COLUMNS)
    append_rows(buffers["product"], PRODUCT_FILE, PRODUCT_COLUMNS)
    for rows in buffers.values():
        rows.clear()


def dedupe_file(file_path, columns, keys):
    """Dedupe cuối cùng, giữ dòng mới nhất."""
    if not file_path.exists():
        return
    df = pd.read_csv(file_path, dtype=str, low_memory=False)
    for col in columns:
        if col not in df.columns:
            df[col] = pd.NA
    for col in columns:
        df[col] = df[col].map(lambda value: value.replace("\ufeff", "") if isinstance(value, str) else value)
    df = df[columns].drop_duplicates(subset=keys, keep="last")
    df.to_csv(file_path, index=False, encoding="utf-8-sig")


def main():
    all_ids = read_product_ids()
    done = read_done_product_ids()
    todo = all_ids[~all_ids["product_id"].isin(done)].reset_index(drop=True)

    print(f"Tổng: {len(all_ids)} | Đã crawl tháng {CRAWL_DATE}: {len(done)} | Còn lại: {len(todo)}")

    buffers = {"product": [], "seller": [], "category": []}
    ok_count = err_count = 0
    stopped = False

    try:
        for i, row in todo.iterrows():
            product_id = row["product_id"]
            seller_product_id = row["seller_product_id"]

            data, status = get_product_detail(product_id, seller_product_id)

            if status == "stop":
                stopped = True
                break
            if status == "ok":
                buffers["product"].append(parse_product(data, product_id))
                buffers["seller"].extend(parse_sellers(data, product_id, seller_product_id))
                buffers["category"].extend(parse_categories(data, product_id))
                ok_count += 1
            else:
                err_count += 1

            if (i + 1) % SAVE_EVERY == 0:
                flush(buffers)
                print(f"[{i + 1}/{len(todo)}] ok={ok_count} lỗi={err_count}")

            time.sleep(random.uniform(*REQUEST_DELAY))
    finally:
        flush(buffers)   # lưu nốt khi hết dữ liệu, lỗi hoặc bấm dừng (Interrupt)

    dedupe_file(PRODUCT_FILE, PRODUCT_COLUMNS, PRODUCT_KEYS)
    dedupe_file(SELLER_FILE, SELLER_COLUMNS, SELLER_KEYS)
    dedupe_file(CATEGORY_FILE, CATEGORY_COLUMNS, CATEGORY_KEYS)

    status_text = "DỪNG SỚM do bị chặn (chạy lại sau để tiếp tục)" if stopped else "Hoàn tất"
    print(f"\n{status_text}: thành công {ok_count}, lỗi {err_count}")

if __name__ == "__main__":
    crawl_product_ids()
    main()
