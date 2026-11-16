from pathlib import Path
import os
import sys
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
PRODUCT_FILE = DATA_DIR / "product_detail.csv"
SELLER_FILE = DATA_DIR / "product_seller.csv"
CATEGORY_FILE = DATA_DIR / "product_category.csv"

# ---------- Tham số chung ----------
LISTING_URL = "https://tiki.vn/api/personalish/v1/blocks/listings"
API_URL = "https://tiki.vn/api/v2/products/{}"
SAVE_EVERY = 100
REQUEST_DELAY = (3, 5)
MAX_RETRIES = 2
RUN_DATE = os.environ.get("TIKI_RUN_DATE")
if RUN_DATE:
    # Scheduled Airflow retries retain their original logical date even if delayed.
    CRAWL_TIMESTAMP = pd.Timestamp(RUN_DATE).normalize()
else:
    CRAWL_TIMESTAMP = pd.Timestamp.now(tz="Asia/Ho_Chi_Minh").tz_localize(None).normalize()
CRAWL_DATE = CRAWL_TIMESTAMP.strftime("%Y-%m")

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
    """Gọi API listing có retry cả lỗi HTTP, timeout và response không phải JSON."""
    last_error = None

    for attempt in range(1, retries + 1):
        try:
            response = session.get(LISTING_URL, params=params, timeout=30)

            # 429: bị giới hạn tần suất; chờ lâu hơn trước khi thử lại.
            if response.status_code == 429:
                wait = min(15 * attempt, 60)
                last_error = RuntimeError(f"HTTP 429 Too Many Requests; chờ {wait}s")
                if attempt < retries:
                    time.sleep(wait)
                    continue
                break

            # Lỗi máy chủ tạm thời: thử lại.
            if response.status_code >= 500:
                last_error = RuntimeError(f"HTTP {response.status_code}")
                if attempt < retries:
                    time.sleep(5 * attempt)
                    continue
                break

            response.raise_for_status()
            payload = response.json()  # Có thể lỗi nếu server trả HTML dù HTTP 200.

            if not isinstance(payload, dict):
                raise ValueError("Response listing không phải JSON object")

            records = payload.get("data")
            if records is None:
                # Không coi response sai cấu trúc là trang rỗng hợp lệ.
                raise ValueError("Response listing không có trường 'data'")
            if not isinstance(records, list):
                raise ValueError("Trường 'data' của listing không phải danh sách")

            return records

        except (requests.RequestException, ValueError, RuntimeError) as exc:
            last_error = exc
            if attempt < retries:
                wait = min(3 * attempt, 10)
                print(f"  [RETRY] Listing lần {attempt}/{retries}: {exc}. Chờ {wait}s")
                time.sleep(wait)

    raise RuntimeError(f"Listing thất bại sau {retries} lần thử: {last_error}; params={params}")


def crawl_product_ids():
    """Quét lại TẤT CẢ danh mục mỗi lần chạy để tìm sản phẩm mới trên Tiki.

    Không sử dụng checkpoint danh mục. Dữ liệu được ghi nối vào product.csv,
    sau đó dedupe theo product_id, seller_product_id, category_id, crawl_date.
    """
    if not CATEGORIES_FILE.exists():
        raise FileNotFoundError(f"Không tìm thấy file danh mục: {CATEGORIES_FILE}")

    categories = pd.read_csv(
        CATEGORIES_FILE,
        dtype={"category_id": "string"},
        low_memory=False,
    )
    required_columns = {"category_id", "name", "src", "urlKey"}
    if not required_columns.issubset(categories.columns):
        raise ValueError(
            f"File category.csv phải có các cột: {sorted(required_columns)}; "
            f"hiện có: {list(categories.columns)}"
        )

    categories = (
        categories.dropna(subset=["category_id"])
        .drop_duplicates("category_id")
        .reset_index(drop=True)
    )
    categories["category_id"] = categories["category_id"].str.strip()

    print(f"Tổng danh mục cần quét lại: {len(categories)}")

    for cat_index, cat in categories.iterrows():
        category_id = str(cat["category_id"]).strip()
        params = {
            "category": category_id,
            "src": cat["src"],
            "urlKey": cat["urlKey"],
        }
        rows = []
        seen_products = set()
        seen_pages = set()
        reached_end = False

        print(f"\n[{cat_index + 1}/{len(categories)}] Bắt đầu crawl: {cat['name']}")

        try:
            for page in range(1, 101):
                params["page"] = page
                try:
                    records = fetch_listing(params)
                except (requests.RequestException, ValueError, RuntimeError) as exc:
                    print(f"[LỖI] {cat['name']} trang {page}: {exc}")
                    break

                # Chỉ response có danh sách data rỗng mới xác nhận hết danh mục.
                if not records:
                    reached_end = True
                    print(f"  Trang {page}: hết dữ liệu.")
                    break

                page_signature = tuple(
                    (str(item.get("id")), str(item.get("seller_product_id")))
                    for item in records if isinstance(item, dict)
                )
                if page_signature and page_signature in seen_pages:
                    print(
                        f"[CẢNH BÁO] {cat['name']} trang {page}: "
                        "API trả lại trang lặp; dừng danh mục để tránh lặp vô hạn."
                    )
                    break
                if page_signature:
                    seen_pages.add(page_signature)

                new_count = 0
                for item in records:
                    if not isinstance(item, dict):
                        continue
                    product_id = item.get("id")
                    if product_id is None:
                        continue
                    product_id = str(product_id).strip()
                    if not product_id:
                        continue

                    seller_product_id = item.get("seller_product_id")
                    seller_product_id = (
                        str(seller_product_id).strip()
                        if seller_product_id is not None else None
                    )
                    # Dùng cặp ID để không làm mất các seller_product_id khác nhau.
                    product_key = (product_id, seller_product_id)
                    if product_key in seen_products:
                        continue
                    seen_products.add(product_key)
                    new_count += 1
                    rows.append({
                        "product_id": product_id,
                        "seller_product_id": seller_product_id,
                        "category_id": category_id,
                        "crawl_date": CRAWL_DATE,
                    })

                if len(rows) >= SAVE_EVERY:
                    save_listing(rows)

                print(
                    f"  Trang {page}: {len(records)} bản ghi API, "
                    f"{new_count} cặp sản phẩm/người bán mới trong lần quét này"
                )
                time.sleep(random.uniform(*REQUEST_DELAY))
            else:
                print(
                    f"[CẢNH BÁO] {cat['name']} đã chạm giới hạn 100 trang; "
                    "lần chạy sau sẽ quét lại từ đầu."
                )
        finally:
            # Lưu phần còn trong bộ nhớ kể cả khi có lỗi hoặc dừng chương trình.
            save_listing(rows)

        status = "HOÀN TẤT" if reached_end else "CHƯA ĐẦY ĐỦ"
        print(
            f"[{status}] {cat['name']}: "
            f"{len(seen_products):,} cặp product_id/seller_product_id đã thấy"
        )

    # Dedupe dữ liệu đã lưu; không xóa dữ liệu của những lần crawl trước.
    if INPUT_FILE.exists() and INPUT_FILE.stat().st_size > 0:
        df = pd.read_csv(INPUT_FILE, dtype=str, low_memory=False)
        for col in LISTING_COLUMNS:
            if col not in df.columns:
                df[col] = pd.NA
        for col in LISTING_COLUMNS:
            df[col] = df[col].map(
                lambda value: value.replace("\ufeff", "").strip()
                if isinstance(value, str) else value
            )
        df = df[LISTING_COLUMNS].drop_duplicates(
            subset=["product_id", "seller_product_id", "category_id", "crawl_date"],
            keep="last",
        )
        df.to_csv(INPUT_FILE, index=False, encoding="utf-8-sig")
        print(f"\nTổng số dòng sau dedupe: {len(df):,}")

    print("\nHoàn tất tiến trình crawl danh sách product_id!")

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

    try:
        df = pd.read_csv(
            INPUT_FILE,
            dtype={"product_id": "string", "seller_product_id": "string", "crawl_date": "string"},
            low_memory=False,
        )
    except pd.errors.EmptyDataError as exc:
        raise ValueError(f"File đầu vào rỗng: {INPUT_FILE}") from exc
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
            CRAWL_TIMESTAMP.normalize() - pd.Timedelta(days=float(day_ago))
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
    if stopped:
        sys.exit(1)

if __name__ == "__main__":
    crawl_product_ids()
    main()
