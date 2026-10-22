"""Incrementally crawl new Tiki product reviews into product_review.csv."""

from __future__ import annotations

import argparse
import os
import random
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests


API_URL = "https://tiki.vn/api/v2/reviews"
PAGE_LIMIT = 20
SORT_MODE = "id|desc"
REQUEST_DELAY = (0.5, 1.2)
SAVE_EVERY_N = 20
MAX_RETRIES = 3
TIMEOUT = 20
REVIEW_KEY_COLS = ["review_id", "product_id", "seller_product_id"]
BAD_KEY_VALUES = {"", "nan", "none", "<na>", "null"}
REVIEW_COLUMNS = [
    "review_id", "product_id", "seller_product_id", "seller_id",
    "rating", "title", "content", "review_created_at", "purchased",
    "thank_count", "comment_count", "is_photo", "crawl_time",
]


def find_project_dir() -> Path:
    current = Path(__file__).resolve().parent
    for path in (current, *current.parents):
        if (path / "01_craw_data").is_dir():
            return path
    raise FileNotFoundError("Không tìm thấy thư mục 01_craw_data")


def normalize_key_value(value) -> str:
    if pd.isna(value):
        return ""
    value = str(value).strip().lstrip("\ufeff")
    if value.lower() in BAD_KEY_VALUES:
        return ""
    if value.endswith(".0") and value[:-2].isdigit():
        value = value[:-2]
    return value


def make_review_key(review_id, product_id, seller_product_id) -> tuple[str, str, str]:
    return tuple(
        normalize_key_value(value)
        for value in (review_id, product_id, seller_product_id)
    )


def load_existing_keys(review_file: Path) -> set[tuple[str, str, str]]:
    if not review_file.exists() or review_file.stat().st_size == 0:
        return set()

    keys: set[tuple[str, str, str]] = set()
    for chunk in pd.read_csv(
        review_file,
        usecols=REVIEW_KEY_COLS,
        dtype={col: "string" for col in REVIEW_KEY_COLS},
        chunksize=150_000,
        low_memory=False,
    ):
        keys.update(make_review_key(*row) for row in chunk[REVIEW_KEY_COLS].itertuples(index=False, name=None))
    return keys


def load_products_to_crawl(detail_file: Path, seller_file: Path, review_file: Path) -> pd.DataFrame:
    if not detail_file.exists():
        raise FileNotFoundError(f"Không tìm thấy file: {detail_file}")
    if not seller_file.exists():
        raise FileNotFoundError(f"Không tìm thấy file: {seller_file}")

    detail = pd.read_csv(
        detail_file,
        usecols=["product_id", "review_count", "crawl_date"],
        dtype={"product_id": "string", "crawl_date": "string"},
        low_memory=False,
    )
    detail["product_id"] = detail["product_id"].map(normalize_key_value)
    detail["review_count"] = pd.to_numeric(detail["review_count"], errors="coerce").fillna(0).astype(int)
    detail["crawl_date"] = detail["crawl_date"].astype(str).str.strip().str[:7]

    latest = (
        detail.sort_values("crawl_date")
        .drop_duplicates("product_id", keep="last")
        .rename(columns={"review_count": "review_count_current"})
    )

    saved_counts = pd.Series(dtype="int64")
    if review_file.exists() and review_file.stat().st_size > 0:
        saved = pd.read_csv(
            review_file,
            usecols=REVIEW_KEY_COLS,
            dtype={col: "string" for col in REVIEW_KEY_COLS},
            low_memory=False,
        )
        for col in REVIEW_KEY_COLS:
            saved[col] = saved[col].map(normalize_key_value)
        saved_counts = saved.drop_duplicates(REVIEW_KEY_COLS).groupby("product_id").size()

    latest["review_count_saved"] = latest["product_id"].map(saved_counts).fillna(0).astype(int)
    latest["review_count_diff"] = latest["review_count_current"] - latest["review_count_saved"]
    candidates = latest.loc[
        (latest["review_count_current"] > 0) & (latest["review_count_diff"] > 0)
    ].sort_values("review_count_diff", ascending=False)

    sellers = pd.read_csv(
        seller_file,
        dtype={"product_id": "string", "seller_product_id": "string", "seller_id": "string"},
        low_memory=False,
    )
    for col in ["product_id", "seller_product_id", "seller_id"]:
        sellers[col] = sellers[col].map(normalize_key_value).replace("", pd.NA)
    sellers = sellers.dropna(subset=["product_id", "seller_product_id"])
    if "is_current_seller" in sellers.columns:
        sellers["_is_current"] = sellers["is_current_seller"].astype(str).str.lower().isin(["true", "1"])
        sellers = sellers.sort_values(
            ["product_id", "_is_current"], ascending=[True, False], kind="stable"
        )
    sellers = sellers.drop_duplicates("product_id", keep="first")[[
        "product_id", "seller_product_id", "seller_id"
    ]]

    crawl = candidates.merge(sellers, on="product_id", how="left")
    crawl["seller_product_id"] = crawl["seller_product_id"].fillna(crawl["product_id"])
    return crawl.reset_index(drop=True)


def create_session() -> requests.Session:
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
    return session


def fetch_reviews_page(session: requests.Session, params: dict) -> tuple[dict | None, str]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(API_URL, params=params, timeout=TIMEOUT)
            if response.status_code in (401, 403):
                return None, "stop"
            if response.status_code == 429 or response.status_code >= 500:
                time.sleep(10 * attempt)
                continue
            response.raise_for_status()
            payload = response.json()
            return (payload, "ok") if isinstance(payload, dict) else (None, "error")
        except (requests.RequestException, ValueError):
            time.sleep(2 * attempt)
    return None, "error"


def append_new_reviews(
    buffer: list[dict], review_file: Path,
    existing_keys: set[tuple[str, str, str]],
) -> int:
    if not buffer:
        return 0

    frame = pd.DataFrame(buffer).reindex(columns=REVIEW_COLUMNS)
    frame["_key"] = [
        make_review_key(*row)
        for row in frame[REVIEW_KEY_COLS].itertuples(index=False, name=None)
    ]
    frame = frame.drop_duplicates("_key", keep="first")
    frame = frame.loc[~frame["_key"].isin(existing_keys)]
    new_keys = frame["_key"].tolist()
    frame = frame.drop(columns="_key")

    if not frame.empty:
        review_file.parent.mkdir(parents=True, exist_ok=True)
        has_data = review_file.exists() and review_file.stat().st_size > 0
        frame.to_csv(
            review_file,
            mode="a",
            header=not has_data,
            index=False,
            encoding="utf-8" if has_data else "utf-8-sig",
        )
        existing_keys.update(new_keys)

    buffer.clear()
    return len(frame)


def crawl_reviews(project_dir: Path, limit: int | None = None) -> None:
    product_dir = project_dir / "01_craw_data" / "product"
    review_dir = project_dir / "01_craw_data" / "review"
    review_file = review_dir / "product_review.csv"

    existing_keys = load_existing_keys(review_file)
    crawl = load_products_to_crawl(
        product_dir / "product_detail.csv",
        product_dir / "product_seller.csv",
        review_file,
    )
    if limit is not None:
        crawl = crawl.head(limit)

    print(f"Review keys đã có: {len(existing_keys):,}")
    print(f"Sản phẩm cần crawl: {len(crawl):,}")
    if crawl.empty:
        print("Không có sản phẩm thiếu review theo review_count hiện tại.")
        return

    session = create_session()
    buffer: list[dict] = []
    failed: list[str] = []
    saved_count = 0
    completed = 0
    stopped = False

    try:
        for row in crawl.itertuples(index=False):
            product_id = normalize_key_value(row.product_id)
            seller_product_id = normalize_key_value(row.seller_product_id)
            seller_id = normalize_key_value(row.seller_id) or None
            product_reviews: list[dict] = []
            page = 1
            product_ok = True
            hit_existing = False

            while True:
                params = {
                    "limit": PAGE_LIMIT,
                    "include": "contribute_info",
                    "sort": SORT_MODE,
                    "page": page,
                    "spid": seller_product_id,
                    "product_id": product_id,
                }
                if seller_id:
                    params["seller_id"] = seller_id

                data, status = fetch_reviews_page(session, params)
                if status == "stop":
                    print(f"[HTTP 401/403] Bị chặn tại product_id={product_id}; dừng crawl.")
                    product_ok = False
                    stopped = True
                    break
                if status != "ok" or data is None:
                    failed.append(product_id)
                    print(f"[Lỗi] product_id={product_id}, page={page}; sẽ thử lại ở lần chạy sau.")
                    product_ok = False
                    break

                items = data.get("data") or []
                for review in items:
                    review_id = review.get("id")
                    key = make_review_key(review_id, product_id, seller_product_id)
                    if key in existing_keys:
                        hit_existing = True
                        break

                    timeline = review.get("timeline") or {}
                    creator = review.get("created_by") or {}
                    product_reviews.append({
                        "review_id": review_id,
                        "product_id": product_id,
                        "seller_product_id": seller_product_id,
                        "seller_id": seller_id,
                        "rating": review.get("rating"),
                        "title": review.get("title"),
                        "content": review.get("content"),
                        "review_created_at": timeline.get("review_created_date"),
                        "purchased": creator.get("purchased"),
                        "thank_count": review.get("thank_count"),
                        "comment_count": review.get("comment_count"),
                        "is_photo": review.get("is_photo"),
                        "crawl_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    })

                last_page = (data.get("paging") or {}).get("last_page", 1)
                if hit_existing or not items or page >= last_page:
                    break
                page += 1
                time.sleep(random.uniform(*REQUEST_DELAY))

            if stopped:
                break
            if product_ok:
                buffer.extend(product_reviews)
                completed += 1

            if completed and completed % SAVE_EVERY_N == 0:
                saved_count += append_new_reviews(buffer, review_file, existing_keys)
                print(f"[{completed}/{len(crawl)}] mới lưu {saved_count:,} review; lỗi {len(failed)}")
            time.sleep(random.uniform(*REQUEST_DELAY))
    finally:
        saved_count += append_new_reviews(buffer, review_file, existing_keys)
        session.close()

    print("Crawl bị dừng do bị chặn." if stopped else "Crawl hoàn tất.")
    print(f"Sản phẩm crawl xong: {completed:,}/{len(crawl):,}")
    print(f"Review mới đã nối: {saved_count:,}")
    print(f"Sản phẩm lỗi: {len(failed):,}")
    print(f"File: {review_file}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Incrementally crawl Tiki product reviews")
    parser.add_argument("--limit", type=int, default=None, help="Giới hạn số sản phẩm để crawl thử")
    args = parser.parse_args()
    data_root = Path(os.environ.get("TIKI_DATA_DIR", find_project_dir())).resolve()
    crawl_reviews(data_root, limit=args.limit)


if __name__ == "__main__":
    main()
