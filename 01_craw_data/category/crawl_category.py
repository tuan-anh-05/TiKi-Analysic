"""
Crawl danh mục và danh mục con từ Tiki.

1. Categories    -> 01_craw_data/category/category.csv
2. Sub_Category  -> 01_craw_data/category/sub_category.csv
"""

from pathlib import Path
from urllib.parse import urlparse
import os
import re
import time

import pandas as pd
import requests

# ==================================================
# CẤU HÌNH
# ==================================================
PROJECT_DIR = Path.cwd().resolve()
if not (PROJECT_DIR / "01_craw_data").is_dir():
    for parent in PROJECT_DIR.parents:
        if (parent / "01_craw_data").is_dir():
            PROJECT_DIR = parent
            break

DATA_ROOT = Path(os.environ.get("TIKI_DATA_DIR", PROJECT_DIR)).resolve()
CATEGORIES_DIR = DATA_ROOT / "01_craw_data" / "category"
CATEGORIES_DIR.mkdir(parents=True, exist_ok=True)

CATEGORIES_FILE = CATEGORIES_DIR / "category.csv"
SUB_CATEGORY_FILE = CATEGORIES_DIR / "sub_category.csv"

MENU_URL = "https://api.tiki.vn/raiden/v2/menu-config"
CATEGORY_URL = "https://tiki.vn/api/v2/categories"

SAVE_EVERY = 100
REQUEST_DELAY = 0.5

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://tiki.vn/",
}


# ==================================================
# 1. CATEGORIES
# ==================================================
def crawl_categories():
    with requests.Session() as session:
        session.headers.update(HEADERS)
        response = session.get(MENU_URL, params={"platform": "desktop"}, timeout=30)
        response.raise_for_status()
        items = response.json().get("menu_block", {}).get("items", [])

    categories = []
    for item in items:
        parts = urlparse(item.get("link") or "").path.strip("/").split("/")

        if len(parts) >= 2 and re.fullmatch(r"c\d+", parts[-1]):
            cid = parts[-1][1:]
            categories.append({
                "category_id": int(cid),
                "name": item.get("text"),
                "urlKey": parts[-2],
                "src": parts[-1],
            })

    if not categories:
        raise ValueError("Không lấy được danh mục nào, kiểm tra lại API menu-config.")

    df_categories = pd.DataFrame(categories).drop_duplicates("category_id")
    df_categories.to_csv(CATEGORIES_FILE, index=False, encoding="utf-8-sig")
    print(f"Đã lưu {len(df_categories)} danh mục vào {CATEGORIES_FILE}")


# ==================================================
# 2. SUB_CATEGORY
# ==================================================
def save_data(rows):
    if not rows:
        return

    new_data = pd.DataFrame(rows)

    if SUB_CATEGORY_FILE.exists():
        old_data = pd.read_csv(SUB_CATEGORY_FILE, low_memory=False)
        data = pd.concat([old_data, new_data], ignore_index=True, sort=False)
    else:
        data = new_data

    data = data.drop_duplicates(keep="last")
    data.to_csv(SUB_CATEGORY_FILE, index=False, encoding="utf-8-sig")
    rows.clear()


def get_children(session, parent_id):
    """Trả về danh sách con; None nếu gọi API lỗi."""
    try:
        response = session.get(
            CATEGORY_URL,
            params={"include": "children", "parent_id": parent_id},
            timeout=20,
        )
        response.raise_for_status()
        data = response.json()
        return data.get("data", data.get("children", []))
    except Exception as e:
        print(f"Lỗi category_id={parent_id}: {e}")
        return None


def dfs(session, root_id, root_name, parent_id, path, rows, counter):
    """Duyệt sâu cây danh mục, ghi mỗi nhánh lá thành một dòng.

    counter: dict {"total": int} dùng để đếm tổng số nhánh lá.
    """
    children = get_children(session, parent_id)

    if children is None:
        return False

    if not children:
        if path:
            row = {"category_id": root_id, "category_name": root_name}
            for level, item in enumerate(path, 1):
                row[f"sub_category_{level}_id"] = item["id"]
                row[f"sub_category_{level}"] = item["name"]
            rows.append(row)
            counter["total"] += 1

            if len(rows) >= SAVE_EVERY:
                save_data(rows)
        return True

    success = True
    for child in children:
        if child.get("id") is None:
            continue

        ok = dfs(
            session, root_id, root_name, child["id"],
            path + [{"id": child["id"], "name": child.get("name")}],
            rows, counter,
        )
        success = success and ok
        time.sleep(REQUEST_DELAY)
    return success


def crawl_sub_categories():
    if not CATEGORIES_FILE.exists():
        raise FileNotFoundError(
            f"Không tìm thấy {CATEGORIES_FILE}. Hãy chạy phần Crawl Categories trước."
        )

    df_roots = pd.read_csv(CATEGORIES_FILE, low_memory=False)
    id_col = "category_id" if "category_id" in df_roots.columns else "id"
    df_roots = df_roots.drop_duplicates(id_col)

    counter = {"total": 0}
    failed_roots = []
    rows = []

    try:
        with requests.Session() as session:
            session.headers.update(HEADERS)
            for _, root in df_roots.iterrows():
                start_count = counter["total"]
                root_id = int(root[id_col])
                root_name = root["name"]
                success = dfs(session, root_id, root_name, root_id, [], rows, counter)
                found = counter["total"] - start_count

                if not success:
                    failed_roots.append(root_name)
                    print(f"Crawl chưa hoàn tất: {root_name} ({found} nhánh lá đã lấy)")
                elif found > 0:
                    print(f"Crawl: {root_name} - {found} sub_categories")
                else:
                    print(f"Không tìm thấy danh mục con: {root_name}")
    finally:
        # Lưu phần còn trong bộ nhớ kể cả khi lỗi hoặc bị dừng giữa chừng.
        save_data(rows)

    print(f"Hoàn tất crawl sub-categories. Tổng nhánh lá: {counter['total']}")

    if failed_roots:
        print("Các danh mục cần kiểm tra/crawl lại:", failed_roots)


def main():
    crawl_categories()
    crawl_sub_categories()


if __name__ == "__main__":
    main()
