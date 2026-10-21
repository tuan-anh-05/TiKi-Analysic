"""
Pipeline Nạp Dữ Liệu Thông Minh (Smart Incremental Load) vào Database
Áp dụng cơ chế lọc chống trùng 4 quy tắc:
1. Category: Chỉ nạp 1 lần duy nhất, nếu đã có thì bỏ qua.
2. Daily Price: Đối chiếu từng khóa của ngày với các khóa trong PostgreSQL.
3. Monthly tables: Đối chiếu từng khóa snapshot với PostgreSQL, kể cả cùng tháng.
4. Review: Lọc theo BỘ 3 KHÓA KẾT HỢP (Composite Key):
   - review_id (Mã đánh giá)
   - product_id (Mã sản phẩm)
   - seller_product_id (Mã sản phẩm của gian hàng)
   -> Đảm bảo độ chính xác tuyệt đối, không bao giờ bị nhận diện nhầm giữa các biến thể/gian hàng.

Tất cả trạng thái nạp được ghi nhận vào file checkpoint riêng của Supabase.
"""
from datetime import datetime
import json
from pathlib import Path
import os
import psycopg2
import sys
import time
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert as pg_insert


def get_project_dir() -> Path:
    current = Path(__file__).resolve().parent if "__file__" in globals() else Path.cwd().resolve()
    for p in [current, *current.parents]:
        if (p / "02_process_raw_data").is_dir():
            return p
    return current


def load_checkpoint(checkpoint_file: Path) -> dict:
    default_cp = {
        "last_sync_time": None,
        "category": {
            "synced": False,
            "synced_at": None,
            "total_categories": 0,
            "total_sub_categories": 0,
        },
        "daily_price": {
            "synced_dates": [],
        },
        "monthly_snapshot": {
            "dim_product_listing": [],
            "product_detail": [],
            "product_seller": [],
            "product_category_map": [],
        },
        "review": {
            "composite_keys_count": 0,
            "key_structure": ["review_id", "product_id", "seller_product_id"],
            "last_sync_time": None,
        },
    }
    if checkpoint_file.exists():
        try:
            with open(checkpoint_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                for k, v in default_cp.items():
                    if k not in data:
                        data[k] = v
                return data
        except Exception as e:
            print(f"[CẢNH BÁO] Không đọc được file checkpoint ({e}), sử dụng mặc định.")
    return default_cp


def save_checkpoint(checkpoint_file: Path, data: dict):
    checkpoint_file.parent.mkdir(parents=True, exist_ok=True)
    data["last_sync_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(checkpoint_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def log_msg(log_file: Path, text: str):
    print(text)
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {text}\n")


def insert_ignore_conflicts(pd_table, connection, keys, data_iter):
    """Insert rows idempotently so a rerun after partial failure is safe."""
    rows = [dict(zip(keys, row)) for row in data_iter]
    if not rows:
        return 0
    statement = pg_insert(pd_table.table).values(rows).on_conflict_do_nothing()
    result = connection.execute(statement)
    return result.rowcount


def normalize_key_value(value):
    if pd.isna(value):
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value).strip()


def normalized_key(row):
    return tuple(normalize_key_value(value) for value in row)


def load_database_keys(cursor, table_name: str, key_columns: list[str]) -> set[tuple]:
    columns_sql = ", ".join(f'"{column}"' for column in key_columns)
    cursor.execute(f'SELECT {columns_sql} FROM "{table_name}";')
    return {normalized_key(row) for row in cursor.fetchall()}


def keep_rows_missing_from_database(
    df: pd.DataFrame,
    key_columns: list[str],
    existing_keys: set[tuple],
    required_key_columns: list[str] | None = None,
) -> tuple[pd.DataFrame, list[tuple]]:
    required = required_key_columns or key_columns
    df = df.dropna(subset=required).drop_duplicates(subset=key_columns, keep="first")
    keys = [normalized_key(row) for row in df[key_columns].itertuples(index=False, name=None)]
    is_new = [key not in existing_keys for key in keys]
    return df.loc[is_new].copy(), [key for key, keep in zip(keys, is_new) if keep]


def push_incremental_postgres(project_dir: Path, db_url: str, log_file: Path):
    # Keep Supabase state separate from the previous local database checkpoint.
    checkpoint_file = project_dir / "03_push_to_sql" / "sync_checkpoint_supabase.json"
    cp = load_checkpoint(checkpoint_file)

    log_msg(log_file, "Kết nối đến cơ sở dữ liệu Supabase PostgreSQL...")
    conn = psycopg2.connect(db_url)
    cursor = conn.cursor()
    engine = create_engine(db_url, pool_pre_ping=True)

    # Tạo bảng nếu chưa có (PostgreSQL dùng execute thay cho executescript)
    ddl_file = project_dir / "03_push_to_sql" / "create_database" / "create_tables_postgres.sql"
    if ddl_file.exists():
        with open(ddl_file, "r", encoding="utf-8") as f:
            cursor.execute(f.read())
        conn.commit()

    proc_dir = project_dir / "02_process_raw_data"
    total_new_rows = 0
    start_total = time.time()

    # ==============================================================================
    # QUY TẮC 1: Category - Chỉ nạp 1 lần duy nhất
    # ==============================================================================
    log_msg(log_file, "\n[QUY TẮC 1] Kiểm tra dữ liệu Danh mục (Category):")
    cursor.execute("SELECT EXISTS (SELECT 1 FROM dim_category LIMIT 1);")
    category_exists_in_database = cursor.fetchone()[0]
    if cp["category"]["synced"] and not category_exists_in_database:
        # A committed Actions checkpoint can outlive a reset/recreated Supabase DB.
        # Let the static category source repopulate an empty target.
        cp["category"]["synced"] = False
        log_msg(log_file, "  [INFO] Checkpoint cũ nhưng DB chưa có category; sẽ nạp lại nguồn tĩnh.")
    if cp["category"]["synced"]:
        log_msg(log_file, f"  -> [BỎ QUA] Danh mục đã được nạp trước đó vào ngày: {cp['category']['synced_at']}")
    else:
        cat_file = proc_dir / "category" / "category.csv"
        sub_file = proc_dir / "category" / "sub_category.csv"
        category_sources_complete = cat_file.exists() and sub_file.exists()
        if cat_file.exists():
            df_cat = pd.read_csv(cat_file, low_memory=False)
            df_cat.to_sql("dim_category", con=engine, if_exists="append", index=False, method=insert_ignore_conflicts, chunksize=1000)
            log_msg(log_file, f"  ✓ Đã nạp {len(df_cat):,} dòng vào [dim_category]")
            total_new_rows += len(df_cat)
            cp["category"]["total_categories"] = len(df_cat)

        if sub_file.exists():
            df_sub = pd.read_csv(sub_file, low_memory=False)
            df_sub.to_sql("dim_sub_category", con=engine, if_exists="append", index=False, method=insert_ignore_conflicts, chunksize=1000)
            log_msg(log_file, f"  ✓ Đã nạp {len(df_sub):,} dòng vào [dim_sub_category]")
            total_new_rows += len(df_sub)
            cp["category"]["total_sub_categories"] = len(df_sub)

        # Don't permanently mark a partial/missing source as loaded. A later run
        # can safely retry because category_id is unique and inserts ignore conflicts.
        if category_sources_complete:
            cp["category"]["synced"] = True
            cp["category"]["synced_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        else:
            log_msg(log_file, "  [CẢNH BÁO] Thiếu một trong hai CSV category; checkpoint vẫn để chưa đồng bộ.")
        conn.commit()

    # ==============================================================================
    # QUY TẮC 2: Daily Price - So sánh từng khóa với PostgreSQL, kể cả cùng ngày.
    # ==============================================================================
    log_msg(log_file, "\n[QUY TẮC 2] Kiểm tra dữ liệu Giá hằng ngày (Daily Price):")
    price_file = proc_dir / "daily_price" / "daily_price_history.csv"
    if price_file.exists():
        df_price = pd.read_csv(price_file, low_memory=False)
        daily_key_columns = ["product_id", "seller_product_id", "crawl_date"]
        db_price_keys = load_database_keys(cursor, "fact_daily_price", daily_key_columns)
        df_new_price, inserted_keys = keep_rows_missing_from_database(
            df_price, daily_key_columns, db_price_keys
        )
        if df_new_price.empty:
            log_msg(log_file, "  -> [BỎ QUA] Không có khóa giá mới trong database.")
        else:
            df_new_price.to_sql("fact_daily_price", con=engine, if_exists="append", index=False, method=insert_ignore_conflicts, chunksize=1000)
            log_msg(log_file, f"  ✓ Đã nạp {len(df_new_price):,} khóa giá chưa có trong database.")
            total_new_rows += len(df_new_price)
            db_price_keys.update(inserted_keys)
            cp["daily_price"]["synced_dates"] = sorted({key[2] for key in db_price_keys if key[2]})
            conn.commit()

    # ==============================================================================
    # QUY TẮC 3: Snapshot tháng - so sánh từng khóa với PostgreSQL.
    # ==============================================================================
    log_msg(log_file, "\n[QUY TẮC 3] Kiểm tra các bảng snapshot tháng (crawl_date: YYYY-MM):")
    monthly_tables = [
        ("dim_product_listing", "product/product.csv", ["product_id", "seller_product_id", "category_id", "crawl_date"]),
        ("product_detail", "product/product_detail.csv", ["product_id", "crawl_date"]),
        ("product_seller", "product/product_seller.csv", ["product_id", "seller_product_id", "seller_id", "crawl_date"]),
        ("product_category_map", "product/product_category.csv", ["product_id", "category_id", "crawl_date"]),
    ]

    for tbl_name, rel_csv, key_columns in monthly_tables:
        csv_fp = proc_dir / rel_csv
        if not csv_fp.exists():
            continue

        df_month = pd.read_csv(csv_fp, low_memory=False)
        df_month["crawl_date"] = df_month["crawl_date"].astype(str).str.strip().str[:7]
        df_month = df_month[df_month["crawl_date"].str.match(r"^\d{4}-\d{2}$", na=False)]
        db_month_keys = load_database_keys(cursor, tbl_name, key_columns)
        df_new_month, inserted_keys = keep_rows_missing_from_database(
            df_month,
            key_columns,
            db_month_keys,
            required_key_columns=[col for col in key_columns if col != "seller_id"],
        )
        if df_new_month.empty:
            log_msg(log_file, f"  -> [BỎ QUA] [{tbl_name}]: Không có khóa mới trong database.")
        else:
            df_new_month.to_sql(tbl_name, con=engine, if_exists="append", index=False, method=insert_ignore_conflicts, chunksize=1000)
            months_to_load = sorted(df_new_month["crawl_date"].dropna().unique().tolist())
            log_msg(log_file, f"  ✓ [{tbl_name}]: Đã nạp {len(df_new_month):,} khóa mới cho các tháng {months_to_load}.")
            total_new_rows += len(df_new_month)
            db_month_keys.update(inserted_keys)
            for m in months_to_load:
                if m not in cp["monthly_snapshot"][tbl_name]:
                    cp["monthly_snapshot"][tbl_name].append(m)
            conn.commit()

    # Keep a one-row-per-product dimension for Power BI while retaining the
    # complete month-by-month history in product_detail.
    cursor.execute("""
        INSERT INTO dim_product (
            product_id, master_id, sku, product_name, product_type, url_key,
            created_date, favourite_count, brand_id, brand_name,
            rating_average, review_count, all_time_quantity_sold,
            latest_crawl_month, updated_at
        )
        SELECT DISTINCT ON (product_id)
            product_id, master_id, sku, product_name, product_type, url_key,
            created_date, favourite_count, brand_id, brand_name,
            rating_average, review_count, all_time_quantity_sold,
            crawl_date, CURRENT_TIMESTAMP
        FROM product_detail
        ORDER BY product_id, crawl_date DESC
        ON CONFLICT (product_id) DO UPDATE SET
            master_id = EXCLUDED.master_id,
            sku = EXCLUDED.sku,
            product_name = EXCLUDED.product_name,
            product_type = EXCLUDED.product_type,
            url_key = EXCLUDED.url_key,
            created_date = EXCLUDED.created_date,
            favourite_count = EXCLUDED.favourite_count,
            brand_id = EXCLUDED.brand_id,
            brand_name = EXCLUDED.brand_name,
            rating_average = EXCLUDED.rating_average,
            review_count = EXCLUDED.review_count,
            all_time_quantity_sold = EXCLUDED.all_time_quantity_sold,
            latest_crawl_month = EXCLUDED.latest_crawl_month,
            updated_at = CURRENT_TIMESTAMP
        WHERE dim_product.latest_crawl_month <= EXCLUDED.latest_crawl_month;
    """)
    conn.commit()

    # ==============================================================================
    # QUY TẮC 4: Review - Lọc theo BỘ 4 KHÓA KẾT HỢP (COMPOSITE KEY)
    # ==============================================================================
    log_msg(log_file, "\n[QUY TẮC 4] Kiểm tra dữ liệu Đánh giá (fact_review) theo BỘ 3 KHÓA KẾT HỢP:")
    log_msg(log_file, "  -> Khóa lọc kết hợp: [review_id + product_id + seller_product_id]")

    rv_file = proc_dir / "review" / "product_review.csv"
    if rv_file.exists():
        cursor.execute("SELECT review_id, product_id, seller_product_id FROM fact_review;")
        existing_composite_keys = set(cursor.fetchall())
        log_msg(log_file, f"  -> Số khóa kết hợp hiện có trong database: {len(existing_composite_keys):,}")

        new_rv_count = 0
        chunksize = 150000

        for idx, chunk in enumerate(pd.read_csv(rv_file, chunksize=chunksize, low_memory=False)):
            for col in ["review_id", "product_id", "seller_product_id"]:
                chunk[col] = pd.to_numeric(chunk[col], errors="coerce")
            chunk = chunk.dropna(subset=["review_id", "product_id", "seller_product_id"])
            chunk["review_id"] = chunk["review_id"].astype("int64")
            chunk["product_id"] = chunk["product_id"].astype("int64")
            chunk["seller_product_id"] = chunk["seller_product_id"].astype("int64")
            chunk["seller_id"] = pd.to_numeric(chunk["seller_id"], errors="coerce").astype("Int64")

            chunk = chunk.drop_duplicates(subset=["review_id", "product_id", "seller_product_id"])
            chunk_keys = list(zip(chunk["review_id"], chunk["product_id"], chunk["seller_product_id"]))

            is_new_mask = [k not in existing_composite_keys for k in chunk_keys]
            new_chunk = chunk[is_new_mask]

            if not new_chunk.empty:
                new_chunk.to_sql("fact_review", con=engine, if_exists="append", index=False, method=insert_ignore_conflicts, chunksize=1000)
                new_rv_count += len(new_chunk)
                for k, is_new in zip(chunk_keys, is_new_mask):
                    if is_new:
                        existing_composite_keys.add(k)
                log_msg(log_file, f"  -> Khối {idx + 1}: Nạp thêm {len(new_chunk):,} review mới theo bộ khóa kết hợp...")

        if new_rv_count == 0:
            log_msg(log_file, "  -> [BỎ QUA] Toàn bộ review đã khớp bộ khóa kết hợp trong database, không có review mới.")
        else:
            log_msg(log_file, f"  ✓ Đã nạp thành công tổng cộng {new_rv_count:,} review mới vào [fact_review]")
            total_new_rows += new_rv_count
            cp["review"]["composite_keys_count"] = len(existing_composite_keys)
            cp["review"]["last_sync_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        conn.commit()

    save_checkpoint(checkpoint_file, cp)

    total_time = time.time() - start_total
    log_msg(log_file, "=" * 60)
    log_msg(log_file, "HOÀN THÀNH TIẾN TRÌNH NẠP THÔNG MINH LÊN SUPABASE!")
    log_msg(log_file, f"Tổng số dòng mới được chèn: {total_new_rows:,} dòng")
    log_msg(log_file, f"Thời gian thực thi: {total_time:.2f} giây")
    log_msg(log_file, f"Trạng thái đã lưu tại: {checkpoint_file}")
    log_msg(log_file, "=" * 60)

    conn.close()
    engine.dispose()


def main():
    project_dir = get_project_dir()
    logs_dir = project_dir / "03_push_to_sql" / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_file = logs_dir / f"push_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    log_msg(log_file, "=" * 60)
    log_msg(log_file, "KHỞI ĐỘNG TIẾN TRÌNH NẠP DỮ LIỆU LÊN SUPABASE POSTGRESQL")
    log_msg(log_file, "Chế độ: nạp gia tăng, chỉ thêm khóa chưa tồn tại trong PostgreSQL")
    log_msg(log_file, "=" * 60)

    # Ưu tiên biến môi trường (GitHub Actions Secrets); nếu không có thì đọc .env local.
    db_url = os.environ.get("SUPABASE_DB_URL")
    if not db_url:
        env_file = project_dir / ".env"
        if env_file.exists():
            for line in env_file.read_text(encoding="utf-8").splitlines():
                key, separator, value = line.partition("=")
                if separator and key.strip() == "SUPABASE_DB_URL":
                    db_url = value.strip().strip("\"'")
                    break
    if not db_url:
        log_msg(log_file, "[LỖI] Không tìm thấy biến môi trường SUPABASE_DB_URL!")
        sys.exit(1)

    push_incremental_postgres(project_dir, db_url, log_file)


if __name__ == "__main__":
    main()
