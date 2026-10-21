"""
Tiền xử lý dữ liệu Theo dõi Giá Hằng Ngày (Daily Price)
Đọc từ: 01_craw_data/daily_price/
Ghi vào: 02_process_raw_data/daily_price/
"""
from datetime import datetime
from pathlib import Path
import re
import pandas as pd
from zoneinfo import ZoneInfo


def get_project_dir() -> Path:
    current = Path(__file__).resolve().parent if "__file__" in globals() else Path.cwd().resolve()
    for p in [current, *current.parents]:
        if (p / "01_craw_data").is_dir():
            return p
    return current


def clean_daily_price_history(input_file: Path, output_file: Path) -> pd.DataFrame:
    print(f"\n[Daily Price History] Đang xử lý: {input_file.name}")
    df = pd.read_csv(input_file, encoding="utf-8-sig", low_memory=False)
    initial_len = len(df)

    for col in df.select_dtypes(include=["object", "string"]).columns:
        df[col] = df[col].map(lambda value: value.replace("\ufeff", "") if isinstance(value, str) else value)

    # 1. Chuẩn hóa ID
    id_cols = ["product_id", "seller_product_id", "category_id"]
    for col in id_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # seller_id có thể thiếu (null) -> ép kiểu Int64
    df["seller_id"] = pd.to_numeric(df["seller_id"], errors="coerce").astype("Int64")

    # 2. Chuẩn hóa ngày crawl (YYYY-MM-DD)
    df["crawl_date"] = pd.to_datetime(df["crawl_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    
    # Xác định ngày xử lý trước khi sử dụng biến today
    today = datetime.now(ZoneInfo("Asia/Bangkok")).strftime("%Y-%m-%d")
    print(f"Ngày xử lý: {today}")
    print("Phân bố trước lọc:")
    print(df["crawl_date"].value_counts(dropna=False).sort_index())
    
    # Chỉ giữ dữ liệu có crawl_date trùng ngày xử lý
    df = df.loc[df["crawl_date"] == today].copy()
    df = df.dropna(subset=id_cols + ["crawl_date"])
    for col in id_cols:
        df[col] = df[col].astype("int64")
    print("Phân bố sau lọc:")
    print(df["crawl_date"].value_counts(dropna=False).sort_index())
    print(f"Số dòng sau lọc: {len(df):,}")

    # 3. Chuẩn hóa giá và giảm giá
    price_cols = ["price", "list_price", "original_price"]
    for col in price_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("int64")

    # Kiểm tra & chuẩn hóa discount: discount = list_price - price (nếu hợp lý)
    if "discount" in df.columns:
        df["discount"] = pd.to_numeric(df["discount"], errors="coerce").fillna(0).astype("int64")
    else:
        df["discount"] = (df["list_price"] - df["price"]).clip(lower=0)

    # discount_rate: [0, 100]
    if "discount_rate" in df.columns:
        df["discount_rate"] = pd.to_numeric(df["discount_rate"], errors="coerce").fillna(0).astype("int64")
        df["discount_rate"] = df["discount_rate"].clip(lower=0, upper=100)

    # 4. Chuẩn hóa chỉ số đánh giá & bán hàng
    df["rating_average"] = pd.to_numeric(df["rating_average"], errors="coerce").fillna(0.0).round(2)
    df["rating_average"] = df["rating_average"].clip(lower=0.0, upper=5.0)

    df["review_count"] = pd.to_numeric(df["review_count"], errors="coerce").fillna(0).astype("int64")
    df["all_time_quantity_sold"] = pd.to_numeric(df["all_time_quantity_sold"], errors="coerce").fillna(0).astype("Int64")

    # 5. Chuẩn hóa trạng thái tồn kho
    if "inventory_status" in df.columns:
        df["inventory_status"] = df["inventory_status"].astype(str).str.strip().str.lower()
        df["inventory_status"] = df["inventory_status"].replace({"nan": "unknown", "none": "unknown"})

    # 6. Loại bỏ trùng lặp theo cặp (product_id, seller_product_id, crawl_date)
    dedup_cols = ["product_id", "seller_product_id", "crawl_date"]
    df = df.drop_duplicates(subset=dedup_cols, keep="last").reset_index(drop=True)

    # 7. Sắp xếp dữ liệu theo ngày và danh mục
    df = df.sort_values(by=["crawl_date", "category_id", "product_id"]).reset_index(drop=True)

    # 8. Nối các bản ghi mới vào lịch sử đã xử lý, không ghi mất các ngày cũ.
    output_file.parent.mkdir(parents=True, exist_ok=True)
    key_cols = ["product_id", "seller_product_id", "crawl_date"]
    if output_file.exists() and output_file.stat().st_size > 0:
        existing_keys = pd.read_csv(
            output_file,
            usecols=key_cols,
            dtype={col: "string" for col in key_cols},
            encoding="utf-8-sig",
            low_memory=False,
        )
        for col in key_cols:
            existing_keys[col] = existing_keys[col].str.replace("\ufeff", "", regex=False).str.strip()
        existing_keys["crawl_date"] = existing_keys["crawl_date"].str[:10]
        known_keys = set(map(tuple, existing_keys[key_cols].itertuples(index=False, name=None)))
        new_keys = list(map(tuple, df[key_cols].astype("string").itertuples(index=False, name=None)))
        new_df = df.loc[[key not in known_keys for key in new_keys]].copy()
    else:
        new_df = df

    if not new_df.empty:
        has_data = output_file.exists() and output_file.stat().st_size > 0
        new_df.to_csv(
            output_file,
            mode="a",
            header=not has_data,
            index=False,
            encoding="utf-8" if has_data else "utf-8-sig",
        )

    print(f"  ✓ Trước: {initial_len:,} dòng | Ngày hiện tại sau lọc: {len(df):,} dòng")
    print(f"  ✓ Dòng mới được nối: {len(new_df):,} | File lịch sử: {output_file}")
    return new_df


def main():
    project_dir = get_project_dir()
    raw_dir = project_dir / "01_craw_data" / "daily_price"
    proc_dir = project_dir / "02_process_raw_data" / "daily_price"

    print("=" * 60)
    print("BẮT ĐẦU TIỀN XỬ LÝ DỮ LIỆU THEO DÕI GIÁ HẰNG NGÀY")
    print("=" * 60)

    clean_daily_price_history(raw_dir / "daily_price_history.csv", proc_dir / "daily_price_history.csv")

    print("\nHoàn tất tiền xử lý daily_price!")


if __name__ == "__main__":
    main()

