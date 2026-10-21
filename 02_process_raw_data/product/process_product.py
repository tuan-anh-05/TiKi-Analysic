"""
Tiền xử lý dữ liệu Sản phẩm (Product, Detail, Seller, Category)
Đọc từ: 01_craw_data/product/
Ghi vào: 02_process_raw_data/product/
"""

from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
import pandas as pd


def get_project_dir() -> Path:
    current = Path(__file__).resolve().parent if "__file__" in globals() else Path.cwd().resolve()
    for p in [current, *current.parents]:
        if (p / "01_craw_data").is_dir():
            return p
    return current


def filter_current_month(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only rows whose crawl_date belongs to the month this script runs."""
    current_month = datetime.now(ZoneInfo("Asia/Bangkok")).strftime("%Y-%m")
    df["crawl_date"] = df["crawl_date"].astype(str).str.strip().str[:7]
    filtered = df.loc[df["crawl_date"] == current_month].copy()
    print(f"  ✓ Lọc crawl_date tháng {current_month}: giữ {len(filtered):,}/{len(df):,} dòng")
    return filtered


def strip_embedded_bom(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.select_dtypes(include=["object", "string"]).columns:
        df[col] = df[col].map(lambda value: value.replace("\ufeff", "") if isinstance(value, str) else value)
    return df


def clean_product_listing(input_file: Path, output_file: Path) -> pd.DataFrame:
    print(f"\n[Product Listing] Đang xử lý: {input_file.name}")
    df = pd.read_csv(input_file, encoding="utf-8-sig", low_memory=False)
    df = strip_embedded_bom(df)
    initial_len = len(df)
    df = filter_current_month(df)

    for col in ["product_id", "seller_product_id", "category_id"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    df["crawl_date"] = df["crawl_date"].astype(str).str.strip().str[:7]

    df = df.dropna(subset=["product_id", "seller_product_id", "category_id"])
    for col in ["product_id", "seller_product_id", "category_id"]:
        df[col] = df[col].astype("int64")
    df = df.drop_duplicates(subset=["product_id", "seller_product_id", "category_id", "crawl_date"], keep="first")
    df = df.sort_values(by=["category_id", "product_id"]).reset_index(drop=True)

    output_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"  ✓ Trước: {initial_len:,} dòng | Sau: {len(df):,} dòng (loại {initial_len - len(df):,} dòng)")
    print(f"  ✓ Đã lưu file sạch tại: {output_file}")
    return df


def clean_product_detail(input_file: Path, output_file: Path) -> pd.DataFrame:
    print(f"\n[Product Detail] Đang xử lý: {input_file.name}")
    df = pd.read_csv(input_file, encoding="utf-8-sig", low_memory=False)
    df = strip_embedded_bom(df)
    initial_len = len(df)
    df = filter_current_month(df)

    # 1. Chuẩn hóa ID
    df["product_id"] = pd.to_numeric(df["product_id"], errors="coerce").astype("Int64")
    df["master_id"] = pd.to_numeric(df["master_id"], errors="coerce").astype("Int64")
    df["sku"] = df["sku"].astype(str).str.strip().replace({"nan": None, "None": None, "": None})

    # 2. Chuẩn hóa chuỗi văn bản
    for c in ["product_name", "url_key"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
            df[c] = df[c].replace({"nan": None, "None": None, "": None})

    # 3. Chuẩn hóa thương hiệu
    df["brand_id"] = pd.to_numeric(df["brand_id"], errors="coerce").astype("Int64")
    if "brand_name" in df.columns:
        df["brand_name"] = df["brand_name"].astype(str).str.strip()
        df["brand_name"] = df["brand_name"].replace({"nan": "No Brand", "None": "No Brand", "": "No Brand"})

    # 4. Chuẩn hóa chỉ số số học
    df["favourite_count"] = pd.to_numeric(df["favourite_count"], errors="coerce").fillna(0).astype("int64")
    df["review_count"] = pd.to_numeric(df["review_count"], errors="coerce").fillna(0).astype("int64")
    df["all_time_quantity_sold"] = pd.to_numeric(df["all_time_quantity_sold"], errors="coerce").fillna(0).astype("Int64")

    df["rating_average"] = pd.to_numeric(df["rating_average"], errors="coerce").fillna(0.0).round(2)
    df["rating_average"] = df["rating_average"].clip(lower=0.0, upper=5.0)

    # 5. Chuẩn hóa ngày tạo & ngày crawl
    df["created_date"] = pd.to_datetime(df["created_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["crawl_date"] = df["crawl_date"].astype(str).str.strip().str[:7]

    # 6. Loại bỏ cột 100% rỗng hoặc điền mặc định
    if "product_type" in df.columns:
        df["product_type"] = df["product_type"].fillna("standard").astype(str)

    # 7. Khử trùng lặp trên product_id
    df = df.dropna(subset=["product_id"])
    df["product_id"] = df["product_id"].astype("int64")
    df = df.drop_duplicates(subset=["product_id", "crawl_date"], keep="last").reset_index(drop=True)
    df = df.sort_values(by="product_id").reset_index(drop=True)

    # 8. Lưu kết quả
    output_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"  ✓ Trước: {initial_len:,} dòng | Sau: {len(df):,} dòng (loại {initial_len - len(df):,} dòng)")
    print(f"  ✓ Đã lưu file sạch tại: {output_file}")
    return df


def clean_product_seller(input_file: Path, output_file: Path) -> pd.DataFrame:
    print(f"\n[Product Seller] Đang xử lý: {input_file.name}")
    df = pd.read_csv(input_file, encoding="utf-8-sig", low_memory=False)
    df = strip_embedded_bom(df)
    initial_len = len(df)
    df = filter_current_month(df)

    # 1. Chuẩn hóa ID
    df["product_id"] = pd.to_numeric(df["product_id"], errors="coerce").astype("Int64")
    df["seller_product_id"] = pd.to_numeric(df["seller_product_id"], errors="coerce").astype("Int64")
    df["seller_id"] = pd.to_numeric(df["seller_id"], errors="coerce").astype("Int64")
    df["store_id"] = pd.to_numeric(df["store_id"], errors="coerce").astype("Int64")

    # 2. Chuẩn hóa văn bản
    for c in ["seller_name", "seller_link"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
            df[c] = df[c].replace({"nan": None, "None": None, "": None})

    # 3. Chuẩn hóa giá bán seller
    df["seller_price"] = pd.to_numeric(df["seller_price"], errors="coerce").fillna(0).astype("int64")

    # 4. Chuẩn hóa kiểu logic (boolean)
    df["is_current_seller"] = df["is_current_seller"].astype(str).str.lower().isin(["true", "1"])
    df["is_best_store"] = df["is_best_store"].astype(str).str.lower().isin(["true", "1"])

    # 5. Chuẩn hóa ngày crawl
    df["crawl_date"] = df["crawl_date"].astype(str).str.strip().str[:7]

    # 6. Khử trùng lặp
    dedup_cols = ["product_id", "seller_product_id", "seller_id", "crawl_date"]
    df = df.dropna(subset=["product_id", "seller_product_id"])
    df["product_id"] = df["product_id"].astype("int64")
    df["seller_product_id"] = df["seller_product_id"].astype("int64")
    df = df.drop_duplicates(subset=dedup_cols, keep="first").reset_index(drop=True)
    df = df.sort_values(by=["product_id", "is_current_seller"], ascending=[True, False]).reset_index(drop=True)

    # 7. Lưu kết quả
    output_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"  ✓ Trước: {initial_len:,} dòng | Sau: {len(df):,} dòng (loại {initial_len - len(df):,} dòng)")
    print(f"  ✓ Đã lưu file sạch tại: {output_file}")
    return df


def clean_product_category(input_file: Path, output_file: Path) -> pd.DataFrame:
    print(f"\n[Product Category] Đang xử lý: {input_file.name}")
    df = pd.read_csv(input_file, encoding="utf-8-sig", low_memory=False)
    df = strip_embedded_bom(df)
    initial_len = len(df)
    df = filter_current_month(df)

    # 1. Chuẩn hóa ID
    df["product_id"] = pd.to_numeric(df["product_id"], errors="coerce").astype("Int64")
    df["category_id"] = pd.to_numeric(df["category_id"], errors="coerce").astype("Int64")
    df["category_level"] = pd.to_numeric(df["category_level"], errors="coerce").fillna(1).astype("int64")

    # 2. Chuẩn hóa tên và URL
    df["category_name"] = df["category_name"].astype(str).str.strip()
    df["category_name"] = df["category_name"].replace({"nan": None, "None": None, "": None})

    if "category_url" in df.columns:
        df["category_url"] = df["category_url"].astype(str).str.strip()
        # Chuẩn hóa URL tương đối thành URL đầy đủ
        is_relative = df["category_url"].str.startswith("/")
        df.loc[is_relative, "category_url"] = "https://tiki.vn" + df.loc[is_relative, "category_url"]
        df["category_url"] = df["category_url"].replace({"nan": None, "None": None, "": None})

    # 3. Chuẩn hóa cờ logic boolean
    df["is_leaf"] = df["is_leaf"].astype(str).str.lower().isin(["true", "1"])
    df["is_primary"] = df["is_primary"].astype(str).str.lower().isin(["true", "1"])

    # 4. Chuẩn hóa crawl_date
    df["crawl_date"] = df["crawl_date"].astype(str).str.strip().str[:7]

    # 5. Khử trùng lặp trên (product_id, category_id)
    dedup_cols = ["product_id", "category_id", "crawl_date"]
    df = df.dropna(subset=["product_id", "category_id"])
    df["product_id"] = df["product_id"].astype("int64")
    df["category_id"] = df["category_id"].astype("int64")
    df = df.drop_duplicates(subset=dedup_cols, keep="first").reset_index(drop=True)
    df = df.sort_values(by=["product_id", "category_level"]).reset_index(drop=True)

    # 6. Lưu kết quả
    output_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"  ✓ Trước: {initial_len:,} dòng | Sau: {len(df):,} dòng (loại {initial_len - len(df):,} dòng)")
    print(f"  ✓ Đã lưu file sạch tại: {output_file}")
    return df


def main():
    project_dir = get_project_dir()
    raw_dir = project_dir / "01_craw_data" / "product"
    proc_dir = project_dir / "02_process_raw_data" / "product"

    print("=" * 60)
    print("BẮT ĐẦU TIỀN XỬ LÝ DỮ LIỆU SẢN PHẨM")
    print("=" * 60)

    clean_product_listing(raw_dir / "product.csv", proc_dir / "product.csv")
    clean_product_detail(raw_dir / "product_detail.csv", proc_dir / "product_detail.csv")
    clean_product_seller(raw_dir / "product_seller.csv", proc_dir / "product_seller.csv")
    clean_product_category(raw_dir / "product_category.csv", proc_dir / "product_category.csv")

    print("\nHoàn tất tiền xử lý product!")


if __name__ == "__main__":
    main()

