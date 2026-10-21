"""
Tiền xử lý dữ liệu Danh mục (Category & Sub-category)
Đọc từ: 01_craw_data/category/
Ghi vào: 02_process_raw_data/category/
"""

from pathlib import Path
import pandas as pd


def get_project_dir() -> Path:
    current = Path(__file__).resolve().parent if "__file__" in globals() else Path.cwd().resolve()
    for p in [current, *current.parents]:
        if (p / "01_craw_data").is_dir():
            return p
    return current


def clean_categories(input_file: Path, output_file: Path) -> pd.DataFrame:
    print(f"\n[Category] Đang xử lý: {input_file.name}")
    df = pd.read_csv(input_file, encoding="utf-8-sig", low_memory=False)
    initial_len = len(df)

    # 1. Chuẩn hóa chuỗi và loại bỏ khoảng trắng thừa
    str_cols = ["name", "urlKey", "src"]
    for c in str_cols:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
            df[c] = df[c].replace({"nan": None, "None": None, "": None})

    # 2. Chuẩn hóa category_id
    df["category_id"] = pd.to_numeric(df["category_id"], errors="coerce").astype("int64")
    df = df.dropna(subset=["category_id"])

    # 3. Loại bỏ trùng lặp
    df = df.drop_duplicates(subset=["category_id"], keep="first").reset_index(drop=True)

    # 4. Sắp xếp
    df = df.sort_values(by="category_id").reset_index(drop=True)

    # 5. Lưu kết quả
    output_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"  ✓ Trước: {initial_len:,} dòng | Sau: {len(df):,} dòng (loại {initial_len - len(df):,} dòng trùng/lỗi)")
    print(f"  ✓ Đã lưu file sạch tại: {output_file}")
    return df


def clean_sub_categories(input_file: Path, output_file: Path, valid_cat_ids: set = None) -> pd.DataFrame:
    print(f"\n[Sub-Category] Đang xử lý: {input_file.name}")
    df = pd.read_csv(input_file, encoding="utf-8-sig", low_memory=False)
    initial_len = len(df)

    # 1. Chuẩn hóa category_id gốc
    df["category_id"] = pd.to_numeric(df["category_id"], errors="coerce").astype("int64")

    # 2. Chuẩn hóa các cột ID phân cấp (1 đến 5) sang kiểu số nguyên có thể chứa null (Int64)
    id_cols = [c for c in df.columns if c.endswith("_id") and c != "category_id"]
    for col in id_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # 3. Chuẩn hóa các cột tên danh mục con
    name_cols = [c for c in df.columns if not c.endswith("_id")]
    for col in name_cols:
        df[col] = df[col].astype(str).str.strip()
        df[col] = df[col].replace({"nan": None, "None": None, "": None, "<NA>": None})

    # 4. Lọc theo danh mục hợp lệ nếu có
    if valid_cat_ids is not None:
        df = df[df["category_id"].isin(valid_cat_ids)]

    # 5. Loại bỏ bản ghi trùng lặp hoàn toàn
    df = df.drop_duplicates().reset_index(drop=True)

    # 6. Sắp xếp theo thứ tự phân cấp
    sort_cols = ["category_id", "sub_category_1_id", "sub_category_2_id", "sub_category_3_id"]
    available_sort = [c for c in sort_cols if c in df.columns]
    df = df.sort_values(by=available_sort, na_position="last").reset_index(drop=True)

    # 7. Lưu kết quả
    output_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"  ✓ Trước: {initial_len:,} dòng | Sau: {len(df):,} dòng (loại {initial_len - len(df):,} dòng)")
    print(f"  ✓ Đã lưu file sạch tại: {output_file}")
    return df


def main():
    project_dir = get_project_dir()
    raw_dir = project_dir / "01_craw_data" / "category"
    proc_dir = project_dir / "02_process_raw_data" / "category"

    print("=" * 60)
    print("BẮT ĐẦU TIỀN XỬ LÝ DỮ LIỆU DANH MỤC")
    print("=" * 60)

    cat_file = raw_dir / "category.csv"
    sub_cat_file = raw_dir / "sub_category.csv"

    out_cat = proc_dir / "category.csv"
    out_sub_cat = proc_dir / "sub_category.csv"

    df_cat = clean_categories(cat_file, out_cat)
    valid_ids = set(df_cat["category_id"])
    clean_sub_categories(sub_cat_file, out_sub_cat, valid_ids)

    print("\nHoàn tất tiền xử lý danh mục!")


if __name__ == "__main__":
    main()

