"""
Tiền xử lý dữ liệu Đánh giá Sản phẩm (Product Reviews)
Đọc từ: 01_craw_data/review/
Ghi vào: 02_process_raw_data/review/
"""

from pathlib import Path
import pandas as pd


def get_project_dir() -> Path:
    current = Path(__file__).resolve().parent if "__file__" in globals() else Path.cwd().resolve()
    for p in [current, *current.parents]:
        if (p / "01_craw_data").is_dir():
            return p
    return current


REVIEW_KEY_COLS = ["review_id", "product_id", "seller_product_id"]


def review_key(row) -> tuple:
    """Build a stable composite key, representing missing IDs as None."""
    return tuple(None if pd.isna(value) else int(value) for value in row)


def clean_reviews(input_file: Path, output_file: Path, chunksize: int = 150000) -> None:
    print(f"\n[Product Review] Đang xử lý: {input_file.name} (xử lý theo khối {chunksize:,} dòng)")
    output_file.parent.mkdir(parents=True, exist_ok=True)

    seen_review_keys = set()
    total_raw = 0
    total_saved = 0
    first_chunk = not output_file.exists() or output_file.stat().st_size == 0

    # Đọc các khóa ghép đã xử lý từ lần chạy trước để chỉ nối review mới.
    if not first_chunk:
        for existing_keys in pd.read_csv(
            output_file,
            usecols=REVIEW_KEY_COLS,
            dtype={col: "string" for col in REVIEW_KEY_COLS},
            chunksize=chunksize,
            low_memory=False,
        ):
            for col in REVIEW_KEY_COLS:
                existing_keys[col] = existing_keys[col].str.replace("\ufeff", "", regex=False).str.strip()
            for col in REVIEW_KEY_COLS:
                existing_keys[col] = pd.to_numeric(existing_keys[col], errors="coerce").astype("Int64")
            seen_review_keys.update(
                review_key(row)
                for row in existing_keys[REVIEW_KEY_COLS].itertuples(index=False, name=None)
            )
        print(f"  -> Đã nạp {len(seen_review_keys):,} khóa review có sẵn để lọc trùng.")

    # Duyệt file theo chunk để tối ưu bộ nhớ
    for chunk_idx, chunk in enumerate(pd.read_csv(input_file, chunksize=chunksize, low_memory=False)):
        total_raw += len(chunk)

        # Clean embedded BOMs that may have been written by older append runs.
        for col in chunk.select_dtypes(include=["object", "string"]).columns:
            chunk[col] = chunk[col].map(lambda value: value.replace("\ufeff", "") if isinstance(value, str) else value)

        # 1. Chuẩn hóa ID
        chunk["review_id"] = pd.to_numeric(chunk["review_id"], errors="coerce").astype("Int64")
        chunk["product_id"] = pd.to_numeric(chunk["product_id"], errors="coerce").astype("Int64")
        chunk["seller_product_id"] = pd.to_numeric(chunk["seller_product_id"], errors="coerce").astype("Int64")
        chunk["seller_id"] = pd.to_numeric(chunk["seller_id"], errors="coerce").astype("Int64")

        # Loại bỏ bản ghi không có review_id hoặc product_id
        chunk = chunk.dropna(subset=["review_id", "product_id"])

        # 2. Loại bỏ trùng lặp theo khóa ghép ba cột đầu
        chunk = chunk.drop_duplicates(subset=REVIEW_KEY_COLS, keep="first")
        chunk_keys = [
            review_key(row)
            for row in chunk[REVIEW_KEY_COLS].itertuples(index=False, name=None)
        ]
        is_new = [key not in seen_review_keys for key in chunk_keys]
        chunk = chunk.loc[is_new].copy()
        seen_review_keys.update(key for key, keep in zip(chunk_keys, is_new) if keep)

        # 3. Chuẩn hóa điểm đánh giá (rating)
        chunk["rating"] = pd.to_numeric(chunk["rating"], errors="coerce").fillna(5).astype("int64")
        chunk["rating"] = chunk["rating"].clip(lower=1, upper=5)

        # 4. Chuẩn hóa chuỗi văn bản (title, content)
        for text_col in ["title", "content"]:
            if text_col in chunk.columns:
                chunk[text_col] = chunk[text_col].astype(str).str.strip()
                chunk[text_col] = chunk[text_col].replace({"nan": None, "None": None, "": None})

        # 5. Chuẩn hóa thời gian review_created_at & crawl_time
        chunk["review_created_at"] = pd.to_datetime(
            chunk["review_created_at"], errors="coerce"
        ).dt.strftime("%Y-%m-%d %H:%M:%S")
        chunk["crawl_time"] = pd.to_datetime(
            chunk["crawl_time"], errors="coerce"
        ).dt.strftime("%Y-%m-%d %H:%M:%S")

        # 6. Chuẩn hóa cờ logic boolean
        chunk["purchased"] = chunk["purchased"].astype(str).str.lower().isin(["true", "1"])
        chunk["is_photo"] = chunk["is_photo"].astype(str).str.lower().isin(["true", "1"])

        # 7. Chuẩn hóa số lượng cảm ơn và phản hồi
        chunk["thank_count"] = pd.to_numeric(chunk["thank_count"], errors="coerce").fillna(0).astype("int64")
        chunk["comment_count"] = pd.to_numeric(chunk["comment_count"], errors="coerce").fillna(0).astype("int64")

        # 8. Sắp xếp các cột theo thứ tự chuẩn
        cols_order = [
            "review_id", "product_id", "seller_product_id", "seller_id",
            "rating", "title", "content", "review_created_at",
            "purchased", "thank_count", "comment_count", "is_photo", "crawl_time"
        ]
        chunk = chunk[cols_order]

        # 9. Chỉ nối các review mới vào file đã xử lý.
        if not chunk.empty:
            has_data = output_file.exists() and output_file.stat().st_size > 0
            chunk.to_csv(
                output_file,
                mode="w" if first_chunk else "a",
                header=first_chunk,
                index=False,
                encoding="utf-8" if has_data else "utf-8-sig"
            )
            first_chunk = False
        total_saved += len(chunk)
        print(f"  -> Xong khối {chunk_idx + 1}: đã lưu lũy kế {total_saved:,} dòng...")

    print(f"\n  ✓ Tổng đọc: {total_raw:,} dòng | Review mới được nối: {total_saved:,} dòng")
    print(f"  ✓ File review tích lũy: {output_file}")


def main():
    project_dir = get_project_dir()
    raw_dir = project_dir / "01_craw_data" / "review"
    proc_dir = project_dir / "02_process_raw_data" / "review"

    print("=" * 60)
    print("BẮT ĐẦU TIỀN XỬ LÝ DỮ LIỆU ĐÁNH GIÁ (REVIEWS)")
    print("=" * 60)

    clean_reviews(raw_dir / "product_review.csv", proc_dir / "product_review.csv")
    print("\nHoàn tất tiền xử lý review!")


if __name__ == "__main__":
    main()

