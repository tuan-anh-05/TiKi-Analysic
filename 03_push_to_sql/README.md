# Supabase PostgreSQL và Power BI (Load / Gold)

Phần này nhận CSV sạch từ `02_process_raw_data/`, nạp incremental vào Supabase PostgreSQL và cung cấp bảng/view cho BI. Airflow gọi loader sau preprocessing; MinIO giữ CSV silver và checkpoint ở `current/state/`. PostgreSQL trong Docker là metadata Airflow riêng, không phải database này.

## Nội dung thư mục

- [`create_database/create_tables_postgres.sql`](create_database/create_tables_postgres.sql): bảng, khóa, index và hai view.
- [`push_to_sql/push_to_database.py`](push_to_sql/push_to_database.py): tạo schema nếu thiếu, lọc khóa đã tồn tại và chèn bản ghi mới.
- `sync_checkpoint_supabase.json`: tiến độ/checkpoint local; trong luồng Airflow được sync vào MinIO. Không commit secrets hoặc checkpoint sinh ra.
- `push_to_sql/logs/`: log loader.

## Quy tắc nạp

| Bảng | Khóa so sánh để tìm dòng mới |
|---|---|
| `fact_daily_price` | `(product_id, seller_product_id, crawl_date)` |
| `dim_product_listing` | `(product_id, seller_product_id, category_id, crawl_date)` |
| `product_detail` | `(product_id, crawl_date)` |
| `product_seller` | `(product_id, seller_product_id, seller_id, crawl_date)`; seller NULL được chuẩn hóa trong unique index. |
| `product_category_map` | `(product_id, category_id, crawl_date)` |
| `fact_review` | `(review_id, product_id, seller_product_id)`; `seller_id` không bắt buộc và không thuộc khóa. |

Loader đọc các khóa trong target trước khi insert. Unique key và `ON CONFLICT DO NOTHING` bảo vệ lần chạy lặp/retry sau khi nạp dở. Daily price, product snapshot và review không tự cập nhật lại dòng cùng key; `dim_product` được làm mới từ snapshot mới nhất. Category là dữ liệu tĩnh, chỉ nạp một lần theo checkpoint; nếu DB rỗng nhưng checkpoint cũ thì loader cho phép nạp lại.

## Từ điển dữ liệu

Định nghĩa chính xác về grain, cột và ý nghĩa từng bảng nằm trong [README tổng](../README.md#mo-hinh-du-lieu-bang-dong-cot); SQL schema là nguồn chính thức về kiểu dữ liệu/constraint.

| Bảng/view | Grain / nội dung |
|---|---|
| `dim_category` | Một dòng mỗi category Tiki. |
| `dim_sub_category` | Đường phân cấp subcategory dưới category cha. |
| `dim_product_listing` | Sản phẩm/variant xuất hiện trong category ở snapshot tháng. |
| `product_detail` | Thuộc tính sản phẩm mỗi tháng snapshot; giữ lịch sử. |
| `dim_product` | Thuộc tính snapshot mới nhất, một dòng mỗi product ID. |
| `product_seller` | Quan hệ offer/seller theo tháng snapshot. |
| `product_category_map` | Quan hệ product-category theo tháng snapshot. |
| `fact_daily_price` | Giá/chỉ số một product variant trong một ngày crawl. |
| `fact_review` | Một review theo ID review + product + variant. |
| `vw_daily_price_analysis` | Daily price bổ sung tên/brand/type từ `dim_product`. |
| `vw_review_analysis` | Review bổ sung tên/brand/type từ `dim_product`. |

## Kết nối Power BI

Power BI Desktop → **Get data → PostgreSQL database**; lấy host/port/user/database từ Supabase → **Connect**. Database thường `postgres`; bật SSL mode `Require`; dùng DB password. Nên chọn **Import** để hạn chế kết nối mở lâu. Bắt đầu với hai view analysis, tạo biểu đồ giá theo ngày/brand, chiết khấu, rating/review, rồi **Refresh** sau khi Airflow loader thành công. Nếu cổng direct không dùng được, lấy thông số pooler trong Supabase Connect. Pooler có giới hạn kết nối nên tránh mở nhiều DirectQuery/session cùng lúc.

## Tạo schema và chạy loader

Tạo schema một lần bằng SQL Editor Supabase với [`create_tables_postgres.sql`](create_database/create_tables_postgres.sql). Loader cũng thực thi DDL trước khi nạp. `CREATE TABLE IF NOT EXISTS` không migration bảng cũ; so sánh cấu trúc trước khi nâng cấp schema.

Trong Compose, dùng task Airflow `push` (task này chạy preprocess trước). Có thể chạy loader trực tiếp từ root nếu cài requirements và cung cấp `SUPABASE_DB_URL`:

```bash
python 03_push_to_sql/push_to_sql/push_to_database.py
```

Từ repo root dùng `.env` ở `04_orchestration/airflow/.env` hoặc export connection URL cho tiến trình. Không để connection string chứa password trong shell history công khai hoặc source control.
