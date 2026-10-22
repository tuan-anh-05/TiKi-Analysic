# Loader incremental Supabase

`push_to_database.py` đọc các CSV sạch trong `02_process_raw_data/`, kết nối Supabase PostgreSQL, tạo các bảng nếu chưa tồn tại, đối chiếu khóa và insert những dòng mới. Sau khi load, script đồng bộ snapshot mới nhất vào `dim_product`. MinIO adapter khôi phục CSV/checkpoint trước khi chạy và lưu các file đổi sau đó.

## Đầu vào và kết quả

Nguồn gồm category, product listing/detail/seller/category-map, daily price và review. Tên file/đường dẫn cụ thể và schema nằm trong [README `03_push_to_sql`](../README.md) và [SQL DDL](../create_database/create_tables_postgres.sql). Kết quả là các bảng Supabase và views `vw_daily_price_analysis`, `vw_review_analysis`.

Khóa incremental: daily price `(product_id,seller_product_id,crawl_date)`; product listing `(product_id,seller_product_id,category_id,crawl_date)`; product detail `(product_id,crawl_date)`; seller `(product_id,seller_product_id,seller_id,crawl_date)`; product-category `(product_id,category_id,crawl_date)`; review `(review_id,product_id,seller_product_id)`. Unique constraints và `ON CONFLICT DO NOTHING` giúp retry sau khi insert dở không nhân đôi record. Dòng cùng khóa không nhất thiết được update; `dim_product` là ngoại lệ được đồng bộ từ snapshot tháng mới nhất.

## Cấu hình kết nối

Loader đọc `SUPABASE_DB_URL` từ environment hoặc `.env` ở project root (trong Docker biến được nạp từ `04_orchestration/airflow/.env`). URI có password thật và SSL; password chứa ký tự reserved cần percent-encode. Không lưu URI có secret trong Git.

## Chạy

Khuyến nghị chạy qua Airflow: trigger DAG config `{"task":"push"}`. Lệnh sẽ chạy preprocessing trước rồi loader. Chạy trực tiếp cần Python dependencies của project và biến kết nối:

```bash
python 03_push_to_sql/push_to_sql/push_to_database.py
```

Chạy thủ công trực tiếp chỉ cập nhật Supabase; trong pipeline Compose adapter còn đồng bộ file/checkpoint vào MinIO. Log được ghi trong `logs/` cạnh loader. Checkpoint hỗ trợ category tĩnh và trạng thái thống kê; với phần incremental chính, target DB và unique keys là cơ chế chống trùng quan trọng.

Nếu lỗi kết nối, xem host/port/password/SSL và giới hạn pooler; nếu lỗi column/constraint, so schema hiện có với DDL trước khi chạy lại. Chi tiết chạy cả stack ở [README tổng](../../README.md).
