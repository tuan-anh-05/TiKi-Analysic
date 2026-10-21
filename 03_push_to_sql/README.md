# 03 — Supabase PostgreSQL và Power BI

Nạp dữ liệu sạch trong `02_process_raw_data/` vào Supabase PostgreSQL. Workflow chạy file push lúc 23:00 mỗi ngày.

## Thành phần

- `create_database/create_tables_postgres.sql`: tạo schema và khóa chống trùng.
- `push_to_sql/push_to_database.py`: đối chiếu khóa nguồn với khóa trong PostgreSQL, chỉ insert các bản ghi chưa có.
- `dim_product`: dimension một dòng cho mỗi `product_id`, được tái đồng bộ từ snapshot mới nhất trong `product_detail`.
- `vw_daily_price_analysis`, `vw_review_analysis`: view bổ sung tên/brand/type sản phẩm để Power BI dùng thuận tiện.
- `sync_checkpoint_supabase.json`: trạng thái phục vụ category tĩnh và thống kê đồng bộ; không quyết định bỏ qua daily/product/review. File được lưu trong repo để các lượt Actions kế tiếp dùng lại.
- `logs/`: log tại máy chạy.

## Quy tắc chọn bản ghi mới

Trước khi insert, script đọc tập khóa hiện có từ đúng bảng Supabase rồi so từng dòng CSV. Do đó lượt chạy lại cùng ngày/tháng sẽ bỏ qua các dòng đã có nhưng vẫn lấy được khóa mới được crawl sau lượt trước. PostgreSQL unique keys và `ON CONFLICT DO NOTHING` là lớp bảo vệ bổ sung nếu job bị chạy lại sau khi insert dở dang.

| Bảng | Khóa đối chiếu |
|---|---|
| `fact_daily_price` | `product_id`, `seller_product_id`, `crawl_date` |
| `dim_product_listing` | `product_id`, `seller_product_id`, `category_id`, `crawl_date` |
| `product_detail` | `product_id`, `crawl_date` |
| `product_seller` | `product_id`, `seller_product_id`, `seller_id`, `crawl_date` |
| `product_category_map` | `product_id`, `category_id`, `crawl_date` |
| `fact_review` | `review_id`, `product_id`, `seller_product_id` |

Các snapshot product dùng `crawl_date` dạng `YYYY-MM`; daily price dùng `YYYY-MM-DD`. Review cho phép `seller_id` rỗng, vì vậy cột này không thuộc khóa review.

## Cấu hình Supabase

Local, lưu `SUPABASE_DB_URL` trong `.env` ở thư mục gốc; file đã bị ignore bởi Git. Trên GitHub, thêm repository secret `SUPABASE_DB_URL` tại **Settings → Secrets and variables → Actions**. Không ghi password vào script, notebook hay README.

```bash
python -m pip install -r requirements.txt
python 03_push_to_sql/push_to_sql/push_to_database.py
```

Workflow cần `contents: write` để cập nhật CSV và checkpoint trong repo. Git LFS cần bật vì các file review vượt giới hạn file thường của GitHub.

## Kết nối Power BI

Power BI Desktop → **Get data → PostgreSQL database**. Host lấy từ Supabase Dashboard → **Connect**, database `postgres`, bật SSL/SSL mode `Require`; đăng nhập bằng user và database password của Supabase. Có thể bắt đầu từ `dim_product`, `dim_category`, `fact_daily_price`, `fact_review`, hoặc dùng hai view `vw_daily_price_analysis` và `vw_review_analysis`. Nếu mạng chặn cổng 5432, dùng thông tin pooler tại Supabase Dashboard → **Connect**.

`product_detail` và các bảng product khác là lịch sử snapshot tháng; `dim_product` là trạng thái mới nhất. Loader chỉ thêm snapshot/review/giá có khóa chưa tồn tại, nhưng cập nhật `dim_product` khi có snapshot tháng mới hơn. Category được nạp một lần theo checkpoint; loader không xóa category đã có.
