# SQL schema Supabase

File [`create_tables_postgres.sql`](create_tables_postgres.sql) định nghĩa schema phân tích mà loader và Power BI sử dụng. File tạo 9 bảng (`dim_category`, `dim_sub_category`, `dim_product_listing`, `product_detail`, `dim_product`, `product_seller`, `product_category_map`, `fact_daily_price`, `fact_review`), các index/unique constraints và hai view `vw_daily_price_analysis`, `vw_review_analysis`.

## Tạo schema

Trong Supabase Dashboard mở **SQL Editor**, dán/chạy toàn bộ file. Đây là thao tác một lần cho database mới. Loader cũng chạy DDL trước khi insert, nhưng tạo schema thủ công giúp xác nhận target sẵn sàng.

`IF NOT EXISTS` giúp chạy lại với bảng chưa tồn tại; nó **không đổi** kiểu cột/constraint của bảng cũ. Trước khi áp dụng repo lên DB đã có schema khác, so sánh với SQL hiện tại và viết migration phù hợp. Không chạy lệnh xóa bảng tùy tiện vì có thể mất dữ liệu.

## Quy tắc schema cần biết

- `dim_category.category_id` là PK; `dim_sub_category.category_id` tham chiếu nó.
- `product_detail` lưu lịch sử theo PK `(product_id,crawl_date)`; `dim_product` lưu trạng thái mới nhất theo `product_id`.
- `dim_product_listing` có PK ghép sản phẩm/variant/category/tháng.
- `product_seller` dùng unique index gồm product, variant, seller đã chuẩn hóa NULL, tháng.
- `product_category_map` unique theo product/category/tháng.
- `fact_daily_price` unique theo product/variant/ngày.
- `fact_review` PK gồm review/product/variant; rating bị giới hạn 1–5.
- Product `crawl_date` là text tháng `YYYY-MM`; daily price là kiểu DATE. Hai ý nghĩa thời gian này khác nhau.
- Hai view dùng LEFT JOIN với dimension hiện tại. Nếu dimension chưa có product, dòng fact vẫn xuất hiện và thuộc tính sản phẩm có thể NULL.

Từ điển dòng/cột có giải thích tiếng Việt trong [README chính](../../README.md#mo-hinh-du-lieu-bang-dong-cot); câu lệnh SQL là chuẩn cuối cùng về kiểu dữ liệu và constraint.
