# 03 — Supabase PostgreSQL và Power BI

Nạp dữ liệu sạch trong `02_process_raw_data/` vào Supabase PostgreSQL. DAG Airflow local gọi bước push sau khi preprocessing thành công và sau mốc 23:00.

## Thành phần

- `create_database/create_tables_postgres.sql`: tạo schema và khóa chống trùng.
- `push_to_sql/push_to_database.py`: đối chiếu khóa nguồn với khóa trong PostgreSQL, chỉ insert các bản ghi chưa có.
- `dim_product`: dimension một dòng cho mỗi `product_id`, được tái đồng bộ từ snapshot mới nhất trong `product_detail`.
- `vw_daily_price_analysis`, `vw_review_analysis`: view bổ sung tên/brand/type sản phẩm để Power BI dùng thuận tiện.
- `sync_checkpoint_supabase.json`: trạng thái phục vụ category tĩnh và thống kê đồng bộ; không quyết định bỏ qua daily/product/review. Trong luồng Airflow, trạng thái này được đồng bộ vào object `current/state/` trong MinIO để giữ qua các lượt chạy.
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

## Từ điển dữ liệu (grain và ý nghĩa cột)

**Grain** là ý nghĩa của một dòng trong bảng. Các tên cột và constraint bên dưới khớp với `create_tables_postgres.sql`.

| Bảng | Một dòng đại diện cho | Ý nghĩa các cột |
|---|---|---|
| `dim_category` | Một danh mục Tiki. | `category_id`: ID danh mục, khóa chính; `name`: tên; `urlKey`: slug URL; `src`: nguồn/nhóm lấy danh mục; `created_at`: thời điểm tạo bản ghi trong DB. |
| `dim_sub_category` | Một cây phân cấp danh mục con gắn với danh mục cha. | `id`: ID dòng DB; `category_id`: khóa ngoại danh mục cha; `category_name`: tên danh mục cha; `sub_category_1_id`…`sub_category_5_id`: ID theo từng cấp; `sub_category_1`…`sub_category_5`: tên tương ứng ở từng cấp. Cấp không tồn tại có thể rỗng. |
| `dim_product_listing` | Một cặp sản phẩm/biến thể được thấy trong một danh mục ở một snapshot tháng. | `product_id`: ID sản phẩm; `seller_product_id`: ID biến thể/sản phẩm tại gian hàng; `category_id`: danh mục; `crawl_date`: tháng snapshot dạng `YYYY-MM`. Bốn cột này tạo khóa chính kết hợp. |
| `product_detail` | Một sản phẩm tại một tháng snapshot. | `product_id`: ID sản phẩm; `master_id`: ID sản phẩm cha/nhóm nếu API cung cấp; `sku`: mã SKU; `product_name`: tên; `product_type`: loại sản phẩm; `url_key`: slug URL; `created_date`: ngày tạo sản phẩm; `favourite_count`: lượt yêu thích; `brand_id`, `brand_name`: thương hiệu; `rating_average`: điểm trung bình; `review_count`: số đánh giá; `all_time_quantity_sold`: lượng bán tích lũy; `crawl_date`: tháng snapshot; `updated_at`: thời điểm lưu/cập nhật trong DB. Khóa chính là `(product_id, crawl_date)`. |
| `dim_product` | Phiên bản hiện tại/mới nhất của một sản phẩm, mỗi `product_id` một dòng. | Các cột thuộc tính sản phẩm giống `product_detail`; `latest_crawl_month` là tháng snapshot mới nhất dùng cập nhật dòng dimension; `updated_at` là thời điểm đồng bộ dimension. Bảng này không thay thế lịch sử trong `product_detail`. |
| `product_seller` | Một sản phẩm do một gian hàng bán trong một tháng snapshot. | `id`: ID dòng DB; `product_id`, `seller_product_id`, `seller_id`: định danh sản phẩm/biến thể/gian hàng; `seller_name`, `seller_link`: tên và URL gian hàng; `store_id`: ID cửa hàng; `seller_price`: giá phía seller khi crawl; `is_current_seller`: cờ seller hiện tại; `is_best_store`: cờ gian hàng tốt nhất theo dữ liệu nguồn; `crawl_date`: tháng snapshot. Khóa duy nhất là bộ product/seller/crawl month, chuẩn hóa seller_id rỗng. |
| `product_category_map` | Một liên kết sản phẩm–danh mục tại tháng snapshot. | `id`: ID dòng DB; `product_id`, `category_id`: hai đầu liên kết; `category_name`: tên danh mục; `category_level`: cấp danh mục; `category_url`: URL danh mục; `is_leaf`: danh mục lá hay không; `is_primary`: danh mục chính hay không; `crawl_date`: tháng snapshot. Khóa duy nhất `(product_id, category_id, crawl_date)`. |
| `fact_daily_price` | Giá và chỉ số bán của một biến thể trong một ngày crawl. | `id`: ID dòng DB; `product_id`, `seller_product_id`, `seller_id`, `category_id`: định danh/quan hệ; `crawl_date`: ngày quan sát; `price`: giá hiện tại; `list_price`: giá niêm yết; `original_price`: giá gốc nguồn; `discount`: số tiền giảm; `discount_rate`: tỷ lệ giảm; `rating_average`, `review_count`: chỉ số đánh giá tại thời điểm crawl; `all_time_quantity_sold`: lượng bán tích lũy; `inventory_status`: trạng thái tồn kho. Khóa duy nhất `(product_id, seller_product_id, crawl_date)`. |
| `fact_review` | Một review của một sản phẩm/biến thể. | `review_id`, `product_id`, `seller_product_id`: khóa chính kết hợp; `seller_id`: gian hàng nếu nguồn có; `rating`: điểm 1–5; `title`, `content`: tiêu đề/nội dung; `review_created_at`: thời điểm người mua tạo review; `purchased`: nguồn có xác nhận mua hàng hay không; `thank_count`: lượt cảm ơn; `comment_count`: số bình luận; `is_photo`: review có ảnh; `crawl_time`: thời điểm crawler thu thập. |

Hai view phân tích dùng `LEFT JOIN` tới `dim_product` theo `product_id`, vì vậy các dòng giá/review vẫn được giữ nếu thiếu thuộc tính dimension. View thêm `product_name`, `brand_name`, `product_type`.

## Cấu hình Supabase

Lưu `SUPABASE_DB_URL` trong `04_orchestration/airflow/.env`; file này đã bị ignore bởi Git. Không ghi password vào script, notebook hay README. Trong luồng chính, Airflow gọi loader sau khi transform thành công. Adapter đồng bộ checkpoint vào MinIO, không commit CSV hoặc checkpoint lên GitHub. Nếu cần chạy loader để gỡ lỗi, chạy trong container Airflow để dùng đúng môi trường và credentials.

## Kết nối Power BI

Power BI Desktop → **Get data → PostgreSQL database**. Host lấy từ Supabase Dashboard → **Connect**, database `postgres`, bật SSL/SSL mode `Require`; đăng nhập bằng user và database password của Supabase. Có thể bắt đầu từ `dim_product`, `dim_category`, `fact_daily_price`, `fact_review`, hoặc dùng hai view `vw_daily_price_analysis` và `vw_review_analysis`. Nếu mạng chặn cổng 5432, dùng thông tin pooler tại Supabase Dashboard → **Connect**.

`product_detail` và các bảng product khác là lịch sử snapshot tháng; `dim_product` là trạng thái mới nhất. Loader chỉ thêm snapshot/review/giá có khóa chưa tồn tại, nhưng cập nhật `dim_product` khi có snapshot tháng mới hơn. Category được nạp một lần theo checkpoint; loader không xóa category đã có.
