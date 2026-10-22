# Làm sạch và chuẩn hóa (Transform / Silver)

Các script ở đây đọc CSV raw trong `01_craw_data/`, chuẩn hóa dữ liệu cho phân tích và ghi CSV đã xử lý. Qua Airflow, adapter MinIO tải raw cần thiết về working copy, chạy transform, rồi đồng bộ kết quả lên prefix `current/silver/`. Raw được giữ để có thể xử lý lại.

## Các bước

| Module | Việc thực hiện / đầu ra |
|---|---|
| `category/process_category.py` | Chuẩn hóa category và đường phân cấp; đầu ra `category/category.csv`, `category/sub_category.csv`. |
| `daily_price/process_daily_price.py` | Xử lý các ngày hợp lệ từ raw, nối các khóa `(product_id, seller_product_id, crawl_date)` chưa có vào `daily_price/daily_price_history.csv`. Giúp bù ngày nếu transform chạy muộn hoặc retry sau nửa đêm. |
| `product/process_product.py` | Chuẩn hóa listing/detail/seller/product-category, lọc snapshot tháng hiện tại và ghi CSV product sạch. |
| `review/process_review.py` | Đọc theo chunk, chuẩn hóa và chỉ nối review mới theo `(review_id, product_id, seller_product_id)`. |
| `run_all_preprocessing.py` | Chạy các module trên theo thứ tự. |
| `validate_silver.py` | Kiểm tra schema, khóa bắt buộc, trùng khóa, định dạng ngày và miền giá trị trước khi nạp Gold. |
| `pareto/pareto_category.ipynb` | Notebook phân tích Pareto, không nằm trong DAG. |

## Cách hiểu dữ liệu đầu ra

- Daily price và review là lịch sử tích lũy, không ghi đè các ngày/review đã có.
- Các CSV product là snapshot tháng hiện tại và được tạo từ raw có `crawl_date` thuộc tháng đó; chúng không phải lịch sử snapshot lâu dài trong cùng file sạch.
- Các khóa giúp tránh lặp khi chạy lại, nhưng không sửa nội dung của một key cũ trong bảng fact.
- Cột `crawl_date` của product biểu diễn tháng (`YYYY-MM`), daily price biểu diễn ngày (`YYYY-MM-DD`).

## Chạy thủ công

Từ thư mục gốc:

```bash
python 02_process_raw_data/run_all_preprocessing.py
```

Hoặc chạy module `process_*.py` riêng. Cần raw đầu vào tương ứng. Lệnh thủ công chỉ cập nhật CSV local; để đồng bộ MinIO hãy gọi adapter trong Compose, còn để nạp Supabase cần chạy loader. Trong DAG, preprocess chạy sau lượt giá 21:00, từ 22:00 giờ Việt Nam (`Asia/Ho_Chi_Minh`). Xem [README tổng](../README.md) cho end-to-end.

Kiểm tra Silver thủ công sau preprocessing:

```bash
python 02_process_raw_data/validate_silver.py
```

Nếu validation thất bại, sửa hoặc chạy lại preprocessing trước khi nạp Gold. Airflow cũng chạy kiểm tra này trước task load Supabase.
