# Crawl dữ liệu Tiki (Extract / Bronze)

Thư mục này gọi API của Tiki và ghi dữ liệu nguồn vào CSV. Khi chạy qua Airflow, script vẫn làm việc trên CSV working copy trong repo được mount vào container; `minio_task.py` khôi phục phiên bản MinIO trước khi chạy và đồng bộ file thay đổi vào `current/bronze/` sau đó. Git/LFS CSV là dữ liệu khởi tạo, MinIO là nơi lưu bền cho lần chạy Docker.

## Các nhóm dữ liệu

| Thư mục | Script/nguồn vào | Kết quả và grain |
|---|---|---|
| `category/` | `category.csv`, `sub_category.csv`; notebook khám phá | Danh sách category và đường phân cấp sub-category. Là nguồn tương đối tĩnh, thường nạp một lần. |
| `daily_price/` | `crawl_daily_price.py`; danh sách `product_pareto.csv` | `daily_price_history.csv`: một sản phẩm/biến thể ở một ngày crawl; `crawl_daily_log.csv`: log kết quả/ lỗi crawl. Sản phẩm đã crawl thành công trong ngày được bỏ qua ở lần sau. |
| `product/` | `product.py` cùng category | `product.csv` (listing), `product_detail.csv` (thuộc tính sản phẩm), `product_seller.csv` (seller/offer), `product_category.csv` (quan hệ sản phẩm-category). Các file product là snapshot theo tháng. |
| `review/` | `review.py` cùng dữ liệu sản phẩm/seller | `product_review.csv`: review gắn product/biến thể; khóa nhận diện `(review_id, product_id, seller_product_id)`. |

Tên cột CSV có thể phản ánh trực tiếp payload nguồn; từ điển cột DB/analytics xem [schema Supabase](../03_push_to_sql/README.md#mo-hinh-du-lieu-bang-dong-cot). Crawler không tự sửa schema cơ sở dữ liệu.

## Cơ chế tiếp tục và giới hạn

- CSV được tích lũy; script không xóa lịch sử trước khi crawl.
- File lớn được flush định kỳ để giữ phần đã thu thập nếu tiến trình bị ngắt.
- Crawler product lưu checkpoint theo danh mục/tháng để có thể tiếp tục phần chưa hoàn tất.
- Daily price giữ log; sản phẩm trả 404/410 được tạm ngừng thử lại theo logic script.
- Review loại trùng theo bộ ba ID và dùng thứ tự API để dừng khi gặp review đã cũ.
- Crawl phụ thuộc API/mạng, có thể lâu và bị giới hạn phía Tiki. Không chạy nhiều job cùng lúc trên cùng CSV; DAG đặt `max_active_runs=1`.

## Chạy thủ công (không cần Airflow)

Từ thư mục gốc repo, trong môi trường Python có các dependency crawler:

```bash
python 01_craw_data/daily_price/crawl_daily_price.py
python 01_craw_data/product/product.py
python 01_craw_data/review/review.py
```

Daily price cần `product_pareto.csv`; product cần category; review cần thông tin product/variant. Chạy xong thủ công không tự đẩy dữ liệu lên MinIO hoặc Supabase: cần chạy transform và loader riêng. Luồng Docker/Airflow đầy đủ nằm trong [README tổng](../README.md#chay-tu-dau-bang-docker-tren-windows).
