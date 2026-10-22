# 01 — Crawl data

Chứa mã nguồn crawl và CSV Bronze ban đầu. Airflow gọi các script Python; MinIO là nơi lưu bền vững cho dữ liệu sau khi bootstrap. Notebook được giữ để khám phá/tham khảo, không nằm trong luồng chạy tự động.

## Thư mục và dữ liệu

- `category/`: `category.csv`, `sub_category.csv` và `crawl_category.ipynb`. Category được coi là dữ liệu tĩnh, crawl/nạp một lần.
- `daily_price/`: `crawl_daily_price.py` và notebook; đọc `product_pareto.csv`, nối lịch sử vào `daily_price_history.csv`, ghi log lỗi riêng. Chạy lần hai trong cùng ngày vẫn giữ dữ liệu lần một.
- `product/`: `product.py` và notebook; ghi listing, detail, seller, product-category vào các CSV cùng tên.
- `review/`: `review.py` và `reviews.ipynb`; thêm review theo khóa `(review_id, product_id, seller_product_id)`.

## Cơ chế lưu và chạy lại

- CSV crawl được nối thêm; file đã có không bị xóa để bắt đầu một lần crawl mới.
- Các file lớn được flush định kỳ để giữ phần dữ liệu đã thu thập khi lượt crawl bị gián đoạn.
- Product dùng checkpoint danh mục theo tháng; detail đã thu thập trong tháng được nhận diện để lần sau tiếp tục phần còn thiếu.
- Daily price giữ log lịch sử, và sản phẩm từng trả 404/410 chỉ tạm ngừng thử trong 30 ngày.
- Review bỏ qua khóa đã có và dừng phân trang khi gặp review cũ theo thứ tự API mới nhất trước.

## Chạy thủ công

Chạy từ thư mục gốc project:

```bash
python 01_craw_data/daily_price/crawl_daily_price.py
python 01_craw_data/product/product.py
python 01_craw_data/review/review.py
```

`daily_price` cần file đầu vào `01_craw_data/daily_price/product_pareto.csv`; product cần category đã có; review cần product detail và seller data. Chạy tự động qua Airflow; xem thiết lập tại [`../04_orchestration/airflow/README.md`](../04_orchestration/airflow/README.md).
