# 02 — Process raw data

Chuẩn hóa CSV từ `01_craw_data/` cho mục tiêu phân tích và nạp PostgreSQL.

## Các nhóm dữ liệu

- `category/process_category.py`: chuẩn hóa danh mục tĩnh.
- `daily_price/process_daily_price.py`: xử lý mọi ngày hợp lệ có trong raw, nối các khóa `(product_id, seller_product_id, crawl_date)` chưa có vào lịch sử sạch. Cách này cho phép bù ngày khi lượt preprocess bị trễ hoặc retry sau nửa đêm.
- `product/process_product.py`: lọc listing/detail/seller/product-category theo tháng hiện tại, chuẩn hóa ID và ghi snapshot tháng ra các CSV sạch.
- `review/process_review.py`: xử lý theo chunk; chuẩn hóa dữ liệu và chỉ nối các review mới theo khóa ba cột `(review_id, product_id, seller_product_id)`.
- `run_all_preprocessing.py`: chạy các module tiền xử lý theo thứ tự.
- `pareto/pareto_category.ipynb`: notebook phân tích Pareto.

## Lưu ý về ghi file

Daily price và review được tích lũy, không ghi đè lịch sử. Các CSV product sạch là snapshot của tháng hiện tại và được tạo lại từ dữ liệu raw có `crawl_date` thuộc tháng đó. Bản raw vẫn được giữ để có thể xử lý lại.

## Chạy

Từ thư mục gốc:

```bash
python 02_process_raw_data/run_all_preprocessing.py
```

Hoặc chạy từng file `process_*.py` trong thư mục dữ liệu tương ứng. Workflow tự động gọi master script hằng ngày lúc 22:00 giờ `Asia/Bangkok`.
