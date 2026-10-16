# 01 - Craw data

- `input/`: danh mục và danh sách ID đầu vào.
- `code/`: notebook và các script crawl.
- `data/`: toàn bộ CSV thu thập được, để chung một cấp theo yêu cầu.

Chạy từ thư mục gốc project:

```bash
python 01_craw_data/code/crawl_product_base.py
python 01_craw_data/code/crawl_price_history.py
```

Notebook `tiki_craw.ipynb` có cell cấu hình đường dẫn ở đầu. Chạy cell đó trước các cell còn lại.
