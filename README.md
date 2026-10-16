# TiKi-Analysic

Project phân tích hành vi và xu hướng tiêu dùng trên sàn TMĐT Tiki.

## Cấu trúc

- `01_craw_data/`: file đầu vào, code crawl và CSV thu thập được.
- `02_process_raw_data/`: code làm sạch và dữ liệu đã xử lý.
- `03_push_to_sql/`: vị trí dành cho script nạp dữ liệu, SQL và log.

## Chạy script

Chạy các lệnh sau tại thư mục gốc `TiKi-Analysic/`:

```bash
python 01_craw_data/code/crawl_product_base.py
python 01_craw_data/code/crawl_price_history.py
python 02_process_raw_data/code/clean_reviews.py
```

Cài thư viện cần thiết trước khi chạy:

```bash
pip install pandas requests
```

**Lưu ý:** Script crawl gọi API Tiki; việc truy cập phụ thuộc vào trạng thái và quyền truy cập của API. Hãy thử với một vài dòng trước khi crawl số lượng lớn. Các file CSV có thể chứa dữ liệu lớn; cân nhắc có đưa chúng lên GitHub hay không.
