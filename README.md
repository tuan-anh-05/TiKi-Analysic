# TiKi-Analysic

Pipeline thu thập dữ liệu Tiki, tiền xử lý CSV, nạp dữ liệu mới vào Supabase PostgreSQL và phân tích bằng Power BI.

## Cấu trúc

- `01_craw_data/`: script/notebook crawl và CSV thô, chia theo `category`, `daily_price`, `product`, `review`.
- `02_process_raw_data/`: script/notebook tiền xử lý và CSV sạch theo các nhóm dữ liệu.
- `03_push_to_sql/`: schema Supabase PostgreSQL, script nạp gia tăng, checkpoint và log.
- `.github/workflows/tiki-data-pipeline.yml`: lịch tự động crawl, xử lý và nạp.
- `requirements.txt`: thư viện Python cần cài.

## Cài đặt và chạy thủ công

```bash
python -m pip install -r requirements.txt
python 02_process_raw_data/run_all_preprocessing.py
python 03_push_to_sql/push_to_sql/push_to_database.py
```

Để crawl thủ công, chạy script tương ứng trong `01_craw_data/`. Hướng dẫn chi tiết ở README của từng thư mục.

## Lịch GitHub Actions

Workflow dùng giờ `Asia/Bangkok` (UTC+7). GitHub cron được khai báo theo UTC; workflow tự phân loại từng lượt chạy:

| Giờ địa phương (UTC) | Tác vụ |
|---|---|
| 18:00 (11:00 UTC) mỗi ngày | Crawl daily price lần 1 |
| 21:00 (14:00 UTC) mỗi ngày | Crawl daily price lần 2 |
| 02:00 (19:00 UTC ngày trước) mỗi ngày | Crawl/tiếp tục crawl product theo checkpoint |
| 10:00 (03:00 UTC) ngày mùng 1 | Crawl review |
| 22:00 (15:00 UTC) mỗi ngày | Chạy `02_process_raw_data/run_all_preprocessing.py` |
| 23:00 (16:00 UTC) mỗi ngày | Chạy `03_push_to_sql/push_to_sql/push_to_database.py` |

“21:00 sáng” được hiểu là 21:00 (9 giờ tối). Product chạy mỗi ngày lúc 02:00 để checkpoint tiếp tục snapshot tháng hiện tại; nếu chưa crawl hết trong giới hạn thời gian của một lượt, lượt ngày sau sẽ tiếp tục. Review chỉ chạy một lần vào 10:00 ngày mùng 1.

Có thể chạy thủ công trong tab **Actions → Tiki scheduled data pipeline → Run workflow**, chọn tác vụ cần chạy.

Product có giới hạn tối đa 350 phút mỗi lượt GitHub Actions và lưu checkpoint để tiếp tục ở lượt chạy hằng ngày kế tiếp cho đến khi hoàn tất snapshot.

## Kết nối Supabase

Local: khai báo `SUPABASE_DB_URL` trong `.env` ở thư mục gốc. `.env` đã có trong `.gitignore`; không commit file này. Trên GitHub, tạo repository secret cùng tên tại **Settings → Secrets and variables → Actions**. Workflow cần quyền `contents: write` để commit CSV/checkpoint.

Push chỉ thêm những khóa chưa có trong PostgreSQL. Nếu chạy lại cùng ngày/tháng, bản ghi đã tồn tại được bỏ qua; bản ghi mới phát sinh vẫn được nạp. Chi tiết khóa từng bảng và kết nối Power BI xem tại [03_push_to_sql/README.md](03_push_to_sql/README.md).

Hai CSV review lớn được quản lý bằng Git LFS. Cần bật Git LFS cho repository và theo dõi quota lưu trữ/băng thông LFS.
