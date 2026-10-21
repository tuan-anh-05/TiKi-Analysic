# Bộ nạp PostgreSQL

`push_to_database.py` đọc CSV sạch trong `02_process_raw_data/`, lấy các khóa hiện có từ từng bảng Supabase, rồi chỉ insert các khóa chưa tồn tại. Chạy lặp lại an toàn nhờ unique keys và `ON CONFLICT DO NOTHING`.

```bash
python -m pip install -r requirements.txt
python 03_push_to_sql/push_to_sql/push_to_database.py
```

Script lấy `SUPABASE_DB_URL` từ biến môi trường hoặc file `.env` bị Git ignore ở thư mục gốc. Trong Actions, cấu hình GitHub Secret cùng tên.

Xem [hướng dẫn thư mục 03](../README.md) để biết khóa đối chiếu, lịch chạy và cách kết nối Power BI.
