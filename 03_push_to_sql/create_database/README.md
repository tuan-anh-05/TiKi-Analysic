# PostgreSQL schema

`create_tables_postgres.sql` khai báo các bảng Supabase dùng cho Power BI: category, product snapshot, daily price và review. Script `push_to_database.py` tự chạy DDL này trước khi nạp dữ liệu.

Khóa chính/unique trong schema khớp với khóa đối chiếu của loader; `crawl_date` của snapshot tháng dùng chuỗi `YYYY-MM`, còn daily price dùng kiểu ngày.

Có thể chạy DDL thủ công trong Supabase Dashboard → **SQL Editor**. Với project đã có bảng từ schema cũ, cần migration các bảng trước khi chạy schema mới; `CREATE TABLE IF NOT EXISTS` không tự đổi kiểu cột hoặc khóa cũ.
