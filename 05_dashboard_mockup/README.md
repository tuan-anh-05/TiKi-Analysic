# Dashboard phân tích Tiki

Dashboard là một trang HTML tĩnh. Dữ liệu được lấy từ Supabase lúc dựng trang và nhúng sẵn vào tệp, nên người xem chỉ cần mở tệp bằng trình duyệt.

## Yêu cầu
- Windows với PowerShell 5.1 trở lên (có sẵn trên Windows 10/11).
- Tệp `.env` chứa địa chỉ và khóa **công khai** (publishable key) của dự án Supabase. Khóa này do người phụ trách dự án cung cấp; không dùng secret key hoặc mật khẩu cơ sở dữ liệu.

## Cách chạy
1. Sao chép `.env.example` thành `.env`, điền `SUPABASE_URL` và `SUPABASE_KEY`.
2. Mở PowerShell tại thư mục này và chạy:

```powershell
powershell -ExecutionPolicy Bypass -File .\build.ps1
```

3. Chương trình tạo `data.json` và `dashboard_v2.html`, sau đó tự mở dashboard trên trình duyệt.

Lần đầu có thể báo lỗi hết thời gian chờ vì truy vấn nguội; chạy lại một lần nữa là được.

## Cấu trúc
| Tệp | Vai trò |
|---|---|
| `build.ps1` | Chạy toàn bộ: xuất dữ liệu rồi ghép vào giao diện |
| `export_data_supabase.ps1` | Đọc các view trên Supabase, ghi `data.json` |
| `supabase_client.ps1` | Hàm kết nối REST, tự phân trang và thử lại |
| `supabase_views.sql` | Câu lệnh tạo view và bảng tổng hợp (chạy một lần trong SQL Editor của Supabase) |
| `dashboard_v2_template.html` | Giao diện, có chỗ trống `__DATA__` để nhúng dữ liệu |

## Bổ sung chuỗi theo ngày từ CSV (tuỳ chọn)
Nếu dữ liệu giá mới đã thu về nhưng chưa nạp vào Supabase, thêm vào `.env` dòng `DAILY_CSV=<đường dẫn tới daily_price_history.csv đã làm sạch>`. Khi tệp CSV có nhiều ngày đầy đủ hơn Supabase, `build.ps1` dựng chuỗi theo ngày từ CSV (script `daily_series_from_csv.ps1`); khi Supabase bắt kịp thì tự dùng lại Supabase. Xóa dòng này để chỉ dùng Supabase.

## Làm mới dữ liệu
Sau khi hệ thống nạp dữ liệu mới vào Supabase, chạy trong SQL Editor:

```sql
REFRESH MATERIALIZED VIEW public.mv_dash_review_stars;
REFRESH MATERIALIZED VIEW public.mv_dash_review_aspect;
REFRESH MATERIALIZED VIEW public.mv_dash_daily_series;
```

rồi chạy lại `build.ps1`.

## Tự động cập nhật dashboard hằng ngày
Dashboard là trang tĩnh nên không tự đổi khi Supabase có dữ liệu mới; cần dựng lại. Để việc này tự chạy:

1. **Làm mới bảng tổng hợp trên Supabase:** bật tiện ích `pg_cron`, rồi chạy `supabase_schedule_refresh.sql` một lần (lịch 22:30 giờ Việt Nam).
2. **Đưa tệp `github_workflow_dashboard.yml` vào `.github/workflows/dashboard.yml`** ở thư mục gốc kho GitHub (sửa `DASH_DIR` nếu cần).
3. **Thêm hai secret** ở Settings → Secrets and variables → Actions: `SUPABASE_URL` và `SUPABASE_KEY` (khóa công khai).
4. **Bật Pages:** Settings → Pages → Source = **GitHub Actions**.

Từ đó mỗi ngày lúc 23:00 giờ Việt Nam, GitHub dựng lại dashboard và đăng lên trang Pages. Có thể chạy tay ở tab Actions → Cập nhật dashboard → Run workflow.

## Lưu ý bảo mật
- Không đưa `.env` lên GitHub (đã nằm trong `.gitignore`).
- `data.json` và `dashboard_v2.html` là tệp sinh ra, không cần đưa lên GitHub.
