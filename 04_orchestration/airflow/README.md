# Airflow + MinIO chạy local

Thư mục này chứa Docker Compose, DAG và adapter đồng bộ MinIO. Airflow điều phối các script crawl/process/push hiện có; MinIO lưu các tệp trung gian có phiên bản; Supabase vẫn là PostgreSQL phục vụ phân tích.

## Vì sao dùng một DAG

`dags/tiki_pipeline.py` là một DAG duy nhất có nhánh theo lịch, điểm nối và các phụ thuộc rõ ràng:

```text
cổng lịch -> crawler được chọn -> extract_complete
          -> (chỉ lượt 21:00) chờ 22:00 -> tiền xử lý
          -> chờ 23:00 -> nạp Supabase
```

Các lượt crawl product/review kết thúc sau bước Extract. Lượt crawl giá 21:00 tiếp tục chạy Transform và Load. Airflow không chạy task sau nếu task trước lỗi; mỗi task được thử lại tối đa 2 lần, bắt đầu cách nhau 10 phút và tăng dần thời gian chờ, tối đa 1 giờ. Điểm nối bỏ qua các nhánh không đúng lịch nhưng không bỏ qua lỗi ở nhánh crawler được chọn. `max_active_runs=1` tránh hai lượt cùng sửa CSV đồng thời.

## Điều kiện và license MinIO

- Docker Desktop cho Windows, bật WSL 2.
- License MinIO AIStor Free Tier, đặt tên `minio.license` cạnh file Compose.
- Supabase đã có schema được tạo từ `03_push_to_sql/create_database/create_tables_postgres.sql`.

Tài liệu container chính thức hiện yêu cầu license để chạy AIStor và có Free Tier cho một compute resource. Repo MinIO Community cũ đã được lưu trữ và thông báo chuyển sang phân phối source-only; vì vậy Compose dùng image `quay.io/minio/aistor/minio:latest`, không dùng image Community cũ trên Docker Hub. Free Tier không có hỗ trợ. Xem [hướng dẫn cài AIStor container](https://docs.min.io/aistor/installation/container/install/) và [thông báo MinIO Community](https://github.com/minio/minio).

## Cấu hình lần đầu trên Windows

Mở PowerShell tại thư mục này:

```powershell
Copy-Item .env.example .env
```

Mở `.env`, đặt mật khẩu riêng cho Airflow/Postgres/MinIO và điền chuỗi kết nối Supabase. Với `AIRFLOW_DB_PASSWORD`, chỉ dùng chữ và số để tránh làm sai URL kết nối nội bộ Airflow. Không commit `.env` hoặc `minio.license`; Git ignore cả hai. Các giá trị trong file mẫu chỉ là ví dụ. Nếu password Supabase có ký tự đặc biệt dành riêng cho URL, hãy percent-encode nó trong PostgreSQL URL.

Tạo Fernet key bằng Python có sẵn trong Windows rồi thay `AIRFLOW_FERNET_KEY` trong `.env` bằng chuỗi được in ra:

```powershell
python -c "import base64,secrets; print(base64.urlsafe_b64encode(secrets.token_bytes(32)).decode())"
```

Khởi chạy các dịch vụ:

```powershell
docker compose build
docker compose up airflow-init
docker compose up -d
docker compose ps
```

Mở Airflow tại `http://localhost:8080`, MinIO Console tại `http://localhost:9001`. Tạo bucket trùng với `MINIO_BUCKET`. Trong MinIO, tạo access key riêng cho ứng dụng rồi đặt key/secret vào `MINIO_ACCESS_KEY`/`MINIO_SECRET_KEY` trong `.env`; tránh dùng root credential của MinIO cho ứng dụng. Tạo key/secret với quyền đọc, ghi, liệt kê và sao chép object trong bucket. Khởi động lại Airflow sau khi thay biến môi trường:

```powershell
docker compose up -d --force-recreate airflow-webserver airflow-scheduler
```

### Chuyển các CSV hiện có vào MinIO

CSV/LFS hiện có trong repo chỉ làm nguồn khởi tạo lần đầu. Trước khi bật lịch, chạy bootstrap một lần:

```powershell
docker compose run --rm airflow-webserver python /opt/tiki/project/04_orchestration/airflow/scripts/minio_task.py bootstrap
```

Adapter lưu raw ở `current/bronze/`, processed ở `current/silver/`, và checkpoint Supabase ở `current/state/`. Những lần sau nó chỉ tải các thư mục mà task cần. Nếu nội dung object `current` thay đổi, adapter sao chép bản cũ sang `history/<run-id>/...` trước khi thay bản hiện tại. Nội dung không đổi thì bỏ qua upload. Adapter không xóa object khi file local bị xóa.

Các script hiện tại vẫn đọc/ghi CSV theo đường dẫn trong repo. MinIO là nguồn lưu bền cho Airflow; thư mục repo được mount làm working copy của task. CSV là định dạng cả file, vì vậy mỗi thay đổi file review lớn vẫn cần truyền lại toàn object. Có thể phân vùng dữ liệu và chuyển sang Parquet trước khi học Iceberg.

## Lịch chạy (Asia/Bangkok)

DAG kiểm tra lịch vào 02:00, 10:00 và 21:00 hằng ngày; các cổng trong DAG chọn đúng công việc:

| Giờ địa phương | Điều kiện | Tác vụ |
|---|---|---|
| 21:00 mỗi ngày | Crawl giá | Chạy `01_craw_data/daily_price/crawl_daily_price.py`. Script lưu một quan sát mỗi sản phẩm/ngày và bỏ qua sản phẩm đã crawl trong ngày, nên chỉ chạy một lần/ngày. |
| 02:00 và 10:00 ngày cuối tháng | Crawl product | Chạy `01_craw_data/product/product.py`; DAG xác nhận đúng ngày cuối tháng. |
| 02:00 và 10:00 ngày mùng 1 | Crawl review | Chạy `01_craw_data/review/review.py`. |
| Sau crawl giá lúc 21:00 | Transform | Chờ đến 22:00 rồi chạy `02_process_raw_data/run_all_preprocessing.py`. |
| Sau khi Transform thành công | Load | Chờ đến 23:00 rồi chạy `03_push_to_sql/push_to_sql/push_to_database.py`. |

Product chạy hai lượt trong ngày cuối tháng để tiếp tục danh mục chưa hoàn tất; review chạy hai lượt ngày mùng 1 để lấy tiếp phần còn thiếu. Checkpoint và khóa duy nhất giúp chạy lại an toàn. Airflow retry lỗi tự động. Nếu máy tắt thì Airflow local không chạy bù trong lúc máy tắt; để lịch hoạt động, cần giữ máy và Docker Desktop bật.

## Chạy thủ công và xem lỗi

Bật DAG `tiki_extract_transform_load` trong Airflow. Chạy thử thủ công bằng **Trigger DAG w/ config** với JSON, ví dụ `{"task":"daily_price"}`, `{"task":"product"}`, `{"task":"review"}`, `{"task":"preprocess"}` hoặc `{"task":"push"}`. Nếu form chưa hiện, dùng lệnh PowerShell sau:

```powershell
docker compose exec airflow-scheduler airflow dags trigger --conf '{"task":"daily_price"}' tiki_extract_transform_load
```

Chế độ crawl thủ công tiếp tục preprocess và push Supabase; `preprocess` và `push` cũng chạy transform trước khi load. Các chế độ thủ công bỏ qua thời gian chờ 22:00/23:00. Đừng dùng **Trigger DAG** thường để chọn nhánh; không có cấu hình thì các nhánh tự chọn theo thời điểm của lịch. Mở log từng task để xem kết quả và số object MinIO thay đổi.

Xem lỗi khi Airflow nạp DAG hoặc log dịch vụ:

```powershell
docker compose exec airflow-scheduler airflow dags list-import-errors
docker compose logs -f airflow-scheduler
docker compose logs -f minio
```

Checkpoint Supabase được đồng bộ vào MinIO sau khi push, kể cả khi script kết thúc bằng lỗi sau một phần công việc. Unique constraint PostgreSQL giúp lượt nạp retry bỏ qua các dòng đã chèn thành công.

Dừng stack nhưng giữ dữ liệu và metadata Airflow:

```powershell
docker compose down
```

Không thêm `-v` trừ khi muốn xóa các named volume. Cấu hình này phục vụ học tập local, chưa dành cho production. Airflow/MinIO không tăng giới hạn kết nối Supabase mà bạn từng gặp ở Power BI; nên dùng Import và refresh tuần tự.
