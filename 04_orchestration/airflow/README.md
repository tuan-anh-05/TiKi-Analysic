# Airflow + MinIO bằng Docker Desktop

Thư mục này chứa Docker Compose, DAG `tiki_extract_transform_load` và adapter `scripts/minio_task.py`. Compose chạy Airflow 2.10.5/Python, PostgreSQL 16 riêng cho metadata Airflow và MinIO AIStor. Supabase PostgreSQL vẫn là đích phân tích; không dùng `airflow-db` thay Supabase.

## Luồng task

```text
Tiki -> crawler -> CSV working copy -> MinIO current/bronze
    -> transform -> CSV processed -> MinIO current/silver
    -> loader -> Supabase PostgreSQL (checkpoint -> MinIO current/state)
```

Adapter tải object hiện hành cần cho task xuống working copy, chạy script gốc trong `01_craw_data/`, `02_process_raw_data/` hoặc `03_push_to_sql/`, rồi upload file thay đổi. Khi thay object current, object trước được copy vào `history/<run-id>/...`. CSV lớn được tải/truyền như một object đầy đủ. Git/LFS chỉ cung cấp snapshot nguồn ban đầu; task không commit dữ liệu phát sinh.

## Điều kiện

- Docker Desktop trên Windows với WSL2 engine hoạt động.
- MinIO AIStor Free Tier license thật tại `04_orchestration/airflow/minio.license` (cùng thư mục Compose). File được mount read-only vào container.
- Supabase PostgreSQL và `SUPABASE_DB_URL` hợp lệ; schema có thể tạo bằng SQL Editor, còn loader cũng chạy DDL.
- `.env` tạo từ `.env.example`, không commit. Các secret/password trong file mẫu chỉ là ví dụ.

MinIO AIStor license cần thiết cho image cấu hình ở đây. Xem [cài AIStor container](https://docs.min.io/aistor/installation/container/install/) và [license](https://docs.min.io/aistor/operations/licenses/).

## Setup lần đầu

Mở Command Prompt hoặc PowerShell tại thư mục này:

```cmd
copy .env.example .env
notepad .env
```

Đặt Airflow admin credentials, `AIRFLOW_DB_PASSWORD` (chữ/số khuyên dùng vì là thành phần URL nội bộ), Fernet key, secret key webserver, MinIO root credential, MinIO app key và bucket. Lấy Supabase connection string từ Supabase Connect; thay placeholder và bật SSL. Password có ký tự dành riêng trong URI cần percent-encode. Không chia sẻ `.env`.

Tạo Fernet key bằng Python:

```cmd
python -c "import base64,secrets; print(base64.urlsafe_b64encode(secrets.token_bytes(32)).decode())"
```

Tại folder này chạy:

```cmd
docker compose build
docker compose up airflow-init
docker compose up -d
docker compose ps
```

`airflow-init` exit code 0 nghĩa là migration Airflow DB/user đã hoàn tất. Sau khi stack lên, Airflow UI là [http://localhost:8080](http://localhost:8080), MinIO Console là [http://localhost:9001](http://localhost:9001); endpoint nội bộ container là `http://minio:9000`.

Trong MinIO tạo bucket đúng `MINIO_BUCKET` (mặc định `tiki-lake`) và app access key có quyền list/get/read/put/write/copy trong bucket. Ghi cặp key/secret vào `.env`, sau đó recreate Airflow:

```cmd
docker compose up -d --force-recreate airflow-webserver airflow-scheduler
```

Tạo schema Supabase bằng `03_push_to_sql/create_database/create_tables_postgres.sql`. Seed CSV ban đầu lên MinIO một lần:

```cmd
docker compose run --rm airflow-webserver python /opt/tiki/project/04_orchestration/airflow/scripts/minio_task.py bootstrap
```

Bootstrap không crawl Tiki, không thay object hiện có. Nó tạo object ban đầu cho dữ liệu CSV/JSON của Bronze/Silver.

## Trigger thử

Bật DAG trong Airflow. Chọn **Trigger DAG w/ config**, nhập một trong các giá trị:

```json
{"task":"daily_price"}
```

Các giá trị hợp lệ: `daily_price`, `product`, `review`, `preprocess`, `push`. Có thể dùng terminal:

```cmd
docker compose exec airflow-scheduler airflow dags trigger --conf "{\"task\":\"daily_price\"}" tiki_extract_transform_load
```

Manual crawl tiếp tục preprocessing và load; `preprocess`/`push` cũng chạy transform trước load. Manual bỏ qua thời gian chờ 22:00/23:00. Để xác nhận pipeline thật sự tới đích, kiểm tra các task crawl, `run_preprocessing`, `load_new_rows_to_supabase` và row count Supabase. Xem log:

```cmd
docker compose exec airflow-scheduler airflow dags list
docker compose exec airflow-scheduler airflow dags list-import-errors
docker compose logs -f airflow-scheduler
docker compose logs -f minio
```

## Lịch DAG (giờ Việt Nam — Asia/Ho_Chi_Minh)

Cron tạo run lúc 02:00, 10:00, 21:00; các gate trong DAG chọn task tương ứng. 21:00 hằng ngày crawl giá; sản phẩm đã crawl trong ngày bị bỏ qua. Ngày cuối tháng 02:00 và 10:00 crawl product. Ngày mùng 1 02:00 và 10:00 crawl review. Sau lượt giá, DAG đợi 22:00 để transform và 23:00 để load. Có tối đa 2 retries/task, delay tăng dần tối đa 1 giờ; `max_active_runs=1`. Tắt máy/Docker thì lịch không chạy và không tự bù.

## Vận hành và sửa lỗi

```cmd
docker compose ps
docker compose logs -f airflow-webserver
docker compose logs -f airflow-scheduler
docker compose logs -f minio
docker compose down
```

`down` giữ volumes. Tránh `docker compose down -v` nếu không chủ định xóa MinIO data và Airflow metadata. Nếu DAG không hiện, kiểm tra import errors và scheduler logs. Nếu run không crawl, dùng config form/JSON; trigger không config phụ thuộc lịch. Nếu MinIO access denied, kiểm tra bucket/policy và recreate scheduler/webserver sau sửa `.env`. Nếu MinIO không start, xác nhận license path/nội dung. Nếu Supabase loader lỗi, xem connection URI, SSL, network và loader logs.

Hướng dẫn đầy đủ từ clone đến Power BI nằm trong [README tổng](../../README.md).
