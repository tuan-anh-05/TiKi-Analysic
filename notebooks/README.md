# Thư Mục Nghiên Cứu & Khám Phá Dữ Liệu (Exploratory Data Analysis - EDA)

Thư mục này lưu trữ toàn bộ các tệp **Jupyter Notebook (`.ipynb`)** được phát triển trong giai đoạn thử nghiệm ban đầu của dự án. Đây là nơi thực hiện nghiên cứu cấu trúc API của Tiki, đo kiểm tần suất giới hạn (Rate Limiting), kiểm thử các thuật toán làm sạch dữ liệu và thực hiện các phân tích nghiệp vụ chuyên sâu (như phân tích Pareto 80/20).

---

## 1. Bảng Ánh Xạ Giữa Notebook Nghiên Cứu & Mã Nguồn Sản Phẩm (Production Scripts)

Toàn bộ logic đã kiểm chứng thành công trong các notebook này đều đã được tái cấu trúc (refactor) và đóng gói thành các tệp mã nguồn Python tiêu chuẩn (`.py`) để vận hành tự động trong Docker và Airflow:

| Giai đoạn | Jupyter Notebook (`.ipynb`) | Tệp mã nguồn tương ứng (`.py`) | Mục tiêu nghiên cứu & kết quả đạt được |
|---|---|---|---|
| **01. Crawl** | `01_crawl/crawl_category.ipynb` | `01_craw_data/category/crawl_category.py` | Phân tích cấu trúc cây phân cấp danh mục API Tiki (từ cấp 1 đến cấp 5). |
| | `01_crawl/crawl_product.ipynb` | `01_craw_data/product/crawl_product.py` | Kiểm thử việc duyệt phân trang danh mục (tối đa 100 trang) và bóc tách thuộc tính chi tiết sản phẩm. |
| | `01_crawl/crawl_daily_price.ipynb` | `01_craw_data/daily_price/crawl_daily_price.py` | Kiểm thử cơ chế theo dõi biến động giá hàng ngày, ghi nhận lỗi và phục hồi từ log lỗi. |
| | `01_crawl/crawl_review.ipynb` | `01_craw_data/review/crawl_review.py` | Kiểm nghiệm việc thu thập review theo thứ tự mới nhất và phát hiện điểm dừng thông minh khi gặp review đã lưu. |
| **02. Process** | `02_process/pareto_category.ipynb` | *(Tạo tệp `product_pareto.csv`)* | **Phân tích Quy luật Pareto (80/20)**: Tính toán lượng bán lũy kế và doanh thu ước tính để lọc ra danh sách các sản phẩm trọng tâm cần theo dõi giá hàng ngày (`product_pareto.csv`). |
| | `02_process/process_category.ipynb` | `02_process_raw_data/category/process_category.py` | Thử nghiệm chuẩn hóa chuỗi và ép kiểu ID phân cấp an toàn. |
| | `02_process/process_daily_price.ipynb` | `02_process_raw_data/daily_price/process_daily_price.py` | Thử nghiệm lọc giá không hợp lệ (`price <= 0`), fallback giá niêm yết và tính toán tỷ lệ giảm giá (`discount_rate`). |
| | `02_process/process_product.ipynb` | `02_process_raw_data/product/process_product.py` | Kiểm thử chuẩn hóa nhãn thương hiệu, lọc snapshot tháng (`YYYY-MM`) và khử trùng lặp. |
| | `02_process/process_review.ipynb` | `02_process_raw_data/review/process_review.py` | Kiểm thử thuật toán đọc theo từng khối (**Chunking 150.000 dòng**) và lọc trùng theo bộ 3 khóa kết hợp. |
| **03. Load** | `03_load/push_to_database.ipynb` | `03_push_to_sql/push_to_database.py` | Kiểm tra kết nối Supabase PostgreSQL, đo đạc tốc độ chèn dữ liệu và kiểm thử cơ chế `ON CONFLICT DO NOTHING`. |

---

## 2. Điểm Nhấn Phân Tích: `pareto_category.ipynb`

Notebook [`02_process/pareto_category.ipynb`](02_process/pareto_category.ipynb) đóng vai trò đặc biệt quan trọng trong nghiệp vụ Data Analytics của dự án:
* **Bài toán đặt ra**: Sàn Tiki có hàng trăm nghìn sản phẩm, nếu theo dõi giá toàn bộ mỗi ngày sẽ gây quá tải hệ thống và lãng phí tài nguyên.
* **Giải pháp**: Ứng dụng quy luật Pareto 80/20, notebook xếp hạng các SKU theo sản lượng bán ra (`all_time_quantity_sold`) và chọn lọc ra tập hợp các sản phẩm chiếm phần lớn doanh thu.
* **Đầu ra**: Xuất tệp `01_craw_data/daily_price/product_pareto.csv` làm danh mục đầu vào cho crawler giá hàng ngày (`crawl_daily_price.py`).

---

## 3. Lưu Ý Về Vận Hành

> 📌 **Nguyên tắc môi trường:**
> * Trong hệ thống tự động hóa bằng **Docker & Apache Airflow**, toàn bộ pipeline chỉ thực thi các tệp Python (`.py`) chính thức trong các thư mục `01_craw_data/`, `02_process_raw_data/` và `03_push_to_sql/`.
> * Các thay đổi trong thư mục `notebooks/` đóng vai trò tài liệu nghiên cứu, ghi nhận lịch sử phát triển và kiểm chứng kết quả phân tích (Ad-hoc Analysis), không ảnh hưởng trực tiếp đến trạng thái của hệ thống tự động.
