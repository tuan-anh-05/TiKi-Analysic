-- Tự động làm mới các bảng tổng hợp của dashboard mỗi ngày (chạy MỘT LẦN trong Supabase -> SQL Editor).
-- Bước 0: bật tiện ích lập lịch: Supabase Dashboard -> Database -> Extensions -> tìm "pg_cron" -> Enable.

-- Làm mới lúc 22:30 giờ Việt Nam = 15:30 UTC (pg_cron tính theo UTC), trước khi GitHub Actions dựng dashboard lúc 23:00.
select cron.schedule(
  'refresh_dashboard_mvs',
  '30 15 * * *',
  $$
    refresh materialized view public.mv_dash_review_stars;
    refresh materialized view public.mv_dash_review_aspect;
    refresh materialized view public.mv_dash_daily_series;
  $$
);

-- Kiểm tra lịch đã tạo và kết quả các lần chạy gần nhất:
--   select * from cron.job;
--   select jobid, status, return_message, start_time, end_time from cron.job_run_details order by start_time desc limit 5;

-- Muốn bỏ lịch:
--   select cron.unschedule('refresh_dashboard_mvs');
