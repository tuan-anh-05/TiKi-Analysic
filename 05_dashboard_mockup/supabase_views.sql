-- =============================================================================
-- View/MV chỉ-đọc cho dashboard_v2 (chạy 1 lần trong Supabase → SQL Editor)
-- Không sửa/xóa bảng gốc. Cùng kiểu với vw_daily_price_analysis / vw_review_analysis
-- (view do postgres sở hữu nên anon đọc được mà không cần mở policy trên bảng gốc).
-- Logic bám sát export_data.ps1 (nguồn CSV) để số liệu giữ nguyên ý nghĩa.
-- =============================================================================

-- 1) Một dòng / sản phẩm: danh mục, nhóm người bán, giá, đã bán, điểm, thương hiệu, năm lên sàn.
--    seller_group: 0 = Tiki Trading (seller_id = 1), 1 = chính hãng (tên chứa official/chính hãng/flagship), 2 = thông thường.
--    seller_price = -1 khi không có người bán hiện tại.
--    Quy tắc chọn bản ghi: snapshot tháng mới nhất; nếu SP thuộc nhiều danh mục thì lấy category_id nhỏ nhất.
create or replace view public.vw_dash_product as
with cur_cat as (
  select distinct on (l.product_id) l.product_id, l.category_id
  from public.dim_product_listing l
  order by l.product_id, l.crawl_date desc, l.category_id
),
cur_seller as (
  select distinct on (s.product_id) s.product_id, s.seller_id, s.seller_name, s.seller_price
  from public.product_seller s
  where s.is_current_seller
  order by s.product_id, s.crawl_date desc, s.seller_product_id
)
select
  p.product_id,
  c.category_id,
  k.name                                                    as category_name,
  case when s.product_id is null then 2
       when s.seller_id = 1 then 0
       when s.seller_name ~* '(official|chính hãng|chinh hang|flagship)' then 1
       else 2 end                                           as seller_group,
  case when s.product_id is null then -1 else s.seller_price end as seller_price,
  coalesce(p.all_time_quantity_sold, 0)                     as all_time_quantity_sold,
  coalesce(p.rating_average, 0)                             as rating_average,
  coalesce(p.review_count, 0)                               as review_count,
  coalesce(nullif(btrim(p.brand_name), ''), 'OEM')          as brand_name,
  coalesce(extract(year from p.created_date)::int, 0)       as created_year,
  p.product_name
from public.dim_product p
join cur_cat c          on c.product_id = p.product_id
left join cur_seller s  on s.product_id = p.product_id
left join public.dim_category k on k.category_id = c.category_id;

-- 2) Số lượt đánh giá theo sao, gộp theo sản phẩm (bỏ trùng review_id giữa các seller_product_id).
--    Dùng materialized view vì phải gộp ~1 triệu dòng fact_review; cần REFRESH sau mỗi lần nạp review mới.
create materialized view if not exists public.mv_dash_review_stars as
select
  product_id,
  count(distinct review_id) filter (where rating = 1) as r1,
  count(distinct review_id) filter (where rating = 2) as r2,
  count(distinct review_id) filter (where rating = 3) as r3,
  count(distinct review_id) filter (where rating = 4) as r4,
  count(distinct review_id) filter (where rating = 5) as r5
from public.fact_review
group by product_id;

create unique index if not exists ux_mv_dash_review_stars on public.mv_dash_review_stars (product_id);

-- 3) Khía cạnh khách hàng phàn nàn: theo sản phẩm, đếm đánh giá 1–2★ CÓ NỘI DUNG và số đánh giá nhắc tới từng khía cạnh.
--    Phân loại bằng KHỚP TỪ KHÓA trên nội dung (đã chuẩn hóa Unicode NFC + chữ thường) → là ước lượng;
--    một đánh giá có thể thuộc nhiều khía cạnh. Thứ tự a1..a7 phải khớp ASPECTS trong dashboard_v2_template.html:
--    a1 Chất lượng sản phẩm · a2 Giao hàng · a3 Đúng mô tả · a4 Đóng gói · a5 Giá cả · a6 Người bán – CSKH · a7 Hàng chính hãng
create materialized view if not exists public.mv_dash_review_aspect as
with neg as (
  select distinct on (review_id, product_id)
         review_id, product_id, lower(normalize(content, NFC)) as t
  from public.fact_review
  where rating <= 2 and nullif(btrim(content), '') is not null
  order by review_id, product_id
)
select
  product_id,
  count(*) as neg_total,
  count(*) filter (where t ~ 'chất lượng|chat luong|kém|tệ|dở|lỗi|hỏng|rách|bể|vỡ|nứt|nhăn|phai|bong|mỏng|mốc|hôi|bẩn|hết hạn|không dùng được|ko dùng được|không hoạt động|mờ') as a1,
  count(*) filter (where t ~ 'giao|ship|vận chuyển|van chuyen|chậm|trễ|chưa nhận|không nhận được|chờ') as a2,
  count(*) filter (where t ~ 'sai mô tả|không đúng|ko đúng|không giống|ko giống|khác hình|khác với|sai màu|sai size|sai kích|sai mẫu|giao sai|gửi sai|nhầm|thiếu|không như|ko như|mô tả|quảng cáo|lừa') as a3,
  count(*) filter (where t ~ 'đóng gói|dong goi|bọc|hộp|móp|méo|bao bì|thùng|gói hàng|bẹp|ẩu|chống sốc|xốp') as a4,
  count(*) filter (where t ~ 'giá|đắt|mắc|không đáng|ko đáng|tiền|phí|đội giá') as a5,
  count(*) filter (where t ~ 'cskh|chăm sóc khách|nhân viên|tư vấn|hỗ trợ|hoàn tiền|hoàn trả|trả hàng|đổi trả|đổi hàng|người bán|shop|liên hệ|phản hồi|bảo hành|khiếu nại|thái độ|không trả lời|tổng đài') as a6,
  count(*) filter (where t ~ 'giả|\yfake\y|nhái|dỏm|không chính hãng|ko chính hãng|hàng thật|replica|lậu') as a7
from neg
group by product_id;

create unique index if not exists ux_mv_dash_review_aspect on public.mv_dash_review_aspect (product_id);

-- 4) Chuỗi giá/lượt bán theo NGÀY của từng sản phẩm (dùng cho trang Xu hướng). Mỗi sản phẩm 1 dòng, mỗi ngày 1 số nguyên đóng gói:
--      v = ((lượt_bán_trong_ngày + 1) * 101 + %giảm_giá) * 2 + hết_hàng        (lượt_bán_trong_ngày + 1 = 0 nghĩa là thiếu/không hợp lệ)
--    Ngày không có dữ liệu ghi -1. Lượt bán trong ngày = hiệu của all_time_quantity_sold với NGÀY LIỀN TRƯỚC (cùng seller_product_id),
--    loại các trường hợp dữ liệu thiếu bị ghi thành 0 (q0 = 0), giảm số (q < q0) hoặc nhảy bất thường (>= 5000) — cùng quy tắc với bản CSV.
--    Mỗi sản phẩm chỉ lấy một seller_product_id (nhỏ nhất) để không đếm trùng. Cột d0 = ngày đầu chuỗi, nd = số ngày.
create materialized view if not exists public.mv_dash_daily_series as
with rng as (
  select min(crawl_date) as d0, max(crawl_date) as d1 from public.fact_daily_price
),
days as (
  select generate_series(d0, d1, interval '1 day')::date as d from rng
),
pick as (
  select distinct on (product_id) product_id, seller_product_id
  from public.fact_daily_price
  order by product_id, seller_product_id
),
x as (
  select f.product_id, f.crawl_date,
         least(greatest(coalesce(f.discount_rate, 0), 0), 100) as disc,
         coalesce(f.all_time_quantity_sold, 0) as q,
         (f.inventory_status = 'out_of_stock') as oos,
         lag(coalesce(f.all_time_quantity_sold, 0)) over w as q0,
         lag(f.crawl_date) over w as prev_d
  from public.fact_daily_price f
  join pick p on p.product_id = f.product_id and p.seller_product_id = f.seller_product_id
  window w as (partition by f.product_id order by f.crawl_date)
)
select
  p.product_id,
  (select d0 from rng) as d0,
  (select count(*) from days) as nd,
  string_agg(
    case when x.product_id is null then '-1'
         else ((case when x.prev_d = x.crawl_date - 1 and x.q0 > 0 and x.q >= x.q0 and x.q - x.q0 < 5000
                     then (x.q - x.q0) + 1 else 0 end) * 101 + x.disc) * 2 + (case when x.oos then 1 else 0 end) || '' end,
    ',' order by dy.d) as series
from pick p
cross join days dy
left join x on x.product_id = p.product_id and x.crawl_date = dy.d
group by p.product_id;

create unique index if not exists ux_mv_dash_daily_series on public.mv_dash_daily_series (product_id);

grant select on public.vw_dash_product        to anon, authenticated;
grant select on public.mv_dash_daily_series   to anon, authenticated;
grant select on public.mv_dash_review_stars   to anon, authenticated;
grant select on public.mv_dash_review_aspect  to anon, authenticated;

-- Làm mới số sao sau khi có review mới:
--   refresh materialized view concurrently public.mv_dash_review_stars;
--   refresh materialized view concurrently public.mv_dash_review_aspect;
--   refresh materialized view concurrently public.mv_dash_daily_series;   -- sau mỗi lần crawl giá hằng ngày mới

-- -----------------------------------------------------------------------------
-- Kiểm tra (tuỳ chọn, chỉ đọc) — chạy để đối chiếu giả định:
--   select count(*) from public.vw_dash_product;                          -- tổng SP
--   select count(*) from public.vw_dash_product where category_id <> 11312; -- sau khi loại Voucher
--   -- SP thuộc >1 danh mục trong cùng snapshot (quy tắc "category_id nhỏ nhất" có ảnh hưởng?):
--   select count(*) from (select product_id from public.dim_product_listing
--     group by product_id, crawl_date having count(distinct category_id) > 1) t;
-- -----------------------------------------------------------------------------
