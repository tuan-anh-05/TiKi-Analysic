# Xuất data.json từ Supabase (thay cho đọc CSV của export_data.ps1). Định dạng data.json giữ nguyên
# ({cats, brands, names, p, d, rs}) nên dashboard_v2_template.html không phải sửa.
# Cần các view trong supabase_views.sql và file .env (xem .env.example).
. (Join-Path $PSScriptRoot "supabase_client.ps1")
$cfg = Read-DotEnv (Join-Path $PSScriptRoot ".env")
$outFile = Join-Path $PSScriptRoot "data.json"
$inv = [System.Globalization.CultureInfo]::InvariantCulture
$VOUCHER = 11312                                   # luôn loại Voucher (như bản CSV)
$dFrom = if ($cfg.DAILY_FROM) { $cfg.DAILY_FROM } else { '2026-10-18' }
$dTo   = if ($cfg.DAILY_TO)   { $cfg.DAILY_TO }   else { '2026-10-21' }

# --- Sản phẩm ---
$prod = Get-SupabaseRows $cfg 'vw_dash_product' "select=*&category_id=neq.$VOUCHER&order=product_id"

$cats = New-Object System.Collections.ArrayList; $catIdx = @{}
$brands = New-Object System.Collections.ArrayList; $brandIdx = @{}
$P = New-Object System.Text.StringBuilder
$names = New-Object System.Collections.ArrayList
$pidIdx = @{}
$i = 0
foreach ($r in $prod) {
  $cid = [string]$r.category_id
  if (-not $catIdx.ContainsKey($cid)) { $catIdx[$cid] = $cats.Count; [void]$cats.Add($r.category_name) }
  $b = [string]$r.brand_name
  if (-not $brandIdx.ContainsKey($b)) { $brandIdx[$b] = $brands.Count; [void]$brands.Add($b) }
  $sold = [double]$r.all_time_quantity_sold; $rc = [double]$r.review_count; $ra = [double]$r.rating_average
  [void]$P.Append(("{0},{1},{2},{3},{4},{5},{6},{7}," -f $catIdx[$cid], [int]$r.seller_group, [math]::Round([double]$r.seller_price), $sold, $ra.ToString('0.0', $inv), $rc, $brandIdx[$b], [int]$r.created_year))
  $nm = [string]$r.product_name; if ($nm.Length -gt 90) { $nm = $nm.Substring(0, 88) + '…' }
  [void]$names.Add($(if ($sold -gt 0 -or $rc -gt 0) { $nm } else { '' }))
  $pidIdx[[string]$r.product_id] = $i; $i++
}

# --- Chuỗi giá/lượt bán theo NGÀY của từng sản phẩm (mv_dash_daily_series) ---
# Mỗi sản phẩm một dòng, mỗi ngày một số nguyên đóng gói (xem supabase_views.sql). Có chuỗi này thì giao diện dùng toàn bộ các ngày;
# chưa tạo view thì quay về cửa sổ cố định $dFrom → $dTo như trước.
$DY = New-Object System.Text.StringBuilder; $dyProducts = 0; $ddJson = 'null'; $useSeries = $false; $mvPids = New-Object System.Collections.ArrayList
try {
  $ser = Get-SupabaseRows $cfg 'mv_dash_daily_series' "select=product_id,d0,nd,series&order=product_id"
  $nDays = 0; $d0 = ''
  foreach ($r in $ser) {
    $pk = [string]$r.product_id
    if (-not $pidIdx.ContainsKey($pk)) { continue }
    $nDays = [int]$r.nd; $d0 = [string]$r.d0
    if (($r.series -split ',').Count -ne $nDays) { throw "Chuỗi của sản phẩm $pk không đủ $nDays ngày" }
    [void]$DY.Append([string]$pidIdx[$pk] + ',' + $r.series + ','); $dyProducts++; [void]$mvPids.Add($pk)
  }
  if ($dyProducts -gt 0) { $ddJson = '{"d0":"' + $d0.Substring(0, 10) + '","n":' + $nDays + '}'; $useSeries = $true }
} catch { Write-Warning "Bỏ qua chuỗi giá theo ngày: $($_.Exception.Message)" }
# Tuỳ chọn (DAILY_CSV trong .env): nếu tệp CSV giá hằng ngày đã làm sạch có nhiều ngày hơn Supabase
# (dữ liệu đã thu về nhưng chưa nạp), dựng chuỗi theo ngày từ CSV. Khi Supabase bắt kịp thì tự dùng lại Supabase.
if ($useSeries -and $cfg.DAILY_CSV -and (Test-Path $cfg.DAILY_CSV)) {
  try {
    $csvRes = & (Join-Path $PSScriptRoot 'daily_series_from_csv.ps1') -Csv $cfg.DAILY_CSV -PidIdx $pidIdx -Pids ([string[]]$mvPids) -D0 ([datetime]$d0.Substring(0, 10)) -CurrentNd $nDays
    if ($csvRes) {
      $DY = New-Object System.Text.StringBuilder; [void]$DY.Append($csvRes.Dy)
      $ddJson = '{"d0":"' + $d0.Substring(0, 10) + '","n":' + $csvRes.Nd + '}'
      Write-Host "[CSV] Chuỗi theo ngày dài hơn Supabase ($($csvRes.Nd) so với $nDays ngày): dùng dữ liệu từ CSV cho phần chuỗi ngày."
      $nDays = $csvRes.Nd
    }
  } catch { Write-Warning "Bỏ qua chuỗi ngày từ CSV: $($_.Exception.Message)" }
}

# --- Giá hằng ngày, cửa sổ cố định (chỉ dùng khi chưa có chuỗi theo ngày) ---
$D = New-Object System.Text.StringBuilder; $seen = @{}
if (-not $useSeries) {
$daily = Get-SupabaseRows $cfg 'vw_daily_price_analysis' "select=product_id,seller_product_id,category_id,crawl_date,discount_rate,all_time_quantity_sold,inventory_status&crawl_date=in.($dFrom,$dTo)&category_id=neq.$VOUCHER&order=id"
$a = @{}; $z = @{}
foreach ($r in $daily) {
  $k = "$($r.product_id)_$($r.seller_product_id)"
  if ($r.crawl_date -eq $dFrom) { $a[$k] = $r } elseif ($r.crawl_date -eq $dTo) { $z[$k] = $r } }
foreach ($k in ($z.Keys | Sort-Object)) {
  $r = $z[$k]; $pk = [string]$r.product_id
  if (-not $pidIdx.ContainsKey($pk) -or $seen.ContainsKey($pk)) { continue }
  $seen[$pk] = 1
  $delta = -1
  if ($a.ContainsKey($k)) { $q0 = [double]$a[$k].all_time_quantity_sold; $q1 = [double]$r.all_time_quantity_sold
    # q0 = 0 là giá trị thiếu bị ghi thành 0 → loại để không thổi phồng lượt bán/ngày
    if ($q0 -gt 0 -and $q1 -ge $q0 -and ($q1 - $q0) -lt 5000) { $delta = $q1 - $q0 } }
  $oos = if ($r.inventory_status -eq 'out_of_stock') { 1 } else { 0 }
  [void]$D.Append(("{0},{1},{2},{3}," -f $pidIdx[$pk], [math]::Round([double]$r.discount_rate), $delta, $oos))
}
}

# --- Số sao thật từng sản phẩm (mv_dash_review_stars) ---
$stars = Get-SupabaseRows $cfg 'mv_dash_review_stars' "select=product_id,r1,r2,r3,r4,r5&order=product_id"
$RS = New-Object System.Text.StringBuilder; $rsProducts = 0
foreach ($r in $stars) {
  $pk = [string]$r.product_id
  if (-not $pidIdx.ContainsKey($pk)) { continue }
  [void]$RS.Append(("{0},{1},{2},{3},{4},{5}," -f $pidIdx[$pk], [int]$r.r1, [int]$r.r2, [int]$r.r3, [int]$r.r4, [int]$r.r5)); $rsProducts++
}

# --- Khía cạnh phàn nàn trong đánh giá 1–2★ (mv_dash_review_aspect) ---
# Tuỳ chọn: chưa tạo view thì bỏ qua (giao diện hiện khung "Chờ dữ liệu nhận xét"), không làm hỏng build.
$AS = New-Object System.Text.StringBuilder; $aspProducts = 0
try {
  $asp = Get-SupabaseRows $cfg 'mv_dash_review_aspect' "select=product_id,neg_total,a1,a2,a3,a4,a5,a6,a7&order=product_id"
  foreach ($r in $asp) {
    $pk = [string]$r.product_id
    if (-not $pidIdx.ContainsKey($pk)) { continue }
    [void]$AS.Append(("{0},{1},{2},{3},{4},{5},{6},{7},{8}," -f $pidIdx[$pk], [int]$r.neg_total, [int]$r.a1, [int]$r.a2, [int]$r.a3, [int]$r.a4, [int]$r.a5, [int]$r.a6, [int]$r.a7)); $aspProducts++
  }
} catch { Write-Warning "Bỏ qua khía cạnh phàn nàn: $($_.Exception.Message)" }

$json = '{"cats":' + (ConvertTo-Json @($cats) -Compress) +
        ',"brands":' + (ConvertTo-Json @($brands) -Compress) +
        ',"names":' + (ConvertTo-Json @($names) -Compress) +
        ',"p":[' + $P.ToString().TrimEnd(',') + ']' +
        ',"d":[' + $D.ToString().TrimEnd(',') + ']' +
        ',"rs":[' + $RS.ToString().TrimEnd(',') + ']' +
        ',"asp":[' + $AS.ToString().TrimEnd(',') + ']' +
        ',"dd":' + $ddJson + ',"dy":[' + $DY.ToString().TrimEnd(',') + ']}'
[System.IO.File]::WriteAllText($outFile, $json, (New-Object System.Text.UTF8Encoding($false)))
"[Supabase] products=$i cats=$($cats.Count) brands=$($brands.Count) daily=$($seen.Count) review_stars=$rsProducts aspect=$aspProducts daily_series=$dyProducts size=$([math]::Round((Get-Item $outFile).Length/1MB,2))MB"
