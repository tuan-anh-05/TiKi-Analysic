# Xuất dữ liệu gọn (đã loại Voucher) để nhúng vào dashboard.html
$base = "C:\Users\Admin\Downloads\BC_TTTN\TiKi-Analysic-main\02_process_raw_data"
$outFile = Join-Path $PSScriptRoot "data.json"
$inv = [System.Globalization.CultureInfo]::InvariantCulture

# Danh mục
$catName = @{}
Import-Csv "$base\category\category.csv" | ForEach-Object { $catName[$_.category_id] = $_.name }
$prodCat = @{}
Import-Csv "$base\product\product.csv" | ForEach-Object { if (-not $prodCat.ContainsKey($_.product_id)) { $prodCat[$_.product_id] = $_.category_id } }

# Người bán hiện tại
$seller = @{}
Import-Csv "$base\product\product_seller.csv" | Where-Object { $_.is_current_seller -eq 'True' } | ForEach-Object {
  if (-not $seller.ContainsKey($_.product_id)) {
    $g = if ($_.seller_id -eq '1') { 0 } elseif ($_.seller_name -match '(?i)(official|chính hãng|chinh hang|flagship)') { 1 } else { 2 }
    $seller[$_.product_id] = @($g, [double]$_.seller_price) } }

$cats = New-Object System.Collections.ArrayList; $catIdx = @{}
$brands = New-Object System.Collections.ArrayList; $brandIdx = @{}
$P = New-Object System.Text.StringBuilder
$names = New-Object System.Collections.ArrayList
$pidIdx = @{}
$i = 0
foreach ($r in (Import-Csv "$base\product\product_detail.csv")) {
  $cid = $prodCat[$r.product_id]
  if ($cid -eq '11312' -or -not $cid) { continue }
  if ($pidIdx.ContainsKey($r.product_id)) { continue }
  if (-not $catIdx.ContainsKey($cid)) { $catIdx[$cid] = $cats.Count; [void]$cats.Add($catName[$cid]) }
  $b = if ($r.brand_name) { $r.brand_name.Trim() } else { 'OEM' }
  if (-not $brandIdx.ContainsKey($b)) { $brandIdx[$b] = $brands.Count; [void]$brands.Add($b) }
  $s = $seller[$r.product_id]; $g = if ($s) { $s[0] } else { 2 }; $price = if ($s) { [math]::Round($s[1]) } else { -1 }
  $sold = [double]$r.all_time_quantity_sold; $rc = [double]$r.review_count; $ra = [double]$r.rating_average
  $year = if ($r.created_date -match '^(\d{4})') { [int]$matches[1] } else { 0 }
  [void]$P.Append(("{0},{1},{2},{3},{4},{5},{6},{7}," -f $catIdx[$cid], $g, $price, $sold, $ra.ToString('0.0', $inv), $rc, $brandIdx[$b], $year))
  $nm = $r.product_name; if ($nm.Length -gt 90) { $nm = $nm.Substring(0, 88) + '…' }
  [void]$names.Add($(if ($sold -gt 0 -or $rc -gt 0) { $nm } else { '' }))
  $pidIdx[$r.product_id] = $i; $i++
}

# Giá hằng ngày 18 → 21/10
$a = @{}; $z = @{}
foreach ($r in (Import-Csv "$base\daily_price\daily_price_history.csv")) {
  if ($r.category_id -eq '11312') { continue }
  $k = "$($r.product_id)_$($r.seller_product_id)"
  if ($r.crawl_date -eq '2026-10-18') { $a[$k] = $r } elseif ($r.crawl_date -eq '2026-10-21') { $z[$k] = $r } }
$D = New-Object System.Text.StringBuilder; $seen = @{}
foreach ($k in $z.Keys) {
  $r = $z[$k]; if (-not $pidIdx.ContainsKey($r.product_id) -or $seen.ContainsKey($r.product_id)) { continue }
  $seen[$r.product_id] = 1
  $delta = -1
  if ($a.ContainsKey($k)) { $q0 = [double]$a[$k].all_time_quantity_sold; $q1 = [double]$r.all_time_quantity_sold
    if ($q0 -gt 0 -and $q1 -ge $q0 -and ($q1 - $q0) -lt 5000) { $delta = $q1 - $q0 } }
  $oos = if ($r.inventory_status -eq 'out_of_stock') { 1 } else { 0 }
  [void]$D.Append(("{0},{1},{2},{3}," -f $pidIdx[$r.product_id], [math]::Round([double]$r.discount_rate), $delta, $oos))
}

# Số sao thật của từng lượt đánh giá (tuỳ chọn): review_stars.csv gồm product_id, rating, n
# xuất từ Supabase:  SELECT product_id, rating, COUNT(*) AS n FROM fact_review GROUP BY product_id, rating;
$RS = New-Object System.Text.StringBuilder; $rsProducts = 0
$rsFile = Join-Path $PSScriptRoot "review_stars.csv"
if (Test-Path $rsFile) {
  $acc = @{}
  foreach ($r in (Import-Csv $rsFile)) {
    if (-not $pidIdx.ContainsKey($r.product_id)) { continue }
    $k = [int]$r.rating; if ($k -lt 1 -or $k -gt 5) { continue }
    $idx = $pidIdx[$r.product_id]
    if (-not $acc.ContainsKey($idx)) { $acc[$idx] = @(0, 0, 0, 0, 0) }
    $acc[$idx][$k - 1] += [int]$r.n
  }
  foreach ($idx in $acc.Keys) { $c = $acc[$idx]; [void]$RS.Append(("{0},{1},{2},{3},{4},{5}," -f $idx, $c[0], $c[1], $c[2], $c[3], $c[4])) }
  $rsProducts = $acc.Count
}

$json = '{"cats":' + (ConvertTo-Json @($cats) -Compress) +
        ',"brands":' + (ConvertTo-Json @($brands) -Compress) +
        ',"names":' + (ConvertTo-Json @($names) -Compress) +
        ',"p":[' + $P.ToString().TrimEnd(',') + ']' +
        ',"d":[' + $D.ToString().TrimEnd(',') + ']' +
        ',"rs":[' + $RS.ToString().TrimEnd(',') + ']}'
[System.IO.File]::WriteAllText($outFile, $json, (New-Object System.Text.UTF8Encoding($false)))
"products=$i cats=$($cats.Count) brands=$($brands.Count) daily=$($seen.Count) review_stars=$rsProducts size=$([math]::Round((Get-Item $outFile).Length/1MB,2))MB"
