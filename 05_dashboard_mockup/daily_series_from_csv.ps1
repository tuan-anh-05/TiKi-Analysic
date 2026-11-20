# Tuỳ chọn: dựng chuỗi giá/lượt bán theo ngày từ tệp CSV đã làm sạch khi CSV có nhiều ngày hơn Supabase
# (ví dụ dữ liệu mới đã thu về nhưng chưa nạp vào Supabase). Cùng quy tắc với mv_dash_daily_series:
#   mỗi sản phẩm chọn biến thể có seller_product_id nhỏ nhất; lượt bán trong ngày = hiệu số lượng đã bán của hai ngày liền nhau,
#   hợp lệ khi ngày trước > 0, không giảm và tăng dưới 5.000; mã hóa v = ((lượt bán + 1) * 101 + giảm giá %) * 2 + hết hàng; -1 = thiếu ngày.
# Chỉ lấy các ngày đã thu đủ (số dòng >= 90% số dòng của ngày đầy đủ), bỏ ngày cuối nếu mới thu một phần.
# Trả về $null nếu CSV không dài hơn Supabase; ngược lại trả về đối tượng { Nd; Dy }.
param(
  [Parameter(Mandatory)][string]$Csv,
  [Parameter(Mandatory)]$PidIdx,        # hashtable product_id -> chỉ số trong data.json
  [Parameter(Mandatory)][string[]]$Pids,  # các product_id có chuỗi, theo thứ tự của mv_dash_daily_series
  [Parameter(Mandatory)][datetime]$D0,
  [Parameter(Mandatory)][int]$CurrentNd
)
$VOUCHER = '11312'
# lượt 1: đếm số dòng mỗi ngày
$perDay = @{}
$sr = New-Object IO.StreamReader($Csv, [Text.Encoding]::UTF8); $null = $sr.ReadLine()
while (($l = $sr.ReadLine()) -ne $null) { $p = $l.IndexOf(','); if ($p -lt 0) { continue }; $c = $l.Split(','); if ($c.Count -lt 14) { continue }; $k = $c[4].Substring(0, [math]::Min(10, $c[4].Length)); $perDay[$k]++ }
$sr.Close()
if ($perDay.Count -eq 0) { return $null }
$full = ($perDay.Values | Measure-Object -Maximum).Maximum
$last = -1
for ($i = 0; $i -lt 400; $i++) { $k = $D0.AddDays($i).ToString('yyyy-MM-dd'); if ($perDay.ContainsKey($k) -and $perDay[$k] -ge 0.9 * $full) { $last = $i } elseif ($i -gt $last + 1) { break } }
$nd = $last + 1
if ($nd -le $CurrentNd) { return $null }
# lượt 2: đọc dữ liệu
$want = @{}; foreach ($p in $Pids) { $want[$p] = 1 }
$minSp = @{}; $rows = New-Object System.Collections.Generic.List[object]
$sr = New-Object IO.StreamReader($Csv, [Text.Encoding]::UTF8); $null = $sr.ReadLine()
while (($l = $sr.ReadLine()) -ne $null) {
  $c = $l.Split(','); if ($c.Count -lt 14 -or $c[3] -eq $VOUCHER -or -not $want.ContainsKey($c[0])) { continue }
  $di = ([datetime]::ParseExact($c[4].Substring(0, 10), 'yyyy-MM-dd', $null) - $D0).Days
  if ($di -lt 0 -or $di -ge $nd) { continue }
  $sp = [long]$c[1]; if (-not $minSp.ContainsKey($c[0]) -or $sp -lt $minSp[$c[0]]) { $minSp[$c[0]] = $sp }
  $q = 0.0; if ($c[12] -ne '') { $q = [double]$c[12] }
  $rows.Add(@($c[0], $sp, $di, [int][math]::Min([math]::Max([double]$c[9], 0), 100), $q, ($c[13] -eq 'out_of_stock')))
}
$sr.Close()
$ser = @{}
foreach ($r in $rows) { if ($r[1] -ne $minSp[$r[0]]) { continue }; if (-not $ser.ContainsKey($r[0])) { $ser[$r[0]] = New-Object 'object[]' $nd }; $ser[$r[0]][$r[2]] = @($r[3], $r[4], $r[5]) }
$sb = New-Object Text.StringBuilder
foreach ($p in $Pids) {
  [void]$sb.Append([string]$PidIdx[$p])
  $sv = $ser[$p]
  for ($t = 0; $t -lt $nd; $t++) {
    $e = $null; if ($sv) { $e = $sv[$t] }
    if ($e -eq $null) { [void]$sb.Append(',-1'); continue }
    $enc = 0
    if ($t -ge 1) { $p0 = $sv[$t - 1]; if ($p0 -ne $null -and $p0[1] -gt 0 -and $e[1] -ge $p0[1] -and ($e[1] - $p0[1]) -lt 5000) { $enc = [long]($e[1] - $p0[1]) + 1 } }
    [void]$sb.Append(',' + [string](((($enc * 101) + $e[0]) * 2) + $(if ($e[2]) { 1 } else { 0 })))
  }
  [void]$sb.Append(',')
}
[pscustomobject]@{ Nd = $nd; Dy = $sb.ToString() }
