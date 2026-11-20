# Kết nối Supabase (REST/PostgREST) — chỉ chứa logic kết nối, không có logic dashboard.
# URL và key đọc từ file .env (không nằm trong HTML, không commit).

function Read-DotEnv([string]$Path) {
  if (-not (Test-Path $Path)) { throw "Không thấy $Path — sao chép .env.example thành .env rồi điền SUPABASE_URL và SUPABASE_KEY." }
  $cfg = @{}
  foreach ($line in [System.IO.File]::ReadAllLines($Path, [System.Text.Encoding]::UTF8)) {
    $t = $line.Trim()
    if (-not $t -or $t.StartsWith('#')) { continue }
    $i = $t.IndexOf('=')
    if ($i -gt 0) { $cfg[$t.Substring(0, $i).Trim()] = $t.Substring($i + 1).Trim().Trim('"').Trim("'") }
  }
  foreach ($k in 'SUPABASE_URL', 'SUPABASE_KEY') { if (-not $cfg[$k]) { throw "Thiếu $k trong $Path" } }
  $cfg
}

# Lấy toàn bộ dòng của một bảng/view, tự phân trang (server giới hạn số dòng mỗi lần).
# $Query: phần sau dấu ? của PostgREST, ví dụ "select=a,b&order=a&category_id=neq.11312"
function Get-SupabaseRows($Cfg, [string]$Table, [string]$Query, [int]$PageSize = 1000) {
  [Net.ServicePointManager]::SecurityProtocol = [Net.ServicePointManager]::SecurityProtocol -bor [Net.SecurityProtocolType]::Tls12
  $base = $Cfg.SUPABASE_URL.TrimEnd('/') + '/rest/v1/' + $Table + '?' + $Query
  $rows = New-Object System.Collections.ArrayList
  $from = 0; $total = $null
  do {
    $headers = @{ apikey = $Cfg.SUPABASE_KEY; Prefer = 'count=exact' }
    # Thử lại tối đa 3 lần: truy vấn nguội có thể vượt statement timeout của key công khai ở lần đầu
    $resp = $null
    for ($try = 1; $try -le 3 -and -not $resp; $try++) {
      try { $resp = Invoke-WebRequest -Uri "$base&limit=$PageSize&offset=$from" -Headers $headers -UseBasicParsing -TimeoutSec 120 }
      catch {
        $msg = $_.Exception.Message
        if ($_.Exception.Response) { $sr = New-Object IO.StreamReader($_.Exception.Response.GetResponseStream()); $msg += ' — ' + $sr.ReadToEnd() }
        if ($try -ge 3) { throw "Supabase lỗi khi đọc '$Table': $msg" }
        Start-Sleep -Seconds (2 * $try)
      }
    }
    # Giải mã UTF-8 thủ công (PowerShell 5.1 có thể đọc sai dấu tiếng Việt)
    $ms = New-Object IO.MemoryStream; $resp.RawContentStream.CopyTo($ms)
    $text = [System.Text.Encoding]::UTF8.GetString($ms.ToArray())
    $page = $text | ConvertFrom-Json
    $n = @($page).Count
    if ($n -gt 0) { [void]$rows.AddRange(@($page)) }
    $cr = [string]($resp.Headers['Content-Range'])
    if ($cr -match '/(\d+)$') { $total = [int]$matches[1] }
    $from += $n
  } while ($n -gt 0 -and ($null -eq $total -or $from -lt $total))
  if ($null -ne $total -and $rows.Count -ne $total) { throw "'$Table': đọc được $($rows.Count) dòng nhưng server báo $total" }
  , $rows
}
