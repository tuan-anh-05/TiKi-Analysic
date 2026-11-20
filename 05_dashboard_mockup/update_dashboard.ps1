# Cập nhật dashboard tự động trên máy này: dựng lại từ Supabase, chép bản mới sang dashboard_public\index.html.
# Chạy tay:  powershell -ExecutionPolicy Bypass -File update_dashboard.ps1
# Nhật ký:   update_log.txt (cùng thư mục). Nếu dựng lỗi, bản dashboard cũ được giữ nguyên.
# Tự thử lại tối đa 4 lần, cách nhau 5 phút (hay gặp khi máy vừa bật, chưa có mạng).
$dir = $PSScriptRoot
$log = Join-Path $dir "update_log.txt"
$data = Join-Path $dir "data.json"
$html = Join-Path $dir "dashboard_v2.html"
$pub = Join-Path (Split-Path $dir -Parent) "dashboard_public"
function Log($m) { "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')  $m" | Out-File -FilePath $log -Append -Encoding UTF8 }

$ok = $false
for ($attempt = 1; $attempt -le 4 -and -not $ok; $attempt++) {
  $start = Get-Date
  Log "Bắt đầu cập nhật (lần $attempt/4)"
  try {
    $out = & powershell -NoProfile -ExecutionPolicy Bypass -File (Join-Path $dir "build.ps1") -NoOpen 2>&1 | Out-String
    $summary = ($out -split "`r?`n" | Where-Object { $_ -match '^\[Supabase\]' } | Select-Object -First 1)
    if (-not (Test-Path $data) -or (Get-Item $data).LastWriteTime -lt $start) { throw "data.json không được làm mới (thường do mất mạng hoặc Supabase lỗi)" }
    if ((Get-Item $html).Length -lt 1MB) { throw "dashboard_v2.html quá nhỏ" }
    if (Select-String -Path $html -Pattern 'sb_publishable|SUPABASE_KEY' -Quiet) { throw "Trang dashboard chứa khóa, dừng sao chép" }
    New-Item -ItemType Directory -Force $pub | Out-Null
    Copy-Item $html (Join-Path $pub "index.html") -Force
    Log "Thành công. $summary"
    $ok = $true
  } catch {
    Log "LỖI lần ${attempt}: $($_.Exception.Message)"
    if ($attempt -lt 4) { Start-Sleep -Seconds 300 }
  }
}
if (-not $ok) { Log "THẤT BẠI sau 4 lần; giữ nguyên dashboard cũ"; exit 1 }