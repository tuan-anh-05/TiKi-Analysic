# Tạo lại dashboard_v2.html từ dữ liệu CSV mới nhất
# Cách chạy: chuột phải file này → "Run with PowerShell"
#   hoặc trong terminal:  powershell -ExecutionPolicy Bypass -File build.ps1
# Thêm -NoOpen để không tự mở trình duyệt (dùng khi chạy tự động trên máy chủ).
param([switch]$NoOpen)
$dir = $PSScriptRoot

if (Test-Path (Join-Path $dir ".env")) {
  Write-Host "1/2  Đang xuất dữ liệu từ Supabase..."
  & (Join-Path $dir "export_data_supabase.ps1")
} else {
  Write-Host "1/2  Đang xuất dữ liệu từ CSV (không có .env)..."
  & (Join-Path $dir "export_data.ps1")
}

Write-Host "2/2  Đang ghép dữ liệu vào giao diện..."
$tpl  = [System.IO.File]::ReadAllText((Join-Path $dir "dashboard_v2_template.html"), [System.Text.Encoding]::UTF8)
$json = [System.IO.File]::ReadAllText((Join-Path $dir "data.json"), [System.Text.Encoding]::UTF8).Replace('</', '<\/')
$out  = Join-Path $dir "dashboard_v2.html"
[System.IO.File]::WriteAllText($out, $tpl.Replace('__DATA__', $json), (New-Object System.Text.UTF8Encoding($false)))

Write-Host "Xong: $out"
if (-not $NoOpen) { Start-Process $out }
