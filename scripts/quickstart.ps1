# ============================================================
# quickstart.ps1
# Cài đặt lần đầu cho Developer mới (Windows)
#
# Cách dùng:
#   .\scripts\quickstart.ps1
#
# Script sẽ:
#   1. Tạo .env từ template
#   2. Tạo Python virtual environment và cài dependencies
#   3. Hướng dẫn chạy SQL setup scripts
#   4. Chạy validate_setup.ps1 để kiểm tra
# ============================================================

$ErrorActionPreference = "Stop"
$ROOT = Split-Path $PSScriptRoot -Parent

function Write-Step { param([int]$N, [string]$Msg)
    Write-Host ""
    Write-Host ("[ {0} ] {1}" -f $N, $Msg) -ForegroundColor Cyan
    Write-Host ("─" * 54)
}

function Write-Ok   { param([string]$Msg) Write-Host ("  ✓ " + $Msg) -ForegroundColor Green }
function Write-Skip { param([string]$Msg) Write-Host ("  ⚠ " + $Msg) -ForegroundColor Yellow }
function Write-Info { param([string]$Msg) Write-Host ("    " + $Msg) }

Write-Host ""
Write-Host "══════════════════════════════════════════════════════" -ForegroundColor White
Write-Host "   ERP Data Warehouse — Quick Start Setup"            -ForegroundColor White
Write-Host "══════════════════════════════════════════════════════" -ForegroundColor White

# ─── Step 1: .env file ────────────────────────────────────────
Write-Step 1 "Environment file (.env)"

$envFile = "$ROOT\.env"
if (-not (Test-Path $envFile)) {
    Copy-Item "$ROOT\.env.example" $envFile
    Write-Ok ".env created from .env.example"
    Write-Host ""
    Write-Host "  ► QUAN TRỌNG: Mở file .env và điền credentials:" -ForegroundColor Yellow
    Write-Host "       MYSQL_PASSWORD = <mật khẩu MySQL của bạn>"
    Write-Host "       PG_PASSWORD    = <mật khẩu PostgreSQL của bạn>"
    Write-Host "       PG_SUPER_PASSWORD = <mật khẩu postgres superuser>"
    Write-Host ""
    $continue = Read-Host "  Nhấn Enter sau khi đã điền .env (hoặc 'skip' để bỏ qua)"
} else {
    Write-Skip ".env đã tồn tại — bỏ qua bước tạo mới"
}

# ─── Step 2: Virtual environment ──────────────────────────────
Write-Step 2 "Python virtual environment"

$venvPath = "$ROOT\.venv"
if (-not (Test-Path $venvPath)) {
    Write-Info "Tạo .venv..."
    python -m venv $venvPath
    Write-Ok ".venv created"
} else {
    Write-Skip ".venv đã tồn tại — bỏ qua tạo mới"
}

Write-Info "Cài đặt dependencies từ elt/requirements.txt..."
& "$venvPath\Scripts\pip.exe" install --upgrade pip --quiet
& "$venvPath\Scripts\pip.exe" install -r "$ROOT\elt\requirements.txt" --quiet
Write-Ok "Dependencies installed"

# ─── Step 3: SQL setup ────────────────────────────────────────
Write-Step 3 "Database setup (SQL scripts)"

Write-Host "  Cần chạy 7 SQL scripts theo thứ tự trên PostgreSQL:"
Write-Host "  (dùng psql, pgAdmin, hoặc DBeaver)"
Write-Host ""

$scripts = Get-ChildItem "$ROOT\sql\0*.sql" | Sort-Object Name
foreach ($s in $scripts) {
    Write-Info ("  psql -U postgres -d erp_dwh -f sql\{0}" -f $s.Name)
}

Write-Host ""
$sqlDone = Read-Host "  Đã chạy xong SQL scripts chưa? (y/n)"

if ($sqlDone -ne "y") {
    Write-Host ""
    Write-Host "  Chạy SQL xong rồi tiếp tục bằng:" -ForegroundColor Yellow
    Write-Host "    .\scripts\quickstart.ps1"
    exit 0
}

# ─── Step 4: ELT setup.py (GRANT + alter schema) ──────────────
Write-Step 4 "ELT setup (GRANT permissions + watermark table)"

Write-Info "Chạy elt/setup.py để tạo bảng watermark và cấp quyền..."
$python = "$venvPath\Scripts\python.exe"

if (-not (Test-Path $python)) {
    $python = "python"
}

try {
    Push-Location "$ROOT\elt"
    & $python setup.py
    Pop-Location
    Write-Ok "setup.py completed"
} catch {
    Pop-Location
    Write-Skip "setup.py failed — kiểm tra PG_SUPER_USER trong .env"
    Write-Info "Lỗi: $_"
}

# ─── Step 5: Validate ─────────────────────────────────────────
Write-Step 5 "Validation"

Write-Info "Chạy validate_setup.ps1..."
& "$ROOT\scripts\validate_setup.ps1"

# ─── Done ─────────────────────────────────────────────────────
Write-Host ""
Write-Host "══════════════════════════════════════════════════════" -ForegroundColor White
Write-Host "   Setup hoàn tất!"                                    -ForegroundColor Green
Write-Host "══════════════════════════════════════════════════════" -ForegroundColor White
Write-Host ""
Write-Host "Bước tiếp theo:"
Write-Host "  1. Chạy pipeline lần đầu:"
Write-Host "       python elt\pipeline.py --stage all"
Write-Host ""
Write-Host "  2. Kiểm tra kết quả:"
Write-Host "       python check_pipeline_health.py"
Write-Host ""
Write-Host "  3. Xem hướng dẫn đầy đủ:"
Write-Host "       docs\how_to_use.md"
Write-Host "       CONTRIBUTING.md"
Write-Host ""
