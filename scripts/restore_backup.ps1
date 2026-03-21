# ============================================================
# restore_backup.ps1
# Restore PostgreSQL DWH (schemas core + mart) từ file backup
#
# Cách dùng:
#   .\scripts\restore_backup.ps1
#       → Hiển thị danh sách backup và chọn tương tác
#
#   .\scripts\restore_backup.ps1 -BackupFile "backups\dwh_20260320_140000.dump"
#       → Restore file cụ thể
#
# ⚠ CẢNH BÁO: Lệnh này sẽ XÓA và ghi đè dữ liệu trong core + mart!
# ============================================================

param(
    [string]$BackupFile = "",
    [string]$PgBin      = "C:\Program Files\PostgreSQL\16\bin",
    [string]$PgHost     = "localhost",
    [string]$PgPort     = "5432",
    [string]$PgUser     = "dwh_admin",
    [string]$PgDb       = "erp_dwh"
)

$ErrorActionPreference = "Stop"
$ROOT        = Split-Path $PSScriptRoot -Parent
$BACKUP_DIR  = "$ROOT\backups"
$ENV_FILE    = "$ROOT\.env"

# ─── Load password from .env ──────────────────────────────────
if (Test-Path $ENV_FILE) {
    Get-Content $ENV_FILE | ForEach-Object {
        if ($_ -match "^PG_PASSWORD\s*=\s*(.+)$") {
            $env:PGPASSWORD = $Matches[1].Trim().Trim('"').Trim("'")
        }
    }
} else {
    Write-Warning ".env not found — ensure PGPASSWORD is set in environment"
}

# ─── Chọn file backup ─────────────────────────────────────────
if ($BackupFile -eq "") {
    # Hiển thị danh sách backup có sẵn
    $backups = Get-ChildItem -Path $BACKUP_DIR -Filter "dwh_*.dump" -ErrorAction SilentlyContinue |
               Sort-Object LastWriteTime -Descending

    if ($backups.Count -eq 0) {
        Write-Error "Không tìm thấy file backup trong $BACKUP_DIR"
        exit 1
    }

    Write-Host ""
    Write-Host "Danh sách backup có sẵn:"
    Write-Host "─────────────────────────────────────────────────"
    for ($i = 0; $i -lt $backups.Count; $i++) {
        $f    = $backups[$i]
        $size = [math]::Round($f.Length / 1MB, 1)
        Write-Host ("  [{0}] {1}  ({2} MB)  {3}" -f ($i+1), $f.Name, $size, $f.LastWriteTime)
    }
    Write-Host ""

    $choice = Read-Host "Chọn số backup (1-$($backups.Count)) hoặc Enter để hủy"
    if ($choice -eq "") { Write-Host "Đã hủy."; exit 0 }

    $idx = [int]$choice - 1
    if ($idx -lt 0 -or $idx -ge $backups.Count) {
        Write-Error "Lựa chọn không hợp lệ"
        exit 1
    }

    $BackupFile = $backups[$idx].FullName
} elseif (-not [System.IO.Path]::IsPathRooted($BackupFile)) {
    # Resolve relative path từ ROOT
    $BackupFile = Join-Path $ROOT $BackupFile
}

if (-not (Test-Path $BackupFile)) {
    Write-Error "File backup không tồn tại: $BackupFile"
    exit 1
}

# ─── Xác nhận ─────────────────────────────────────────────────
Write-Host ""
Write-Host "══════════════════════════════════════════════════════"
Write-Host " RESTORE DATA WAREHOUSE"
Write-Host "══════════════════════════════════════════════════════"
Write-Host " File  : $BackupFile"
Write-Host " Target: $PgDb @ $PgHost`:$PgPort (schemas: core, mart)"
Write-Host ""
Write-Host " ⚠  Toàn bộ dữ liệu core + mart sẽ bị GHI ĐÈ!"
Write-Host "══════════════════════════════════════════════════════"
Write-Host ""

$confirm = Read-Host "Nhập 'yes' để xác nhận restore"
if ($confirm -ne "yes") {
    Write-Host "Đã hủy."
    exit 0
}

# ─── Locate pg_restore ────────────────────────────────────────
$pgRestore = Join-Path $PgBin "pg_restore.exe"
if (-not (Test-Path $pgRestore)) {
    $pgRestore = "pg_restore"
}

# ─── Run pg_restore ───────────────────────────────────────────
$timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
Write-Host "[$timestamp] Bắt đầu restore..."

& $pgRestore `
    --host=$PgHost `
    --port=$PgPort `
    --username=$PgUser `
    --dbname=$PgDb `
    --format=custom `
    --clean `
    --if-exists `
    --no-password `
    --schema=core `
    --schema=mart `
    $BackupFile

if ($LASTEXITCODE -ne 0) {
    Write-Error "pg_restore kết thúc với exit code $LASTEXITCODE — kiểm tra log ở trên"
    exit 1
}

Write-Host ""
Write-Host "✓ Restore thành công!"
Write-Host ""
Write-Host "Chạy health check để xác minh:"
Write-Host "  python check_pipeline_health.py"
