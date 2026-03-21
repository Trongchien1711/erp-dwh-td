# ============================================================
# backup_dwh.ps1
# Backup PostgreSQL DWH: schemas core + mart
#
# Cách dùng thủ công:
#   .\scripts\backup_dwh.ps1
#
# Backup lưu tại: d:\Data Warehouse\backups\dwh_YYYYMMDD_HHMMSS.dump
# Giữ 7 bản backup gần nhất, xoá bản cũ hơn tự động.
# ============================================================

$ErrorActionPreference = "Stop"

# ─── Config ──────────────────────────────────────────────────
$PG_BIN     = "C:\Program Files\PostgreSQL\16\bin"   # Điều chỉnh nếu khác version
$PGHOST     = "localhost"
$PGPORT     = "5432"
$PGUSER     = "dwh_admin"
$PGDB       = "erp_dwh"
$BACKUP_DIR = "d:\Data Warehouse\backups"
$KEEP_DAYS  = 7   # Giữ bao nhiêu ngày backup

$DATE       = Get-Date -Format "yyyyMMdd_HHmmss"
$OUTFILE    = "$BACKUP_DIR\dwh_$DATE.dump"

# ─── Load password from .env ──────────────────────────────────
$ENV_FILE = "d:\Data Warehouse\.env"
if (Test-Path $ENV_FILE) {
    Get-Content $ENV_FILE | ForEach-Object {
        if ($_ -match "^PG_PASSWORD\s*=\s*(.+)$") {
            $env:PGPASSWORD = $Matches[1].Trim().Trim('"').Trim("'")
        }
    }
} else {
    Write-Warning ".env không tìm thấy — PGPASSWORD cần được set thủ công"
}

# ─── Ensure backup directory ──────────────────────────────────
if (-not (Test-Path $BACKUP_DIR)) {
    New-Item -ItemType Directory -Path $BACKUP_DIR | Out-Null
    Write-Host "Created backup directory: $BACKUP_DIR"
}

# ─── Run pg_dump ──────────────────────────────────────────────
Write-Host "[$DATE] Starting backup → $OUTFILE"

$pgDump = Join-Path $PG_BIN "pg_dump.exe"
if (-not (Test-Path $pgDump)) {
    # Try PATH fallback
    $pgDump = "pg_dump"
}

& $pgDump `
    --host=$PGHOST `
    --port=$PGPORT `
    --username=$PGUSER `
    --format=custom `
    --compress=9 `
    --schema=core `
    --schema=mart `
    --no-password `
    --file=$OUTFILE `
    $PGDB

if ($LASTEXITCODE -ne 0) {
    Write-Error "pg_dump failed with exit code $LASTEXITCODE"
    exit 1
}

$sizeMB = [math]::Round((Get-Item $OUTFILE).Length / 1MB, 2)
Write-Host "Backup complete: $OUTFILE ($sizeMB MB)"

# ─── Remove backups older than $KEEP_DAYS ─────────────────────
Write-Host "Pruning backups older than $KEEP_DAYS days..."
Get-ChildItem -Path $BACKUP_DIR -Filter "dwh_*.dump" |
    Where-Object { $_.LastWriteTime -lt (Get-Date).AddDays(-$KEEP_DAYS) } |
    ForEach-Object {
        Remove-Item $_.FullName
        Write-Host "  Removed: $($_.Name)"
    }

# ─── List current backups ─────────────────────────────────────
Write-Host "`nCurrent backups:"
Get-ChildItem -Path $BACKUP_DIR -Filter "dwh_*.dump" |
    Sort-Object LastWriteTime -Descending |
    ForEach-Object {
        $mb = [math]::Round($_.Length / 1MB, 2)
        Write-Host "  $($_.Name)  ($mb MB)"
    }

Write-Host "`nDone."

# ─── Restore instructions (for reference) ─────────────────────
# pg_restore --host=localhost --port=5432 --username=dwh_admin
#            --dbname=erp_dwh --schema=core --schema=mart
#            --clean --if-exists dwh_YYYYMMDD_HHMMSS.dump
