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
# Tu dong tim pg_dump: uu tien tren PATH, sau do thu cac version pho bien
$pgDumpCmd = Get-Command pg_dump -ErrorAction SilentlyContinue
$pgDump = if ($pgDumpCmd) { $pgDumpCmd.Source } else { $null }
if (-not $pgDump) {
    foreach ($v in @(18,17,16,15,14)) {
        $candidate = "C:\Program Files\PostgreSQL\$v\bin\pg_dump.exe"
        if (Test-Path $candidate) { $pgDump = $candidate; break }
    }
}
if (-not $pgDump) { Write-Error "pg_dump not found. Install PostgreSQL or add bin to PATH."; exit 1 }
$PG_BIN     = Split-Path $pgDump -Parent
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
    Write-Warning ".env not found -- PGPASSWORD must be set manually"
}

# ─── Ensure backup directory ──────────────────────────────────
if (-not (Test-Path $BACKUP_DIR)) {
    New-Item -ItemType Directory -Path $BACKUP_DIR | Out-Null
    Write-Host "Created backup directory: $BACKUP_DIR"
}

# ─── Run pg_dump ──────────────────────────────────────────────
Write-Host "[$DATE] Starting backup -> $OUTFILE"
Write-Host "  Using: $pgDump"

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
