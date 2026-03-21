# ============================================================
# register_task.ps1
# Đăng ký Windows Task Scheduler để chạy pipeline hàng ngày
#
# Chạy 1 lần với quyền Administrator:
#   Right-click PowerShell → "Run as Administrator"
#   cd "d:\Data Warehouse"
#   .\scripts\register_task.ps1
#
# Task sẽ được đặt tên: "DWH Daily Pipeline"
# Chạy lúc: 06:00 mỗi ngày
# ============================================================

$ErrorActionPreference = "Stop"

# ─── Cấu hình ────────────────────────────────────────────────
$TASK_NAME   = "DWH Daily Pipeline"
$SCRIPT_PATH = "d:\Data Warehouse\scripts\run_daily.ps1"
$RUN_TIME    = "06:00"   # Giờ chạy hàng ngày (HH:mm)
$LOG_DIR     = "d:\Data Warehouse\logs"

# Lấy username hiện tại để đăng ký task
$currentUser = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name

# ─── Check đang chạy với quyền Admin không ───────────────────
$isAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole(
    [Security.Principal.WindowsBuiltInRole] "Administrator"
)
if (-not $isAdmin) {
    Write-Error "Script cần chạy với quyền Administrator. Right-click PowerShell → Run as Administrator."
    exit 1
}

# ─── Ensure log directory ─────────────────────────────────────
if (-not (Test-Path $LOG_DIR)) {
    New-Item -ItemType Directory -Path $LOG_DIR | Out-Null
    Write-Host "Created log directory: $LOG_DIR"
}

# ─── Xoá task cũ nếu có ──────────────────────────────────────
$existingTask = Get-ScheduledTask -TaskName $TASK_NAME -ErrorAction SilentlyContinue
if ($existingTask) {
    Unregister-ScheduledTask -TaskName $TASK_NAME -Confirm:$false
    Write-Host "Removed existing task: $TASK_NAME"
}

# ─── Tạo Task Scheduler action ───────────────────────────────
$action = New-ScheduledTaskAction `
    -Execute "powershell.exe" `
    -Argument "-NonInteractive -ExecutionPolicy Bypass -File `"$SCRIPT_PATH`"" `
    -WorkingDirectory "d:\Data Warehouse"

# ─── Trigger: mỗi ngày lúc $RUN_TIME ─────────────────────────
$trigger = New-ScheduledTaskTrigger -Daily -At $RUN_TIME

# ─── Settings ─────────────────────────────────────────────────
$settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -RunOnlyIfNetworkAvailable `
    -ExecutionTimeLimit "02:00:00" `
    -MultipleInstances IgnoreNew

# ─── Principal (chạy dưới user hiện tại) ─────────────────────
$principal = New-ScheduledTaskPrincipal `
    -UserId $currentUser `
    -LogonType S4U `
    -RunLevel Highest

# ─── Đăng ký task ────────────────────────────────────────────
Register-ScheduledTask `
    -TaskName   $TASK_NAME `
    -Action     $action `
    -Trigger    $trigger `
    -Settings   $settings `
    -Principal  $principal `
    -Description "ERP Data Warehouse daily pipeline: MySQL → staging → core (dbt) → mart. Log: $LOG_DIR\daily_YYYYMMDD.log"

Write-Host ""
Write-Host "=============================="
Write-Host "Task registered successfully!"
Write-Host "  Name    : $TASK_NAME"
Write-Host "  Runs at : $RUN_TIME every day"
Write-Host "  Script  : $SCRIPT_PATH"
Write-Host "  Logs    : $LOG_DIR\daily_YYYYMMDD.log"
Write-Host ""
Write-Host "Kiểm tra:"
Write-Host "  Get-ScheduledTask -TaskName '$TASK_NAME'"
Write-Host ""
Write-Host "Chạy thử ngay:"
Write-Host "  Start-ScheduledTask -TaskName '$TASK_NAME'"
Write-Host "=============================="

# ─── Bonus: Weekly backup task ───────────────────────────────
$BACKUP_TASK = "DWH Weekly Backup"
$BACKUP_SCRIPT = "d:\Data Warehouse\scripts\backup_dwh.ps1"

$existingBackup = Get-ScheduledTask -TaskName $BACKUP_TASK -ErrorAction SilentlyContinue
if ($existingBackup) {
    Unregister-ScheduledTask -TaskName $BACKUP_TASK -Confirm:$false
}

$backupAction  = New-ScheduledTaskAction `
    -Execute "powershell.exe" `
    -Argument "-NonInteractive -ExecutionPolicy Bypass -File `"$BACKUP_SCRIPT`"" `
    -WorkingDirectory "d:\Data Warehouse"

$backupTrigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Sunday -At "02:00"

Register-ScheduledTask `
    -TaskName   $BACKUP_TASK `
    -Action     $backupAction `
    -Trigger    $backupTrigger `
    -Settings   $settings `
    -Principal  $principal `
    -Description "Weekly pg_dump backup of core + mart schemas. Kept 7 days."

Write-Host ""
Write-Host "Backup task also registered:"
Write-Host "  Name    : $BACKUP_TASK"
Write-Host "  Runs at : Sunday 02:00"
Write-Host "  Output  : d:\Data Warehouse\backups\dwh_YYYYMMDD_HHMMSS.dump"
