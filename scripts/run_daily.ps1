# ============================================================
# run_daily.ps1
# Chạy full pipeline hàng ngày: Extract → Load → Transform → dbt
#
# Cách dùng:
#   .\scripts\run_daily.ps1               # full pipeline + dbt
#   .\scripts\run_daily.ps1 -SkipDbt      # chỉ chạy Python pipeline
#   .\scripts\run_daily.ps1 -Stage extract # chỉ stage extract
#
# Log: d:\Data Warehouse\logs\daily_YYYYMMDD.log
# ============================================================

param(
    [switch]$SkipDbt,
    [string]$Stage = "all"
)

$ErrorActionPreference = "Stop"

# ─── Paths ───────────────────────────────────────────────────
$ROOT    = "d:\Data Warehouse"
$ELT_DIR = "$ROOT\elt"
$LOG_DIR = "$ROOT\logs"
$PYTHON  = "$ROOT\.venv\Scripts\python.exe"
$DBT     = "$ROOT\.venv\Scripts\dbt.exe"
$DATE    = Get-Date -Format "yyyyMMdd"
$NOW     = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
$LOG     = "$LOG_DIR\daily_$DATE.log"

# ─── Ensure log directory ─────────────────────────────────────
if (-not (Test-Path $LOG_DIR)) { New-Item -ItemType Directory -Path $LOG_DIR | Out-Null }

function Write-Log {
    param([string]$Message, [string]$Level = "INFO")
    $ts = Get-Date -Format "HH:mm:ss"
    $line = "[$ts] [$Level] $Message"
    Write-Host $line
    Add-Content -Path $LOG -Value $line -Encoding UTF8
}

# ─── Start ───────────────────────────────────────────────────
Write-Log "============================== DAILY RUN START =============================="
Write-Log "Date: $NOW | Stage: $Stage | SkipDbt: $SkipDbt"

$exitCode = 0

# ─── Step 1: Python ELT Pipeline ─────────────────────────────
Write-Log "--- Step 1: Python ELT Pipeline ---"
try {
    $pipelineArgs = @("$ELT_DIR\pipeline.py", "--stage", $Stage)
    $output = & $PYTHON @pipelineArgs 2>&1
    $output | ForEach-Object { Write-Log "  $_" }
    Write-Log "Step 1 PASSED" "INFO"
} catch {
    Write-Log "Step 1 FAILED: $_" "ERROR"
    $exitCode = 1
}

# ─── Step 2: dbt run ─────────────────────────────────────────
if (-not $SkipDbt -and $exitCode -eq 0) {
    Write-Log "--- Step 2: dbt run ---"
    try {
        $env:PYTHONUTF8 = "1"
        $dbtArgs = @(
            "run",
            "--profiles-dir", "$ROOT\dbt_project",
            "--project-dir",  "$ROOT\dbt_project"
        )
        $output = & $DBT @dbtArgs 2>&1
        $output | ForEach-Object { Write-Log "  $_" }

        # Check for failures in dbt output
        $failures = $output | Where-Object { $_ -match "ERROR|FAIL" }
        if ($failures) {
            Write-Log "Step 2 had failures:" "WARN"
            $failures | ForEach-Object { Write-Log "  $_" "WARN" }
        } else {
            Write-Log "Step 2 PASSED" "INFO"
        }
    } catch {
        Write-Log "Step 2 FAILED: $_" "ERROR"
        $exitCode = 1
    }
} elseif ($exitCode -ne 0) {
    Write-Log "Step 2 SKIPPED (pipeline failed)" "WARN"
}

# ─── Step 3: dbt test ────────────────────────────────────────
if (-not $SkipDbt -and $exitCode -eq 0) {
    Write-Log "--- Step 3: dbt test ---"
    try {
        $env:PYTHONUTF8 = "1"
        $dbtTestArgs = @(
            "test",
            "--profiles-dir", "$ROOT\dbt_project",
            "--project-dir",  "$ROOT\dbt_project"
        )
        $output = & $DBT @dbtTestArgs 2>&1
        $output | ForEach-Object { Write-Log "  $_" }

        $testFails = $output | Where-Object { $_ -match "FAIL|ERROR" }
        if ($testFails) {
            Write-Log "Step 3 had test failures — review dbt test output" "WARN"
        } else {
            Write-Log "Step 3 PASSED" "INFO"
        }
    } catch {
        Write-Log "Step 3 FAILED: $_" "ERROR"
        $exitCode = 1
    }
}

# ─── Step 4: Post-pipeline health check ─────────────────────
if ($exitCode -eq 0) {
    Write-Log "--- Step 4: Post-pipeline health check ---"
    try {
        $output = & $PYTHON "$ROOT\check_pipeline_health.py" 2>&1
        $output | ForEach-Object { Write-Log "  $_" }
        Write-Log "Step 4 health check done" "INFO"
    } catch {
        Write-Log "Step 4 WARNING: health check failed: $_" "WARN"
    }
}

# ─── Summary ─────────────────────────────────────────────────
if ($exitCode -eq 0) {
    Write-Log "============================== DAILY RUN COMPLETE (SUCCESS) =============================="
} else {
    Write-Log "============================== DAILY RUN COMPLETE (FAILED — see above) =============================="  "ERROR"
}

exit $exitCode
