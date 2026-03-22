# run_pipeline.ps1 -- Run the full DWH pipeline end-to-end
#
# Usage:
#   .\scripts\run_pipeline.ps1            # full run (ELT + dbt)
#   .\scripts\run_pipeline.ps1 -EltOnly   # ELT only, skip dbt
#   .\scripts\run_pipeline.ps1 -DbtOnly   # dbt only, skip ELT

param(
    [switch]$EltOnly,
    [switch]$DbtOnly
)

Set-StrictMode -Off
$ErrorActionPreference = "Continue"

$root     = Split-Path $PSScriptRoot -Parent
$python   = "$root\.venv\Scripts\python.exe"
$dbt      = "$root\.venv_dbt\Scripts\dbt.exe"
$profiles = "$root\dbt_project"

function Banner($text) {
    $line = "=" * 60
    Write-Host ""
    Write-Host $line -ForegroundColor Cyan
    Write-Host "  $text" -ForegroundColor Cyan
    Write-Host $line -ForegroundColor Cyan
    Write-Host ""
}

function Fail($msg) {
    Write-Host ""
    Write-Host "[FAIL] $msg" -ForegroundColor Red
    exit 1
}

function Import-DotEnv($path) {
    Get-Content $path | Where-Object { $_ -match "^\s*[^#]\S+=\S" } | ForEach-Object {
        $key, $val = $_ -split "=", 2
        $key = $key.Trim()
        $val = $val.Trim().Trim('"').Trim("'")
        [System.Environment]::SetEnvironmentVariable($key, $val, "Process")
    }
}

# -- Preflight -------------------------------------------------------
if (-not (Test-Path $python)) {
    Fail ".venv not found. Run: python -m venv .venv && .venv\Scripts\pip install -r elt\requirements.txt"
}
if (-not $EltOnly -and -not (Test-Path $dbt)) {
    Fail ".venv_dbt not found. Run: py -3.11 -m venv .venv_dbt && .venv_dbt\Scripts\pip install dbt-postgres==1.9.0"
}
if (-not (Test-Path "$root\.env")) {
    Fail ".env file missing. Copy .env.example -> .env and fill in credentials."
}

# Load .env into current process environment (dbt reads env vars, not .env directly)
Import-DotEnv "$root\.env"
Write-Host "[ENV] Loaded .env from $root\.env" -ForegroundColor DarkGray

# -- Stage 1: ELT (MySQL -> staging -> core) -------------------------
if (-not $DbtOnly) {
    Banner "STAGE 1 -- ELT Pipeline (MySQL -> staging -> core)"
    $start = Get-Date
    & $python "$root\elt\pipeline.py" --stage all
    if ($LASTEXITCODE -ne 0) { Fail "ELT pipeline exited with code $LASTEXITCODE" }
    $elapsed = [int]((Get-Date) - $start).TotalSeconds
    Write-Host ""
    Write-Host "[OK] ELT completed in ${elapsed}s" -ForegroundColor Green
}

# -- Stage 2: dbt (core -> mart) -------------------------------------
if (-not $EltOnly) {
    Banner "STAGE 2 -- dbt (core -> mart)"
    $env:PYTHONUTF8 = "1"
    $start = Get-Date
    & $dbt run --profiles-dir $profiles --project-dir $profiles
    if ($LASTEXITCODE -ne 0) { Fail "dbt run exited with code $LASTEXITCODE" }
    $elapsed = [int]((Get-Date) - $start).TotalSeconds
    Write-Host ""
    Write-Host "[OK] dbt completed in ${elapsed}s" -ForegroundColor Green
}

Write-Host ""
Write-Host "[DONE] Full pipeline finished successfully." -ForegroundColor Green
