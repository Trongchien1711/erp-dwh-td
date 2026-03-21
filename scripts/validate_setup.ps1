# ============================================================
# validate_setup.ps1
# Kiểm tra toàn bộ môi trường trước khi chạy pipeline
#
# Cách dùng:
#   .\scripts\validate_setup.ps1           # Kiểm tra cơ bản
#   .\scripts\validate_setup.ps1 -Strict   # Thoát lỗi nếu có check nào fail
#
# Các check bao gồm:
#   ✓ File .env tồn tại và có đủ biến cần thiết
#   ✓ Python + các module cần thiết cài đặt đúng
#   ✓ dbt executable tìm thấy
#   ✓ PostgreSQL kết nối được
#   ✓ MySQL kết nối được
#   ✓ Schemas tồn tại (staging, core, mart)
#   ✓ ETL watermark table tồn tại
#   ✓ SQL DDL files đầy đủ (01-07)
# ============================================================

param([switch]$Strict)

$ErrorActionPreference = "Continue"
$ROOT     = Split-Path $PSScriptRoot -Parent
$ENV_FILE = "$ROOT\.env"

$pass = 0
$fail = 0

function Write-Check {
    param([bool]$Ok, [string]$Label, [string]$Detail = "")
    if ($Ok) {
        Write-Host ("  [PASS] {0}" -f $Label) -ForegroundColor Green
        $script:pass++
    } else {
        $msg = if ($Detail) { "  [FAIL] {0} — {1}" -f $Label, $Detail } else { "  [FAIL] {0}" -f $Label }
        Write-Host $msg -ForegroundColor Red
        $script:fail++
    }
}

Write-Host ""
Write-Host "══════════════════════════════════════════════════════"
Write-Host " ERP DWH — Environment Validation"
Write-Host "══════════════════════════════════════════════════════"
Write-Host ""

# ─── 1. File .env ─────────────────────────────────────────────
Write-Host "[ 1 / 6 ] Environment file"

$envExists = Test-Path $ENV_FILE
Write-Check $envExists ".env file exists" "Run: copy .env.example .env"

$envVars = @{}
if ($envExists) {
    Get-Content $ENV_FILE | ForEach-Object {
        if ($_ -match "^([^#=]+)=(.*)$") {
            $envVars[$Matches[1].Trim()] = $Matches[2].Trim().Trim('"').Trim("'")
        }
    }
}

$required = @("MYSQL_HOST","MYSQL_PORT","MYSQL_USER","MYSQL_PASSWORD","MYSQL_DATABASE",
              "PG_HOST","PG_PORT","PG_USER","PG_PASSWORD","PG_DATABASE")
foreach ($v in $required) {
    $present = $envVars.ContainsKey($v) -and $envVars[$v] -ne ""
    Write-Check $present "  .env: $v is set"
}

# ─── 2. Python & packages ─────────────────────────────────────
Write-Host ""
Write-Host "[ 2 / 6 ] Python environment"

$python = $null
foreach ($candidate in @("python","python3","python3.11","python3.12")) {
    try {
        $ver = & $candidate --version 2>&1
        if ($ver -match "Python 3\.(1[01234])") { $python = $candidate; break }
    } catch {}
}
Write-Check ($null -ne $python) "Python 3.10+ found" "Install Python 3.11 or 3.12"

if ($python) {
    $modules = @("sqlalchemy","pymysql","psycopg2","pandas","dotenv","loguru")
    foreach ($mod in $modules) {
        $ok = (& $python -c "import $mod" 2>&1) -notmatch "ModuleNotFoundError"
        Write-Check $ok "  Python module: $mod" "Run: pip install -r elt/requirements.txt"
    }
}

# ─── 3. dbt executable ────────────────────────────────────────
Write-Host ""
Write-Host "[ 3 / 6 ] dbt"

$dbtFound = $false
foreach ($candidate in @("dbt","$ROOT\.venv\Scripts\dbt.exe")) {
    try {
        $ver = & $candidate --version 2>&1
        if ($ver -match "dbt") { $dbtFound = $true; break }
    } catch {}
}
Write-Check $dbtFound "dbt executable found" "Run: pip install dbt-postgres==1.9.0"

$profilesOk = Test-Path "$ROOT\dbt_project\profiles.yml"
Write-Check $profilesOk "dbt profiles.yml exists" "Copy dbt_project/profiles.yml.example to profiles.yml and fill credentials"

# ─── 4. PostgreSQL ────────────────────────────────────────────
Write-Host ""
Write-Host "[ 4 / 6 ] PostgreSQL connection"

$pgHost = if ($envVars["PG_HOST"]) { $envVars["PG_HOST"] } else { "localhost" }
$pgPort = if ($envVars["PG_PORT"]) { $envVars["PG_PORT"] } else { "5432" }
$pgUser = if ($envVars["PG_USER"]) { $envVars["PG_USER"] } else { "dwh_admin" }
$pgPass = $envVars["PG_PASSWORD"]
$pgDb   = if ($envVars["PG_DATABASE"]) { $envVars["PG_DATABASE"] } else { "erp_dwh" }

$pgOk = $false
if ($pgPass -and $python) {
    $pgTest = @"
import sys
from sqlalchemy import create_engine, text
try:
    e = create_engine('postgresql+psycopg2://$pgUser`:$pgPass@$pgHost`:$pgPort/$pgDb')
    with e.connect() as c: c.execute(text('SELECT 1'))
    print('ok')
except Exception as ex:
    print(str(ex))
"@
    $result = & $python -c $pgTest 2>&1
    $pgOk = ($result -eq "ok")
}
Write-Check $pgOk "PostgreSQL connection" "Check PG_HOST, PG_PORT, PG_USER, PG_PASSWORD in .env"

if ($pgOk -and $python) {
    foreach ($schema in @("staging","core","mart")) {
        $schemaTest = @"
from sqlalchemy import create_engine, text
e = create_engine('postgresql+psycopg2://$pgUser`:$pgPass@$pgHost`:$pgPort/$pgDb')
with e.connect() as c:
    r = c.execute(text("SELECT schema_name FROM information_schema.schemata WHERE schema_name='$schema'")).fetchone()
    print('ok' if r else 'missing')
"@
        $r = & $python -c $schemaTest 2>&1
        Write-Check ($r -eq "ok") "  Schema '$schema' exists" "Run SQL setup scripts 01-07"
    }

    $wmTest = @"
from sqlalchemy import create_engine, text
e = create_engine('postgresql+psycopg2://$pgUser`:$pgPass@$pgHost`:$pgPort/$pgDb')
with e.connect() as c:
    r = c.execute(text("SELECT to_regclass('staging.etl_watermark')")).fetchone()
    print('ok' if r and r[0] else 'missing')
"@
    $r = & $python -c $wmTest 2>&1
    Write-Check ($r -eq "ok") "  staging.etl_watermark exists" "Run: python elt/setup.py"
}

# ─── 5. MySQL ─────────────────────────────────────────────────
Write-Host ""
Write-Host "[ 5 / 6 ] MySQL (ERP source) connection"

$myHost = if ($envVars["MYSQL_HOST"]) { $envVars["MYSQL_HOST"] } else { "localhost" }
$myPort = if ($envVars["MYSQL_PORT"]) { $envVars["MYSQL_PORT"] } else { "3306" }
$myUser = if ($envVars["MYSQL_USER"]) { $envVars["MYSQL_USER"] } else { "root" }
$myPass = $envVars["MYSQL_PASSWORD"]
$myDb   = if ($envVars["MYSQL_DATABASE"]) { $envVars["MYSQL_DATABASE"] } else { "test" }

$myOk = $false
if ($myPass -and $python) {
    $myTest = @"
from sqlalchemy import create_engine, text
try:
    e = create_engine('mysql+pymysql://$myUser`:$myPass@$myHost`:$myPort/$myDb')
    with e.connect() as c: c.execute(text('SELECT 1'))
    print('ok')
except Exception as ex:
    print(str(ex))
"@
    $result = & $python -c $myTest 2>&1
    $myOk = ($result -eq "ok")
}
Write-Check $myOk "MySQL connection" "Check MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD in .env"

# ─── 6. SQL DDL files ─────────────────────────────────────────
Write-Host ""
Write-Host "[ 6 / 6 ] SQL DDL files"

for ($i = 1; $i -le 7; $i++) {
    $prefix = "{0:D2}" -f $i
    $files  = Get-ChildItem "$ROOT\sql\${prefix}_*.sql" -ErrorAction SilentlyContinue
    Write-Check ($files.Count -gt 0) "  sql/${prefix}_*.sql exists"
}

# ─── Summary ──────────────────────────────────────────────────
Write-Host ""
Write-Host "══════════════════════════════════════════════════════"
Write-Host (" Result: {0} passed, {1} failed" -f $pass, $fail)
Write-Host "══════════════════════════════════════════════════════"
Write-Host ""

if ($fail -gt 0) {
    if ($Strict) {
        Write-Host "Validation failed ($fail issues) — see details above." -ForegroundColor Red
        exit 1
    } else {
        Write-Host "Fix the issues above before running the pipeline." -ForegroundColor Yellow
    }
} else {
    Write-Host "All checks passed — environment is ready!" -ForegroundColor Green
    Write-Host ""
    Write-Host "Next step: python elt/pipeline.py --stage all"
}
