$env:PG_PASSWORD='881686'
Set-Location "d:\Data Warehouse"
& ".\.venv_dbt\Scripts\dbt.exe" run --select fct_stock_monthly_snapshot --profiles-dir dbt_project --project-dir dbt_project
