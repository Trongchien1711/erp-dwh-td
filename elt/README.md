# ETL Pipeline – ERP DWH

Python ELT pipeline: **MySQL (ERP) → staging (PostgreSQL) → core (PostgreSQL)**

---

## Cấu trúc
etl/ ├── .env.example # Mẫu biến môi trường ├── requirements.txt # Python dependencies ├── connections.py # Kết nối MySQL + PostgreSQL ├── watermark.py # Quản lý incremental watermark ├── extractor.py # Extract từ MySQL ├── loader.py # Load vào staging PostgreSQL ├── transform_core.py # Transform staging → core (dim + fact) └── pipeline.py # Main runner

Code

---

## Setup

```bash
# 1. Tạo virtual environment
python -m venv .venv
source .venv/bin/activate   # Linux/Mac
.venv\Scripts\activate      # Windows

# 2. Cài dependencies
pip install -r etl/requirements.txt

# 3. Tạo file .env
cp etl/.env.example etl/.env
# Điền thông tin kết nối MySQL và PostgreSQL vào .env
Chạy pipeline
bash
# Chạy toàn bộ (extract + load + transform)
python etl/pipeline.py

# Chỉ extract + load staging
python etl/pipeline.py --stage extract

# Chỉ transform staging → core
python etl/pipeline.py --stage transform

# Chỉ chạy 1 bảng
python etl/pipeline.py --stage extract --table tbl_orders
Load Strategy
Bảng	Strategy	Watermark Column
tbl_orders	Incremental	date_updated
tbl_order_items	Incremental	date_active
tbl_deliveries	Incremental	date_updated
tblclients	Incremental	date_update
tbl_products	Incremental	date_updated
tblwarehouse_product	Incremental	date_warehouse
tblwarehouse_export	Incremental	date_warehouse
tblsuppliers	Incremental	date_update
tblpurchase_order	Incremental	date_create
tbl_productions_orders	Incremental	date_updated
tbl_manufactures	Incremental	date_updated
tblstaff	Incremental	date_update
tbl_delivery_items	Full load	-
tblwarehouse	Full load	-
tbldepartments	Full load	-
Logs
Logs được lưu tại logs/pipeline_YYYY-MM-DD.log

Code
Please confirm you want Copilot to make this change in the Trongchien1711/erp-dwh-td repository on branch main.

Make these code changes?
.gitignore

gitignore
# Environment
.env
*.env

# Python
__pycache__/
*.py[cod]
*.pyo
.venv/
venv/
env/
*.egg-info/
dist/
build/

# Logs
logs/
*.log

# IDE
.vscode/
.idea/
*.swp

# OS
.DS_Store
Thumbs.db

# dbt
dbt_project/target/
dbt_project/dbt_packages/
dbt_project/logs/
dbt_project/.user.yml



