# ETL Pipeline – ERP DWH

## Kiến trúc

```
MySQL (ERP) → staging (PostgreSQL) → core (PostgreSQL)
```

## Setup

```bash
cd etl
cp .env.example .env
# Điền thông tin kết nối MySQL và PostgreSQL vào .env

pip install -r requirements.txt
```

## Chạy pipeline

```bash
# Toàn bộ (extract + transform)
python pipeline.py

# Chỉ extract MySQL → staging
python pipeline.py --stage extract

# Chỉ transform staging → core
python pipeline.py --stage transform

# Full refresh 1 bảng (truncate + reload)
python pipeline.py --stage extract --table tbl_orders --mode truncate

# Debug 1 bảng
python pipeline.py --stage extract --table tblclients
```

## Cấu trúc file

| File | Vai trò |
|------|---------|
| `connections.py` | Engine MySQL + PostgreSQL |
| `watermark.py` | Incremental load watermark |
| `extractor.py` | Extract từ MySQL |
| `loader.py` | Load vào staging PostgreSQL |
| `transform_core.py` | staging → core (dim + fact) |
| `pipeline.py` | Main runner |
