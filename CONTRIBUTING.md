# Contributing to ERP Data Warehouse

Hướng dẫn này dành cho developer muốn mở rộng hoặc chỉnh sửa project.

## Mục lục

1. [Thêm bảng nguồn mới từ MySQL](#1-thêm-bảng-nguồn-mới-từ-mysql)
2. [Thêm transform mới (staging → core)](#2-thêm-transform-mới-staging--core)
3. [Thêm dbt mart model mới](#3-thêm-dbt-mart-model-mới)
4. [Quy ước đặt tên](#4-quy-ước-đặt-tên)
5. [Quy trình test trước khi commit](#5-quy-trình-test-trước-khi-commit)
6. [CI/CD checklist](#6-cicd-checklist)

---

## 1. Thêm bảng nguồn mới từ MySQL

### Bước 1 — DDL staging table

Thêm `CREATE TABLE` vào [sql/06_staging_tables.sql](sql/06_staging_tables.sql):

```sql
CREATE TABLE staging.tbl_new_source (
    id              INT,
    date            TIMESTAMP,
    -- ... tất cả cột cần extract ...
    etl_loaded_at   TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_stg_new_id ON staging.tbl_new_source(id);
```

Sau đó chạy trực tiếp trên PostgreSQL để tạo bảng:

```sql
-- Chạy trên psql hoặc pgAdmin:
CREATE TABLE staging.tbl_new_source (...);
```

### Bước 2 — Khai báo trong extractor

Mở [elt/extractor.py](elt/extractor.py) và thêm vào `TABLE_CONFIG`:

```python
{
    "source": "tbl_new_source",       # Tên bảng trong MySQL
    "wm_col": "date",                 # Cột watermark (timestamp hoặc id)
    "wm_type": "timestamp",           # "timestamp" | "id"
    "load_type": "incremental",       # "incremental" | "full"
},
```

> Dùng `load_type: "full"` cho bảng master nhỏ không có cột timestamp.

### Bước 3 — Chạy thử

```powershell
python elt/pipeline.py --stage extract
python check_watermark.py
```

---

## 2. Thêm transform mới (staging → core)

### Cấu trúc SQL trong transform_core.py

Mở [elt/transform_core.py](elt/transform_core.py) và thêm hằng số SQL:

```python
SQL_DIM_NEW = """
INSERT INTO core.dim_new (new_key, natural_key, name, ...)
SELECT ...
FROM staging.tbl_new_source
ON CONFLICT (natural_key) DO UPDATE SET
    name = EXCLUDED.name,
    ...
    updated_at = NOW();
"""
```

Thêm vào `_SOFT_STEPS` (bảng dimension) hoặc `_HARD_STEPS` (fact table):

```python
_SOFT_STEPS = [
    ("dim_customer",   SQL_DIM_CUSTOMER),
    ("dim_product",    SQL_DIM_PRODUCT),
    ("dim_new",        SQL_DIM_NEW),       # ← Thêm vào đây
    ...
]
```

> **Soft step**: lỗi sẽ log WARN nhưng pipeline tiếp tục.  
> **Hard step**: lỗi sẽ dừng pipeline ngay lập tức.

### Chạy thử transform

```powershell
python elt/pipeline.py --stage transform
python check_db.py
```

---

## 3. Thêm dbt mart model mới

### Bước 1 — Tạo file SQL

```
dbt_project/models/marts/{domain}/fct_new_metric.sql
```

```sql
-- fct_new_metric.sql
{{ config(materialized='table', schema='mart') }}

SELECT
    d.date_key          AS metric_date_key,
    c.customer_key,
    p.product_key,
    SUM(oi.amount)      AS total_amount
FROM {{ ref('stg_order_items') }} oi
JOIN {{ ref('stg_orders') }} o ON oi.order_id = o.id
JOIN {{ ref('dim_date') }} d   ON d.full_date = o.date::DATE
JOIN {{ ref('dim_customer_mart') }} c ON c.customer_id = o.customer_id
JOIN {{ ref('dim_product_mart') }} p  ON p.product_id = oi.item_id
WHERE oi.active = 1
GROUP BY 1, 2, 3
```

### Bước 2 — Thêm YAML documentation

Mở file `_[domain]_models.yml` trong cùng thư mục và thêm:

```yaml
- name: fct_new_metric
  description: "Mô tả model: grain, purpose, data source"
  columns:
    - name: metric_date_key
      description: "FK → dim_date (YYYYMMDD)"
      tests: [not_null]
    - name: customer_key
      description: "FK → dim_customer_mart"
      tests: [not_null]
    - name: product_key
      description: "FK → dim_product_mart"
      tests: [not_null]
    - name: total_amount
      description: "Tổng doanh thu trước thuế"
      tests: [not_null]
```

### Bước 3 — Test

```powershell
cd dbt_project
dbt parse --profiles-dir . --project-dir .
dbt run  --profiles-dir . --select fct_new_metric
dbt test --profiles-dir . --select fct_new_metric
```

### Bước 4 — Cập nhật tài liệu

- Nếu là fact mới: cập nhật [docs/bus_matrix.md](docs/bus_matrix.md)
- Thêm mô tả domain vào `docs/domain_{domain}.md`
- Cập nhật [docs/grain_definition.md](docs/grain_definition.md) nếu grain mới

---

## 4. Quy ước đặt tên

| Layer | Prefix | Ví dụ |
|-------|--------|-------|
| Staging SQL | `stg_` | `stg_orders.sql` |
| Intermediate | `int_` | `int_orders_enriched.sql` |
| Dimension mart | `dim_` | `dim_customer_mart.sql` |
| Fact mart | `fct_` | `fct_revenue.sql` |
| Staging table (PG) | `tbl_` | `staging.tbl_orders` |
| Core dimension | `dim_` | `core.dim_customer` |
| Core fact | `fact_` | `core.fact_orders` |

**Cột khóa:**
- Surrogate key: `{table}_key` (INTEGER, SERIAL) — ví dụ `customer_key`
- Natural key: `{entity}_id` — ví dụ `customer_id`
- Date key: `{context}_date_key` — ví dụ `order_date_key` (INT YYYYMMDD)

---

## 5. Quy trình test trước khi commit

```powershell
# 1. Lint Python
ruff check elt/ check_db.py check_watermark.py --ignore E501,E402

# 2. Syntax check tất cả module ELT
python -m py_compile elt/pipeline.py
python -m py_compile elt/extractor.py
# ... tương tự cho các file khác

# 3. dbt parse (không cần DB thật)
cd dbt_project
dbt parse --profiles-dir . --project-dir .

# 4. Nếu có DB local — chạy full pipeline
python elt/pipeline.py --stage all
cd dbt_project && dbt run --profiles-dir . && dbt test --profiles-dir .

# 5. Health check
python check_pipeline_health.py
```

---

## 6. CI/CD checklist

Trước khi tạo PR / push lên `master`:

- [ ] `ruff check` không có lỗi
- [ ] `dbt parse` thành công (không có compilation error)
- [ ] Không có credentials hardcode trong code (CI tự động scan)
- [ ] File `.yml` hợp lệ (CI validate YAML)
- [ ] SQL DDL files `01`–`07` đầy đủ (CI kiểm tra)
- [ ] Nếu thêm model mới: có YAML documentation
- [ ] Nếu đổi schema core: cập nhật `sql/06_staging_tables.sql`

CI sẽ tự chạy khi push lên `master` — xem kết quả tại GitHub Actions.
