# Hướng dẫn sử dụng Data Warehouse

## Mục lục

1. [Kiến trúc tổng quan](#1-kiến-trúc-tổng-quan)
2. [Thông tin kết nối](#2-thông-tin-kết-nối)
3. [Chạy pipeline hằng ngày](#3-chạy-pipeline-hằng-ngày)
4. [Các tùy chọn chạy pipeline](#4-các-tùy-chọn-chạy-pipeline)
5. [Thêm bảng nguồn mới từ MySQL](#5-thêm-bảng-nguồn-mới-từ-mysql)
6. [Thêm transform mới (staging → core)](#6-thêm-transform-mới-staging--core)
7. [Thêm dbt mart model mới](#7-thêm-dbt-mart-model-mới)
8. [Reset và re-extract dữ liệu](#8-reset-và-re-extract-dữ-liệu)
9. [Những điểm quan trọng cần lưu ý](#9-những-điểm-quan-trọng-cần-lưu-ý)
10. [Kiểm tra sức khỏe DWH](#10-kiểm-tra-sức-khỏe-dwh)
11. [Xử lý sự cố thường gặp](#11-xử-lý-sự-cố-thường-gặp)

---

## 1. Kiến trúc tổng quan

```
MySQL (ERP nguồn)
     │
     │  STAGE 1 — Extract + Load    (elt/pipeline.py)
     │  • Incremental (watermark) cho bảng có cột timestamp/id
     │  • Full load cho bảng master nhỏ hoặc không có timestamp
     ▼
PostgreSQL: staging       ← Bản sao thô của MySQL (TRUNCATE + COPY mỗi lần)
     │
     │  STAGE 2 — Transform         (elt/transform_core.py)
     │  • UPSERT 9 Dimension tables (SCD Type 1)
     │  • INSERT-only 9 Fact tables (+ self-healing CTE cho NULL keys)
     ▼
PostgreSQL: core          ← Star Schema — Dimensions & Facts
     │
     │  STAGE 3 — Analytics Layer   (dbt Core)
     │  • 12 mart models (table materialization)
     ▼
PostgreSQL: mart          ← Bảng phân tích sẵn sàng dùng cho BI
```

### Thư mục project

```
d:\Data Warehouse\
│
├── elt/
│   ├── pipeline.py         ← ĐIỂM VÀO CHÍNH
│   ├── extractor.py        ← TABLE_CONFIG + logic extract từ MySQL
│   ├── loader.py           ← Bulk load vào staging (COPY protocol)
│   ├── transform_core.py   ← SQL transforms staging → core
│   ├── watermark.py        ← Quản lý trạng thái "đã sync đến đâu"
│   ├── connections.py      ← DB engine factories (đọc từ .env)
│   └── setup.py            ← Chạy 1 lần: GRANT + ALTER schema
│
├── dbt_project/
│   ├── models/
│   │   ├── staging/        ← dbt views trên core schema
│   │   ├── intermediate/   ← Bảng trung gian
│   │   └── marts/          ← Bảng mart cuối cùng (finance/sales/inventory)
│   └── profiles.yml        ← Kết nối PostgreSQL cho dbt
│
├── sql/                    ← DDL scripts — chạy 1 lần theo thứ tự
├── docs/                   ← Tài liệu
├── check_db.py             ← Kiểm tra row counts core + mart
└── check_watermark.py      ← Kiểm tra watermarks + staging row counts
```

---

## 2. Thông tin kết nối

| Thành phần | Giá trị |
|---|---|
| **Python (pipeline)** | `C:\Users\bus_an\AppData\Local\Python\pythoncore-3.14-64\python.exe` |
| **Python (dbt)** | `C:\Users\bus_an\AppData\Local\Programs\Python\Python311\Scripts\dbt.exe` |
| **PostgreSQL** | `localhost:5432` · database `erp_dwh` · user `dwh_admin` |
| **MySQL (ERP)** | `localhost:3306` · database `test` · user `root` |

> Credentials đọc từ file `elt/.env` — không hardcode trong code.

---

## 3. Chạy pipeline hằng ngày

Thứ tự chuẩn: **pipeline → dbt → kiểm tra**

```powershell
# BƯỚC 1: Chạy ELT pipeline (Extract + Load + Transform)
cd "d:\Data Warehouse\elt"
& "C:\Users\bus_an\AppData\Local\Python\pythoncore-3.14-64\python.exe" pipeline.py --stage all

# BƯỚC 2: Chạy dbt để cập nhật mart layer
$env:PYTHONUTF8 = "1"
& "C:\Users\bus_an\AppData\Local\Programs\Python\Python311\Scripts\dbt.exe" run `
    --profiles-dir "d:\Data Warehouse\dbt_project" `
    --project-dir  "d:\Data Warehouse\dbt_project"

# BƯỚC 3: Kiểm tra kết quả
cd "d:\Data Warehouse"
& "C:\Users\bus_an\AppData\Local\Python\pythoncore-3.14-64\python.exe" check_db.py
& "C:\Users\bus_an\AppData\Local\Python\pythoncore-3.14-64\python.exe" check_watermark.py
```

**Output bình thường của pipeline:**
```
08:14:09 | INFO    | Pipeline started at 2026-03-21 08:14:09
08:14:28 | SUCCESS | [Transform] fact_orders -> 0 keys fixed, 0 new rows inserted.
08:14:28 | SUCCESS | Pipeline finished in 19.2s
```

**Output bình thường của dbt:**
```
Done. PASS=20 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=20
```

---

## 4. Các tùy chọn chạy pipeline

| Lệnh | Khi nào dùng |
|---|---|
| `pipeline.py --stage all` | **Chuẩn** — chạy đầy đủ hằng ngày |
| `pipeline.py --stage extract` | Chỉ kéo dữ liệu vào staging, không transform |
| `pipeline.py --stage transform` | Chỉ chạy transform (staging đã có sẵn) |
| `pipeline.py --stage extract --table tbl_orders` | Debug 1 bảng cụ thể |

### Ví dụ chạy một bảng để debug

```powershell
cd "d:\Data Warehouse\elt"
& "C:\Users\bus_an\AppData\Local\Python\pythoncore-3.14-64\python.exe" `
    pipeline.py --stage extract --table tbl_orders
```

---

## 5. Thêm bảng nguồn mới từ MySQL

### Bước 1 — Thêm vào `TABLE_CONFIG` trong `elt/extractor.py`

Mở file [elt/extractor.py](../elt/extractor.py) và thêm entry vào `TABLE_CONFIG`:

```python
TABLE_CONFIG = [
    # ... các bảng hiện có ...
    {
        "source_table":  "ten_bang_mysql",   # tên chính xác trong MySQL
        "watermark_col": "date_updated",     # xem bảng chọn watermark bên dưới
    },
]
```

### Chọn `watermark_col` như thế nào

| Trường hợp | `watermark_col` | Lưu ý |
|---|---|---|
| Bảng có cột `datetime` NOT NULL, đáng tin | `"date_updated"` | Kiểm tra không có rows NULL trước |
| Bảng có `id` auto_increment, chỉ thêm mới | `"id"` | Không dùng nếu ERP có thể xóa rows |
| Bảng nhỏ (<20K rows) hoặc không có timestamp | `None` | Full load — TRUNCATE + COPY mỗi lần |

> ⚠️ **Bẫy phổ biến**: Nếu dùng `date_updated` nhưng có rows `date_updated = NULL`, những rows đó sẽ **không bao giờ được extract** (COALESCE trả về `'2000-01-01'` < watermark). Xem bài học từ `tbl_products`.

### Bước 2 — Chạy pipeline lần đầu để tạo bảng staging

```powershell
cd "d:\Data Warehouse\elt"
& "C:\Users\bus_an\AppData\Local\Python\pythoncore-3.14-64\python.exe" `
    pipeline.py --stage extract --table ten_bang_mysql
```

Bảng `staging.ten_bang_mysql` sẽ tự động được tạo với đầy đủ columns từ MySQL.

### Bước 3 — Kiểm tra staging

```powershell
cd "d:\Data Warehouse"
& "C:\Users\bus_an\AppData\Local\Python\pythoncore-3.14-64\python.exe" check_watermark.py
```

---

## 6. Thêm transform mới (staging → core)

### Bước 1 — Tạo bảng core (DDL)

Viết DDL vào `sql/` và chạy trong pgAdmin hoặc psql **trước** khi viết transform:

```sql
-- Ví dụ: sql/09_new_dim.sql
CREATE TABLE IF NOT EXISTS core.dim_moi (
    moi_key     SERIAL PRIMARY KEY,
    moi_id      INTEGER UNIQUE NOT NULL,
    moi_name    VARCHAR(200),
    is_active   BOOLEAN DEFAULT TRUE,
    etl_loaded_at TIMESTAMP,
    etl_source  VARCHAR(100)
);
```

### Bước 2 — Viết SQL transform trong `elt/transform_core.py`

**Pattern cho Dimension (UPSERT — SCD Type 1):**

```python
SQL_DIM_MOI = """
WITH updated AS (
    UPDATE core.dim_moi d
    SET
        moi_name      = src.name,
        is_active     = (src.status = 1),
        etl_loaded_at = NOW()
    FROM staging.ten_bang_mysql src
    WHERE d.moi_id = src.id
    RETURNING d.moi_id
)
INSERT INTO core.dim_moi (moi_id, moi_name, is_active, etl_loaded_at, etl_source)
SELECT src.id, src.name, (src.status = 1), NOW(), 'ten_bang_mysql'
FROM staging.ten_bang_mysql src
WHERE NOT EXISTS (SELECT 1 FROM updated u WHERE u.moi_id = src.id);
"""
```

**Pattern cho Fact (INSERT-only, tự heal NULL keys):**

```python
SQL_FACT_MOI = """
WITH fix_keys AS (
    -- Backfill NULL keys khi dim được thêm sau
    UPDATE core.fact_moi f
    SET aaa_key = COALESCE(da.aaa_key, f.aaa_key)
    FROM staging.ten_bang_mysql s
    LEFT JOIN core.dim_aaa da ON da.aaa_id = s.aaa_id
    WHERE f.moi_id = s.id
      AND f.aaa_key IS NULL
    RETURNING 1
),
new_rows AS (
    INSERT INTO core.fact_moi (moi_id, aaa_key, bbb_key, date_key, ...)
    SELECT
        s.id,
        da.aaa_key,
        db.bbb_key,
        TO_CHAR(s.date::DATE, 'YYYYMMDD')::INT,
        ...
    FROM staging.ten_bang_mysql s
    LEFT JOIN core.dim_aaa da ON da.aaa_id = s.aaa_id
    LEFT JOIN core.dim_bbb db ON db.bbb_id = s.bbb_id
    WHERE NOT EXISTS (
        SELECT 1 FROM core.fact_moi f WHERE f.moi_id = s.id
    )
    RETURNING 1
)
SELECT
    (SELECT count(*) FROM fix_keys) AS keys_fixed,
    (SELECT count(*) FROM new_rows) AS rows_inserted;
"""
```

> Nếu fact đơn giản (không cần fix_keys), dùng `INSERT ... WHERE NOT EXISTS` thông thường.

### Bước 3 — Thêm vào `TRANSFORM_STEPS`

```python
TRANSFORM_STEPS = [
    # Dimensions PHẢI chạy trước facts
    ("dim_customer",  SQL_DIM_CUSTOMER),
    # ...
    ("dim_moi",       SQL_DIM_MOI),      # ← thêm dim mới ở đây, TRƯỚC facts
    # Facts
    ("fact_orders",   SQL_FACT_ORDERS),
    # ...
    ("fact_moi",      SQL_FACT_MOI),     # ← thêm fact mới ở đây, SAU tất cả dims
]
```

### Bước 4 — Test

```powershell
cd "d:\Data Warehouse\elt"
& "C:\Users\bus_an\AppData\Local\Python\pythoncore-3.14-64\python.exe" `
    pipeline.py --stage transform
```

---

## 7. Thêm dbt mart model mới

### Bước 1 — Tạo file SQL trong thư mục marts phù hợp

```
dbt_project/models/marts/
├── sales/      ← doanh thu, đơn hàng
├── finance/    ← lợi nhuận, công nợ
├── inventory/  ← tồn kho, nhập xuất
└── shared_dim/ ← dim dùng chung
```

Ví dụ tạo `dbt_project/models/marts/sales/fct_new_model.sql`:

```sql
{{ config(materialized='table') }}

SELECT
    f.order_id,
    dd.date_key         AS order_date_key,
    dd.year,
    dd.month_name,
    dc.customer_name,
    dp.product_name,
    f.quantity,
    f.amount
FROM core.fact_orders f
LEFT JOIN core.dim_customer dc ON dc.customer_key = f.customer_key
LEFT JOIN core.dim_product  dp ON dp.product_key  = f.product_key   -- qua fact_order_items
LEFT JOIN core.dim_date     dd ON dd.date_key     = f.order_date_key
```

### Bước 2 — Khai báo trong `_models.yml` (cùng thư mục)

```yaml
models:
  - name: fct_new_model
    description: "Mô tả ngắn về model này"
    columns:
      - name: order_id
        description: "Order ID"
        tests:
          - not_null
```

### Bước 3 — Chạy dbt

```powershell
$env:PYTHONUTF8 = "1"

# Chỉ chạy model mới
& "C:\Users\bus_an\AppData\Local\Programs\Python\Python311\Scripts\dbt.exe" run `
    --select fct_new_model `
    --profiles-dir "d:\Data Warehouse\dbt_project" `
    --project-dir  "d:\Data Warehouse\dbt_project"

# Chạy model mới + tất cả downstream
& "C:\Users\bus_an\AppData\Local\Programs\Python\Python311\Scripts\dbt.exe" run `
    --select fct_new_model+ `
    --profiles-dir "d:\Data Warehouse\dbt_project" `
    --project-dir  "d:\Data Warehouse\dbt_project"
```

---

## 8. Reset và re-extract dữ liệu

### Re-extract toàn bộ 1 bảng (giữ core không đổi)

```sql
-- Chạy trong pgAdmin / psql
UPDATE staging.etl_watermark
SET last_loaded_at = '2020-01-01 00:00:00', updated_at = NOW()
WHERE table_name = 'tbl_orders';
```

Sau đó:
```powershell
pipeline.py --stage extract --table tbl_orders
```

> Core không bị ảnh hưởng vì fact dùng `WHERE NOT EXISTS` — không insert duplicate.

### Re-transform toàn bộ (xóa core và build lại)

> ⚠️ **Thao tác nguy hiểm** — chỉ dùng khi cần sửa lỗi logic transform lớn.

```sql
-- Xóa dữ liệu fact (dim có thể giữ lại)
TRUNCATE core.fact_orders CASCADE;
TRUNCATE core.fact_order_items CASCADE;
-- ... các fact khác ...
```

Sau đó chạy lại pipeline với `--stage all`.

### Re-extract tất cả bảng từ đầu

```sql
-- Xóa toàn bộ watermarks → pipeline sẽ full-load tất cả
DELETE FROM staging.etl_watermark;
```

---

## 9. Những điểm quan trọng cần lưu ý

### Staging chỉ giữ batch cuối cùng
`staging.*` bị **TRUNCATE** mỗi lần pipeline chạy — chỉ có dữ liệu của lần extract gần nhất. Lịch sử dài hạn nằm ở `core.*`. Đừng dùng staging cho analytics hay báo cáo.

### Watermark quyết định extract bao nhiêu
- Bảng incremental: chỉ kéo dữ liệu mới hơn watermark lần trước
- Nếu pipeline lỗi giữa chừng, watermark không được cập nhật → lần sau sẽ tự re-extract phần bị thiếu

### Tránh dùng `date_updated` watermark nếu có rows NULL
Bài học thực tế: `tbl_products` có 348 rows `date_updated = NULL` → bị bỏ sót → 8,729 fact rows mất `product_key`.
**Cách kiểm tra trước khi dùng watermark:**
```sql
-- Chạy trong MySQL
SELECT COUNT(*) FROM ten_bang WHERE date_updated IS NULL;
```
Nếu kết quả > 0 → dùng `watermark_col: None` (full load).

### Thứ tự transform: Dim trước, Fact sau
Dim phải có dữ liệu trước khi Fact JOIN vào để lấy surrogate key (`customer_key`, `product_key`, v.v.). `TRANSFORM_STEPS` đã được sắp xếp đúng thứ tự.

### Fact tables: INSERT-only, không UPDATE
Nếu ERP sửa một giao dịch cũ (đơn hàng, phiếu xuất kho...), fact table sẽ **không tự cập nhật** — vì `WHERE NOT EXISTS` chặn re-insert. Đây là thiết kế có chủ ý theo Kimball. Chỉ dim được UPSERT (SCD Type 1).

### NULL keys còn lại là irrecoverable
Một số fact rows vẫn có `product_key = NULL` sau tất cả các fix — vì product đó đã bị **xóa hoàn toàn** khỏi MySQL ERP. Không thể phục hồi. Đây là ERP gap, không phải lỗi pipeline.

| Fact | NULL product_key | Lý do |
|---|---|---|
| `fact_purchase_order_items` | ~14.78% (1,952 rows) | Product bị hard-delete khỏi ERP |
| `fact_transfer_warehouse` | ~3.03% (3,069 rows) | Product bị hard-delete khỏi ERP |
| `fact_warehouse_stock` | ~1.01% (8,530 rows) | Product xóa trước khi DWH tồn tại |

---

## 10. Kiểm tra sức khỏe DWH

### Sau mỗi lần chạy pipeline + dbt

```powershell
cd "d:\Data Warehouse"

# Row counts: core + mart
& "C:\Users\bus_an\AppData\Local\Python\pythoncore-3.14-64\python.exe" check_db.py

# Watermarks + staging row counts
& "C:\Users\bus_an\AppData\Local\Python\pythoncore-3.14-64\python.exe" check_watermark.py
```

### Những con số cần theo dõi

| Bảng | Rows hiện tại | Xu hướng |
|---|---|---|
| `core.fact_orders` | ~72,363 | Tăng dần theo đơn hàng mới |
| `core.fact_warehouse_stock` | ~845,079 | Tăng mạnh (mỗi lô hàng) |
| `mart.fct_revenue` | ~13,426 | Groupby ngày × khách hàng |
| `mart.fct_stock_snapshot` | ~845,079 | Bằng `fact_warehouse_stock` |

---

## 11. Xử lý sự cố thường gặp

### Pipeline lỗi: "Table not found in TABLE_CONFIG"
```
[Pipeline] ERROR: Table 'xyz' not found in TABLE_CONFIG.
```
→ Tên bảng viết sai. Kiểm tra lại tên chính xác trong MySQL.

### Pipeline lỗi: "column does not exist"
```
psycopg2.errors.UndefinedColumn: column "abc" does not exist
```
→ MySQL đã thêm cột mới. Loader sẽ tự `ALTER TABLE` thêm cột nếu có quyền. Nếu không, chạy `setup.py` để cấp lại GRANT.

### dbt lỗi: "relation does not exist"
```
Database Error: relation "core.fact_xxx" does not exist
```
→ Bảng core chưa được tạo DDL. Chạy DDL script trong `sql/` hoặc chạy `pipeline.py --stage transform` để tạo qua transform.

### dbt lỗi encoding tiếng Việt
```
UnicodeDecodeError
```
→ Thiếu `$env:PYTHONUTF8 = "1"`. Luôn set biến này trước khi chạy dbt.

### Watermark bị advance sai (extract quá ít/nhiều)
Xem watermark hiện tại:
```sql
SELECT table_name, last_loaded_at FROM staging.etl_watermark ORDER BY table_name;
```
Reset về một mốc thời gian cụ thể:
```sql
UPDATE staging.etl_watermark
SET last_loaded_at = '2026-01-01 00:00:00'
WHERE table_name = 'tbl_orders';
```

### Pipeline chạy từ thư mục sai
`pipeline.py` có `os.chdir(Path(__file__).parent)` ở đầu file — có thể chạy từ bất kỳ thư mục nào. Nhưng để chắc chắn:
```powershell
cd "d:\Data Warehouse\elt"
python pipeline.py --stage all
```

---

## Tóm tắt flow hoàn chỉnh

```
Có dữ liệu mới trong MySQL ERP
          │
          ▼
[1] pipeline.py --stage all          (~2 phút)
    ├── Extract: MySQL → DataFrame (watermark-based incremental)
    ├── Load:    DataFrame → staging.* (TRUNCATE + COPY)
    └── Transform: staging.* → core.dim_* + core.fact_*
          │
          ▼
[2] dbt run                          (~10 giây)
    └── core.* → mart.* (12 models: revenue, stock, production, ...)
          │
          ▼
[3] Kiểm tra
    ├── check_db.py        → row counts core + mart
    └── check_watermark.py → watermarks + staging row counts
          │
          ▼
[4] BI Tool (Power BI / Metabase / ...)
    └── Connect to PostgreSQL schema: mart
```
