# ERP Data Warehouse (erp-dwh-td)

> Dự án cá nhân — xây dựng một Data Warehouse hoàn chỉnh từ đầu từ hệ thống ERP thực tế của công ty.  
> Mục tiêu: hiểu sâu từng tầng kỹ thuật, từ extract đến analytics mart.  
> Stack: MySQL ERP → Python ELT → PostgreSQL Star Schema → dbt Analytics Marts

---

## Mục lục

1. [Luồng dữ liệu tổng quan](#1-luồng-dữ-liệu-tổng-quan)
2. [ELT vs ETL — tại sao dùng ELT](#2-elt-vs-etl--tại-sao-dùng-elt)
3. [Stage 1 — Extract & Load (Python ELT)](#3-stage-1--extract--load-python-elt)
   - [Watermark — incremental load](#31-watermark--incremental-load)
   - [COPY protocol — bulk load nhanh](#32-copy-protocol--bulk-load-nhanh)
   - [Schema evolution — tự thêm cột mới](#33-schema-evolution--tự-thêm-cột-mới)
4. [Stage 2 — Transform: Star Schema](#4-stage-2--transform-star-schema)
   - [Tại sao cần Star Schema](#41-tại-sao-cần-star-schema)
   - [Surrogate Key vs Natural Key](#42-surrogate-key-vs-natural-key)
   - [Upsert pattern bằng CTE](#43-upsert-pattern-bằng-cte)
   - [Partitioning — fact table lớn](#44-partitioning--fact-table-lớn)
5. [Stage 3 — dbt Analytics Mart](#5-stage-3--dbt-analytics-mart)
   - [Tại sao dùng dbt](#51-tại-sao-dùng-dbt)
   - [Ba tầng model trong dbt](#52-ba-tầng-model-trong-dbt)
   - [ref() và source()](#53-ref-và-source)
   - [Materialization](#54-materialization)
6. [Orchestration — Airflow & PowerShell](#6-orchestration--airflow--powershell)
7. [Monitoring & Health Check](#7-monitoring--health-check)
8. [Cấu trúc thư mục](#8-cấu-trúc-thư-mục)
9. [Quick Start](#9-quick-start)
10. [Schema Reference](#10-schema-reference)
11. [Known ERP Data Gaps](#11-known-erp-data-gaps)
12. [Progress](#12-progress)

---

## 1. Luồng dữ liệu tổng quan

```
MySQL (ERP — hệ thống nguồn, ~25 bảng)
        │
        │  Stage 1 — Extract + Load   (elt/extractor.py + elt/loader.py)
        │  - Incremental: chỉ lấy data mới hơn watermark
        │  - Bulk load bằng PostgreSQL COPY protocol
        ▼
PostgreSQL  schema: staging.*     ← Mirror thô của ERP, giữ nguyên cấu trúc gốc
        │
        │  Stage 2 — Transform        (elt/transform_core.py)
        │  - Upsert vào dimension tables (CTE pattern)
        │  - Insert vào fact tables (có fix NULL foreign key)
        ▼
PostgreSQL  schema: core.*        ← Star Schema: 10 dim + 9 fact, có partitioning
        │
        │  Stage 3 — Analytics layer  (dbt Core)
        │  - staging views → intermediate CTEs → mart tables
        ▼
PostgreSQL  schema: mart.*        ← 12 mart models cho Sales / Inventory / Finance
        │
        ▼
BI Tools (Power BI, Metabase, eda_mart.py...)
```

### Stack

| Tầng | Công nghệ |
|---|---|
| Nguồn | MySQL (ERP) |
| ELT Pipeline | Python 3 · pandas · SQLAlchemy · psycopg2 · loguru |
| Data Warehouse | PostgreSQL 18 |
| Schema Design | Star Schema — Kimball Dimensional Modeling |
| Analytics Mart | dbt-core + dbt-postgres 1.9 (Python 3.11) |
| Orchestration | Apache Airflow (Docker/WSL) + PowerShell (Windows Task Scheduler) |
| Monitoring | check_pipeline_health.py (threshold-based) |

---

## 2. ELT vs ETL — tại sao dùng ELT

**ETL (cách cũ):** Extract → **Transform** → Load  
Dữ liệu được làm sạch trước khi vào database. Cần staging server riêng, khó debug, mất dữ liệu gốc.

**ELT (dự án này):** Extract → Load → **Transform**  
Load thô vào staging trước, transform ngay trong database đích.

```
ETL:  MySQL ──transform──► PostgreSQL core   (mất staging)
ELT:  MySQL ──────────────► staging ──transform──► core
                                ↑
                         Có thể debug bất kỳ lúc nào,
                         so sánh gốc vs đã xử lý
```

**Lợi ích ELT trong dự án này:**
- `staging.*` là mirror của MySQL — nếu transform sai, dữ liệu gốc vẫn còn
- Transform bằng SQL thuần trong PostgreSQL, tận dụng tốc độ của DB
- Dễ thêm bảng mới: chỉ cần thêm vào `TABLE_CONFIG` trong `extractor.py`

---

## 3. Stage 1 — Extract & Load (Python ELT)

### 3.1 Watermark — incremental load

**Vấn đề:** MySQL có hàng triệu row. Mỗi ngày không thể load lại toàn bộ.

**Giải pháp:** Lưu mốc "đã load đến đâu" (watermark) vào bảng `staging.etl_watermark`, lần sau chỉ lấy dữ liệu mới hơn.

```
staging.etl_watermark
┌──────────────────────────┬──────────────────────┐
│ table_name               │ last_loaded_at       │
├──────────────────────────┼──────────────────────┤
│ tblorders                │ 2026-03-21 18:46:18  │  ← timestamp
│ tblorderitemsdetail      │ 2026-03-21 18:46:18  │  ← timestamp
│ tblwarehouse_product     │ 0                    │  ← integer ID (full load)
│ tblproducts              │ (none)               │  ← always full load
└──────────────────────────┴──────────────────────┘
```

**3 chiến lược watermark trong `extractor.py`:**

| Loại | Cột theo dõi | Câu SQL sinh ra | Dùng khi |
|---|---|---|---|
| **Timestamp** | `date_create`, `date_warehouse`... | `WHERE COALESCE(date_create, '2000-01-01') >= :wm` | Bảng có cột ngày tạo/sửa |
| **Integer ID** | `id` | `WHERE id > :wm` (mặc định `0`) | Bảng append-only, ID tăng dần |
| **Full load** | `None` | Không filter, load toàn bộ | Bảng nhỏ, không có cột theo dõi |

**Tại sao dùng `COALESCE(..., '2000-01-01')`?**  
MySQL hay có giá trị `NULL` trong cột date. Nếu `date_create IS NULL` và không dùng COALESCE, row đó sẽ không bao giờ được load (NULL `>=` bất cứ gì đều = FALSE).

**Watermark column type là TEXT** — lưu cả datetime string lẫn integer ID trong cùng một cột. Đây là trade-off để đơn giản hóa schema, đổi lại phải xử lý type thủ công trong code.

**Flow hoàn chỉnh của một lần extract:**

```python
# 1. Đọc watermark hiện tại
wm = get_watermark("tblorders")           # "2026-03-21 18:46:18"

# 2. Query MySQL chỉ lấy row mới
df = extract_table("tblorders", since=wm) # ~500 rows mới

# 3. Load vào staging (TRUNCATE + COPY — xem phần 3.2)
load_table(df, "staging.tblorders")

# 4. Cập nhật watermark = max(date) của batch vừa load
new_wm = df["date"].max()                 # "2026-03-22 08:15:33"
update_watermark("tblorders", new_wm)
```

**Edge case NaT:** Nếu tất cả row trong batch có date = NULL, `df["date"].max()` trả về `NaT`. Code kiểm tra điều này và **giữ nguyên watermark cũ** thay vì ghi `NaT`, tránh reset về default `2020-01-01`.

---

### 3.2 COPY protocol — bulk load nhanh

**Vấn đề:** `pandas.to_sql()` dùng INSERT từng row — 100,000 rows = 100,000 round-trips đến DB.

```python
# CÁCH CHẬM — to_sql():
df.to_sql('tblorders', engine, if_exists='append')
# → Mỗi row = 1 INSERT, 100K rows ≈ 30-60 giây

# CÁCH NHANH — COPY:
cursor.copy_expert("COPY staging.tblorders FROM STDIN WITH CSV", buffer)
# → Toàn bộ DataFrame = 1 lần stream, 100K rows ≈ 1-2 giây
```

**COPY** là lệnh bulk load gốc của PostgreSQL — đọc data như stream file CSV thay vì parse từng SQL statement. Nhanh hơn 17-50x tùy kích thước.

**Flow trong `loader.py`:**

```
DataFrame
    ↓  df.to_csv(StringIO, index=False)    ← chuyển sang CSV trong RAM
StringIO buffer
    ↓  cursor.copy_expert(COPY ... FROM STDIN)
PostgreSQL staging table
```

**Tại sao TRUNCATE trước khi COPY?**  
Staging là mirror của MySQL — mỗi lần load lại row đó với data mới nhất. TRUNCATE + COPY đảm bảo không có row cũ lẫn lộn. (Khác với core — ở đó dùng UPSERT để giữ lịch sử.)

**Data cleaning trước khi load:**
- `0000-00-00` → `NULL` (MySQL zero-date không hợp lệ với PostgreSQL)
- Tên cột trùng reserved keyword → thêm hậu tố `_col` (ví dụ `user` → `user_col`)
- Float64 nullable integer → Int64 (ghi `0` thay vì `0.0` vào cột số nguyên)

---

### 3.3 Schema evolution — tự thêm cột mới

Khi MySQL thêm cột mới vào bảng ERP, `loader.py` tự phát hiện và ALTER TABLE mà không cần can thiệp thủ công:

```python
# 1. So sánh cột trong DataFrame với cột thực tế trong bảng staging
existing_cols = get_existing_columns("staging.tblorders")
missing_cols  = set(df.columns) - set(existing_cols)
# → missing_cols = {"new_field_abc"}

# 2. ALTER TABLE thêm cột thiếu
# Dùng connection RIÊNG (ngoài transaction chính)
ALTER TABLE staging.tblorders ADD COLUMN new_field_abc TEXT;

# 3. Tiếp tục load bình thường
```

**Tại sao dùng connection riêng cho ALTER?**  
PostgreSQL: nếu trong một transaction có lỗi (ví dụ constraint violation), toàn bộ transaction bị abort — kể cả `ALTER TABLE`. Dùng connection riêng đảm bảo `ALTER` commit độc lập, không bị cuốn theo rollback của transaction chính.

---

## 4. Stage 2 — Transform: Star Schema

### 4.1 Tại sao cần Star Schema

Sau khi data thô ở staging, cần chuẩn hóa thành Star Schema để BI tools query hiệu quả.

**Star Schema** (Kimball Methodology) là mô hình DWH phổ biến nhất:

```
                    dim_customer (260 rows)
                         │
dim_product (7,854) ─── fact_orders (72,363) ─── dim_staff (50)
                         │
                    dim_date (3,650 rows = 10 năm)
```

- **Dimension table**: "Ai, cái gì, ở đâu" — mô tả thực thể, ít thay đổi
- **Fact table**: "Bao nhiêu, khi nào" — đo lường sự kiện kinh doanh, nhiều row

**Lợi ích so với schema của ERP (3NF):**

| ERP Schema (normalized) | Star Schema |
|---|---|
| JOIN 8-10 bảng cho 1 report | JOIN 2-3 bảng là đủ |
| Tốt cho write (OLTP) | Tốt cho read/aggregate (OLAP) |
| Khó cho người không biết DB | Trực quan: fact ở giữa, dim xung quanh |

---

### 4.2 Surrogate Key vs Natural Key

Mỗi dimension có **hai loại key**:

```sql
-- Natural key: mã từ hệ thống ERP nguồn
customer_code = 'KH001'   -- có thể thay đổi, không ổn định, là text

-- Surrogate key: do DWH tự sinh (SERIAL)
customer_key  = 1042      -- integer nhỏ, bất biến, không có ý nghĩa nghiệp vụ
```

**Fact table chỉ lưu surrogate key:**

```sql
fact_orders:
- customer_key  = 1042    ← integer, JOIN nhanh
- product_key   = 7201    ← integer, JOIN nhanh
- date_key      = 20260322 ← integer YYYYMMDD
- total_amount  = 5000000  ← measure
```

**Tại sao không dùng natural key trong fact?**
1. Text JOIN chậm hơn integer JOIN (collation, length so sánh)
2. Nếu ERP đổi `customer_code` từ `KH001` → `KHM001`, fact table tự báo lỗi NULL key
3. Surrogate key tách biệt DWH khỏi sự thay đổi của hệ thống nguồn

**`date_key = 19000101`** là giá trị mặc định khi ngày bị NULL — hướng đến partition `fact_orders_default`, không làm vỡ logic JOIN hay partition.

---

### 4.3 Upsert pattern bằng CTE

`transform_core.py` dùng pattern "Upsert bằng CTE" thay vì `INSERT OR REPLACE`:

```sql
-- Lý do: PostgreSQL không có MERGE (trước v15), UPSERT bằng ON CONFLICT
-- thiếu linh hoạt cho update nhiều cột. CTE pattern rõ ràng hơn:

WITH updated AS (
    -- Bước 1: UPDATE những row đã tồn tại, RETURNING key để biết ai đã update
    UPDATE core.dim_customer dst
    SET
        company          = src.company,
        representative   = src.sales_name,
        phone            = src.phone,
        etl_loaded_at    = NOW()
    FROM staging.tblclients src
    WHERE dst.customer_code = src.code   -- natural key làm điều kiện match
    RETURNING dst.customer_code
),
-- Bước 2: INSERT những row mới (không có trong UPDATE above)
inserted AS (
    INSERT INTO core.dim_customer (customer_code, company, representative, phone)
    SELECT src.code, src.company, src.sales_name, src.phone
    FROM staging.tblclients src
    WHERE src.code NOT IN (SELECT customer_code FROM updated)
)
SELECT 'done';
```

**Tại sao không chỉ INSERT mà cần UPDATE?**  
Dữ liệu dimension thay đổi theo thời gian: khách hàng đổi tên, số điện thoại... Đây là **SCD Type 1** (Slowly Changing Dimension) — ghi đè giá trị cũ bằng giá trị mới, không giữ lịch sử thay đổi. Đủ cho use case analytics cơ bản.

---

### 4.4 Partitioning — fact table lớn

`fact_orders` và 7 fact table khác được **partition theo năm**:

```sql
-- Bảng cha (không chứa data):
CREATE TABLE core.fact_orders (
    order_date_key INTEGER,
    ...
) PARTITION BY RANGE (order_date_key);

-- Bảng con (chứa data thật):
CREATE TABLE core.fact_orders_2022
    PARTITION OF core.fact_orders
    FOR VALUES FROM (20220101) TO (20230101);

CREATE TABLE core.fact_orders_2023 ... FOR VALUES FROM (20230101) TO (20240101);
CREATE TABLE core.fact_orders_2024 ... FOR VALUES FROM (20240101) TO (20250101);
CREATE TABLE core.fact_orders_2025 ... FOR VALUES FROM (20250101) TO (20260101);
CREATE TABLE core.fact_orders_2026 ... FOR VALUES FROM (20260101) TO (20270101);
CREATE TABLE core.fact_orders_default PARTITION OF core.fact_orders DEFAULT;
```

**Lợi ích:**
```sql
-- Query chỉ đọc năm 2026 → PostgreSQL tự biết chỉ scan fact_orders_2026
-- Bỏ qua 4 partition năm trước (~288K rows không cần đọc)
SELECT SUM(total_amount)
FROM core.fact_orders
WHERE order_date_key BETWEEN 20260101 AND 20261231;
```

**Composite PK `(surrogate_key, date_key)`** thay vì chỉ `surrogate_key` — vì PostgreSQL yêu cầu partition key phải là một phần của PK.

---

## 5. Stage 3 — dbt Analytics Mart

### 5.1 Tại sao dùng dbt

`transform_core.py` chạy SQL bằng Python — đủ dùng cho core schema. Nhưng khi cần 12+ mart models với dependency phức tạp:

| Vấn đề nếu tự viết Python | dbt giải quyết thế nào |
|---|---|
| Phải tự quản lý thứ tự chạy | Tự xây DAG từ `{{ ref() }}` |
| SQL nằm trong Python string — không highlight, không test | `.sql` file thuần, có IDE support |
| Không có test data quality | `dbt test` — not_null, unique, relationships |
| Không có documentation | `dbt docs generate` → website tự động |
| Không biết model nào chạy lâu | `dbt run` có timing per model |

---

### 5.2 Ba tầng model trong dbt

```
core.dim_* + core.fact_*      ← nguồn (không phải dbt quản lý)
        │
        ▼ staging/ (views — không lưu data)
staging_dbt.stg_orders        ← đổi tên cột, chuẩn hóa kiểu dữ liệu
staging_dbt.stg_customers
staging_dbt.stg_products
        │
        ▼ intermediate/ (ephemeral — chỉ là CTE, không tạo bảng)
int_orders_enriched           ← JOIN orders + customers + date, dùng nhiều mart share
        │
        ▼ marts/ (tables — kết quả cuối, BI đọc từ đây)
mart.fct_revenue              ← Sales
mart.fct_gross_profit         ← Finance
mart.fct_stock_snapshot       ← Inventory
...
```

**Tại sao 3 tầng?**
- **Staging**: tách biệt logic "rename/recast" khỏi logic "business" — nếu upstream đổi tên cột, chỉ sửa 1 chỗ
- **Intermediate**: tránh lặp lại cùng một CTE phức tạp trong nhiều mart — DRY principle
- **Mart**: gần với câu hỏi kinh doanh cụ thể — "doanh thu theo ngày", "tồn kho theo lô"

---

### 5.3 ref() và source()

```sql
-- ref(): tham chiếu model dbt khác
-- dbt tự biết phải chạy stg_orders TRƯỚC mart này
SELECT * FROM {{ ref('stg_orders') }}         -- → staging_dbt.stg_orders

-- source(): tham chiếu bảng ngoài dbt (core.*)
-- khai báo trong sources.yml, có thể thêm freshness check
SELECT * FROM {{ source('core', 'fact_orders') }}  -- → core.fact_orders
```

**dbt tự vẽ DAG từ `ref()`:**

```
source(core.fact_orders)
        │
        ▼
stg_orders   stg_customers   stg_date
        │          │             │
        └──────────┴─────────────┘
                   │
                   ▼
          int_orders_enriched   (ephemeral)
                   │
          ┌────────┼────────┐
          ▼        ▼        ▼
    fct_revenue  fct_order_performance  fct_gross_profit
```

Không cần config thứ tự chạy — dbt tự suy luận từ dependency graph.

---

### 5.4 Materialization

| Kiểu | Tạo object gì | Hiệu năng | Dùng khi |
|---|---|---|---|
| `view` | SQL View — không lưu data | Query mỗi lần → chậm nếu phức tạp | Staging — nhẹ, luôn fresh |
| `ephemeral` | Chỉ là CTE inline | Không tốn storage | Intermediate — không cần truy vấn trực tiếp |
| `table` | Bảng thật, CTAS mỗi lần dbt run | Query nhanh, tốn storage + rebuild time | Mart — BI cần query nhanh |
| `incremental` | Chỉ INSERT/UPDATE row mới | Nhanh nhất khi data lớn | (chưa dùng trong dự án này) |

Trong `dbt_project.yml`:
```yaml
models:
  erp_dwh:
    staging:
      +materialized: view       # stg_* → views
    intermediate:
      +materialized: ephemeral  # int_* → CTEs
    marts:
      +materialized: table      # fct_*, dim_* → tables
```

---

## 6. Orchestration — Airflow & PowerShell

### Airflow DAG (`airflow/dags/erp_dwh_dag.py`)

Airflow trả lời: **"Chạy cái gì, lúc nào, theo thứ tự nào, và làm gì khi lỗi?"**

```
Schedule: 0 1 * * *  (01:00 UTC = 08:00 ICT mỗi ngày)

extract_sales_tables ─────┐
extract_inventory_tables ─┤  (4 nhóm chạy SONG SONG)
extract_production_tables ┤
extract_master_tables ────┘
                          │
                          ▼
                   transform_group
                   ├─ upsert_dimensions
                   └─ insert_facts
                          │
                          ▼
                     dbt_group
                     ├─ dbt run
                     └─ dbt test
                          │
                          ▼
                    health_check
```

**Concepts quan trọng:**

| Khái niệm | Ý nghĩa trong dự án |
|---|---|
| **DAG** | Sơ đồ dependency giữa các task — thứ tự chạy |
| **TaskGroup** | Gom nhiều task liên quan vào 1 nhóm trực quan trong UI |
| **@task decorator** | TaskFlow API (Airflow 2.0+) — Python function = 1 task |
| **XCom** | Cross-task communication — `health_check` trả dict về Airflow metadata |
| **`{{ ds }}`** | Template: ngày thực tế của DAG run (execution date) |
| **catchup=False** | Không chạy bù những ngày đã qua khi restart |
| **retries=1** | Thử lại 1 lần sau 5 phút nếu task lỗi |

### PowerShell Scripts (`scripts/`)

Dùng cho chạy thủ công và Windows Task Scheduler (không cần Docker):

| Script | Tác dụng |
|---|---|
| `run_pipeline.ps1` | Chạy full pipeline: ELT → dbt. Flags: `-EltOnly`, `-DbtOnly` |
| `run_daily.ps1` | Wrapper cho Task Scheduler — ghi log file `logs/daily_YYYYMMDD.log` |
| `backup_dwh.ps1` | pg_dump schemas core + mart, nén level 9, giữ 7 ngày gần nhất |
| `restore_backup.ps1` | pg_restore từ file backup được chọn |
| `validate_setup.ps1` | Kiểm tra môi trường trước khi chạy lần đầu |

---

## 7. Monitoring & Health Check

`check_pipeline_health.py` chạy 5 loại kiểm tra sau mỗi lần pipeline hoàn thành:

```
[1] Dimension row counts       dim_customer >= 100 rows?   ✓ OK (260)
[2] Fact row counts            fact_orders >= 1,000 rows?  ✓ OK (72,363)
[3] NULL foreign key rates     fact_orders.product_key     ✓ 0.00% (threshold: 1%)
                               fact_purchase_order_items   ⚠ 14.78% (known baseline)
[4] Mart row counts            fct_revenue > 0 rows?       ✓ OK (13,426)
[5] Watermark freshness        tblorders loaded < 2 days?  ✓ 2026-03-22
```

**Baseline tolerance:** Một số NULL key là "không thể khôi phục" vì ERP gốc không có dữ liệu liên kết. Health check ghi nhận baseline này và chỉ cảnh báo khi tỷ lệ **vượt** baseline, không phải khi không = 0.

**Các script kiểm tra khác:**

| Script | Tác dụng |
|---|---|
| `check_watermark.py` | Liệt kê watermark hiện tại + row count staging |
| `check_db.py` | Inventory schema: đếm table/view/row theo từng schema |
| `eda_mart.py` | EDA đầy đủ across 12 mart: revenue, segments, stock, P&L |

---

## 8. Cấu trúc thư mục

```
erp-dwh-td/
│
├── elt/                          ← Python ELT pipeline
│   ├── pipeline.py               ← Entrypoint: --stage extract|transform|all, --table X
│   ├── extractor.py              ← Incremental extract từ MySQL (watermark-based)
│   │                                TABLE_CONFIG: 25 bảng, 3 loại watermark
│   ├── loader.py                 ← Bulk load vào PostgreSQL staging (COPY protocol)
│   │                                Xử lý: zero-date, reserved keywords, schema evolution
│   ├── transform_core.py         ← SQL transforms: staging → core (19 bước)
│   │                                9 dimension upserts + 10 fact inserts
│   ├── watermark.py              ← Đọc/ghi staging.etl_watermark
│   ├── connections.py            ← Engine factory: MySQL + PostgreSQL (pool + timeout)
│   ├── setup.py                  ← Chạy 1 lần: ALTER TABLE, GRANT, schema setup
│   └── requirements.txt
│
├── sql/                          ← DDL scripts — chạy theo thứ tự
│   ├── 01_setup_database.sql     ← Tạo DB, users (dwh_admin, bi_reader), schemas
│   ├── 02_extensions.sql         ← PostgreSQL extensions (uuid-ossp, pg_trgm...)
│   ├── 03_dim_date.sql           ← Bảng calendar date (10 năm, ~3,650 rows)
│   ├── 04_core_dimensions.sql    ← DDL 9 dimension tables + unique indexes
│   ├── 05_core_fact_tables.sql   ← DDL 9 fact tables + partitioning 2022-2027
│   ├── 06_staging_tables.sql     ← DDL staging mirrors
│   ├── 07_dwh_structure_tests.sql← Kiểm tra cấu trúc DB sau setup
│   └── 08_add_missing_indexes.sql← Migration: thêm indexes vào DB đang chạy
│
├── dbt_project/                  ← dbt project (erp_dwh)
│   ├── dbt_project.yml           ← Config: materialization mặc định theo tầng
│   ├── profiles.yml              ← Kết nối PostgreSQL (đọc env vars)
│   └── models/
│       ├── staging/              ← Views đặt lại tên cột, chuẩn hóa kiểu
│       │   ├── sources.yml       ← Khai báo core.* là "source" của dbt
│       │   ├── _stg_models.yml   ← Docs + tests cho staging models
│       │   ├── stg_customers.sql
│       │   ├── stg_orders.sql
│       │   ├── stg_order_items.sql
│       │   ├── stg_products.sql
│       │   ├── stg_date.sql
│       │   ├── stg_staff.sql
│       │   ├── stg_suppliers.sql
│       │   └── stg_warehouses.sql
│       ├── intermediate/
│       │   ├── _int_models.yml
│       │   └── int_orders_enriched.sql   ← CTE: orders JOIN customers JOIN date
│       └── marts/
│           ├── sales/
│           │   ├── fct_revenue.sql               ← Doanh thu theo ngày × KH × chi nhánh
│           │   ├── fct_order_items_detail.sql     ← Chi tiết dòng hàng
│           │   ├── fct_order_performance.sql      ← Tỷ lệ giao hàng & thanh toán
│           │   └── dim_customer_segmentation.sql  ← RFM segmentation (NTILE 5)
│           ├── inventory/
│           │   ├── fct_stock_snapshot.sql         ← Tồn kho theo lô hàng
│           │   ├── fct_inbound_outbound.sql       ← Nhập/xuất kho theo ngày
│           │   └── fct_production_efficiency.sql  ← Kế hoạch vs thực tế sản xuất
│           ├── finance/
│           │   ├── fct_gross_profit.sql           ← P&L: doanh thu, COGS, lợi nhuận gộp
│           │   ├── fct_purchase_cost.sql          ← Chi phí mua hàng theo nhà cung cấp
│           │   └── dim_customer_credit.sql        ← AR và mức sử dụng hạn mức
│           └── shared_dim/
│               ├── dim_customer_mart.sql          ← Khách hàng đầy đủ cho BI
│               └── dim_product_mart.sql           ← Sản phẩm đầy đủ cho BI
│
├── airflow/
│   ├── docker-compose.yml        ← Airflow services (webserver, scheduler, postgres)
│   └── dags/
│       ├── erp_dwh_dag.py        ← DAG chính: extract → transform → dbt → health check
│       └── init_connections.py   ← Setup Airflow connections lần đầu
│
├── scripts/                      ← PowerShell automation
│   ├── run_pipeline.ps1          ← ELT + dbt (flags: -EltOnly, -DbtOnly)
│   ├── run_daily.ps1             ← Wrapper cho Task Scheduler, ghi log
│   ├── backup_dwh.ps1            ← pg_dump + rolling 7-day retention
│   ├── restore_backup.ps1        ← pg_restore từ file backup
│   ├── validate_setup.ps1        ← Kiểm tra môi trường trước khi chạy
│   └── register_task.ps1         ← Đăng ký Windows Task Scheduler
│
├── docs/                         ← Tài liệu nghiệp vụ
│   ├── architecture_overview.md
│   ├── bus_matrix.md             ← Ma trận fact × dimension
│   ├── grain_definition.md       ← Định nghĩa grain từng fact table
│   ├── domain_sales.md
│   ├── domain_inventory.md
│   ├── domain_finance.md
│   └── GLOSSARY.md
│
├── diagrams/                     ← Star schema diagrams (Mermaid)
├── data_samples/                 ← Sample CSV cho test
├── check_db.py                   ← Inventory schema + row counts
├── check_watermark.py            ← Xem watermark hiện tại
├── check_pipeline_health.py      ← Health check 5 nhóm + threshold
└── eda_mart.py                   ← EDA đầy đủ 12 mart tables
```

---

## 9. Quick Start

### Yêu cầu
- PostgreSQL 18 (local hoặc cập nhật `.env`)
- MySQL / MariaDB với ERP data
- Python 3.11 cho dbt; Python 3.x bất kỳ cho ELT

### Bước 1 — Cấu hình credentials
```bash
cp .env.example .env
# Điền MYSQL_* và PG_* variables
```

### Bước 2 — Setup database (chạy 1 lần theo thứ tự)
```sql
-- Chạy dưới quyền postgres superuser:
\i sql/01_setup_database.sql    -- DB, users, schemas
\i sql/02_extensions.sql        -- PostgreSQL extensions
\i sql/03_dim_date.sql          -- Calendar dimension
\i sql/04_core_dimensions.sql   -- 9 dimension tables
\i sql/05_core_fact_tables.sql  -- 9 fact tables + partitions
\i sql/06_staging_tables.sql    -- staging mirrors
```
```bash
cd elt
python setup.py   # GRANT + ALTER TABLE (cần PG_SUPER_PASSWORD trong .env)
```

### Bước 3 — Chạy ELT pipeline
```bash
# Full run — toàn bộ 25 bảng
python elt/pipeline.py --stage all

# Chỉ extract + load vào staging
python elt/pipeline.py --stage extract

# Chỉ transform staging → core
python elt/pipeline.py --stage transform

# Một bảng cụ thể
python elt/pipeline.py --table tblwarehouse_product
```

### Bước 4 — Chạy dbt (cần Python 3.11 venv riêng)
```powershell
# Tạo venv một lần duy nhất:
py -3.11 -m venv .venv_dbt
.venv_dbt\Scripts\pip install dbt-postgres==1.9.0

# Chạy toàn bộ mart models:
$env:PYTHONUTF8 = "1"
.venv_dbt\Scripts\dbt.exe run `
    --profiles-dir "d:\Data Warehouse\dbt_project" `
    --project-dir  "d:\Data Warehouse\dbt_project"

# Chạy 1 model + tất cả downstream:
.venv_dbt\Scripts\dbt.exe run --select fct_stock_snapshot+ `
    --profiles-dir "d:\Data Warehouse\dbt_project" `
    --project-dir  "d:\Data Warehouse\dbt_project"
```

### Bước 5 — Chạy full pipeline (PowerShell)
```powershell
# Full: ELT + dbt
.\scripts\run_pipeline.ps1

# Chỉ ELT
.\scripts\run_pipeline.ps1 -EltOnly

# Chỉ dbt
.\scripts\run_pipeline.ps1 -DbtOnly
```

### Bước 6 — Kiểm tra kết quả
```bash
python check_pipeline_health.py   # 5 nhóm health check
python check_watermark.py         # Xem watermark hiện tại
python check_db.py                # Row counts toàn bộ schema
python eda_mart.py                # EDA đầy đủ mart layer
```

---

## 10. Schema Reference

### Core Dimensions (`core` schema)

| Bảng | Grain | Cột chính |
|---|---|---|
| `dim_customer` | 1 khách hàng | customer_key, customer_code, company, representative |
| `dim_product` | 1 sản phẩm | product_key, product_code, product_name, price_sell |
| `dim_staff` | 1 nhân viên | staff_key, staff_code, fullname |
| `dim_warehouse` | 1 kho | warehouse_key, warehouse_name, branch |
| `dim_supplier` | 1 nhà cung cấp | supplier_key, supplier_code, company |
| `dim_department` | 1 phòng ban | department_key, department_name |
| `dim_price_group` | 1 nhóm giá | price_group_key, price_group_code |
| `dim_warehouse_location` | 1 vị trí kho | location_key, warehouse_key, rack, shelf |
| `dim_manufacture` | 1 lệnh sản xuất | manufacture_key, manufacture_code |
| `dim_date` | 1 ngày | date_key (YYYYMMDD), year, month, quarter, day_of_week |

### Core Facts (`core` schema — partitioned by year)

| Bảng | Grain | Rows (~) | Partition |
|---|---|---|---|
| `fact_orders` | 1 đơn hàng | 72,363 | Có (2022-2026) |
| `fact_order_items` | 1 dòng hàng trong đơn | 73,553 | Có |
| `fact_delivery_items` | 1 dòng giao hàng | — | Có |
| `fact_warehouse_stock` | 1 lô tồn kho | 845,079 | Không |
| `fact_purchase_order_items` | 1 dòng đơn mua | 13,206 | Có |
| `fact_purchase_product_items` | 1 dòng nhập kho | 722,880 | Có |
| `fact_production_order_items` | 1 dòng lệnh SX | — | Có |
| `fact_production_stages` | 1 công đoạn SX | 749,308 | Có |
| `fact_transfer_warehouse` | 1 dòng chuyển kho | — | Không |

### Mart Layer (`mart` schema — dbt tables)

| Model | Domain | Grain | Rows (~) |
|---|---|---|---|
| `fct_revenue` | Sales | ngày × khách hàng × chi nhánh | 13,426 |
| `fct_order_items_detail` | Sales | dòng hàng | 73,553 |
| `fct_order_performance` | Sales | 1 đơn hàng | 72,363 |
| `dim_customer_segmentation` | Sales | 1 khách hàng (RFM) | 260 |
| `fct_stock_snapshot` | Inventory | lô tồn kho | 845,079 |
| `fct_inbound_outbound` | Inventory | ngày × sản phẩm × kho × loại | 197,980 |
| `fct_production_efficiency` | Inventory | lệnh SX × công đoạn | 196,146 |
| `fct_gross_profit` | Finance | ngày × khách hàng × chi nhánh | 13,426 |
| `fct_purchase_cost` | Finance | ngày PO × NCC × sản phẩm | 11,835 |
| `dim_customer_credit` | Finance | 1 khách hàng (AR) | 260 |
| `dim_customer_mart` | Shared | 1 khách hàng | 260 |
| `dim_product_mart` | Shared | 1 sản phẩm | 7,854 |

---

## 11. Known ERP Data Gaps

Các trường **không có dữ liệu trong ERP nguồn** — không thể fix ở tầng DWH:

| Trường | Tác động | Model bị ảnh hưởng |
|---|---|---|
| `total_cost` / item `cost` | COGS = ~0 cho 99.7% đơn hàng | `fct_gross_profit`, `fct_revenue` |
| `status_payment` / `total_payment` | AR toàn bộ hiển thị là chưa thu | `fct_order_performance`, `dim_customer_credit` |
| `price_import` (dim_product) | `value_in = 0` trong biến động kho | `fct_inbound_outbound` |
| `number_hours` (công đoạn SX) | `output_per_hour = NULL` | `fct_production_efficiency` |
| `debt_limit` (khách hàng) | Tất cả KH hiện "No Limit Set" | `dim_customer_credit` |

NULL foreign keys không thể khôi phục (do ERP thiếu liên kết gốc):

| Foreign key | NULL rate | Ngưỡng cảnh báo |
|---|---|---|
| `fact_warehouse_stock.product_key` | ~1.01% | 2% |
| `fact_purchase_order_items.product_key` | ~14.78% | 20% |
| `fact_transfer_warehouse.product_key` | ~3.03% | 5% |

---

## 12. Progress

- [x] Star schema design (dims + facts) — Kimball methodology
- [x] Python ELT pipeline — incremental watermark-based load
- [x] 3 watermark strategies: timestamp, integer ID, full load
- [x] COPY protocol bulk load (17x faster than INSERT)
- [x] Schema evolution: tự ALTER TABLE khi MySQL thêm cột mới
- [x] SCD Type 1 dimensions — CTE upsert pattern
- [x] Auto-fix NULL foreign keys — retroactive UPDATE steps
- [x] Fact table partitioning — by year (2022-2026) + default
- [x] dbt staging layer (8 views)
- [x] dbt intermediate layer (`int_orders_enriched` ephemeral)
- [x] dbt mart layer — Sales (4 models)
- [x] dbt mart layer — Inventory (3 models)
- [x] dbt mart layer — Finance (3 models)
- [x] dbt mart layer — Shared dims (2 models)
- [x] Health check script — 5 groups, baseline-aware thresholds
- [x] Full EDA across 12 mart tables
- [x] Backup automation — pg_dump + 7-day rolling retention
- [x] PowerShell orchestration — run_pipeline.ps1, run_daily.ps1
- [x] Airflow DAG — parallel extract groups + dbt integration
- [ ] Windows Task Scheduler — đăng ký chạy hàng ngày
- [ ] BI dashboard (Power BI / Metabase)
