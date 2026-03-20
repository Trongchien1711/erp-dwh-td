# ERP Data Warehouse — ETL Pipeline

Tài liệu này giải thích toàn bộ hệ thống Data Warehouse: kiến trúc, luồng xử lý, từng file code, và cách vận hành. Được viết để người mới học có thể hiểu từng bước mà không cần nền tảng DWH trước.

---

## Mục lục

1. [Tổng quan hệ thống](#1-tổng-quan-hệ-thống)
2. [Kiến trúc & luồng dữ liệu](#2-kiến-trúc--luồng-dữ-liệu)
3. [Cấu trúc thư mục](#3-cấu-trúc-thư-mục)
4. [Cài đặt môi trường](#4-cài-đặt-môi-trường)
5. [Giải thích từng file](#5-giải-thích-từng-file)
   - [connections.py](#51-connectionspy)
   - [extractor.py](#52-extractorpy)
   - [watermark.py](#53-watermarkpy)
   - [loader.py](#54-loaderpy)
   - [transform_core.py](#55-transform_corepy)
   - [pipeline.py](#56-pipelinepy)
   - [setup.py](#57-setuppy)
6. [Database Schema](#6-database-schema)
7. [Cách chạy pipeline](#7-cách-chạy-pipeline)
8. [Giám sát & Logging](#8-giám-sát--logging)
9. [Xử lý sự cố](#9-xử-lý-sự-cố)

---

## 1. Tổng quan hệ thống

### Bài toán

Công ty đang dùng hệ thống ERP lưu trữ dữ liệu trong **MySQL**. Dữ liệu này rất khó phân tích trực tiếp vì:
- Bảng được tối ưu cho ghi/đọc nhanh (OLTP), không phải cho báo cáo (OLAP)
- Không có lịch sử thay đổi
- Cấu trúc phức tạp, nhiều bảng JOIN

### Giải pháp: Data Warehouse

Chúng ta xây dựng một **Data Warehouse (DWH)** trong **PostgreSQL** — một bản sao đã được làm sạch, chuẩn hóa và tối ưu cho phân tích.

```
MySQL (ERP)  ──→  Python ETL  ──→  PostgreSQL (DWH)
  (nguồn)       (xử lý)          (phân tích)
```

### ETL là gì?

**ETL = Extract → Transform → Load** — 3 bước cốt lõi:

| Bước | Ý nghĩa | Trong dự án này |
|------|---------|-----------------|
| **E**xtract | Kéo dữ liệu từ nguồn | Đọc từ MySQL |
| **T**ransform | Làm sạch, chuyển đổi cấu trúc | SQL trong PostgreSQL |
| **L**oad | Ghi vào đích | Ghi vào PostgreSQL |

> **Lưu ý kiến trúc**: Dự án này thực ra dùng mô hình **ELT** (Extract → Load → Transform): kéo dữ liệu thô vào `staging` trước, rồi mới transform trong PostgreSQL — giúp dễ debug và tái xử lý.

---

## 2. Kiến trúc & luồng dữ liệu

### Sơ đồ tổng thể

```
┌─────────────────────────────────────────────────────────────────┐
│                         MySQL (ERP)                             │
│  tbl_orders, tblclients, tbl_products, tblwarehouse, ...        │
│  (25 bảng nguồn)                                                │
└────────────────────────────┬────────────────────────────────────┘
                             │  STAGE 1: Extract + Load
                             │  (extractor.py + loader.py)
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                  PostgreSQL — schema: staging                   │
│                                                                 │
│  staging.tbl_orders          (bản sao thô của MySQL)           │
│  staging.tblclients          TRUNCATE + reload mỗi lần chạy   │
│  staging.tbl_products        ...                                │
│  ...  (25 bảng mirror)                                          │
│                                                                 │
│  staging.etl_watermark       (bảng meta — lưu last sync time)  │
└────────────────────────────┬────────────────────────────────────┘
                             │  STAGE 2: Transform
                             │  (transform_core.py)
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                   PostgreSQL — schema: core                     │
│                                                                 │
│  DIMENSIONS (bảng chiều — mô tả thực thể):                     │
│  ├── core.dim_customer       (khách hàng)                       │
│  ├── core.dim_product        (sản phẩm)                         │
│  ├── core.dim_staff          (nhân viên)                        │
│  ├── core.dim_department     (phòng ban)                        │
│  ├── core.dim_warehouse      (kho)                              │
│  ├── core.dim_warehouse_location (vị trí trong kho)             │
│  ├── core.dim_supplier       (nhà cung cấp)                     │
│  ├── core.dim_manufacture    (đơn vị sản xuất)                  │
│  └── core.dim_price_group    (nhóm giá)                         │
│                                                                 │
│  FACTS (bảng sự kiện — giao dịch, số liệu):                    │
│  ├── core.fact_orders                (đơn hàng bán)             │
│  ├── core.fact_order_items           (chi tiết đơn hàng bán)    │
│  ├── core.fact_delivery_items        (giao hàng)                │
│  ├── core.fact_warehouse_stock       (tồn kho)                  │
│  ├── core.fact_purchase_order_items  (đơn mua hàng)             │
│  ├── core.fact_production_order_items (lệnh sản xuất)           │
│  ├── core.fact_production_stages     (công đoạn sx)             │
│  ├── core.fact_purchase_product_items (nhập hàng)               │
│  └── core.fact_transfer_warehouse   (chuyển kho)                │
└─────────────────────────────────────────────────────────────────┘
```

### Luồng chi tiết theo thời gian

```
pipeline.py --stage all
│
├── [01] Khởi tạo kết nối
│     ├── get_pg_engine()    → kết nối PostgreSQL
│     └── get_mysql_engine() → kết nối MySQL
│
├── [02] STAGE 1: Extract + Load (chạy 25 bảng tuần tự)
│     │
│     ├── Mỗi bảng làm 4 việc:
│     │    ① get_watermark()       → "lần trước load đến đâu?"
│     │    ② extract_table()       → đọc từ MySQL (incremental hoặc full)
│     │    ③ load_table()          → ghi vào staging.* (COPY protocol)
│     │    └── set_watermark()     → "đã load đến thời điểm này"
│     │
│     └── Kết quả: 25 bảng trong staging.* được cập nhật
│
└── [03] STAGE 2: Transform (chạy 21 bước SQL tuần tự)
      │
      ├── Bước 1–9:   Upsert 9 Dimension tables (UPDATE existing + INSERT new)
      ├── Bước 10:    Cập nhật price_group_key trong dim_customer
      ├── Bước 11–19: Insert vào 9 Fact tables (chỉ insert bản ghi mới)
      └── Bước 20–21: FIX — vá lại NULL foreign keys trong fact tables

      Kết quả: 21 bước SUCCESS, dims và facts được cập nhật
```

---

## 3. Cấu trúc thư mục

```
d:\Data Warehouse\
│
├── pipeline.py          ← ĐIỂM VÀO CHÍNH — chạy file này
├── connections.py       ← Quản lý kết nối database
├── extractor.py         ← Đọc dữ liệu từ MySQL
├── watermark.py         ← Tracking "đã đồng bộ đến đâu"
├── loader.py            ← Ghi dữ liệu vào PostgreSQL staging
├── transform_core.py    ← SQL transform staging → core
├── setup.py             ← (Chạy 1 lần) GRANT quyền + ALTER schema — cần postgres
│
├── debug_check.py       ← Tool debug: kiểm tra TABLE_CONFIG
├── debug_privs.py       ← Tool debug: kiểm tra quyền DB
│
├── requirements.txt     ← Thư viện Python cần cài
├── .env                 ← Cấu hình kết nối DB (KHÔNG commit lên git)
│
└── logs/
    └── pipeline_YYYY-MM-DD.log   ← Log tự động theo ngày
```

---

## 4. Cài đặt môi trường

### 4.1 Yêu cầu phần mềm

- Python 3.8+
- MySQL 5.7+ (server ERP đang chạy)
- PostgreSQL 13+ (server DWH — cần tạo trước)

### 4.2 Cài thư viện Python

```powershell
# Tạo virtual environment (môi trường ảo Python)
python -m venv .venv

# Kích hoạt (Windows)
.venv\Scripts\Activate.ps1

# Cài tất cả thư viện
pip install -r requirements.txt
```

### 4.3 Tạo file `.env`

Tạo file `.env` trong thư mục gốc (cùng chỗ với `pipeline.py`):

```ini
# Kết nối MySQL (nguồn ERP)
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=your_mysql_user
MYSQL_PASSWORD=your_mysql_password
MYSQL_DATABASE=your_erp_db

# Kết nối PostgreSQL (Data Warehouse)
PG_HOST=localhost
PG_PORT=5432
PG_USER=dwh_admin
PG_PASSWORD=your_pg_password
PG_DATABASE=erp_dwh

# (Chỉ cần cho setup.py — dùng 1 lần)
PG_SUPER_USER=postgres
PG_SUPER_PASSWORD=your_postgres_password
```

> **Bảo mật**: File `.env` chứa mật khẩu thật — **không bao giờ** commit lên Git. Thêm `.env` vào `.gitignore`.

### 4.4 Chạy lần đầu tiên

```powershell
# 1. Kiểm tra kết nối
python connections.py

# 2. Cấp quyền DB + ALTER schema (chạy 1 lần duy nhất, cần quyền postgres)
#    Thêm PG_SUPER_PASSWORD vào .env trước khi chạy
python setup.py

# 3. Chạy toàn bộ pipeline
python pipeline.py --stage all
```

---

## 5. Giải thích từng file

### 5.1 `connections.py`

**Vai trò**: Tạo kết nối đến MySQL và PostgreSQL.

**Khái niệm**: `SQLAlchemy Engine` là đối tượng quản lý pool kết nối — thay vì mở/đóng connection mới cho mỗi query, Engine giữ sẵn một "hồ" connection để tái sử dụng.

```python
# Đọc thông tin từ .env, tạo connection string, trả về Engine
def get_mysql_engine():
    url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
    return create_engine(url, pool_pre_ping=True)
    #                         ↑
    #   pool_pre_ping=True: kiểm tra connection còn sống trước khi dùng
    #   (tránh lỗi "connection lost" sau idle lâu)

def get_pg_engine():
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    return create_engine(url, pool_pre_ping=True)
```

**Khi nào dùng**: `pipeline.py` gọi hai hàm này khi khởi động. Quan trọng: `mysql_engine.dispose()` sau khi extract xong để giải phóng kết nối.

---

### 5.2 `extractor.py`

**Vai trò**: Khai báo danh sách 25 bảng cần đồng bộ và logic đọc từ MySQL.

#### TABLE_CONFIG — Danh sách bảng

```python
TABLE_CONFIG = [
    {
        "source_table":  "tbl_orders",   # tên bảng trong MySQL
        "watermark_col": "date",         # cột dùng để đọc incremental
    },
    {
        "source_table":  "tbl_order_items",
        "watermark_col": None,           # None = full load (đọc toàn bộ)
    },
    # ... 23 bảng khác
]
```

**Hai chiến lược đọc dữ liệu:**

| Chiến lược | `watermark_col` | Cách hoạt động | Dùng khi nào |
|-----------|-----------------|----------------|--------------|
| **Incremental** | Có (vd. `"date"`) | Chỉ đọc các bản ghi có `date >= last_sync_time` | Bảng lớn, có cột timestamp phản ánh khi nào bản ghi thay đổi |
| **Full Load** | `None` | Đọc toàn bộ bảng | Bảng nhỏ (dim), hoặc không có cột timestamp đáng tin cậy |

```python
def extract_table(mysql_engine, source_table, watermark_col, last_watermark):
    if watermark_col:
        # Incremental: lấy bản ghi mới hơn watermark
        query = f"SELECT * FROM `{source_table}` WHERE `{watermark_col}` >= :wm"
        params = {"wm": last_watermark}
    else:
        # Full load: lấy toàn bộ
        query = f"SELECT * FROM `{source_table}`"
        params = {}

    # Đọc theo từng chunk 5000 dòng để tránh out-of-memory với bảng lớn
    chunks = []
    with mysql_engine.connect() as conn:
        for chunk in pd.read_sql(text(query), conn, chunksize=5000):
            chunks.append(chunk)
    
    return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
```

**Tại sao dùng `chunksize`?** Ví dụ bảng `tbl_purchase_product_items` có 722.000 dòng — nếu đọc một lần, Python cần ~1GB RAM. Đọc 5.000 dòng một lúc chỉ cần vài MB.

---

### 5.3 `watermark.py`

**Vai trò**: Ghi nhớ "pipeline đã đọc đến đâu" để lần sau không đọc lại từ đầu.

**Bảng `staging.etl_watermark`:**

```
table_name          | last_loaded_at       | updated_at
--------------------|----------------------|--------------------
tbl_orders          | 2026-03-20 14:52:10  | 2026-03-20 14:52:11
tblclients          | 2026-03-20 14:50:30  | 2026-03-20 14:50:31
tbl_products        | 2026-03-19 23:00:00  | 2026-03-20 14:50:55
...
```

**Luồng watermark cho mỗi bảng:**

```
get_watermark("tbl_orders")
  → trả về "2026-03-19 23:00:00"  ← last_loaded_at

extract_table(..., last_watermark="2026-03-19 23:00:00")
  → SELECT * FROM tbl_orders WHERE date >= '2026-03-19 23:00:00'
  → 47 bản ghi mới

load_table(...)  → ghi 47 bản ghi vào staging

set_watermark("tbl_orders", "2026-03-20 14:52:10")
  → cập nhật last_loaded_at = max(date) của 47 bản ghi vừa đọc
```

**Lần chạy tiếp theo**: chỉ đọc bản ghi có `date >= 2026-03-20 14:52:10` — tránh đọc lại toàn bộ lịch sử.

**Lưu ý quan trọng**: Nếu bảng `watermark_col = None` (full load), watermark được set bằng `NOW()` — không dùng để filter MySQL, chỉ ghi nhận thời điểm đồng bộ gần nhất.

---

### 5.4 `loader.py`

**Vai trò**: Nhận DataFrame từ `extractor.py` và ghi vào `staging` schema trong PostgreSQL.

#### Quy trình load một bảng

```
load_table(pg_engine, df, staging_table="tbl_orders", watermark_col="date")
│
├── [1] Thêm cột "etl_loaded_at" vào df (timestamp lúc pipeline chạy)
│
├── [2] _fix_zero_dates(df)
│     → MySQL cho phép "0000-00-00" làm ngày — PostgreSQL không chấp nhận
│     → Thay tất cả "0000-00-00*" bằng NULL
│
├── [3] _sanitize_columns(df)
│     → Đổi tên cột trùng PostgreSQL reserved words (vd. "order" → "order_col")
│
├── [4] Kiểm tra bảng staging đã tồn tại chưa?
│     ├── Chưa có → dùng df.to_sql() để tạo bảng + insert (lần đầu tiên)
│     └── Đã có  → _copy_insert() (lần thứ 2 trở đi)
│
└── [5] _add_missing_columns() — nếu MySQL thêm cột mới, ALTER TABLE staging
```

#### `_copy_insert()` — Tại sao nhanh hơn 17 lần?

Phương pháp thông thường (`to_sql`):
```
Python → [INSERT row1, row2, ..., row2000] → PostgreSQL  (1 round trip)
Python → [INSERT row2001, ..., row4000]    → PostgreSQL  (1 round trip)
...379 round trips cho 757k dòng → 350 giây
```

PostgreSQL COPY protocol:
```
Python → [TRUNCATE staging.tbl]            → PostgreSQL  (1 lệnh)
Python → [COPY 757,000 dòng CSV dạng stream] → PostgreSQL  (1 lệnh)
Tổng: 2 lệnh → 21 giây
```

```python
def _copy_insert(pg_engine, df, staging_table):
    # 1. Xử lý float → Int (pandas lưu integer nullable là float64: 0 → "0.0")
    #    PostgreSQL từ chối "0.0" cho cột INTEGER, cần chuyển về "0"
    for col in df.select_dtypes(include="float").columns:
        notna = df[col].dropna()
        if (notna % 1 == 0).all():  # tất cả giá trị là số nguyên?
            df[col] = df[col].astype(pd.Int64Dtype())  # → "0" thay vì "0.0"

    # 2. Ghi DataFrame ra CSV trong memory (không cần file tạm)
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, na_rep="")
    #                                         ↑ NULL = chuỗi rỗng trong CSV
    buf.seek(0)

    # 3. TRUNCATE + COPY trong cùng 1 transaction
    raw = pg_engine.raw_connection()
    with raw.cursor() as cur:
        cur.execute(f'TRUNCATE TABLE staging."{staging_table}"')
        cur.copy_expert(
            f'COPY staging."{staging_table}" (...) FROM STDIN WITH (FORMAT CSV)',
            buf,
        )
    raw.commit()
```

---

### 5.5 `transform_core.py`

**Vai trò**: Chứa toàn bộ SQL để chuyển dữ liệu thô từ `staging.*` sang mô hình phân tích trong `core.*`.

#### Khái niệm: Dimensional Modeling (Mô hình chiều)

Data Warehouse dùng kiến trúc **Star Schema** (hình ngôi sao):

```
              dim_customer
                   │
dim_product ──── fact_orders ──── dim_staff
                   │
              dim_warehouse
```

- **Dimension (chiều)**: Bảng mô tả thực thể (Who? What? Where?). Ví dụ: `dim_customer` có tên, địa chỉ, trạng thái khách hàng.
- **Fact (sự kiện)**: Bảng chứa giao dịch, số đo (How many? How much?). Ví dụ: `fact_orders` chứa số lượng, giá trị đơn hàng.
- **Surrogate Key**: Mỗi dim có một `*_key` tự tăng (SERIAL) — đây là khóa nối với fact, thay cho natural key (`customer_id` từ MySQL).

#### Tại sao cần Surrogate Key?

```
MySQL:   tblclients.userid = 1001  (natural key — từ hệ thống gốc)
                                   
DWH:     dim_customer.customer_key = 5  (surrogate key — tự tăng trong DWH)
         dim_customer.customer_id  = 1001
         
fact_orders.customer_key = 5  (nối với dim_customer bằng surrogate key)
```

Lý do: Nếu `userid` thay đổi hoặc trùng giữa hệ thống, surrogate key vẫn đảm bảo tính toàn vẹn.

#### Pattern Upsert cho Dimensions (SCD Type 1)

"**Upsert**" = Update if exists, Insert if not. Trong DWH, khi thông tin khách hàng thay đổi, chúng ta cập nhật bản ghi cũ (không giữ lịch sử) — đây là **SCD Type 1** (Slowly Changing Dimension Type 1).

```sql
-- Pattern được dùng cho cả 9 dimension tables
WITH updated AS (
    -- Bước 1: Cập nhật bản ghi ĐÃ TỒN TẠI
    UPDATE core.dim_customer d
    SET customer_code = src.code_client,
        fullname      = src.fullname,
        -- ... các cột khác
        etl_loaded_at = NOW()
    FROM staging.tblclients src
    WHERE d.customer_id = src.userid  -- nối theo natural key
    RETURNING d.customer_id           -- trả về id các bản ghi đã update
)
-- Bước 2: Thêm bản ghi MỚI (chưa có trong dim)
INSERT INTO core.dim_customer (customer_id, customer_code, fullname, ...)
SELECT src.userid, src.code_client, src.fullname, ...
FROM staging.tblclients src
WHERE NOT EXISTS (
    SELECT 1 FROM updated u WHERE u.customer_id = src.userid
    -- "chỉ insert nếu không nằm trong danh sách vừa update"
);
```

> **Tại sao không dùng `ON CONFLICT DO UPDATE`?** Vì PostgreSQL yêu cầu cột trong `ON CONFLICT(col)` phải có UNIQUE constraint. `customer_id` (natural key) trong `dim_customer` không có UNIQUE — chỉ `customer_key` (surrogate PK SERIAL) có. Nên phải dùng CTE pattern trên.

#### Pattern Insert cho Facts

Fact tables chỉ insert thêm, không cập nhật bản ghi cũ:

```sql
INSERT INTO core.fact_orders (order_id, customer_key, employee_key, ...)
SELECT
    o.id,
    dc.customer_key,   -- lookup surrogate key từ dim
    ds.staff_key,      -- lookup surrogate key từ dim
    ...
FROM staging.tbl_orders o
LEFT JOIN core.dim_customer dc ON dc.customer_id = o.userid
LEFT JOIN core.dim_staff    ds ON ds.staff_id    = o.employee_id
WHERE NOT EXISTS (
    SELECT 1 FROM core.fact_orders f WHERE f.order_id = o.id
    -- chỉ insert bản ghi chưa có (tránh duplicate)
);
```

#### Date Key — Thay timestamp bằng số nguyên

Thay vì lưu `date = '2026-03-20'`, fact tables lưu `order_date_key = 20260320` (số nguyên 8 chữ số YYYYMMDD). Lý do: JOIN với `dim_date` nhanh hơn so với JOIN kiểu DATE, và dễ filter theo năm/tháng.

```sql
TO_CHAR(o.date::DATE, 'YYYYMMDD')::INT  -- '2026-03-20' → 20260320
COALESCE(..., 19000101)                 -- NULL date → fallback 1900-01-01
```

#### FIX Steps — Vá lỗi khóa ngoại NULL

Khi pipeline chạy lần đầu, có thể fact được insert trước khi dim có đủ dữ liệu — dẫn đến `customer_key = NULL` trong fact. FIX steps giải quyết:

```sql
-- Chỉ chạy nếu thực sự có NULL (có pre-check để skip nếu không cần)
UPDATE core.fact_orders f
SET customer_key  = dc.customer_key,
    employee_key  = ds.staff_key
FROM staging.tbl_orders o
LEFT JOIN core.dim_customer dc ON dc.customer_id = o.userid
LEFT JOIN core.dim_staff    ds ON ds.staff_id    = o.employee_id
WHERE f.order_id = o.id
  AND (f.customer_key IS NULL OR f.employee_key IS NULL);
```

#### Thứ tự 21 bước Transform

```
Step  1–9  : Upsert 9 Dimensions (Customer, Product, Staff, Department,
                                    Warehouse, Supplier, Location,
                                    Manufacture, PriceGroup)
Step 10    : UPDATE dim_customer — gán price_group_key (cần setup.py trước)
Step 11–19 : INSERT 9 Facts (Orders, OrderItems, DeliveryItems, WarehouseStock,
                              PurchaseOrderItems, ProductionOrderItems,
                              ProductionStages, PurchaseProductItems,
                              TransferWarehouse)
Step 20–21 : FIX NULL foreign keys trong fact_orders và fact_order_items
```

---

### 5.6 `pipeline.py`

**Vai trò**: File điều phối trung tâm — gọi tất cả module theo đúng thứ tự.

#### Sơ đồ luồng chính

```python
def main():
    # 1. Parse arguments (--stage, --table)
    args = parser.parse_args()

    # 2. Khởi tạo kết nối
    pg_engine = get_pg_engine()
    if stage in ("extract", "all"):
        mysql_engine = get_mysql_engine()

    # 3. STAGE 1: Extract + Load
    if stage in ("extract", "all"):
        run_extract_load(mysql_engine, pg_engine)
        mysql_engine.dispose()   # giải phóng kết nối MySQL ngay sau khi dùng xong

    # 4. STAGE 2: Transform
    if stage in ("transform", "all"):
        run_transforms(pg_engine)

    # 5. Cleanup
    pg_engine.dispose()
```

#### Error Handling — Pipeline không dừng khi 1 bảng lỗi

```python
for cfg in configs:
    try:
        # ... xử lý bảng
    except Exception as e:
        logger.error(f"[Pipeline] {source} FAILED: {e}")
        continue   # ← bỏ qua bảng lỗi, tiếp tục bảng tiếp theo
```

Thiết kế này đảm bảo nếu 1 trong 25 bảng bị lỗi (ví dụ timeout), 24 bảng còn lại vẫn được đồng bộ.

#### Cập nhật Watermark — Xử lý trường hợp NaT

```python
if wm_col and wm_col in df.columns:
    max_wm = df[wm_col].max()
    # Nếu toàn bộ giá trị trong cột là NULL → max() = NaT
    # Không advance watermark về now() (sẽ bỏ sót data cũ)
    new_wm = last_wm if str(max_wm) == "NaT" else str(max_wm)
else:
    # Bảng không có watermark_col → set now() (chỉ để ghi nhận thời điểm sync)
    new_wm = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
```

---

### 5.7 `setup.py`

**Vai trò**: Script chạy **một lần duy nhất** với quyền superuser PostgreSQL. Thực hiện toàn bộ cấu hình ban đầu trong 4 bước:

| Bước | Việc làm |
|------|----------|
| 1 | ALTER TABLE `core.dim_customer` — thêm 3 cột price_group |
| 2 | Đổi owner tất cả bảng `staging.*` → `dwh_admin` (cần để loader có thể ALTER TABLE) |
| 3 | GRANT USAGE, CREATE, ALL TABLES trên schema `staging` → `dwh_admin` |
| 4 | GRANT USAGE, SELECT/INSERT/UPDATE trên schema `core` → `dwh_admin` |

**Tại sao cần file riêng?** ETL user (`dwh_admin`) chỉ có quyền INSERT/UPDATE/SELECT — không có quyền ALTER TABLE hay GRANT. Cần dùng tài khoản `postgres` (superuser).

```powershell
# Thêm vào .env trước:
# PG_SUPER_PASSWORD=<mật_khẩu_postgres>
python setup.py
```

Sau khi chạy `setup.py`, bước "dim_customer [UPDATE price_group_key]" trong transform sẽ hoạt động.

---

## 6. Database Schema

### Schema `staging` — Mirror của MySQL

- Mỗi bảng MySQL có một bảng tương ứng trong `staging`
- Cấu trúc cột giống MySQL (thêm cột `etl_loaded_at`)
- **TRUNCATE + reload** mỗi lần pipeline chạy (không giữ lịch sử)
- Bảng đặc biệt: `staging.etl_watermark` (meta table — không mirror MySQL)

### Schema `core` — Data Warehouse thật sự

#### Dimensions — Cấu trúc chung

```sql
CREATE TABLE core.dim_customer (
    customer_key   SERIAL PRIMARY KEY,  -- surrogate key, tự tăng
    customer_id    INTEGER,             -- natural key từ MySQL (userid)
    customer_code  VARCHAR,
    fullname       VARCHAR,
    -- ... các thuộc tính mô tả khách hàng
    etl_loaded_at  TIMESTAMP,           -- pipeline cập nhật lần cuối
    etl_source     VARCHAR              -- tên bảng nguồn (tblclients)
);
```

#### Facts — Cấu trúc chung

```sql
CREATE TABLE core.fact_orders (
    order_id        INTEGER PRIMARY KEY,  -- natural key từ MySQL
    customer_key    INTEGER REFERENCES core.dim_customer,  -- FK đến dim
    employee_key    INTEGER REFERENCES core.dim_staff,
    order_date_key  INTEGER,              -- YYYYMMDD (join với dim_date)
    total_amount    DECIMAL,
    -- ... các số đo
    etl_loaded_at   TIMESTAMP,
    etl_source      VARCHAR
);
```

### Mapping MySQL → Core (25 nguồn → 18 bảng core)

| Nguồn MySQL | Staging | Đích Core |
|-------------|---------|-----------|
| `tblclients` | `staging.tblclients` | `core.dim_customer` |
| `tbl_products` | `staging.tbl_products` | `core.dim_product` |
| `tblstaff` | `staging.tblstaff` | `core.dim_staff` |
| `tbldepartments` | `staging.tbldepartments` | `core.dim_department` |
| `tblwarehouse` | `staging.tblwarehouse` | `core.dim_warehouse` |
| `tbllocaltion_warehouses` | `staging.tbllocaltion_warehouses` | `core.dim_warehouse_location` |
| `tblsuppliers` | `staging.tblsuppliers` | `core.dim_supplier` |
| `tbl_manufactures` | `staging.tbl_manufactures` | `core.dim_manufacture` |
| `tblcustomers_groups` | `staging.tblcustomers_groups` | `core.dim_price_group` |
| `tbl_orders` | `staging.tbl_orders` | `core.fact_orders` |
| `tbl_order_items` | `staging.tbl_order_items` | `core.fact_order_items` |
| `tbl_deliveries` + `tbl_delivery_items` | staging.* | `core.fact_delivery_items` |
| `tblwarehouse_product` | `staging.tblwarehouse_product` | `core.fact_warehouse_stock` |
| `tblpurchase_order` + `tblpurchase_order_items` | staging.* | `core.fact_purchase_order_items` |
| `tbl_productions_orders_items` | staging.* | `core.fact_production_order_items` |
| `tbl_productions_orders_items_stages` | staging.* | `core.fact_production_stages` |
| `tbl_purchase_product_items` | staging.* | `core.fact_purchase_product_items` |
| `tbltransfer_warehouse_detail` | staging.* | `core.fact_transfer_warehouse` |

---

## 7. Cách chạy pipeline

### Các lệnh cơ bản

```powershell
# Kích hoạt virtual environment (cần chạy trước)
.venv\Scripts\Activate.ps1

# Chạy toàn bộ pipeline (extract + load + transform)
python pipeline.py

# Chỉ chạy extract + load staging (không transform)
python pipeline.py --stage extract

# Chỉ chạy transform (staging đã có sẵn)
python pipeline.py --stage transform

# Chỉ extract + load 1 bảng cụ thể (debug)
python pipeline.py --stage extract --table tbl_orders

# Kiểm tra kết nối
python connections.py
```

### Thứ tự chạy lần đầu tiên

```
1. python connections.py          # Xác nhận kết nối DB OK
2. python setup.py                # Cấp quyền DB + ALTER schema (cần postgres password)
3. python pipeline.py --stage all # Chạy toàn bộ
```

### Performance thực tế (đo trên môi trường dev)

| Bảng | Số dòng | Thời gian |
|------|---------|-----------|
| `tbl_purchase_product_items` | ~722.000 | ~21s |
| `tbllocaltion_warehouses` | ~757.000 | ~22s |
| Các bảng còn lại (full load) | 1k–50k | < 5s mỗi bảng |
| **STAGE 1 (25 bảng)** | **~2M dòng** | **~100s** |
| **STAGE 2 (21 transform steps)** | — | **~18s** |
| **Tổng pipeline** | — | **~2 phút 38 giây** |

---

## 8. Giám sát & Logging

### File log

Log được ghi tự động vào `logs/pipeline_YYYY-MM-DD.log`:

```
logs/
├── pipeline_2026-03-18.log
├── pipeline_2026-03-19.log
└── pipeline_2026-03-20.log   ← hôm nay
```

Retention: **30 ngày** (file cũ hơn tự động xóa).

### Đọc log

```powershell
# Xem log hôm nay
Get-Content "logs\pipeline_2026-03-20.log"

# Lọc chỉ lỗi
Select-String "ERROR" "logs\pipeline_2026-03-20.log"

# Xem 50 dòng cuối
Get-Content "logs\pipeline_2026-03-20.log" -Tail 50
```

### Ý nghĩa các cấp log

| Level | Ý nghĩa | Màu |
|-------|---------|-----|
| `DEBUG` | Chi tiết kỹ thuật (chỉ ghi file, không in console) | — |
| `INFO` | Tiến trình bình thường | Xanh lá |
| `WARNING` | Cảnh báo (không nghiêm trọng, VD: đổi tên cột) | Vàng |
| `ERROR` | Lỗi một bảng/bước cụ thể (pipeline vẫn tiếp tục) | Đỏ |
| `SUCCESS` | Hoàn thành thành công | Xanh đậm |

### Ví dụ output pipeline chạy thành công

```
14:49:55 | INFO    | ==================================================
14:49:55 | INFO    | Pipeline started at 2026-03-20 14:49:55
14:49:55 | INFO    | Stage: all | Table: ALL
14:49:55 | INFO    | ==================================================
14:49:55 | INFO    | >>> STAGE 1: Extract + Load to Staging
14:49:56 | INFO    | [Watermark] tbl_orders -> last_loaded_at = 2026-03-19 23:00:01
14:49:56 | INFO    | [Extract] tbl_orders | incremental from 2026-03-19 23:00:01
14:49:57 | SUCCESS | [Load] staging.tbl_orders <- 1 rows inserted.
...
14:52:10 | SUCCESS | [Load] staging.tbl_purchase_product_items <- 722,880 rows inserted.
14:52:10 | INFO    | >>> STAGE 2: Transform Staging -> Core
14:52:11 | SUCCESS | [Transform] dim_customer -> 45 rows affected.
14:52:11 | SUCCESS | [Transform] dim_product -> 12 rows affected.
...
14:52:33 | INFO    | [Transform] fact_orders [FIX customer+employee keys] -> skipped (no NULL keys).
14:52:33 | SUCCESS | Pipeline finished in 157.9s
```

---

## 9. Xử lý sự cố

### Lỗi kết nối database

```
[MySQL] Connection FAILED: (2003, "Can't connect to MySQL server on 'localhost'")
```
→ Kiểm tra MySQL service đang chạy, thông tin trong `.env` đúng chưa.

```
[PostgreSQL] Connection FAILED: FATAL: password authentication failed for user "dwh_admin"
```
→ Kiểm tra `PG_USER`, `PG_PASSWORD` trong `.env`.

### Lỗi permission

```
[Transform] dim_customer [UPDATE price_group_key] FAILED: column "price_group_key" does not exist
```
→ Chưa chạy `setup.py`. Chạy lại: `python setup.py`

```
ERROR: permission denied for table dim_customer
```
→ User `dwh_admin` thiếu quyền. Chạy `python debug_privs.py` để kiểm tra.

### Lỗi constraint

```
duplicate key value violates unique constraint "dim_customer_pkey"
```
→ Thường xảy ra khi chạy transform nhiều lần liên tiếp trên cùng data. Kiểm tra `WHERE NOT EXISTS` trong SQL có đúng không.

### Bảng không có data mới

```
[Pipeline] tbl_orders -> no new data, skip load.
```
→ Bình thường — không có đơn hàng mới kể từ lần chạy trước. Pipeline bỏ qua, không báo lỗi.

### Reset watermark (chạy lại từ đầu)

Nếu muốn đồng bộ lại toàn bộ data từ đầu:

```sql
-- Chạy trong pgAdmin hoặc psql
UPDATE staging.etl_watermark SET last_loaded_at = '2020-01-01 00:00:00';
-- Hoặc reset riêng 1 bảng:
UPDATE staging.etl_watermark SET last_loaded_at = '2020-01-01 00:00:00'
WHERE table_name = 'tbl_orders';
```

Sau đó chạy lại `python pipeline.py --stage all`.

---

## Tóm tắt kiến trúc quyết định

| Quyết định | Lý do |
|------------|-------|
| Dùng **PostgreSQL COPY** thay vì `INSERT` thông thường | 17x nhanh hơn cho bảng lớn (722k dòng: 350s → 21s) |
| **CTE UPDATE+INSERT** cho dim thay vì `ON CONFLICT` | Schema dùng surrogate SERIAL PK, natural key không có UNIQUE constraint |
| **Staging schema** trước khi transform | Có thể debug, tái transform mà không cần extract lại từ MySQL |
| **Incremental load** dựa trên watermark | Tránh đọc toàn bộ lịch sử mỗi lần chạy |
| Pipeline **không dừng** khi 1 bảng lỗi | Đảm bảo 24 bảng còn lại vẫn được đồng bộ |
| **FIX steps** cuối transform | Vá lỗi FK NULL do dim/fact chạy không đồng bộ lần đầu |
