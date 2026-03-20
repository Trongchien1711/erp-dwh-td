# ERP Data Warehouse (erp-dwh-td)

> Business Data Analyst — Building a real Data Warehouse end-to-end for my company.  
> Source: MySQL ERP → Python ELT → PostgreSQL Star Schema → dbt Analytics Marts

---

## Architecture

```
MySQL (ERP — source system)
        │
        │  Stage 1 — Extract + Load   (Python ELT pipeline)
        ▼
PostgreSQL  schema: staging       ← Raw mirror of ERP tables (TRUNCATE + COPY)
        │
        │  Stage 2 — Transform        (SQL in transform_core.py)
        ▼
PostgreSQL  schema: core          ← Star Schema — dimensions & facts
        │
        │  Stage 3 — Analytics layer  (dbt Core)
        ▼
PostgreSQL  schema: mart          ← Business-ready marts (dbt table models)
```

---

## Stack

| Layer | Technology |
|---|---|
| Source | MySQL (ERP system) |
| ELT Pipeline | Python 3 · pandas · SQLAlchemy · psycopg2 · loguru |
| Data Warehouse | PostgreSQL 16 |
| Schema Design | Star Schema — Dimensional Modeling (Kimball) |
| Analytics / Marts | dbt-core 1.10 · dbt-postgres 1.9 |

---

## Project Structure

```
erp-dwh-td/
│
├── elt/                        ← Python ELT pipeline
│   ├── pipeline.py             ← Main entrypoint  (python pipeline.py --table X)
│   ├── extractor.py            ← Incremental extract from MySQL (watermark-based)
│   ├── loader.py               ← Bulk load to PostgreSQL staging (COPY protocol)
│   ├── transform_core.py       ← SQL transforms: staging → core dims & facts
│   │                              Includes FIX steps for retroactive NULL key updates
│   ├── watermark.py            ← Tracks last_loaded_at per table
│   ├── connections.py          ← DB engine factories (reads .env)
│   ├── setup.py                ← One-time DB setup (schemas, GRANT, ALTER)
│   └── requirements.txt
│
├── sql/                        ← DDL scripts — run once in order
│   ├── 01_setup_database.sql
│   ├── 02_extensions.sql
│   ├── 03_dim_date.sql
│   ├── 04_core_dimensions.sql
│   ├── 05_core_fact_tables.sql
│   ├── 06_staging_tables.sql
│   └── 07_fix_staging_columns.sql
│
├── dbt_project/                ← dbt project (erp_dwh)
│   ├── models/
│   │   ├── staging/            ← Thin views over core schema
│   │   │   ├── stg_customers.sql
│   │   │   ├── stg_date.sql
│   │   │   ├── stg_orders.sql
│   │   │   ├── stg_order_items.sql
│   │   │   ├── stg_products.sql
│   │   │   ├── stg_staff.sql
│   │   │   ├── stg_suppliers.sql
│   │   │   └── stg_warehouses.sql
│   │   ├── intermediate/
│   │   │   └── int_orders_enriched.sql   ← Orders + dims + date spine
│   │   └── marts/
│   │       ├── sales/
│   │       │   ├── fct_revenue.sql               ← Daily revenue by customer
│   │       │   ├── fct_order_items_detail.sql     ← Line-item sales detail
│   │       │   ├── fct_order_performance.sql      ← Fulfilment & payment status
│   │       │   └── dim_customer_segmentation.sql  ← RFM scoring (ntile 5)
│   │       ├── inventory/
│   │       │   ├── fct_stock_snapshot.sql         ← Stock levels by lot
│   │       │   ├── fct_inbound_outbound.sql       ← Daily stock movements
│   │       │   └── fct_production_efficiency.sql  ← Planned vs actual output
│   │       ├── finance/
│   │       │   ├── fct_gross_profit.sql           ← P&L by date × customer
│   │       │   ├── fct_purchase_cost.sql          ← Procurement spend by supplier
│   │       │   └── dim_customer_credit.sql        ← AR & credit utilisation
│   │       └── shared_dim/
│   │           ├── dim_customer_mart.sql
│   │           └── dim_product_mart.sql
│   └── profiles.yml
│
├── docs/                       ← Domain documentation
│   ├── architecture_overview.md
│   ├── bus_matrix.md
│   ├── grain_definition.md
│   ├── domain_sales.md
│   ├── domain_inventory.md
│   └── domain_finance.md
│
├── diagrams/                   ← Star schema diagrams
├── data_samples/               ← Sample data for testing
├── check_db.py                 ← Quick DB connectivity check
├── check_watermark.py          ← Inspect current ELT watermarks
└── eda_mart.py                 ← Full EDA script across all mart tables
```

---

## Quick Start

### 1. Prerequisites
- PostgreSQL 16 running locally (or update `.env`)
- MySQL / MariaDB running with ERP data
- Python 3.11+ (dbt requires ≤ 3.13; ELT works on 3.14)

### 2. Configure credentials
```bash
cp .env.example .env
# Fill in MYSQL_* and PG_* variables
```

### 3. Setup database (run once)
```sql
-- Run in order:
sql/01_setup_database.sql
sql/02_extensions.sql
sql/03_dim_date.sql
sql/04_core_dimensions.sql
sql/05_core_fact_tables.sql
sql/06_staging_tables.sql
```
```bash
cd elt
python setup.py   # GRANT privileges + ALTER TABLE fixes
```

### 4. Run the ELT pipeline
```bash
cd elt

# Full run — all tables
python pipeline.py --stage all

# Single table (e.g. re-extract warehouse stock)
python pipeline.py --table tblwarehouse_product
```

### 5. Run dbt (requires Python 3.11)
```powershell
$env:PYTHONUTF8 = "1"
$dbt = "C:\Users\...\Python311\Scripts\dbt.exe"

# Run all models
& $dbt run --profiles-dir "d:\Data Warehouse\dbt_project" `
           --project-dir  "d:\Data Warehouse\dbt_project"

# Run specific model + downstream
& $dbt run --select fct_stock_snapshot+ ...
```

---

## Core Schema (PostgreSQL — `core` schema)

### Dimensions
| Table | Grain | Key fields |
|---|---|---|
| `dim_customer` | 1 customer | customer_key, company, representative |
| `dim_product` | 1 product | product_key, product_code, price_sell |
| `dim_staff` | 1 staff | staff_key, fullname |
| `dim_warehouse` | 1 warehouse | warehouse_key, warehouse_name |
| `dim_supplier` | 1 supplier | supplier_key, company |
| `dim_department` | 1 dept | department_key |
| `dim_price_group` | 1 price group | price_group_key |
| `dim_warehouse_location` | 1 location | location_key |
| `dim_manufacture` | 1 manufacture order | manufacture_key |
| `dim_date` | 1 calendar day | date_key (YYYYMMDD) |

### Facts
| Table | Grain | Rows (~) |
|---|---|---|
| `fact_orders` | 1 order | 72,363 |
| `fact_order_items` | 1 order line | 73,553 |
| `fact_delivery_items` | 1 delivery line | — |
| `fact_warehouse_stock` | 1 stock lot | 845,079 |
| `fact_purchase_order_items` | 1 PO line | 13,206 |
| `fact_purchase_product_items` | 1 receipt line | 722,880 |
| `fact_production_order_items` | 1 prod order item | — |
| `fact_production_stages` | 1 production stage | 749,308 |
| `fact_transfer_warehouse` | 1 transfer line | — |

---

## Mart Layer (dbt — `mart` schema)

| Model | Domain | Grain | Rows (~) |
|---|---|---|---|
| `fct_revenue` | Sales | date × customer × branch | 13,426 |
| `fct_order_items_detail` | Sales | order line item | 73,553 |
| `fct_order_performance` | Sales | 1 order | 72,363 |
| `dim_customer_segmentation` | Sales | 1 customer (RFM) | 260 |
| `fct_stock_snapshot` | Inventory | stock lot | 845,079 |
| `fct_inbound_outbound` | Inventory | date × product × warehouse × type | 197,980 |
| `fct_production_efficiency` | Inventory | prod order item × stage date | 196,146 |
| `fct_gross_profit` | Finance | date × customer × branch | 13,426 |
| `fct_purchase_cost` | Finance | po_date × supplier × product | 11,835 |
| `dim_customer_credit` | Finance | 1 customer (AR) | 260 |
| `dim_customer_mart` | Shared | 1 customer | 260 |
| `dim_product_mart` | Shared | 1 product | 7,854 |

---

## Known ERP Data Gaps

These fields are **not populated** in the source ERP and cannot be fixed at the DWH layer:

| Field | Impact | Affected Models |
|---|---|---|
| `total_cost` / item `cost` | COGS = ~0 for 99.7% orders | `fct_gross_profit`, `fct_revenue` |
| `status_payment` / `total_payment` | All AR shows as outstanding | `fct_order_performance`, `fct_revenue`, `dim_customer_credit` |
| `price_import` (dim_product) | `value_in = 0` in stock movements | `fct_inbound_outbound` |
| `number_hours` (production stages) | `output_per_hour = NULL` | `fct_production_efficiency` |
| `debt_limit` (customers) | All customers show "No Limit Set" | `dim_customer_credit` |

---

## Progress

- [x] Star schema design (dims + facts) — Kimball methodology
- [x] Python ELT pipeline — incremental watermark-based load
- [x] SCD Type 1 dimensions (CTE UPSERT pattern)
- [x] COPY protocol bulk load (17× faster than INSERT)
- [x] Auto-fix NULL foreign keys (retroactive UPDATE steps)
- [x] dbt staging layer (8 models — thin views over core)
- [x] dbt intermediate layer (`int_orders_enriched`)
- [x] dbt mart layer — Sales (4 models)
- [x] dbt mart layer — Inventory (3 models)
- [x] dbt mart layer — Finance (3 models)
- [x] dbt mart layer — Shared dims (2 models)
- [x] Full EDA across all 12 mart tables
- [x] Data quality issues identified & resolved (7 issues)
- [ ] Scheduled pipeline runs (Task Scheduler / Airflow)
- [ ] BI dashboard (Power BI / Metabase)
