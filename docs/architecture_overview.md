# Architecture Overview

## Stack

| Layer | Technology |
|-------|------------|
| Source (OLTP) | MySQL 8 — FOSO ERP system |
| ELT Pipeline | Python 3.8+ (pandas, SQLAlchemy, psycopg2, loguru) |
| Data Warehouse | PostgreSQL 13+ |
| Analytics (WIP) | dbt Core |
| BI | Power BI / Metabase *(planned)* |

---

## Data Flow

```
MySQL (ERP / FOSO)
    │
    │  STAGE 1 — Extract + Load
    │  python pipeline.py --stage extract
    │  • Incremental (watermark) for dated tables
    │  • Full reload for master/ref tables
    ▼
PostgreSQL — schema: staging
    │  Raw mirror of MySQL tables (25 tables)
    │  TRUNCATE + COPY each run (using COPY protocol ~17x faster)
    │
    │  STAGE 2 — Transform
    │  python pipeline.py --stage transform
    │  • Upsert 9 Dimension tables (SCD Type 1)
    │  • Insert-only 9 Fact tables
    ▼
PostgreSQL — schema: core
    │  Star Schema: 9 dims + 9 facts
    │  Facts are range-partitioned by date_key (YYYYMMDD INT)
    │
    │  STAGE 3 — Analytics Layer (dbt)
    │  dbt run
    ▼
PostgreSQL — schema: mart
    3 domains: sales / inventory / finance
```

---

## Schemas

### `staging` — Raw layer
- Direct mirror of MySQL source tables
- Added column: `etl_loaded_at TIMESTAMP`
- Tracking: `staging.etl_watermark` (per-table last sync timestamp)
- Strategy: TRUNCATE + COPY on each run (idempotent)

### `core` — Dimensional model
- **Dimensions** (9): customer, product, staff, department, warehouse,
  warehouse_location, supplier, manufacture, price_group
- **Facts** (9): orders, order_items, delivery_items, warehouse_stock,
  purchase_order_items, production_order_items, production_stages,
  purchase_product_items, transfer_warehouse
- Surrogate keys (SERIAL) — natural keys kept as plain columns
- Fact tables partitioned by `*_date_key INT` (YYYYMMDD)

### `mart` — Analytics layer (dbt)
- `mart.sales` — revenue, orders, delivery performance
- `mart.inventory` — stock levels, warehouse movements
- `mart.finance` — purchase costs, production costs

---

## Modeling Approach
- **Star Schema** (Dimensional Modeling — Kimball)
- **Conformed dimensions** — same dim_customer used across all fact domains
- **SCD Type 1** for dimensions (overwrite on change, no history)
- **Incremental facts** — append-only, deduplication via `WHERE NOT EXISTS`
- **Date key** — integer YYYYMMDD for fast partition pruning

---

## Data Volume (as of 2026-03)

| Table | Rows | Load strategy |
|-------|------|---------------|
| `tbl_purchase_product_items` | ~722k | Full COPY |
| `tbllocaltion_warehouses` | ~757k | Full COPY |
| Other tables | 1k–50k each | Incremental / Full |
| **Total** | **~2M rows** | **~2m38s pipeline** |
