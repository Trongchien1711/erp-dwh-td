# ERP Data Warehouse (erp-dwh-td)

> Business Data Analyst — Learning by building a real Data Warehouse for my company.

---

## Project Structure

```
erp-dwh-td/
│
├── elt/                    ← Python ELT pipeline (Extract → Load → Transform)
│   ├── pipeline.py         ← Main entrypoint
│   ├── extractor.py        ← Read from MySQL (incremental + full load)
│   ├── loader.py           ← Write to PostgreSQL staging (COPY protocol)
│   ├── transform_core.py   ← SQL: staging → core dims & facts
│   ├── watermark.py        ← Incremental load tracking
│   ├── connections.py      ← DB connection factories
│   ├── setup.py            ← One-time DB setup (GRANT + ALTER)
│   ├── requirements.txt    ← Python dependencies
│   └── README.md           ← Detailed ELT documentation
│
├── sql/                    ← Schema DDL scripts (run in order)
│   ├── 01_setup_database.sql
│   ├── 02_extensions.sql
│   ├── 03_dim_date.sql
│   ├── 04_core_dimensions.sql
│   ├── 05_core_fact_tables.sql
│   ├── 06_staging_tables.sql
│   └── 07_fix_staging_columns.sql
│
├── dbt_project/            ← dbt transformation layer (analytics)
│   └── models/
│       ├── staging/        ← Source cleaning models
│       ├── intermediate/   ← Business logic
│       └── marts/          ← Final analytics tables
│           ├── sales/
│           ├── inventory/
│           ├── finance/
│           └── shared_dim/
│
├── docs/                   ← Architecture & domain documentation
│   ├── architecture_overview.md
│   ├── bus_matrix.md
│   ├── grain_definition.md
│   ├── domain_sales.md
│   ├── domain_inventory.md
│   └── domain_finance.md
│
└── diagrams/               ← Star schema diagrams
    ├── sales_star_schema.png
    └── inventory_star_schema.png
```

---

## Quick Start

```bash
# 1. Clone repo
git clone https://github.com/Trongchien1711/erp-dwh-td.git
cd erp-dwh-td

# 2. Setup Python env
cd elt
python -m venv .venv
.venv\Scripts\Activate.ps1       # Windows
pip install -r requirements.txt

# 3. Configure .env
cp .env.example .env
# Edit .env with your DB credentials

# 4. Setup database (run once, needs postgres superuser)
python setup.py

# 5. Run the pipeline
python pipeline.py --stage all
```

---

## Architecture

```
MySQL (ERP)
    │
    │  Stage 1: Extract + Load (Python ELT)
    ▼
PostgreSQL — schema: staging    ← raw mirror of MySQL tables
    │
    │  Stage 2: Transform (SQL in transform_core.py)
    ▼
PostgreSQL — schema: core       ← Star Schema (dims + facts)
    │
    │  Analytics Layer (dbt — work in progress)
    ▼
dbt marts                       ← sales, inventory, finance
```

---

## Stack

| Layer | Technology |
|-------|-----------|
| Source | MySQL (ERP system) |
| ELT Pipeline | Python + pandas + SQLAlchemy + psycopg2 |
| Data Warehouse | PostgreSQL 13+ |
| Schema Design | Star Schema (Dimensional Modeling) |
| Analytics | dbt Core *(learning)* |
| Logging | loguru |

---

## Learning Progress

- [x] Design star schema (dims + facts)
- [x] Build Python ELT pipeline
- [x] Incremental load with watermark
- [x] SCD Type 1 for dimensions (CTE UPDATE + INSERT)
- [x] Performance optimization (PostgreSQL COPY — 17x speedup)
- [ ] dbt staging models
- [ ] dbt intermediate models  
- [ ] dbt marts (sales, inventory, finance)
- [ ] Scheduled runs (Task Scheduler / Airflow)
