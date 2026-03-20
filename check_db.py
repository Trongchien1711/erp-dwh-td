import psycopg2, os, sys
from dotenv import load_dotenv

load_dotenv(r"d:\Data Warehouse\.env")

conn = psycopg2.connect(
    host="localhost", port=5432,
    user=os.getenv("PG_USER", "dwh_admin"),
    password=os.getenv("PG_PASSWORD"),
    dbname="erp_dwh"
)
cur = conn.cursor()

# ── 1. Schemas ──────────────────────────────────────────────────────────────
cur.execute("""
    SELECT schema_name FROM information_schema.schemata
    WHERE schema_name IN ('staging','core','mart','staging_dbt')
    ORDER BY 1
""")
schemas = [r[0] for r in cur.fetchall()]
print("=== SCHEMAS ===")
for s in schemas:
    print(f"  {s}")

# ── 2. Table + view counts ──────────────────────────────────────────────────
print("\n=== OBJECTS PER SCHEMA ===")
for schema in ["staging", "core", "mart", "staging_dbt"]:
    cur.execute(
        """SELECT table_type, COUNT(*) FROM information_schema.tables
           WHERE table_schema = %s GROUP BY table_type ORDER BY 1""",
        (schema,)
    )
    rows = cur.fetchall()
    summary = ", ".join(f"{cnt} {t.lower()}s" for t, cnt in rows) or "empty"
    print(f"  {schema:15s}: {summary}")

# ── 3. Core table row counts ────────────────────────────────────────────────
print("\n=== CORE TABLE ROW COUNTS ===")
core_tables = [
    "dim_date","dim_customer","dim_product","dim_staff","dim_department",
    "dim_warehouse","dim_warehouse_location","dim_supplier","dim_manufacture","dim_price_group",
    "fact_orders","fact_order_items","fact_delivery_items","fact_warehouse_stock",
    "fact_purchase_order_items","fact_production_order_items","fact_production_stages",
    "fact_purchase_product_items","fact_transfer_warehouse","fact_warehouse_export"
]
for t in core_tables:
    cur.execute(f"SELECT COUNT(*) FROM core.{t}")
    print(f"  core.{t:40s}: {cur.fetchone()[0]:>10,}")

# ── 4. Mart table row counts ────────────────────────────────────────────────
print("\n=== MART TABLE ROW COUNTS ===")
cur.execute("""
    SELECT table_name FROM information_schema.tables
    WHERE table_schema='mart' AND table_type='BASE TABLE' ORDER BY 1
""")
mart_tables = [r[0] for r in cur.fetchall()]
for t in mart_tables:
    cur.execute(f'SELECT COUNT(*) FROM mart."{t}"')
    print(f"  mart.{t:40s}: {cur.fetchone()[0]:>10,}")

cur.close()
conn.close()
print("\nDB check complete.")
