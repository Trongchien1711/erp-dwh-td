"""
Fix NVL rows in core.fact_warehouse_stock:
- Adds material_key by joining MySQL tblwarehouse_product (nvl) → core.dim_material
- Clears product_key for NVL rows (was wrong — material_id accidentally hit a product_id)
"""
import sys, os
sys.path.insert(0, r"d:\Data Warehouse")

from dotenv import load_dotenv
load_dotenv()

import pandas as pd
from sqlalchemy import text
from elt.connections import get_mysql_engine, get_pg_engine

mysql = get_mysql_engine()
pg    = get_pg_engine()

# 1. Pull NVL rows from MySQL source
print("Querying MySQL for NVL lots...")
df = pd.read_sql("SELECT id, product_id FROM tblwarehouse_product WHERE type_items='nvl'", mysql)
print(f"  MySQL NVL rows: {len(df):,}")

if df.empty:
    print("No NVL rows found in MySQL. Nothing to do.")
    sys.exit(0)

# 2. Load into PG temp table and UPDATE
with pg.begin() as conn:
    conn.execute(text("""
        CREATE TEMP TABLE tmp_nvl_ids (
            stock_id          INT,
            material_source_id INT
        ) ON COMMIT DROP
    """))

    rows = [{"s": int(r.id), "m": int(r.product_id)} for _, r in df.iterrows()]
    conn.execute(text("INSERT INTO tmp_nvl_ids VALUES (:s, :m)"), rows)
    print("  Temp table loaded.")

    result = conn.execute(text("""
        UPDATE core.fact_warehouse_stock f
        SET material_key = dm.material_key,
            product_key  = NULL
        FROM tmp_nvl_ids t
        JOIN core.dim_material dm ON dm.material_id = t.material_source_id
        WHERE f.stock_id = t.stock_id
    """))
    print(f"  Updated rows: {result.rowcount:,}")

    cnt = conn.execute(text(
        "SELECT COUNT(*) FROM core.fact_warehouse_stock WHERE type_items='nvl' AND material_key IS NOT NULL"
    )).fetchone()[0]
    print(f"  NVL rows with material_key set: {cnt:,}")

    null_mat = conn.execute(text(
        "SELECT COUNT(*) FROM core.fact_warehouse_stock WHERE type_items='nvl' AND material_key IS NULL"
    )).fetchone()[0]
    print(f"  NVL rows still missing material_key: {null_mat:,}")

print("Done.")
