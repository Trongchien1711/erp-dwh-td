"""
Fix NVL rows in core.fact_purchase_order_items:
- tblpurchase_order_items.type = 'nvl' → product_id is actually material_id
- Set material_key from dim_material, clear product_key
"""
import sys; sys.path.insert(0, r"d:\Data Warehouse")
from dotenv import load_dotenv; load_dotenv()

from sqlalchemy import text
from elt.connections import get_mysql_engine, get_pg_engine
import pandas as pd

mysql = get_mysql_engine()
pg    = get_pg_engine()

# Pull NVL rows from MySQL
print("Querying MySQL for NVL purchase order items...")
df = pd.read_sql("SELECT id, product_id FROM tblpurchase_order_items WHERE type='nvl'", mysql)
print(f"  MySQL NVL po_items: {len(df):,}")

with pg.begin() as conn:
    # Load to temp table
    conn.execute(text("""
        CREATE TEMP TABLE tmp_poi_nvl (
            po_item_id        INT,
            material_source_id INT
        ) ON COMMIT DROP
    """))
    rows = [{"i": int(r.id), "m": int(r.product_id)} for _, r in df.iterrows()]
    conn.execute(text("INSERT INTO tmp_poi_nvl VALUES (:i, :m)"), rows)
    print("  Temp table loaded.")

    result = conn.execute(text("""
        UPDATE core.fact_purchase_order_items f
        SET material_key = dm.material_key,
            product_key  = NULL
        FROM tmp_poi_nvl t
        JOIN core.dim_material dm ON dm.material_id = t.material_source_id
        WHERE f.po_item_id = t.po_item_id
    """))
    print(f"  Updated rows: {result.rowcount:,}")

    cnt = conn.execute(text(
        "SELECT COUNT(*) FROM core.fact_purchase_order_items WHERE material_key IS NOT NULL"
    )).fetchone()[0]
    print(f"  Rows with material_key set: {cnt:,}")

    null_mat = conn.execute(text(
        "SELECT COUNT(*) FROM core.fact_purchase_order_items f "
        "JOIN staging.tblpurchase_order_items s ON s.id = f.po_item_id "
        "WHERE s.type='nvl' AND f.material_key IS NULL"
    )).fetchone()[0]
    print(f"  NVL rows still missing material_key: {null_mat:,}")

print("Done.")
