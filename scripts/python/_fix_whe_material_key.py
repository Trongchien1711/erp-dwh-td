"""
Fix NVL rows in core.fact_warehouse_export:
- tblwarehouse_export.type_items = 'nvl' → product_id is actually material_id
- Set material_key from dim_material, clear product_key (for all NVL rows).

Note: a prior wrong migration set material_key for 12,438 rows incorrectly
(joined material_id to product_key surrogate by accident). This script
resets ALL NVL rows from MySQL source and remigrates correctly.
"""
import sys; sys.path.insert(0, r"d:\Data Warehouse")
from dotenv import load_dotenv; load_dotenv()

from sqlalchemy import text
from elt.connections import get_mysql_engine, get_pg_engine
import pandas as pd

mysql = get_mysql_engine()
pg    = get_pg_engine()

# Pull NVL rows from MySQL
print("Querying MySQL for NVL warehouse export rows...")
df = pd.read_sql(
    "SELECT id, product_id FROM tblwarehouse_export WHERE type_items = 'nvl'",
    mysql
)
print(f"  MySQL NVL export rows: {len(df):,}")

with pg.begin() as conn:
    # Load to temp table
    conn.execute(text("""
        CREATE TEMP TABLE tmp_whe_nvl (
            export_id          INT,
            material_source_id INT
        ) ON COMMIT DROP
    """))
    rows = [{"i": int(r.id), "m": int(r.product_id)} for _, r in df.iterrows()]
    conn.execute(text("INSERT INTO tmp_whe_nvl VALUES (:i, :m)"), rows)
    print(f"  Temp table loaded: {len(rows):,} rows")

    # Step 1: Reset all NVL rows — clear both keys first
    reset = conn.execute(text("""
        UPDATE core.fact_warehouse_export f
        SET    material_key = NULL,
               product_key  = NULL
        FROM   tmp_whe_nvl t
        WHERE  f.export_id = t.export_id
    """))
    print(f"  Reset rows (cleared both keys): {reset.rowcount:,}")

    # Step 2: Set material_key where material_id exists in dim_material
    result = conn.execute(text("""
        UPDATE core.fact_warehouse_export f
        SET    material_key = dm.material_key
        FROM   tmp_whe_nvl t
        JOIN   core.dim_material dm ON dm.material_id = t.material_source_id
        WHERE  f.export_id = t.export_id
    """))
    print(f"  Rows with material_key set: {result.rowcount:,}")

    # Verify
    total_nvl = conn.execute(text(
        "SELECT COUNT(*) FROM core.fact_warehouse_export WHERE type_items = 'nvl'"
    )).fetchone()[0]
    with_mat = conn.execute(text(
        "SELECT COUNT(*) FROM core.fact_warehouse_export WHERE type_items = 'nvl' AND material_key IS NOT NULL"
    )).fetchone()[0]
    no_key = conn.execute(text(
        "SELECT COUNT(*) FROM core.fact_warehouse_export WHERE type_items = 'nvl' AND material_key IS NULL AND product_key IS NULL"
    )).fetchone()[0]
    print(f"\n  Total NVL rows : {total_nvl:,}")
    print(f"  With material_key : {with_mat:,}  ({with_mat/total_nvl*100:.1f}%)")
    print(f"  No key (unmatched): {no_key:,}   ({no_key/total_nvl*100:.1f}%)")

print("\nDone.")
