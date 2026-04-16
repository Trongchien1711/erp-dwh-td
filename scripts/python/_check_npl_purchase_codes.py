"""Check NPL code vs purchase code alignment"""
import sys; sys.path.insert(0, r"d:\Data Warehouse")
from dotenv import load_dotenv; load_dotenv()
from elt.connections import get_mysql_engine, get_pg_engine
from sqlalchemy import text

mysql = get_mysql_engine()
pg    = get_pg_engine()

# 1. Find purchase-related tables in MySQL
with mysql.connect() as conn:
    r = conn.execute(text("SHOW TABLES LIKE 'tbl_purchase%'"))
    print("Purchase tables:", [row[0] for row in r])

# 2. Final confirmation: tblpurchase_order_items.product_id → dim_material match rate
import pandas as pd
from sqlalchemy import text

# Full match check
poi_all = pd.read_sql(
    "SELECT poi.product_id, poi.unit_cost, poi.price_suppliers FROM tblpurchase_order_items poi",
    mysql
)
mat = pd.read_sql("SELECT material_id, material_code FROM core.dim_material", pg)

m = poi_all.merge(mat, left_on='product_id', right_on='material_id', how='left')
matched   = m['material_id'].notna().sum()
unmatched = m['material_id'].isna().sum()
print(f"tblpurchase_order_items rows: {len(poi_all):,}")
print(f"  → matched to dim_material: {matched:,} ({100*matched/len(poi_all):.1f}%)")
print(f"  → unmatched:               {unmatched:,}")

# Price coverage in purchase orders for NPL
has_price = (poi_all['price_suppliers'] > 0).sum()
print(f"\nRows with price_suppliers > 0: {has_price:,}/{len(poi_all):,} ({100*has_price/len(poi_all):.1f}%)")

# 3. Check tblwarehouse_product.import_id → tblpurchase_order.id link
with mysql.connect() as conn:
    r = conn.execute(text("DESCRIBE tblpurchase_order"))
    cols = [row[0] for row in r]
    print(f"\ntblpurchase_order columns: {cols}")

# Does import_id in nvl warehouse rows match a tblpurchase_order.id?
nvl_imports = pd.read_sql(
    "SELECT DISTINCT import_id FROM tblwarehouse_product WHERE type_items='nvl' AND import_id IS NOT NULL",
    mysql
)
po_ids = pd.read_sql("SELECT id FROM tblpurchase_order", mysql)
matched_imports = nvl_imports['import_id'].isin(set(po_ids['id'])).sum()
print(f"\nNVL lots: {len(nvl_imports):,} distinct import_ids")
print(f"  → match tblpurchase_order.id: {matched_imports:,} ({100*matched_imports/len(nvl_imports):.1f}%)")
