import sys
sys.path.insert(0, 'd:/Data Warehouse')
from elt.connections import get_mysql_engine
from sqlalchemy import text

engine = get_mysql_engine()
with engine.connect() as conn:
    rows = conn.execute(text("SHOW TABLES")).fetchall()
    tables = [r[0] for r in rows]

print(f"Total tables: {len(tables)}")
print()

# Highlight NPL/material related
keywords = ['material', 'nvl', 'npl', 'nguyen', 'vat_tu', 'vattu', 'raw']
print("=== NPL/Material related ===")
for t in tables:
    if any(k in t.lower() for k in keywords):
        print(f"  {t}")

print()
print("=== All tables ===")
for t in sorted(tables):
    print(f"  {t}")
