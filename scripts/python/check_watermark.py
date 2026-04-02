import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

_project_root = Path(__file__).resolve().parent.parent.parent
load_dotenv(_project_root / ".env")

conn = psycopg2.connect(
    host=os.getenv("PG_HOST", "localhost"),
    port=int(os.getenv("PG_PORT", "5432")),
    user=os.getenv("PG_USER", "dwh_admin"),
    password=os.getenv("PG_PASSWORD"),
    dbname=os.getenv("PG_DATABASE", "erp_dwh")
)
cur = conn.cursor()

# ── Watermark table ─────────────────────────────────────────────────────────
print("=== ETL WATERMARKS (last successful extract) ===")
cur.execute("""
    SELECT table_name, last_loaded_at, updated_at
    FROM staging.etl_watermark
    ORDER BY last_loaded_at DESC NULLS LAST
""")
rows = cur.fetchall()
if rows:
    print(f"  {'Table':<40} {'Last Loaded':>25} {'Updated At':>20}")
    print("  " + "-"*78)
    for table, last_loaded, updated in rows:
        loaded_str = str(last_loaded) if last_loaded else "never"
        updated_str = str(updated)[:19] if updated else "-"
        print(f"  {table:<40} {loaded_str:>25} {updated_str:>20}")
else:
    print("  (no rows)")

# ── Staging row counts ──────────────────────────────────────────────────────
print("\n=== STAGING TABLE ROW COUNTS ===")
cur.execute("""
    SELECT table_name FROM information_schema.tables
    WHERE table_schema='staging' AND table_type='BASE TABLE'
      AND table_name != 'etl_watermark'
    ORDER BY table_name
""")
staging_tables = [r[0] for r in cur.fetchall()]
for t in staging_tables:
    cur.execute(f'SELECT COUNT(*) FROM staging."{t}"')
    print(f"  staging.{t:<40}: {cur.fetchone()[0]:>10,}")

cur.close()
conn.close()
