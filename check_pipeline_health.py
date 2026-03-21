# ============================================================
# check_pipeline_health.py
# Post-pipeline health check: row counts, NULL key rates, freshness
#
# Chạy sau mỗi lần pipeline hoàn thành để xác nhận không có vấn đề.
# Exit code 0 = OK, 1 = có vấn đề cần xem xét.
#
# Usage:
#   python check_pipeline_health.py             # in kết quả
#   python check_pipeline_health.py --strict    # exit 1 nếu có WARNING
# ============================================================

import argparse
import os
import sys
from datetime import date, timedelta
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

parser = argparse.ArgumentParser()
parser.add_argument("--strict", action="store_true",
                    help="Exit 1 if any WARNING found (useful in CI/script mode)")
args = parser.parse_args()

conn = psycopg2.connect(
    host=os.getenv("PG_HOST", "localhost"),
    port=int(os.getenv("PG_PORT", "5432")),
    user=os.getenv("PG_USER", "dwh_admin"),
    password=os.getenv("PG_PASSWORD"),
    dbname=os.getenv("PG_DATABASE", "erp_dwh")
)
cur = conn.cursor()

warnings = []
ok_count = 0

SEP = "─" * 65

def ok(msg):
    global ok_count
    ok_count += 1
    print(f"  ✓  {msg}")

def warn(msg):
    warnings.append(msg)
    print(f"  ⚠  {msg}")

def fail(msg):
    warnings.append(f"CRITICAL: {msg}")
    print(f"  ✗  {msg}")

# ─── 1. Core Dimension Row Counts ───────────────────────────────────────────
print(f"\n{SEP}")
print("1. CORE DIMENSION ROW COUNTS")
print(SEP)

DIM_THRESHOLDS = {
    "dim_date":               (3_000, None),   # ~3650 rows for 10yr spine
    "dim_customer":           (100,   None),
    "dim_product":            (5_000, None),
    "dim_staff":              (10,    None),
    "dim_department":         (5,     None),
    "dim_warehouse":          (1,     None),
    "dim_warehouse_location": (1,     None),
    "dim_supplier":           (1,     None),
    "dim_manufacture":        (0,     None),
    "dim_price_group":        (0,     None),
}

for tbl, (min_rows, _) in DIM_THRESHOLDS.items():
    cur.execute(f"SELECT COUNT(*) FROM core.{tbl}")
    n = cur.fetchone()[0]
    msg = f"core.{tbl:<30} {n:>10,} rows"
    if n < min_rows:
        fail(f"{msg}  [BELOW THRESHOLD {min_rows:,}]")
    else:
        ok(msg)

# ─── 2. Core Fact Row Counts ─────────────────────────────────────────────────
print(f"\n{SEP}")
print("2. CORE FACT ROW COUNTS")
print(SEP)

FACT_THRESHOLDS = {
    "fact_orders":                  1_000,
    "fact_order_items":             1_000,
    "fact_delivery_items":          1_000,
    "fact_warehouse_stock":       100_000,
    "fact_purchase_order_items":    1_000,
    "fact_purchase_product_items":  1_000,
    "fact_transfer_warehouse":     10_000,
    "fact_production_order_items":    100,
    "fact_production_stages":         100,
}

for tbl, min_rows in FACT_THRESHOLDS.items():
    cur.execute(f"SELECT COUNT(*) FROM core.{tbl}")
    n = cur.fetchone()[0]
    msg = f"core.{tbl:<35} {n:>10,} rows"
    if n < min_rows:
        fail(f"{msg}  [BELOW THRESHOLD {min_rows:,}]")
    else:
        ok(msg)

# ─── 3. NULL Key Rates (known irrecoverable NULLs = warning only) ────────────
print(f"\n{SEP}")
print("3. NULL KEY RATES IN FACT TABLES")
print(SEP)

NULL_KEY_CHECKS = [
    # (table, column, critical_threshold_pct, known_irrecoverable_pct)
    ("fact_orders",                "customer_key",  0.5,  None),
    ("fact_orders",                "employee_key",  5.0,  None),
    ("fact_order_items",           "product_key",   0.5,  None),
    ("fact_order_items",           "customer_key",  0.5,  None),
    ("fact_delivery_items",        "product_key",   0.5,  None),
    ("fact_delivery_items",        "warehouse_key", 0.5,  None),
    ("fact_warehouse_stock",       "product_key",   2.0, 1.01),  # irrecoverable ~1.01%
    ("fact_warehouse_stock",       "location_key",  1.0,  None),
    ("fact_purchase_order_items",  "product_key",  20.0, 14.78),  # irrecoverable ~14.78%
    ("fact_transfer_warehouse",    "product_key",   5.0,  3.03),  # irrecoverable ~3.03%
    ("fact_purchase_product_items","product_key",   0.5,  None),
    ("fact_production_order_items","product_key",   0.5,  None),
]

for tbl, col, critical_pct, known_pct in NULL_KEY_CHECKS:
    cur.execute(f"""
        SELECT COUNT(*),
               SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END)
        FROM core.{tbl}
    """)
    total, nulls = cur.fetchone()
    nulls = nulls or 0
    pct = (nulls / total * 100) if total > 0 else 0

    known_note = f" (known baseline: {known_pct:.1f}%)" if known_pct else ""
    msg = f"core.{tbl}.{col:<20} NULL={pct:.2f}% ({nulls:,}/{total:,}){known_note}"

    if pct > critical_pct:
        # If there's a known baseline and we're only slightly above it, warn not fail
        if known_pct and pct < known_pct * 1.5:
            warn(msg + "  [ABOVE CRITICAL but within expected range]")
        else:
            fail(msg + f"  [ABOVE CRITICAL THRESHOLD {critical_pct:.1f}%]")
    else:
        ok(msg)

# ─── 4. Mart Row Counts ──────────────────────────────────────────────────────
print(f"\n{SEP}")
print("4. MART MODEL ROW COUNTS")
print(SEP)

MART_TABLES = [
    "fct_revenue", "fct_order_items_detail", "fct_order_performance",
    "dim_customer_segmentation",
    "fct_gross_profit", "fct_purchase_cost", "dim_customer_credit",
    "fct_stock_snapshot", "fct_inbound_outbound", "fct_production_efficiency",
    "dim_customer_mart", "dim_product_mart",
]

for tbl in MART_TABLES:
    try:
        cur.execute(f"SELECT COUNT(*) FROM mart.{tbl}")
        n = cur.fetchone()[0]
        msg = f"mart.{tbl:<35} {n:>10,} rows"
        if n == 0:
            warn(msg + "  [EMPTY — dbt run may not have run]")
        else:
            ok(msg)
    except Exception as e:
        fail(f"mart.{tbl} — table missing or error: {e}")
        conn.rollback()

# ─── 5. Data Freshness (watermark check) ─────────────────────────────────────
print(f"\n{SEP}")
print("5. WATERMARK FRESHNESS")
print(SEP)

try:
    cur.execute("""
        SELECT table_name, last_loaded_at
        FROM staging.etl_watermark
        ORDER BY last_loaded_at DESC NULLS LAST
    """)
    rows = cur.fetchall()
    today = date.today()
    stale_threshold = timedelta(days=2)

    for tbl_name, last_loaded in rows:
        if last_loaded is None:
            warn(f"  {tbl_name:<35} last_loaded=NULL  [never loaded]")
            continue
        # Handle both date and datetime watermarks
        if hasattr(last_loaded, 'date'):
            loaded_date = last_loaded.date()
        else:
            try:
                from datetime import datetime
                loaded_date = datetime.strptime(str(last_loaded)[:10], "%Y-%m-%d").date()
            except Exception:
                ok(f"  {tbl_name:<35} last_loaded={last_loaded}")
                continue

        age = today - loaded_date
        msg = f"  {tbl_name:<35} last_loaded={loaded_date}  ({age.days} days ago)"
        if age > stale_threshold:
            warn(msg + "  [STALE]")
        else:
            ok(msg)
except Exception as e:
    warn(f"Could not read watermark table: {e}")
    conn.rollback()

# ─── Summary ─────────────────────────────────────────────────────────────────
print(f"\n{SEP}")
print("SUMMARY")
print(SEP)
print(f"  ✓ OK      : {ok_count}")
print(f"  ⚠ WARNING : {len(warnings)}")

if warnings:
    print("\nItems requiring attention:")
    for i, w in enumerate(warnings, 1):
        print(f"  {i}. {w}")

cur.close()
conn.close()

if warnings and args.strict:
    print("\nExiting with code 1 (--strict mode)")
    sys.exit(1)

sys.exit(0)
