# ============================================================
# check_inventory_reconciliation.py
# Kiem tra doi chieu nhat-xuat-ton kho
#
# So sanh:
#   1. Lot-level  : quantity_left = quantity - quantity_export (ERP internal)
#   2. Aggregate  : SUM(quantity_left) vs SUM(nhap) - SUM(xuat)
#   3. Cross-check: fct_stock_snapshot vs fct_inbound_outbound cumulative
#
# Chay:
#   cd "d:\Data Warehouse"
#   .\.venv\Scripts\Activate.ps1
#   python scripts/python/check_inventory_reconciliation.py
# ============================================================

import os
import sys
from decimal import Decimal
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

_project_root = Path(__file__).resolve().parent.parent.parent
load_dotenv(_project_root / ".env")

conn = psycopg2.connect(
    host=os.getenv("PG_HOST", "localhost"),
    port=int(os.getenv("PG_PORT", "5432")),
    user=os.getenv("PG_USER", "dwh_admin"),
    password=os.getenv("PG_PASSWORD"),
    dbname=os.getenv("PG_DATABASE", "erp_dwh"),
)
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

SEP  = "=" * 70
SEP2 = "-" * 70
issues = []

def section(title):
    print(f"\n{SEP}")
    print(f"  {title}")
    print(SEP)

def ok(msg):
    print(f"  [OK]  {msg}")

def warn(msg):
    issues.append(msg)
    print(f"  [!!]  {msg}")

def fmt(v, dec=0):
    if v is None:
        return "NULL"
    if isinstance(v, Decimal):
        v = float(v)
    if isinstance(v, float):
        return f"{v:,.{dec}f}"
    if isinstance(v, int):
        return f"{v:,}"
    return str(v)

def q(sql):
    cur.execute(sql)
    return cur.fetchall()

def q1(sql):
    cur.execute(sql)
    return cur.fetchone()


# ─────────────────────────────────────────────────────────────────────────────
# 1. LOT-LEVEL: quantity_left = quantity - quantity_export
# ─────────────────────────────────────────────────────────────────────────────
section("1. LOT-LEVEL BALANCE  (quantity_left = quantity - quantity_export)")

row = q1("""
    SELECT
        count(*)                                                AS total_lots,
        count(*) FILTER (
            WHERE abs(quantity_left - (quantity - quantity_export)) > 0.001
        )                                                       AS lots_drift,
        round(sum(quantity)::numeric, 2)                        AS total_nhap,
        round(sum(quantity_export)::numeric, 2)                 AS total_xuat,
        round(sum(quantity_left)::numeric, 2)                   AS total_ton,
        round((sum(quantity) - sum(quantity_export))::numeric, 2)
                                                                AS expected_ton,
        round(
            sum(quantity_left) - (sum(quantity) - sum(quantity_export))
        , 2)                                                    AS drift
    FROM core.fact_warehouse_stock
""")

print(f"  Tong so lot    : {fmt(row['total_lots'])}")
print(f"  Tong nhap      : {fmt(row['total_nhap'])} don vi")
print(f"  Tong xuat (lot): {fmt(row['total_xuat'])} don vi")
print(f"  Ton thuc te    : {fmt(row['total_ton'])} don vi")
print(f"  Ton tinh toan  : {fmt(row['expected_ton'])} don vi")
drift = float(row['drift'] or 0)
print(f"  Drift          : {fmt(row['drift'])} don vi")

if abs(drift) < 0.01:
    ok("Ton kho lot-level KHOP (drift ~ 0)")
else:
    warn(f"Drift lot-level = {fmt(row['drift'])} don vi — can kiem tra")

lots_drift = int(row['lots_drift'])
if lots_drift == 0:
    ok(f"Tat ca {fmt(row['total_lots'])} lot co quantity_left chinh xac")
else:
    warn(f"{fmt(lots_drift)} lot bi sai lech (quantity_left != quantity - quantity_export)")

    top_drifts = q("""
        SELECT
            stock_id,
            product_key,
            warehouse_key,
            quantity,
            quantity_export,
            quantity_left,
            round((quantity - quantity_export)::numeric, 2) AS expected_left,
            round((quantity_left - (quantity - quantity_export))::numeric, 2) AS drift
        FROM core.fact_warehouse_stock
        WHERE abs(quantity_left - (quantity - quantity_export)) > 0.001
        ORDER BY abs(quantity_left - (quantity - quantity_export)) DESC
        LIMIT 10
    """)
    print(f"\n  Top 10 lot bi drift:")
    print(f"  {'stock_id':<12} {'prod_key':<10} {'wh_key':<8} "
          f"{'nhap':>10} {'xuat':>10} {'ton_actual':>12} {'ton_calc':>10} {'drift':>10}")
    print(f"  {SEP2}")
    for r in top_drifts:
        print(f"  {str(r['stock_id']):<12} {str(r['product_key']):<10} "
              f"{str(r['warehouse_key']):<8} "
              f"{fmt(r['quantity']):>10} {fmt(r['quantity_export']):>10} "
              f"{fmt(r['quantity_left']):>12} {fmt(r['expected_left']):>10} "
              f"{fmt(r['drift']):>10}")


# ─────────────────────────────────────────────────────────────────────────────
# 2. AGGREGATE: fct_stock_snapshot vs fct_inbound_outbound (cumulative)
# ─────────────────────────────────────────────────────────────────────────────
section("2. MART AGGREGATE  (fct_stock_snapshot vs fct_inbound_outbound)")

snap = q1("""
    SELECT
        count(*)                        AS rows,
        round(sum(quantity_left)::numeric, 0)       AS ton_snapshot,
        round(sum(quantity)::numeric, 0)             AS nhap_snapshot,
        round(sum(quantity_exported)::numeric, 0)    AS xuat_snapshot
    FROM mart.fct_stock_snapshot
""")

io = q1("""
    SELECT
        round(sum(quantity_in)::numeric, 0)   AS total_nhap,
        round(sum(quantity_out)::numeric, 0)  AS total_xuat,
        round((sum(quantity_in) - sum(quantity_out))::numeric, 0) AS net_ton
    FROM mart.fct_inbound_outbound
""")

print(f"  --- fct_stock_snapshot ---")
print(f"  So lot           : {fmt(snap['rows'])}")
print(f"  Tong nhap (lot)  : {fmt(snap['nhap_snapshot'])} don vi")
print(f"  Tong xuat (lot)  : {fmt(snap['xuat_snapshot'])} don vi")
print(f"  Ton hien tai     : {fmt(snap['ton_snapshot'])} don vi")

print(f"\n  --- fct_inbound_outbound (cumulative) ---")
print(f"  Tong nhap        : {fmt(io['total_nhap'])} don vi")
print(f"  Tong xuat        : {fmt(io['total_xuat'])} don vi")
print(f"  Net ton (cumul.) : {fmt(io['net_ton'])} don vi")

snap_ton  = float(snap['ton_snapshot']  or 0)
snap_nhap = float(snap['nhap_snapshot'] or 0)
snap_xuat = float(snap['xuat_snapshot'] or 0)
io_nhap   = float(io['total_nhap']  or 0)
io_xuat   = float(io['total_xuat']  or 0)
io_net    = float(io['net_ton']     or 0)

print(f"\n  --- So sanh ---")

# Nhap khop?
nhap_diff = abs(snap_nhap - io_nhap)
nhap_pct  = nhap_diff / io_nhap * 100 if io_nhap else 0
print(f"  Nhap diff        : {nhap_diff:,.0f} don vi ({nhap_pct:.3f}%)")
if nhap_pct < 0.1:
    ok("Nhap KHOP giua snapshot va inbound_outbound (<0.1%)")
else:
    warn(f"Nhap LECH {nhap_pct:.2f}% — snapshot={snap_nhap:,.0f} vs io={io_nhap:,.0f}")

# Xuat khop? (snapshot dung quantity_export = xuat tren lot;
#              io dung fact_warehouse_export — 2 nguon khac nhau)
xuat_diff = abs(snap_xuat - io_xuat)
xuat_pct  = xuat_diff / io_xuat * 100 if io_xuat else 0
print(f"  Xuat diff        : {xuat_diff:,.0f} don vi ({xuat_pct:.3f}%)")
if xuat_pct < 5:
    ok(f"Xuat cach biet {xuat_pct:.1f}% (binh thuong — 2 nguon khac nhau)")
else:
    warn(f"Xuat LECH lon {xuat_pct:.1f}% — kiem tra fact_warehouse_export vs lot export")

# Ton khop?
ton_diff = abs(snap_ton - io_net)
ton_pct  = ton_diff / snap_ton * 100 if snap_ton else 0
print(f"  Ton diff         : {ton_diff:,.0f} don vi ({ton_pct:.3f}%)")
if ton_pct < 5:
    ok(f"Ton sai lech {ton_pct:.1f}% (chap nhan duoc — xuat co 2 nguon)")
else:
    warn(f"Ton LECH {ton_pct:.1f}% — {snap_ton:,.0f} (snapshot) vs {io_net:,.0f} (nhap-xuat)")


# ─────────────────────────────────────────────────────────────────────────────
# 3. BY WAREHOUSE — Ton snapshot vs (nhap - xuat) theo tung kho
# ─────────────────────────────────────────────────────────────────────────────
section("3. BY WAREHOUSE  (top 10 kho co sai lech lon nhat)")

wh_rows = q("""
    WITH snap AS (
        SELECT
            warehouse_key,
            warehouse_name,
            round(sum(quantity_left)::numeric, 0)    AS ton,
            round(sum(quantity)::numeric, 0)          AS nhap,
            round(sum(quantity_exported)::numeric, 0) AS xuat
        FROM mart.fct_stock_snapshot
        GROUP BY warehouse_key, warehouse_name
    ),
    io AS (
        SELECT
            warehouse_key,
            round(sum(quantity_in)::numeric, 0)   AS io_nhap,
            round(sum(quantity_out)::numeric, 0)  AS io_xuat
        FROM mart.fct_inbound_outbound
        GROUP BY warehouse_key
    )
    SELECT
        s.warehouse_name,
        s.nhap                              AS snap_nhap,
        s.xuat                              AS snap_xuat,
        s.ton                               AS snap_ton,
        i.io_nhap,
        i.io_xuat,
        (i.io_nhap - i.io_xuat)             AS io_net,
        (s.ton - (i.io_nhap - i.io_xuat))   AS drift
    FROM snap s
    LEFT JOIN io i USING (warehouse_key)
    ORDER BY abs(s.ton - (i.io_nhap - i.io_xuat)) DESC NULLS LAST
    LIMIT 10
""")

col_w = [20, 12, 12, 12, 12, 12, 12, 12]
hdr   = ["warehouse", "snap_nhap", "snap_xuat", "snap_ton",
         "io_nhap", "io_xuat", "io_net", "drift"]
print("  " + " ".join(h.rjust(w) for h, w in zip(hdr, col_w)))
print(f"  {SEP2}")
for r in wh_rows:
    vals = [
        (r['warehouse_name'] or '')[:20],
        fmt(r['snap_nhap']),
        fmt(r['snap_xuat']),
        fmt(r['snap_ton']),
        fmt(r['io_nhap']),
        fmt(r['io_xuat']),
        fmt(r['io_net']),
        fmt(r['drift']),
    ]
    print("  " + " ".join(v.rjust(w) for v, w in zip(vals, col_w)))


# ─────────────────────────────────────────────────────────────────────────────
# 4. LOT ANOMALIES — ton am hoac xuat > nhap
# ─────────────────────────────────────────────────────────────────────────────
section("4. LOT ANOMALIES  (ton am / xuat vuot nhap)")

anom = q1("""
    SELECT
        count(*) FILTER (WHERE quantity_left < 0)       AS lots_negative,
        count(*) FILTER (WHERE quantity_export > quantity) AS lots_overexport,
        round(sum(quantity_left) FILTER (WHERE quantity_left < 0)::numeric, 0) AS total_negative
    FROM core.fact_warehouse_stock
""")

neg     = int(anom['lots_negative']   or 0)
over    = int(anom['lots_overexport'] or 0)
neg_val = float(anom['total_negative'] or 0)

if neg == 0:
    ok("Khong co lot nao co ton am")
else:
    warn(f"{fmt(neg)} lot co quantity_left < 0  (tong = {neg_val:,.0f} don vi)")

if over == 0:
    ok("Khong co lot nao bi xuat vuot nhap (quantity_export > quantity)")
else:
    warn(f"{fmt(over)} lot co quantity_export > quantity")

# Negative lots detail
if neg > 0:
    neg_rows = q("""
        SELECT
            s.stock_id,
            p.product_code,
            p.product_name,
            w.warehouse_name,
            s.quantity,
            s.quantity_export,
            s.quantity_left
        FROM core.fact_warehouse_stock s
        JOIN core.dim_products p ON p.product_key = s.product_key
        JOIN core.dim_warehouses w ON w.warehouse_key = s.warehouse_key
        WHERE s.quantity_left < 0
        ORDER BY s.quantity_left ASC
        LIMIT 10
    """)
    print(f"\n  Top 10 lot ton am:")
    print(f"  {'stock_id':<10} {'product_code':<18} {'warehouse':<20} "
          f"{'nhap':>8} {'xuat':>8} {'ton':>8}")
    print(f"  {'-'*75}")
    for r in neg_rows:
        print(f"  {str(r['stock_id']):<10} {(r['product_code'] or ''):<18} "
              f"{(r['warehouse_name'] or ''):<20} "
              f"{fmt(r['quantity']):>8} {fmt(r['quantity_export']):>8} "
              f"{fmt(r['quantity_left']):>8}")


# ─────────────────────────────────────────────────────────────────────────────
# 5. YEAR-LEVEL: nhap > xuat moi nam (kiem tra xu huong hop le)
# ─────────────────────────────────────────────────────────────────────────────
section("5. BY YEAR  (nhap phai > xuat moi nam = tang tru luong)")

yr = q("""
    SELECT
        year,
        round(sum(quantity_in)::numeric, 0)   AS nhap,
        round(sum(quantity_out)::numeric, 0)  AS xuat,
        round((sum(quantity_in) - sum(quantity_out))::numeric, 0) AS net,
        round(
            (sum(quantity_in) - sum(quantity_out)) / nullif(sum(quantity_in), 0) * 100
        , 1)                                                      AS net_pct
    FROM mart.fct_inbound_outbound
    JOIN core.dim_date ON date_key = movement_date_key
    GROUP BY year
    ORDER BY year
""")

print(f"  {'Nam':<6} {'Nhap':>14} {'Xuat':>14} {'Net':>14} {'Net%':>7}")
print(f"  {'-'*55}")
for r in yr:
    flag = " <-- XUAT > NHAP!" if float(r['net'] or 0) < 0 else ""
    print(f"  {r['year']:<6} {fmt(r['nhap']):>14} {fmt(r['xuat']):>14} "
          f"{fmt(r['net']):>14} {str(r['net_pct'] or 0):>6}%{flag}")
    if float(r['net'] or 0) < 0:
        warn(f"Nam {r['year']}: Xuat ({fmt(r['xuat'])}) > Nhap ({fmt(r['nhap'])})")


# ─────────────────────────────────────────────────────────────────────────────
# SUMMARY
# ─────────────────────────────────────────────────────────────────────────────
section("TONG KET")
total_checks = 6  # so kiem tra chinh
if not issues:
    print(f"  Tat ca kiem tra DAT — Nhat-xuat-ton KHOP.\n")
    sys.exit(0)
else:
    print(f"  Co {len(issues)} van de can xem xet:\n")
    for i, msg in enumerate(issues, 1):
        print(f"  {i}. {msg}")
    print()
    sys.exit(1)
