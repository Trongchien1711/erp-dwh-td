"""
Final NPL validation report from mart.fct_order_npl_cost
"""
import psycopg2
import psycopg2.extras
import pandas as pd

PG = dict(host="localhost", port=5432, user="dwh_admin", password="881686", dbname="erp_dwh")
conn = psycopg2.connect(**PG)
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

# ── Overview ──────────────────────────────────
cur.execute("""
SELECT
    COUNT(*) AS total_orders,
    COUNT(*) FILTER(WHERE year = 2025) AS orders_2025,
    COUNT(*) FILTER(WHERE year = 2025 AND npl_quality = 'normal') AS normal_2025,
    COUNT(*) FILTER(WHERE year = 2025 AND npl_quality = 'high_cost') AS high_cost_2025,
    COUNT(*) FILTER(WHERE year = 2025 AND npl_quality = 'suspect_data') AS suspect_2025,
    COUNT(*) FILTER(WHERE year = 2025 AND is_sample_order = true) AS sample_2025,
    ROUND(SUM(revenue_vnd) FILTER(WHERE year=2025) / 1e9, 2) AS total_rev_2025_B,
    ROUND(SUM(allocated_npl_materials_vnd) FILTER(WHERE year=2025) / 1e9, 2) AS total_npl_2025_B
FROM mart.fct_order_npl_cost
""")
row = dict(cur.fetchone())
print("== OVERVIEW ==")
for k, v in row.items(): print(f"  {k}: {v}")

# ── 2025 distribution by npl_quality ─────────
cur.execute("""
SELECT
    npl_quality,
    COUNT(*) AS n,
    ROUND(AVG(npl_materials_pct)::numeric, 1) AS avg_npl_pct,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY npl_materials_pct)::numeric, 1) AS median_npl_pct,
    ROUND(SUM(revenue_vnd)/1e9, 2) AS rev_B,
    ROUND(SUM(allocated_npl_materials_vnd)/1e9, 2) AS npl_B,
    ROUND(SUM(allocated_npl_materials_vnd)/NULLIF(SUM(revenue_vnd),0)*100, 2) AS weighted_pct
FROM mart.fct_order_npl_cost
WHERE year = 2025
GROUP BY npl_quality
ORDER BY n DESC
""")
print("\n== 2025 Distribution by NPL Quality ==")
for r in cur.fetchall(): print(f"  {dict(r)}")

# ── 2025 Regular orders (normal + not sample) ─
cur.execute("""
WITH base AS (
    SELECT *
    FROM mart.fct_order_npl_cost
    WHERE year = 2025
      AND npl_quality = 'normal'
      AND is_sample_order = false
)
SELECT
    COUNT(*) AS n,
    ROUND(AVG(npl_materials_pct)::numeric, 2) AS avg_pct,
    ROUND(PERCENTILE_CONT(0.1) WITHIN GROUP(ORDER BY npl_materials_pct)::numeric, 1) AS p10,
    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP(ORDER BY npl_materials_pct)::numeric, 1) AS p25,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY npl_materials_pct)::numeric, 1) AS p50,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP(ORDER BY npl_materials_pct)::numeric, 1) AS p75,
    ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP(ORDER BY npl_materials_pct)::numeric, 1) AS p90,
    ROUND(SUM(allocated_npl_materials_vnd)/NULLIF(SUM(revenue_vnd),0)*100, 2) AS weighted_avg_pct,
    COUNT(*) FILTER(WHERE npl_materials_pct BETWEEN 20 AND 35) AS in_target_range
FROM base
""")
row = dict(cur.fetchone())
print("\n== 2025 Regular Orders (normal quality, not sample) ==")
for k, v in row.items(): print(f"  {k}: {v}")

# ── Monthly trend 2025 ─────────────────────────
cur.execute("""
SELECT
    month,
    COUNT(*) FILTER(WHERE npl_quality = 'normal') AS normal_orders,
    COUNT(*) FILTER(WHERE npl_quality != 'normal') AS other_orders,
    ROUND(SUM(CASE WHEN npl_quality='normal' THEN allocated_npl_materials_vnd ELSE 0 END)
          / NULLIF(SUM(CASE WHEN npl_quality='normal' THEN revenue_vnd ELSE 0 END), 0) * 100, 2) AS weighted_npl_pct_normal
FROM mart.fct_order_npl_cost
WHERE year = 2025
GROUP BY month ORDER BY month
""")
print("\n== Monthly Trend 2025 ==")
rows = cur.fetchall()
for r in rows: print(f"  {dict(r)}")

# ── Cross-validate against sample csv ─────────
SAMPLE_ORDERS = [46734, 47079, 47108, 47301, 47312, 47372, 47502, 47526, 47611]
expected_basic = {
    46734: 48.44,  # from sample_npl_order_2025.csv (WITHOUT zinc injection)
    47079: 5.75,
    47108: 20.37,
    47301: 17.88,
    47312: 1.72,
    47372: 7.39,
    47502: 24.24,
    47526: 13.83,
    47611: 19.19,
}
cur.execute(f"""
SELECT order_id, order_reference_no, revenue_vnd,
       allocated_npl_materials_vnd, allocated_npl_total_vnd,
       npl_materials_pct, npl_total_pct, npl_quality, is_sample_order,
       linked_plan_count
FROM mart.fct_order_npl_cost
WHERE order_id = ANY(ARRAY{SAMPLE_ORDERS})
ORDER BY order_id
""")
print(f"\n== Cross-validation (9 sample orders) ==")
print(f"NOTE: My model includes Zinc injection; sample CSV does not.")
print(f"{'Order':<8} {'Rev':>12} {'NPL_mat':>12} {'%mat_mine':>10} {'%exp_csv':>10} {'Quality':<12} {'Sample'}")
print("-"*80)
for r in cur.fetchall():
    rd = dict(r)
    exp = expected_basic.get(rd['order_id'], None)
    diff = f"{float(rd['npl_materials_pct'])-exp:+.1f}" if exp else "N/A"
    print(f"{rd['order_id']:<8} {float(rd['revenue_vnd']):>12,.0f} {float(rd['allocated_npl_materials_vnd']):>12,.0f} "
          f"{float(rd['npl_materials_pct']):>10.2f} {(exp or 0):>10.2f} ({diff}) {rd['npl_quality']:<12} {rd['is_sample_order']}")

# ── Top 15 suspect_data orders for review ─────
cur.execute("""
SELECT order_id, order_reference_no, revenue_vnd, allocated_npl_materials_vnd,
       npl_materials_pct, total_quantity, linked_plan_count, total_bom_items
FROM mart.fct_order_npl_cost
WHERE year = 2025 AND npl_quality = 'suspect_data'
ORDER BY allocated_npl_materials_vnd DESC LIMIT 15
""")
print("\n== Top 15 suspect_data orders (BOM data review needed) ==")
for r in cur.fetchall():
    rd = dict(r)
    print(f"  DDH={rd['order_reference_no']} rev={float(rd['revenue_vnd']):>12,.0f} "
          f"npl={float(rd['allocated_npl_materials_vnd']):>15,.0f} ({float(rd['npl_materials_pct']):.0f}%) "
          f"qty={float(rd['total_quantity']):>8,.0f} plans={rd['linked_plan_count']}")

# ── final summary stats ────────────────────────
cur.execute("""
SELECT
    COUNT(*) AS total_2025,
    COUNT(*) FILTER(WHERE npl_quality = 'normal' AND is_sample_order = false) AS regular_normal,
    COUNT(*) FILTER(WHERE is_sample_order = true) AS sample_orders,
    COUNT(*) FILTER(WHERE npl_quality = 'suspect_data') AS suspect_data_review,
    ROUND(SUM(revenue_vnd) FILTER(WHERE npl_quality='normal' AND NOT is_sample_order) / 1e9, 2) AS normal_rev_B,
    ROUND(SUM(allocated_npl_materials_vnd) FILTER(WHERE npl_quality='normal' AND NOT is_sample_order) / 1e9, 2) AS normal_npl_B,
    ROUND(SUM(allocated_npl_materials_vnd) FILTER(WHERE npl_quality='normal' AND NOT is_sample_order)
          / NULLIF(SUM(revenue_vnd) FILTER(WHERE npl_quality='normal' AND NOT is_sample_order), 0) * 100, 2) AS weighted_npl_pct,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY npl_materials_pct)::numeric
          FILTER(WHERE npl_quality='normal' AND NOT is_sample_order), 2) AS median_npl_pct
FROM mart.fct_order_npl_cost
WHERE year = 2025
""")
row = dict(cur.fetchone())
print("\n== FINAL SUMMARY 2025 ==")
for k, v in row.items(): print(f"  {k}: {v}")

conn.close()
print("\nDone.")
