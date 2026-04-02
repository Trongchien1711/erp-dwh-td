"""
EDA script — khám phá toàn bộ dữ liệu trong mart schema
"""
import psycopg2
import psycopg2.extras
from decimal import Decimal

PG = dict(host='localhost', port=5432, user='dwh_admin', password='881686', dbname='erp_dwh')

def run(sql, conn):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(sql)
    return cur.fetchall()

def fmt(v):
    if v is None: return 'NULL'
    if isinstance(v, Decimal): return f"{v:,.0f}"
    if isinstance(v, float): return f"{v:,.2f}"
    return str(v)

def section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print('='*60)

def print_rows(rows, indent=2):
    if not rows:
        print(' '*indent + '(no data)')
        return
    pad = ' ' * indent
    for row in rows:
        parts = [f"{k}: {fmt(v)}" for k, v in row.items()]
        print(pad + ' | '.join(parts))

conn = psycopg2.connect(**PG)

# ── 1. Table overview ─────────────────────────────────────────
section("1. MART TABLES — ROW COUNTS & SIZE")
rows = run("""
    SELECT table_name,
           pg_size_pretty(pg_total_relation_size('mart.'||table_name)) AS size
    FROM information_schema.tables
    WHERE table_schema = 'mart'
    ORDER BY table_name
""", conn)
print_rows(rows)

# ── 2. fct_revenue — Sales KPIs overview ─────────────────────
section("2. fct_revenue — SALES OVERVIEW")
rows = run("""
    SELECT
        count(*)                        AS rows,
        count(DISTINCT customer_key)    AS unique_customers,
        count(DISTINCT order_date_key)  AS unique_dates,
        min(order_date)                 AS date_from,
        max(order_date)                 AS date_to,
        round(sum(revenue))             AS total_revenue,
        round(sum(cogs))                AS total_cogs,
        round(sum(gross_profit))        AS total_gross_profit,
        round(avg(gross_margin_pct),2)  AS avg_gp_margin_pct,
        round(sum(outstanding_ar))      AS total_outstanding_ar
    FROM mart.fct_revenue
""", conn)
print_rows(rows)

section("2b. fct_revenue — Revenue by YEAR")
rows = run("""
    SELECT year,
           count(DISTINCT customer_key)  AS customers,
           sum(order_count)              AS orders,
           round(sum(revenue))           AS revenue,
           round(sum(gross_profit))      AS gross_profit,
           round(avg(gross_margin_pct),2) AS avg_gp_pct
    FROM mart.fct_revenue
    GROUP BY year ORDER BY year
""", conn)
print_rows(rows)

section("2c. fct_revenue — Top 10 Customers by Revenue")
rows = run("""
    SELECT customer_code, customer_name,
           round(sum(revenue))       AS revenue,
           round(sum(gross_profit))  AS gross_profit,
           sum(order_count)          AS orders
    FROM mart.fct_revenue
    GROUP BY customer_code, customer_name
    ORDER BY sum(revenue) DESC LIMIT 10
""", conn)
print_rows(rows)

# ── 3. fct_order_performance ──────────────────────────────────
section("3. fct_order_performance — FULFILMENT OVERVIEW")
rows = run("""
    SELECT
        count(*)                            AS total_orders,
        sum(is_cancel)                      AS cancelled,
        sum(is_fully_delivered)             AS fully_delivered,
        sum(is_paid)                        AS fully_paid,
        sum(is_completed)                   AS completed,
        round(avg(fulfilment_rate_pct),2)   AS avg_fulfilment_pct,
        round(sum(outstanding_ar))          AS total_outstanding_ar
    FROM mart.fct_order_performance
""", conn)
print_rows(rows)

section("3b. fct_order_performance — Orders by status_payment")
rows = run("""
    SELECT status_payment,
           count(*)              AS orders,
           round(sum(revenue))   AS revenue,
           round(sum(outstanding_ar)) AS outstanding_ar
    FROM mart.fct_order_performance
    GROUP BY status_payment ORDER BY status_payment
""", conn)
print_rows(rows)

# ── 4. dim_customer_segmentation ─────────────────────────────
section("4. dim_customer_segmentation — RFM SEGMENTS")
rows = run("""
    SELECT rfm_segment,
           count(*)                        AS customers,
           round(sum(lifetime_revenue))    AS total_revenue,
           round(avg(lifetime_revenue))    AS avg_revenue,
           round(avg(total_orders),1)      AS avg_orders,
           round(avg(days_since_last_order),0) AS avg_days_since_order
    FROM mart.dim_customer_segmentation
    WHERE lifetime_revenue IS NOT NULL
    GROUP BY rfm_segment
    ORDER BY sum(lifetime_revenue) DESC
""", conn)
print_rows(rows)

# ── 5. fct_stock_snapshot ─────────────────────────────────────
section("5. fct_stock_snapshot — INVENTORY OVERVIEW")
rows = run("""
    SELECT
        count(*)                        AS lot_count,
        count(DISTINCT product_key)     AS unique_products,
        count(DISTINCT warehouse_key)   AS unique_warehouses,
        round(sum(quantity_left))       AS total_qty_in_stock,
        round(sum(stock_value))         AS total_stock_value,
        count(*) FILTER (WHERE quantity_left = 0) AS empty_lots
    FROM mart.fct_stock_snapshot
""", conn)
print_rows(rows)

section("5b. fct_stock_snapshot — Top 10 Products by Stock Value")
rows = run("""
    SELECT product_code, product_name,
           round(sum(quantity_left))   AS qty_left,
           round(sum(stock_value))     AS stock_value
    FROM mart.fct_stock_snapshot
    WHERE quantity_left > 0
    GROUP BY product_code, product_name
    ORDER BY sum(stock_value) DESC LIMIT 10
""", conn)
print_rows(rows)

section("5c. fct_stock_snapshot — Stock by Warehouse")
rows = run("""
    SELECT warehouse_name,
           count(DISTINCT product_key)     AS products,
           round(sum(quantity_left))       AS qty_left,
           round(sum(stock_value))         AS stock_value
    FROM mart.fct_stock_snapshot
    WHERE quantity_left > 0
    GROUP BY warehouse_name ORDER BY sum(stock_value) DESC LIMIT 10
""", conn)
print_rows(rows)

# ── 6. fct_inbound_outbound ───────────────────────────────────
section("6. fct_inbound_outbound — STOCK MOVEMENT OVERVIEW")
rows = run("""
    SELECT movement_type,
           count(*)                        AS rows,
           count(DISTINCT product_key)     AS unique_products,
           round(sum(quantity_in))         AS total_qty_in,
           round(sum(quantity_out))        AS total_qty_out,
           round(sum(value_in))            AS total_value_in,
           round(sum(value_out))           AS total_value_out,
           min(movement_date)              AS date_from,
           max(movement_date)              AS date_to
    FROM mart.fct_inbound_outbound
    GROUP BY movement_type
""", conn)
print_rows(rows)

section("6b. fct_inbound_outbound — Net Movement by Year")
rows = run("""
    SELECT year, movement_type,
           round(sum(quantity_in))   AS qty_in,
           round(sum(quantity_out))  AS qty_out,
           round(sum(net_movement))  AS net_movement
    FROM mart.fct_inbound_outbound
    WHERE year IS NOT NULL
    GROUP BY year, movement_type
    ORDER BY year, movement_type
""", conn)
print_rows(rows)

# ── 7. fct_production_efficiency ─────────────────────────────
section("7. fct_production_efficiency — PRODUCTION OVERVIEW")
rows = run("""
    SELECT
        count(DISTINCT productions_orders_id)   AS orders,
        count(DISTINCT product_key)             AS unique_products,
        round(sum(qty_planned))                 AS total_planned,
        round(sum(qty_produced))                AS total_produced,
        round(avg(efficiency_pct),2)            AS avg_efficiency_pct,
        round(sum(total_hours))                 AS total_hours_logged,
        round(avg(output_per_hour),4)           AS avg_output_per_hour
    FROM mart.fct_production_efficiency
    WHERE qty_planned > 0
""", conn)
print_rows(rows)

# ── 8. fct_purchase_cost ──────────────────────────────────────
section("8. fct_purchase_cost — PROCUREMENT OVERVIEW")
rows = run("""
    SELECT
        count(DISTINCT supplier_key)        AS unique_suppliers,
        count(DISTINCT product_key)         AS unique_products,
        sum(po_count)                       AS total_pos,
        round(sum(qty_ordered))             AS total_qty_ordered,
        round(sum(actual_cost))             AS total_actual_cost,
        round(sum(price_variance))          AS total_price_variance,
        min(po_date)                        AS date_from,
        max(po_date)                        AS date_to
    FROM mart.fct_purchase_cost
""", conn)
print_rows(rows)

section("8b. fct_purchase_cost — Top 10 Suppliers by Spend")
rows = run("""
    SELECT supplier_name,
           sum(po_count)              AS pos,
           round(sum(actual_cost))    AS actual_cost,
           round(sum(price_variance)) AS price_variance
    FROM mart.fct_purchase_cost
    GROUP BY supplier_name
    ORDER BY sum(actual_cost) DESC LIMIT 10
""", conn)
print_rows(rows)

# ── 9. fct_gross_profit ───────────────────────────────────────
section("9. fct_gross_profit — P&L OVERVIEW")
rows = run("""
    SELECT year,
           round(sum(revenue))       AS revenue,
           round(sum(cogs))          AS cogs,
           round(sum(gross_profit))  AS gross_profit,
           round(avg(gp_margin_pct),2) AS avg_gp_pct,
           round(sum(total_vat))     AS total_vat,
           round(sum(outstanding_ar)) AS outstanding_ar
    FROM mart.fct_gross_profit
    GROUP BY year ORDER BY year
""", conn)
print_rows(rows)

# ── 10. dim_customer_credit ───────────────────────────────────
section("10. dim_customer_credit — CREDIT STATUS")
rows = run("""
    SELECT credit_status,
           count(*)                        AS customers,
           round(sum(outstanding_ar))      AS outstanding_ar,
           round(sum(debt_limit))          AS total_credit_limit,
           round(avg(credit_utilisation_pct),2) AS avg_utilisation_pct
    FROM mart.dim_customer_credit
    GROUP BY credit_status ORDER BY count(*) DESC
""", conn)
print_rows(rows)

section("10b. dim_customer_credit — Top 10 Customers by Outstanding AR")
rows = run("""
    SELECT customer_code, customer_name,
           round(outstanding_ar)           AS outstanding_ar,
           round(debt_limit)               AS debt_limit,
           round(credit_utilisation_pct,1) AS utilisation_pct,
           credit_status
    FROM mart.dim_customer_credit
    WHERE outstanding_ar > 0
    ORDER BY outstanding_ar DESC LIMIT 10
""", conn)
print_rows(rows)

conn.close()
print("\n✅ EDA hoàn thành.")
