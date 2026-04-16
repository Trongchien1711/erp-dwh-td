"""
Fix product_prices and nvl_prices CTEs to use stock_value IS NOT NULL
(covers both lot_price and PO fallback, not just is_valued=true which is lot_price only)
"""
import pathlib

SQL_FILE = pathlib.Path(r"d:\Data Warehouse\dbt_project\models\marts\inventory\fct_stock_monthly_snapshot.sql")
c = SQL_FILE.read_bytes().lstrip(b"\xef\xbb\xbf").decode("utf-8")

OLD = """materials as (
    select material_key, material_code, material_name, npl_type, unit_id,
           price_import as nvl_unit_price
    from {{ ref("dim_material_mart") }}
),

warehouses as (
    select warehouse_key, warehouse_code, warehouse_name
    from {{ ref("stg_warehouses") }}
),

-- Weighted avg unit price per product from current snapshot (lot_price priority -> PO fallback)
product_prices as (
    select
        product_key,
        round(sum(stock_value) / nullif(sum(quantity_left), 0), 2) as avg_unit_price
    from {{ ref("fct_stock_snapshot") }}
    where item_type = 'product'
      and quantity_left > 0
      and is_valued = true
    group by product_key
)"""

NEW = """materials as (
    select material_key, material_code, material_name, npl_type, unit_id
    from {{ ref("dim_material_mart") }}
),

warehouses as (
    select warehouse_key, warehouse_code, warehouse_name
    from {{ ref("stg_warehouses") }}
),

-- Weighted avg unit price from current snapshot.
-- Use stock_value IS NOT NULL (covers lot_price + PO fallback, not just is_valued lots).
product_prices as (
    select
        product_key,
        round(sum(stock_value) / nullif(sum(quantity_left), 0), 2) as avg_unit_price
    from {{ ref("fct_stock_snapshot") }}
    where item_type = 'product'
      and quantity_left > 0
      and stock_value is not null
    group by product_key
),

nvl_prices as (
    select
        material_key,
        round(sum(stock_value) / nullif(sum(quantity_left), 0), 2) as avg_unit_price
    from {{ ref("fct_stock_snapshot") }}
    where item_type = 'nvl'
      and quantity_left > 0
      and stock_value is not null
    group by material_key
)"""

OLD_VALUE = """    -- ── estimated stock value at month-end ─────────────────────────────────
    -- product: qty * weighted avg unit_price from current snapshot
    -- nvl:     qty * dim_material_mart.price_import (ERP standard catalog price)
    case
        when p.item_type = 'product' and pp.avg_unit_price is not null
            then round(greatest(p.est_qty_end_month, 0) * pp.avg_unit_price, 0)
        when p.item_type = 'nvl' and m.nvl_unit_price > 0
            then round(greatest(p.est_qty_end_month, 0) * m.nvl_unit_price, 0)
        else null
    end                                         as est_value_end_month,

    -- is_valued = true when a unit price is available for this item
    case
        when p.item_type = 'product' and pp.avg_unit_price is not null then true
        when p.item_type = 'nvl'     and m.nvl_unit_price  > 0         then true
        else false
    end                                         as is_valued

from projected p
left join products      pr on pr.product_key  = p.product_key
left join materials     m  on m.material_key  = p.material_key
left join warehouses    w  using (warehouse_key)
left join product_prices pp on pp.product_key = p.product_key and p.item_type = 'product'"""

NEW_VALUE = """    -- ── estimated stock value at month-end ─────────────────────────────────
    -- Uses weighted avg unit_price from current fct_stock_snapshot per item.
    -- Price priority: lot_price > PO fallback (npl_po for NVL, po for products).
    case
        when p.item_type = 'product' and pp.avg_unit_price is not null
            then round(greatest(p.est_qty_end_month, 0) * pp.avg_unit_price, 0)
        when p.item_type = 'nvl' and np.avg_unit_price is not null
            then round(greatest(p.est_qty_end_month, 0) * np.avg_unit_price, 0)
        else null
    end                                         as est_value_end_month,

    -- is_valued = true when a unit price is available for this item
    case
        when p.item_type = 'product' and pp.avg_unit_price is not null then true
        when p.item_type = 'nvl'     and np.avg_unit_price is not null then true
        else false
    end                                         as is_valued

from projected p
left join products      pr on pr.product_key  = p.product_key
left join materials     m  on m.material_key  = p.material_key
left join warehouses    w  using (warehouse_key)
left join product_prices pp on pp.product_key  = p.product_key  and p.item_type = 'product'
left join nvl_prices     np on np.material_key = p.material_key and p.item_type = 'nvl'"""

changes = 0
if OLD in c:
    c = c.replace(OLD, NEW)
    changes += 1
else:
    print("ERROR: CTEs block not found")

if OLD_VALUE in c:
    c = c.replace(OLD_VALUE, NEW_VALUE)
    changes += 1
else:
    print("ERROR: VALUE block not found")

if changes == 2:
    SQL_FILE.write_bytes(c.encode("utf-8"))
    print(f"OK. {changes} replacements. Lines: {c.count(chr(10))}")
else:
    print(f"Only {changes}/2 replacements made — NOT saved")
