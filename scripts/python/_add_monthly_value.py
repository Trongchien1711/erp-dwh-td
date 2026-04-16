"""
Add est_value_end_month and is_valued columns to fct_stock_monthly_snapshot.sql
Price sources:
  - product: weighted avg unit_price_capped from fct_stock_snapshot (lot_price -> PO fallback)
  - nvl: dim_material_mart.price_import
"""
import pathlib

SQL_FILE = pathlib.Path(r"d:\Data Warehouse\dbt_project\models\marts\inventory\fct_stock_monthly_snapshot.sql")

# Read and strip BOM
c = SQL_FILE.read_bytes().lstrip(b"\xef\xbb\xbf").decode("utf-8")

OLD = """materials as (
    select material_key, material_code, material_name, npl_type, unit_id
    from {{ ref("dim_material_mart") }}
),

warehouses as (
    select warehouse_key, warehouse_code, warehouse_name
    from {{ ref("stg_warehouses") }}
)

select
    p.month_end,
    p.item_type,
    p.product_key,
    p.material_key,
    p.warehouse_key,
    coalesce(pr.product_code, m.material_code) as item_code,
    coalesce(pr.product_name, m.material_name) as item_name,
    coalesce(pr.unit_id,      m.unit_id)       as unit_id,
    pr.type_products,
    m.npl_type,
    w.warehouse_code,
    w.warehouse_name,
    greatest(p.est_qty_end_month, 0)            as est_qty_end_month,
    p.inbound_this_month,
    p.outbound_this_month,
    p.inbound_this_month - p.outbound_this_month as net_movement

from projected p
left join products   pr using (product_key)
left join materials  m  using (material_key)
left join warehouses w  using (warehouse_key)

where p.est_qty_end_month > 0

order by p.month_end, p.item_type, p.warehouse_key, p.product_key, p.material_key"""

NEW = """materials as (
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
)

select
    p.month_end,
    p.item_type,
    p.product_key,
    p.material_key,
    p.warehouse_key,
    coalesce(pr.product_code, m.material_code) as item_code,
    coalesce(pr.product_name, m.material_name) as item_name,
    coalesce(pr.unit_id,      m.unit_id)       as unit_id,
    pr.type_products,
    m.npl_type,
    w.warehouse_code,
    w.warehouse_name,
    greatest(p.est_qty_end_month, 0)            as est_qty_end_month,
    p.inbound_this_month,
    p.outbound_this_month,
    p.inbound_this_month - p.outbound_this_month as net_movement,

    -- ── estimated stock value at month-end ─────────────────────────────────
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
left join product_prices pp on pp.product_key = p.product_key and p.item_type = 'product'

where p.est_qty_end_month > 0

order by p.month_end, p.item_type, p.warehouse_key, p.product_key, p.material_key"""

if OLD in c:
    c = c.replace(OLD, NEW)
    SQL_FILE.write_bytes(c.encode("utf-8"))
    print(f"OK. Lines: {c.count(chr(10))}, Bytes: {len(c.encode('utf-8'))}")
else:
    print("NOT FOUND — checking first 3 lines of pattern:")
    for i, line in enumerate(OLD.split("\n")[:3]):
        found = line in c
        print(f"  [{i}] found={found}: {repr(line[:80])}")
