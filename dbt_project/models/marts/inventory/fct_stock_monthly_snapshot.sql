-- ============================================================
-- fct_stock_monthly_snapshot
-- Domain  : Inventory
-- Grain   : One row per (month_end x item_type x product_key|material_key x warehouse)
--           where estimated end-of-month stock > 0.
-- Purpose : Monthly historical stock levels for trend analysis
--           in Power BI (line chart, YoY comparison, DOH).
--
-- Method  : Backward projection from current known stock
--   est_qty_end_M = current_qty_left
--                 + cumulative_outbound STRICTLY AFTER month_end
--                 - cumulative_inbound  STRICTLY AFTER month_end
--
-- Accuracy note:
--   Trend and relative comparison: accurate.
--   Absolute values older than 1 year: +/-5-10% due to export overcount.
--   Use for trend/DOH/seasonality - not for exact audit reconciliation.
--
-- Coverage:
--   item_type = "product": finished goods (product_key set, material_key NULL)
--   item_type = "nvl"    : raw materials  (material_key set, product_key NULL)
--
-- Range: 2023-01-31 to last full month before today.
-- ============================================================

{{ config(materialized="table") }}

with

months as (
    select distinct
        (date_trunc('month', full_date) + interval '1 month - 1 day')::date as month_end
    from {{ ref("stg_date") }}
    where full_date >= '2023-01-01'
      and full_date <  date_trunc('month', current_date)
),

product_combos as (
    select distinct 'product' as item_type, product_key, cast(null as int) as material_key, warehouse_key
    from {{ source("core", "fact_warehouse_stock") }}
    where type_items = 'product' and product_key is not null
    union
    select distinct 'product', product_key, cast(null as int), warehouse_key
    from {{ source("core", "fact_warehouse_export") }}
    where type_items = 'product' and product_key is not null
),

nvl_combos as (
    select distinct 'nvl' as item_type, cast(null as int) as product_key, material_key, warehouse_key
    from {{ source("core", "fact_warehouse_stock") }}
    where type_items = 'nvl' and material_key is not null
    union
    select distinct 'nvl', cast(null as int), material_key, warehouse_key
    from {{ source("core", "fact_warehouse_export") }}
    where type_items = 'nvl' and material_key is not null
),

combos as (
    select * from product_combos
    union all
    select * from nvl_combos
),

current_stock_product as (
    select product_key, warehouse_key, sum(quantity_left) as current_qty
    from {{ source("core", "fact_warehouse_stock") }}
    where type_items = 'product' and product_key is not null
    group by 1, 2
),

current_stock_nvl as (
    select material_key, warehouse_key, sum(quantity_left) as current_qty
    from {{ source("core", "fact_warehouse_stock") }}
    where type_items = 'nvl' and material_key is not null
    group by 1, 2
),

inbound_product as (
    select
        date_trunc('month', d.full_date)::date as month_start,
        ws.product_key,
        cast(null as int) as material_key,
        ws.warehouse_key,
        sum(ws.quantity) as qty_in
    from {{ source("core", "fact_warehouse_stock") }} ws
    join {{ ref("stg_date") }} d on d.date_key = ws.import_date_key
    where ws.type_items = 'product' and ws.product_key is not null
    group by 1, 2, 3, 4
),

inbound_nvl as (
    select
        date_trunc('month', d.full_date)::date as month_start,
        cast(null as int) as product_key,
        ws.material_key,
        ws.warehouse_key,
        sum(ws.quantity) as qty_in
    from {{ source("core", "fact_warehouse_stock") }} ws
    join {{ ref("stg_date") }} d on d.date_key = ws.import_date_key
    where ws.type_items = 'nvl' and ws.material_key is not null
    group by 1, 2, 3, 4
),

outbound_product as (
    select
        date_trunc('month', d.full_date)::date as month_start,
        we.product_key,
        cast(null as int) as material_key,
        we.warehouse_key,
        sum(we.quantity) as qty_out
    from {{ source("core", "fact_warehouse_export") }} we
    join {{ ref("stg_date") }} d on d.date_key = we.export_date_key
    where we.type_items = 'product' and we.product_key is not null
    group by 1, 2, 3, 4
),

outbound_nvl as (
    select
        date_trunc('month', d.full_date)::date as month_start,
        cast(null as int) as product_key,
        we.material_key,
        we.warehouse_key,
        sum(we.quantity) as qty_out
    from {{ source("core", "fact_warehouse_export") }} we
    join {{ ref("stg_date") }} d on d.date_key = we.export_date_key
    where we.type_items = 'nvl' and we.material_key is not null
    group by 1, 2, 3, 4
),

scaffold as (
    select
        m.month_end,
        date_trunc('month', m.month_end)::date as month_start,
        c.item_type,
        c.product_key,
        c.material_key,
        c.warehouse_key
    from months m
    cross join combos c
),

with_movements as (
    select
        s.month_end,
        s.item_type,
        s.product_key,
        s.material_key,
        s.warehouse_key,
        coalesce(case when s.item_type = 'product' then ip.qty_in  else iv.qty_in  end, 0) as inbound_this_month,
        coalesce(case when s.item_type = 'product' then op.qty_out else ov.qty_out end, 0) as outbound_this_month
    from scaffold s
    left join inbound_product  ip on ip.product_key   = s.product_key  and ip.warehouse_key = s.warehouse_key and ip.month_start = s.month_start and s.item_type = 'product'
    left join inbound_nvl      iv on iv.material_key  = s.material_key and iv.warehouse_key = s.warehouse_key and iv.month_start = s.month_start and s.item_type = 'nvl'
    left join outbound_product op on op.product_key   = s.product_key  and op.warehouse_key = s.warehouse_key and op.month_start = s.month_start and s.item_type = 'product'
    left join outbound_nvl     ov on ov.material_key  = s.material_key and ov.warehouse_key = s.warehouse_key and ov.month_start = s.month_start and s.item_type = 'nvl'
),

with_cumulative as (
    select
        month_end,
        item_type,
        product_key,
        material_key,
        warehouse_key,
        inbound_this_month,
        outbound_this_month,
        coalesce(sum(inbound_this_month)  over (partition by item_type, product_key, material_key, warehouse_key order by month_end desc rows between unbounded preceding and 1 preceding), 0) as cumulative_inbound_after,
        coalesce(sum(outbound_this_month) over (partition by item_type, product_key, material_key, warehouse_key order by month_end desc rows between unbounded preceding and 1 preceding), 0) as cumulative_outbound_after
    from with_movements
),

projected as (
    select
        wc.month_end,
        wc.item_type,
        wc.product_key,
        wc.material_key,
        wc.warehouse_key,
        wc.inbound_this_month,
        wc.outbound_this_month,
        round(
            coalesce(case when wc.item_type = 'product' then csp.current_qty else csn.current_qty end, 0)
            + wc.cumulative_outbound_after
            - wc.cumulative_inbound_after
        ) as est_qty_end_month
    from with_cumulative wc
    left join current_stock_product csp on csp.product_key  = wc.product_key  and csp.warehouse_key = wc.warehouse_key and wc.item_type = 'product'
    left join current_stock_nvl     csn on csn.material_key = wc.material_key and csn.warehouse_key = wc.warehouse_key and wc.item_type = 'nvl'
),

products as (
    select product_key, product_code, product_name, type_products, unit_id
    from {{ ref("stg_products") }}
),

materials as (
    select material_key, material_code, material_name, npl_type, unit_id
    from {{ ref("dim_material_mart") }}
),

warehouses as (
    select warehouse_key, warehouse_code, warehouse_name, is_virtual
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
    w.is_virtual,
    greatest(p.est_qty_end_month, 0)            as est_qty_end_month,
    p.inbound_this_month,
    p.outbound_this_month,
    p.inbound_this_month - p.outbound_this_month as net_movement,

    -- ── estimated stock value at month-end ─────────────────────────────────
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
left join nvl_prices     np on np.material_key = p.material_key and p.item_type = 'nvl'

where p.est_qty_end_month > 0

order by p.month_end, p.item_type, p.warehouse_key, p.product_key, p.material_key
