-- ============================================================
-- fct_order_npl_cost
-- Domain  : Inventory / Sales / Finance
-- Grain   : 1 row per order_id (customer order)
-- Purpose : Allocate production plan NPL cost to each customer order.
--
-- Allocation logic (matches BC_SP formula: fixed_cost x order_quantity):
--   A production plan combines multiple orders. NPL is split proportionally
--   by ORDER QUANTITY for the specific finished product:
--     alloc_ratio = order_product_qty / SUM(order_product_qty for all orders in plan)
--     allocated_npl = plan_total_npl * alloc_ratio
--   Fallback (when qty not available): split by revenue, then equal split.
--
--   This matches BC_SP logic exactly:
--     variable material cost = (1/N) x conversion_value x price x order_qty
--     Kem (fixed per lenh)   = proportional to order qty (fair split)
--
--   Multiple plans can contribute NPL to the same order.
--   Revenue (for NPL%) is always order-level grand_total (not per-product).
--
-- Revenue conversion:
--   currencies = 1 (USD)  -> grand_total * 26202
--   currencies = 4 (CZK)  -> grand_total * 27
--   currencies = 5 (VND)  -> grand_total (no conversion)
--
-- Data quality flag (npl_quality):
--   'normal'       : allocated_npl_pct <= 150%   (likely correct)
--   'high_cost'    : 150% < pct <= 500%           (specialty product / small order)
--   'suspect_data' : pct > 500%                   (likely BOM data entry error)
--
-- NOTE: fact_order_items.active is always 0 in this ERP (field unused).
--       Do NOT filter active = 1.
-- ============================================================

with

-- -- Step 1: Aggregate NPL per (plan, finished_product) --------
npl_by_plan as (

    select
        productions_plan_id,
        source_order_ids,
        finished_product_id,
        count(distinct productions_plan_items_id)   as bom_item_count,
        sum(npl_cost_vnd)                           as plan_npl_materials_vnd,
        sum(waste_cost_per_order_vnd)               as plan_npl_waste_vnd,
        sum(npl_cost_with_waste_vnd)                as plan_npl_total_vnd,
        count(*) filter (where has_price = false)   as items_without_price

    from {{ ref('fct_production_npl_cost') }}
    where source_order_ids is not null
      and source_order_ids <> ''
    group by
        productions_plan_id,
        source_order_ids,
        finished_product_id

),

-- -- Step 2: One row per (plan, product, linked_order_id) ------
plan_order_pairs as (

    select
        p.productions_plan_id,
        p.source_order_ids,
        p.finished_product_id,
        p.bom_item_count,
        p.plan_npl_materials_vnd,
        p.plan_npl_waste_vnd,
        p.plan_npl_total_vnd,
        p.items_without_price,
        cast(trim(oid.s) as bigint)                 as order_id

    from npl_by_plan p,
         lateral unnest(string_to_array(p.source_order_ids, ',')) as oid(s)

),

-- -- Step 3: Order-level revenue + product-level quantity ------
-- revenue_vnd : total order revenue (used for NPL% denominator)
-- product_qty : quantity of the finished product in this order
--               (matched by product_id to identify the right order item)
-- NOTE: fact_order_items.active is always 0 - never filter active = 1
order_detail as (

    select
        fo.order_id,
        dp.product_id                               as finished_product_id,
        sum(oi.quantity)                            as product_qty,
        case
            when fo.currencies = 1 then fo.grand_total * 26202.0
            when fo.currencies = 4 then fo.grand_total * 27.0
            else fo.grand_total
        end                                         as revenue_vnd,
        fo.total_quantity,
        fo.status,
        fo.status_payment,
        fo.is_end,
        fo.currencies,
        fo.grand_total

    from {{ source('core', 'fact_orders') }} fo
    join {{ source('core', 'fact_order_items') }} oi
        on oi.order_id = fo.order_id
    join {{ source('core', 'dim_product') }} dp
        on dp.product_key = oi.product_key
    where fo.is_cancel = 0
    group by
        fo.order_id,
        dp.product_id,
        fo.grand_total,
        fo.currencies,
        fo.total_quantity,
        fo.status,
        fo.status_payment,
        fo.is_end

),

-- -- Step 4: Join plan-order with order details ----------------
-- Match on both order_id AND finished_product_id to get per-product qty
joined as (

    select
        pop.*,
        od.product_qty,
        od.revenue_vnd,
        od.total_quantity,
        od.status,
        od.status_payment,
        od.is_end,
        od.currencies,
        od.grand_total

    from plan_order_pairs pop
    left join order_detail od
        on  od.order_id            = pop.order_id
        and od.finished_product_id = pop.finished_product_id

),

-- -- Step 5: Plan-product totals (qty + revenue) ---------------
plan_product_totals as (

    select
        productions_plan_id,
        finished_product_id,
        sum(product_qty)            as plan_total_qty,
        sum(revenue_vnd)            as plan_total_rev_vnd,
        count(*)                    as plan_order_count,
        count(product_qty)          as plan_orders_with_qty

    from joined
    group by productions_plan_id, finished_product_id

),

-- -- Step 6: Compute allocation ratio per (plan, product, order) -
-- Primary  : quantity-based (order_product_qty / plan_total_qty)
-- Fallback : revenue-based  (order_revenue / plan_total_revenue)
-- Last     : equal split
allocated as (

    select
        j.order_id,
        j.productions_plan_id,
        j.finished_product_id,
        j.source_order_ids                          as plan_source_order_ids,
        j.bom_item_count,
        j.items_without_price,
        pt.plan_order_count,
        pt.plan_total_qty,
        pt.plan_total_rev_vnd,
        case
            when pt.plan_total_qty > 0
            then j.product_qty / pt.plan_total_qty
            when pt.plan_total_rev_vnd > 0
            then j.revenue_vnd / pt.plan_total_rev_vnd
            else 1.0 / nullif(pt.plan_order_count, 0)
        end                                         as alloc_ratio,
        j.plan_npl_materials_vnd,
        j.plan_npl_waste_vnd,
        j.plan_npl_total_vnd,
        j.product_qty,
        j.revenue_vnd,
        j.total_quantity,
        j.status,
        j.status_payment,
        j.is_end,
        j.currencies,
        j.grand_total

    from joined j
    join plan_product_totals pt
        on  pt.productions_plan_id = j.productions_plan_id
        and pt.finished_product_id = j.finished_product_id

),

-- -- Step 7: Aggregate to order level --------------------------
-- Revenue: MAX (same for all plan rows of this order, not additive)
-- NPL: SUM across all (plan, product) combinations for this order
order_npl as (

    select
        order_id,
        -- NPL cost aggregated across ALL plans serving this order
        round(sum(alloc_ratio * plan_npl_materials_vnd))    as allocated_npl_materials_vnd,
        round(sum(alloc_ratio * plan_npl_waste_vnd))        as allocated_npl_waste_vnd,
        round(sum(alloc_ratio * plan_npl_total_vnd))        as allocated_npl_total_vnd,
        count(distinct productions_plan_id)                 as linked_plan_count,
        -- Revenue from fact_orders (MAX - same for all plan rows)
        max(revenue_vnd)                                    as revenue_vnd,
        max(total_quantity)                                 as total_quantity,
        max(status)                                         as status,
        max(status_payment)                                 as status_payment,
        max(is_end)                                         as is_end,
        max(currencies)                                     as currencies,
        max(grand_total)                                    as grand_total,
        -- Plan count with/without price coverage
        sum(bom_item_count)                                 as total_bom_items,
        sum(items_without_price)                            as total_items_without_price

    from allocated
    group by order_id

),

-- -- Step 8: Enrich with order metadata ------------------------
enriched as (

    select
        on2.order_id,
        fo.reference_no                             as order_reference_no,
        fo.date_created::date                       as order_date,
        extract(year  from fo.date_created)::int    as year,
        extract(month from fo.date_created)::int    as month,
        extract(quarter from fo.date_created)::int  as quarter,
        on2.currencies,
        on2.grand_total                             as grand_total_original,
        on2.revenue_vnd,
        on2.total_quantity,
        on2.status,
        on2.is_end,
        on2.linked_plan_count,
        on2.total_bom_items,
        on2.total_items_without_price,
        on2.allocated_npl_materials_vnd,
        on2.allocated_npl_waste_vnd,
        on2.allocated_npl_total_vnd,
        -- NPL percentages
        case
            when on2.revenue_vnd > 0
            then round(on2.allocated_npl_materials_vnd / on2.revenue_vnd * 100, 2)
        end                                         as npl_materials_pct,
        case
            when on2.revenue_vnd > 0
            then round(on2.allocated_npl_total_vnd / on2.revenue_vnd * 100, 2)
        end                                         as npl_total_pct,
        -- Data quality flag
        case
            when on2.revenue_vnd <= 0 or on2.revenue_vnd is null     then 'no_revenue'
            when on2.allocated_npl_materials_vnd / on2.revenue_vnd > 5.0 then 'suspect_data'
            when on2.allocated_npl_materials_vnd / on2.revenue_vnd > 1.5 then 'high_cost'
            else 'normal'
        end                                         as npl_quality,
        -- Sample order flag: small qty or low revenue
        case
            when on2.total_quantity < 500
              or on2.revenue_vnd < 2000000            then true
            else false
        end                                         as is_sample_order

    from order_npl on2
    join {{ source('core', 'fact_orders') }} fo on fo.order_id = on2.order_id

)

select * from enriched
