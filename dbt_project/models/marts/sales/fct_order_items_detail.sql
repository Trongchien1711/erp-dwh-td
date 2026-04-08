-- ============================================================
-- fct_order_items_detail
-- Domain  : Sales
-- Grain   : One row per order line item (product on an order)
-- Purpose : Product-level sales detail for SKU analysis,
--           category performance, and individual order drill-down.
--
-- Key metrics per line:
--   quantity        — units sold
--   total_amount    — net line revenue after discounts + tax
--   cost            — COGS for this line
--   profit          — line-level gross profit
-- ============================================================

with order_items as (

    select * from {{ ref('stg_order_items') }}

),

orders as (

    -- pull only the dimensional context we need from int_orders_enriched
    select
        order_id,
        order_date_key,
        order_date,
        year,
        quarter,
        month_num,
        month_name,
        customer_key,
        customer_code,
        customer_name,
        price_group_name,
        city,
        id_branch,
        is_cancel

    from {{ ref('int_orders_enriched') }}

),

products as (

    select
        product_key,
        product_code,
        product_name,
        type_products,
        category_id,
        brand,
        price_import,
        price_sell

    from {{ ref('stg_products') }}

),

monthly_price as (

    -- Weighted monthly unit price by (customer, product).
    select
        date_trunc('month', o.order_date)::date                 as month_start,
        oi.customer_key,
        oi.product_key,
        sum(oi.total_amount)                                    as month_revenue,
        sum(oi.quantity)                                        as month_qty,
        sum(oi.total_amount) / nullif(sum(oi.quantity), 0)      as month_unit_price

    from order_items oi
    inner join orders o
        on oi.order_id = o.order_id
        and o.is_cancel = 0
    where oi.quantity > 0
      and oi.total_amount > 0
    group by 1, 2, 3

),

monthly_price_lag as (

    select
        month_start,
        customer_key,
        product_key,
        month_unit_price,
        lag(month_unit_price) over (
            partition by customer_key, product_key
            order by month_start
        )                                                       as prev_month_unit_price

    from monthly_price

),

price_thresholds as (

    -- Compute global percentile thresholds (p33 / p67) from adjusted unit price.
    -- Rows with price = 0 or null are excluded so they don't drag thresholds down.
    -- Used downstream to tag every line as Low / Mid / High.
    select
        percentile_cont(0.33) within group (
            order by total_amount / nullif(quantity, 0)
        ) as p33,
        percentile_cont(0.67) within group (
            order by total_amount / nullif(quantity, 0)
        ) as p67
    from order_items oi
    inner join orders o
        on oi.order_id = o.order_id
        and o.is_cancel = 0
    where oi.quantity  > 0
      and oi.total_amount > 0

),

item_fix as (

    select
        oi.order_item_key,
        oi.order_item_id,
        oi.order_id,
        date_trunc('month', o.order_date)::date                 as month_start,
        oi.customer_key,
        oi.product_key,
        oi.quantity,
        oi.total_amount,
        oi.total_amount / nullif(oi.quantity, 0)                as line_unit_price,
        mpl.prev_month_unit_price,

        case
            when oi.quantity > 0
             and oi.total_amount > 0
             and mpl.prev_month_unit_price > 0
             and (oi.total_amount / nullif(oi.quantity, 0)) > mpl.prev_month_unit_price * 3
                then true
            else false
        end                                                     as is_price_spike,

        case
            when oi.quantity > 0
             and oi.total_amount > 0
             and mpl.prev_month_unit_price > 0
                then (oi.total_amount / nullif(oi.quantity, 0)) / mpl.prev_month_unit_price
            else null
        end                                                     as spike_multiplier,

        case
            when oi.quantity > 0
             and oi.total_amount > 0
             and mpl.prev_month_unit_price > 0
             and (oi.total_amount / nullif(oi.quantity, 0)) > mpl.prev_month_unit_price * 3
                then mpl.prev_month_unit_price
            else oi.total_amount / nullif(oi.quantity, 0)
        end                                                     as unit_price_adjusted,

        round(
            oi.quantity * case
                when oi.quantity > 0
                 and oi.total_amount > 0
                 and mpl.prev_month_unit_price > 0
                 and (oi.total_amount / nullif(oi.quantity, 0)) > mpl.prev_month_unit_price * 3
                    then mpl.prev_month_unit_price
                else oi.total_amount / nullif(oi.quantity, 0)
            end,
            2
        )                                                       as total_amount_adjusted

    from order_items oi
    inner join orders o
        on oi.order_id = o.order_id
        and o.is_cancel = 0
    left join monthly_price_lag mpl
        on mpl.month_start = date_trunc('month', o.order_date)::date
       and mpl.customer_key = oi.customer_key
       and mpl.product_key = oi.product_key

),

final as (

    select
        -- ── line identifiers ───────────────────────────────────
        oi.order_item_key,
        oi.order_item_id,
        oi.order_id,

        -- ── date ───────────────────────────────────────────────
        o.order_date_key,
        o.order_date,
        o.year,
        o.quarter,
        o.month_num,
        o.month_name,

        -- ── customer ───────────────────────────────────────────
        oi.customer_key,
        o.customer_code,
        o.customer_name,
        o.price_group_name,
        o.city,

        -- ── product ────────────────────────────────────────────
        oi.product_key,
        p.product_code,
        p.product_name,
        p.type_products,
        p.brand,

        -- ── branch ─────────────────────────────────────────────
        o.id_branch,

        -- ── unit info ──────────────────────────────────────────
        oi.unit_id,
        oi.type_item,
        oi.type_gift,

        -- ── measures ───────────────────────────────────────────
        oi.quantity,
        oi.price                                                     as unit_price,
        fx.line_unit_price,
        fx.prev_month_unit_price,
        fx.is_price_spike,
        round(fx.spike_multiplier, 2)                               as spike_multiplier,
        round(fx.unit_price_adjusted, 2)                            as unit_price_adjusted,
        oi.amount,
        oi.tax_amount_item,
        oi.discount_percent_item,
        oi.discount_percent_amount_item,
        oi.discount_direct_amount_item,
        oi.total_amount,                    -- net line revenue
        fx.total_amount_adjusted,
        round(oi.total_amount - fx.total_amount_adjusted, 2)        as spike_amount_removed,
        oi.cost,                            -- line COGS
        oi.profit,                          -- line gross profit

        -- ── fulfilment ─────────────────────────────────────────
        oi.quantity_delivery,
        oi.quantity_not_delivery,
        oi.quantity_returned,

        -- ── price tier ─────────────────────────────────────────
        -- Phân tầng giá dựa trên unit_price_adjusted (đã loại spike).
        -- Ngưỡng p33 / p67 tính trên toàn bộ dữ liệu (không hardcode).
        -- NULL khi price = 0 hoặc quantity = 0 (line không tính được giá).
        case
            when fx.unit_price_adjusted is null
              or fx.unit_price_adjusted = 0     then null
            when fx.unit_price_adjusted <= pt.p33 then 'Low'
            when fx.unit_price_adjusted <= pt.p67 then 'Mid'
            else                                       'High'
        end                                                     as price_tier,
        pt.p33                                                  as price_tier_p33,
        pt.p67                                                  as price_tier_p67,

        -- ── metadata ───────────────────────────────────────────
        oi.active,
        oi.etl_loaded_at

    from order_items oi
    -- only include lines belonging to non-cancelled orders
    inner join orders o
        on oi.order_id = o.order_id
        and o.is_cancel = 0
    inner join item_fix fx
        on oi.order_item_key = fx.order_item_key
    left join products p
        on p.product_key = oi.product_key
    cross join price_thresholds pt

)

select * from final
