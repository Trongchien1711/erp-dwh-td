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
        oi.amount,
        oi.tax_amount_item,
        oi.discount_percent_item,
        oi.discount_percent_amount_item,
        oi.discount_direct_amount_item,
        oi.total_amount,                    -- net line revenue
        oi.cost,                            -- line COGS
        oi.profit,                          -- line gross profit

        -- ── fulfilment ─────────────────────────────────────────
        oi.quantity_delivery,
        oi.quantity_not_delivery,
        oi.quantity_returned,

        -- ── metadata ───────────────────────────────────────────
        oi.active,
        oi.etl_loaded_at

    from order_items oi
    -- only include lines belonging to non-cancelled orders
    inner join orders o
        on oi.order_id = o.order_id
        and o.is_cancel = 0
    left join products p using (product_key)

)

select * from final
