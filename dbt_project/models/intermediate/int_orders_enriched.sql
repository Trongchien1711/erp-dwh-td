-- ============================================================
-- int_orders_enriched
-- Purpose : Enrich fact_orders with customer and date attributes.
--           This intermediate model is consumed by all sales and
--           finance mart models, avoiding repeated joins.
--
-- Materialisation : ephemeral  (inlined as a CTE at query time)
--                  → no physical table; recomputed every dbt run
-- ============================================================

with orders as (

    select * from {{ ref('stg_orders') }}

),

customers as (

    select
        customer_key,
        customer_code,
        fullname            as customer_name,
        company,
        company_short,
        city,
        type_client,
        price_group_key,
        price_group_code,
        price_group_name,
        vip_rating,
        is_active           as customer_is_active

    from {{ ref('stg_customers') }}

),

dates as (

    select
        date_key,
        full_date,
        day_of_week,
        day_name,
        day_of_month,
        week_of_year,
        month_num,
        month_name,
        quarter,
        year,
        is_weekend

    from {{ ref('stg_date') }}

)

select
    -- ── order identifiers ──────────────────────────────────────
    o.order_key,
    o.order_id,
    o.reference_no,

    -- ── date dimension ─────────────────────────────────────────
    o.order_date_key,
    d.full_date          as order_date,
    d.day_of_week,
    d.day_name,
    d.day_of_month,
    d.week_of_year,
    d.month_num,
    d.month_name,
    d.quarter,
    d.year,
    d.is_weekend,

    -- ── customer dimension ─────────────────────────────────────
    o.customer_key,
    c.customer_code,
    c.customer_name,
    c.company,
    c.company_short,
    c.city,
    c.type_client,
    c.price_group_key,
    c.price_group_code,
    c.price_group_name,
    c.vip_rating,

    -- ── branch / warehouse ─────────────────────────────────────
    o.id_branch,
    o.employee_key,
    o.warehouse_id,

    -- ── revenue measures ───────────────────────────────────────
    o.count_items,
    o.total_quantity,
    o.grand_total,          -- total revenue (incl. tax + delivery)
    o.total_cost,           -- COGS
    o.total_profit,         -- grand_total - total_cost
    o.total_payment,        -- cash collected
    o.total_tax,
    o.total_discount_percent,
    o.total_discount_direct,
    o.cost_delivery,

    -- ── status flags ───────────────────────────────────────────
    o.status,
    o.status_payment,
    o.status_orders,
    o.type_orders,
    o.type_bills,
    o.is_cancel,            -- 1 = cancelled (exclude in most marts)
    o.is_end,               -- 1 = fully completed

    -- ── timestamps ─────────────────────────────────────────────
    o.date_created,
    o.date_updated

from orders o
left join customers c using (customer_key)
left join dates     d on d.date_key = o.order_date_key
