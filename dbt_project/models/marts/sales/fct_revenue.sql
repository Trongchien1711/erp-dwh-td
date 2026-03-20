-- ============================================================
-- fct_revenue
-- Domain  : Sales
-- Grain   : One row per (order_date × customer × branch)
-- Purpose : Daily revenue summary — primary KPI table for
--           sales dashboards and monthly reporting.
--
-- Key metrics:
--   order_count     — how many orders were placed
--   revenue         — grand_total (incl. tax + delivery)
--   cogs            — cost of goods sold (total_cost)
--   gross_profit    — revenue – cogs
--   collected       — cash actually received (total_payment)
--   outstanding_ar  — revenue – collected  (trade receivable)
-- ============================================================

with orders as (

    select * from {{ ref('int_orders_enriched') }}
    where is_cancel = 0         -- exclude cancelled orders

),

final as (

    select
        -- ── time grain ─────────────────────────────────────────
        order_date_key,
        order_date,
        year,
        quarter,
        month_num,
        month_name,
        week_of_year,
        is_weekend,

        -- ── customer segment ───────────────────────────────────
        customer_key,
        customer_code,
        customer_name,
        company,
        city,
        type_client,
        price_group_code,
        price_group_name,
        vip_rating,

        -- ── branch ─────────────────────────────────────────────
        id_branch,

        -- ── aggregated measures ────────────────────────────────
        count(distinct order_id)                        as order_count,
        sum(grand_total)                                as revenue,
        sum(total_cost)                                 as cogs,
        sum(total_profit)                               as gross_profit,
        round(
            sum(total_profit) / nullif(sum(grand_total), 0) * 100,
            2
        )                                               as gross_margin_pct,
        sum(total_payment)                              as collected,
        sum(grand_total) - sum(total_payment)           as outstanding_ar,
        avg(grand_total)                                as avg_order_value,
        sum(total_tax)                                  as total_tax,
        sum(total_discount_percent + total_discount_direct)
                                                        as total_discount

    from orders
    group by 1, 2, 3, 4, 5, 6, 7, 8,
             9, 10, 11, 12, 13, 14, 15, 16, 17,
             18

)

select * from final
