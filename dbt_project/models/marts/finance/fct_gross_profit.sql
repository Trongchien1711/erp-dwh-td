-- ============================================================
-- fct_gross_profit
-- Domain  : Finance
-- Grain   : One row per (order_date × customer × branch)
--           (same grain as fct_revenue for easy cross-ref)
-- Purpose : P&L-style gross profit summary.
--           Revenue, COGS, Gross Profit, and margin % by day,
--           customer, and price group — ready for finance dashboards.
--
-- NOTE    : Metric definitions
--   revenue      = grand_total (includes VAT + delivery charge)
--   cogs         = total_cost  (cost of goods sold)
--   gross_profit = revenue − cogs
--   gp_margin_pct = gross_profit / revenue * 100
--   delivery_rev = cost_delivery (earmarked delivery charge)
--   collected    = total_payment (invoiced cash received)
-- ============================================================

with orders as (

    select * from {{ ref('int_orders_enriched') }}
    where is_cancel = 0

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

        -- ── customer segment ───────────────────────────────────
        customer_key,
        customer_code,
        customer_name,
        price_group_code,
        price_group_name,
        type_client,
        vip_rating,

        -- ── branch ─────────────────────────────────────────────
        id_branch,

        -- ── revenue ────────────────────────────────────────────
        count(distinct order_id)                        as order_count,
        sum(grand_total)                                as revenue,
        sum(cost_delivery)                              as delivery_revenue,
        sum(total_tax)                                  as total_vat,

        -- ── costs ──────────────────────────────────────────────
        sum(total_cost)                                 as cogs,

        -- ── profit ─────────────────────────────────────────────
        sum(total_profit)                               as gross_profit,
        round(
            sum(total_profit) / nullif(sum(grand_total), 0) * 100,
            2
        )                                               as gp_margin_pct,

        -- ── discounts ──────────────────────────────────────────
        sum(total_discount_percent + total_discount_direct)
                                                        as total_discount,

        -- ── cash flow ──────────────────────────────────────────
        sum(total_payment)                              as collected,
        sum(grand_total) - sum(total_payment)           as outstanding_ar

    from orders
    group by 1, 2, 3, 4, 5, 6,
             7, 8, 9, 10, 11, 12, 13,
             14

)

select * from final
