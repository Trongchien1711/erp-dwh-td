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
--   cogs         = total_cost  (ERP-recorded; populated for only ~244 of 72,363 orders)
--   gross_profit = revenue − cogs  (from ERP total_profit; near-zero due to missing COGS)
--   gp_margin_pct = gross_profit / revenue * 100
--   delivery_rev = cost_delivery (earmarked delivery charge)
--   collected    = total_payment (always 0 — payment module not configured in ERP)
--
-- ERP DATA GAPS:
--   • COGS (total_cost): populated for only ~244 orders. ERP does not record item
--     cost at order time. No per-unit cost is available in any source table.
--   • Payments (total_payment / status_payment): not tracked in ERP source system.
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
        -- cogs: ERP-recorded (only ~244 orders have cost > 0 — see header note)
        sum(total_cost)                                 as cogs,
        -- is_cogs_available: TRUE when at least one order in this group has cost recorded.
        -- Use to filter out rows where cogs / gp_margin_pct are near-zero due to missing data.
        bool_or(total_cost > 0)                         as is_cogs_available,

        -- ── profit ─────────────────────────────────────────────
        sum(total_profit)                               as gross_profit,
        round(
            sum(total_profit) / nullif(sum(grand_total), 0) * 100,
            2
        )                                               as gp_margin_pct,

        -- ── discounts ──────────────────────────────────────────
        sum(total_discount_percent + total_discount_direct)
                                                        as total_discount

    from orders
    group by 1, 2, 3, 4, 5, 6,
             7, 8, 9, 10, 11, 12, 13,
             14

)

select * from final
