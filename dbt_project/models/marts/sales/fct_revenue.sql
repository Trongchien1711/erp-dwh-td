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

order_line_adjustments as (

    -- Build order-level corrected revenue from line-level spike fixes.
    select
        order_id,
        sum(total_amount)                                        as revenue_raw_lines,
        sum(total_amount_adjusted)                               as revenue_adjusted_lines,
        sum(case when is_price_spike then 1 else 0 end)         as spike_line_count

    from {{ ref('fct_order_items_detail') }}
    group by 1

),

orders_adjusted as (

    select
        o.*,
        coalesce(a.revenue_adjusted_lines, o.grand_total)        as grand_total_adjusted,
        coalesce(a.revenue_raw_lines, o.grand_total)             as grand_total_from_lines,
        coalesce(a.spike_line_count, 0)                          as spike_line_count,
        case
            when o.grand_total = 0 then 1
            else coalesce(a.revenue_adjusted_lines, o.grand_total) / nullif(o.grand_total, 0)
        end                                                      as adjustment_ratio

    from orders o
    left join order_line_adjustments a using (order_id)

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
        sum(grand_total_adjusted)                       as revenue,
        sum(total_cost * adjustment_ratio)              as cogs,
        sum(grand_total_adjusted - (total_cost * adjustment_ratio))
                                                        as gross_profit,
        round(
            sum(grand_total_adjusted - (total_cost * adjustment_ratio))
            / nullif(sum(grand_total_adjusted), 0) * 100,
            2
        )                                               as gross_margin_pct,
        sum(total_payment)                              as collected,
        sum(grand_total_adjusted) - sum(total_payment)  as outstanding_ar,
        avg(grand_total_adjusted)                       as avg_order_value,
        sum(total_tax)                                  as total_tax,
        sum(total_discount_percent + total_discount_direct)
                                                        as total_discount,
        sum(grand_total)                                as revenue_raw,
        sum(grand_total_adjusted) - sum(grand_total)    as revenue_adjustment,
        sum(spike_line_count)                           as spike_line_count

    from orders_adjusted
    group by 1, 2, 3, 4, 5, 6, 7, 8,
             9, 10, 11, 12, 13, 14, 15, 16, 17,
             18

)

select * from final
