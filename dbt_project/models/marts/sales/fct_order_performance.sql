-- ============================================================
-- fct_order_performance
-- Domain  : Sales
-- Grain   : One row per order (order_id)
-- Purpose : Order-level fulfilment and payment performance.
--           Answers: how quickly are orders delivered and paid?
--           Which orders have outstanding delivery or AR?
--
-- Key metrics:
--   fulfilment_rate     — % of ordered qty actually delivered
--   outstanding_qty     — quantity not yet delivered
--   is_fully_delivered  — boolean flag (fulfilment_rate = 100%)
--   is_paid             — status_payment = 1 (fully paid)
--   is_completed        — is_end = 1 (order closed)
--   total_items         — number of product lines
-- ============================================================

with orders as (

    select * from {{ ref('int_orders_enriched') }}

),

order_items as (

    select
        order_id,
        sum(quantity)              as total_qty_ordered,
        sum(quantity_delivery)     as total_qty_delivered,
        sum(quantity_not_delivery) as total_qty_outstanding,
        sum(quantity_returned)     as total_qty_returned,
        count(*)                   as line_item_count

    from {{ ref('stg_order_items') }}
    group by order_id

),

final as (

    select
        -- ── order identifiers ──────────────────────────────────
        o.order_key,
        o.order_id,
        o.reference_no,

        -- ── time ───────────────────────────────────────────────
        o.order_date_key,
        o.order_date,
        o.year,
        o.quarter,
        o.month_num,
        o.month_name,
        o.is_weekend,

        -- ── customer ───────────────────────────────────────────
        o.customer_key,
        o.customer_code,
        o.customer_name,
        o.city,
        o.type_client,
        o.price_group_name,
        o.vip_rating,

        -- ── branch / staff ─────────────────────────────────────
        o.id_branch,
        o.employee_key,

        -- ── order value ────────────────────────────────────────
        o.grand_total       as revenue,
        o.total_cost        as cogs,
        o.total_profit      as gross_profit,
        o.total_payment     as collected,
        o.grand_total - o.total_payment
                            as outstanding_ar,

        -- ── fulfilment ─────────────────────────────────────────
        coalesce(i.total_qty_ordered, 0)      as qty_ordered,
        coalesce(i.total_qty_delivered, 0)    as qty_delivered,
        coalesce(i.total_qty_outstanding, 0)  as qty_outstanding,
        coalesce(i.total_qty_returned, 0)     as qty_returned,
        coalesce(i.line_item_count, 0)        as line_item_count,

        round(
            coalesce(i.total_qty_delivered, 0)
            / nullif(coalesce(i.total_qty_ordered, 0), 0) * 100,
            2
        )                                     as fulfilment_rate_pct,

        case
            when coalesce(i.total_qty_outstanding, 0) = 0 then 1
            else 0
        end                                   as is_fully_delivered,

        -- ── payment & completion flags ─────────────────────────
        o.status_payment,
        case when o.status_payment = 1 then 1 else 0 end
                                              as is_paid,
        o.is_end                              as is_completed,
        o.is_cancel,

        -- ── order type ─────────────────────────────────────────
        o.status,
        o.status_orders,
        o.type_orders,
        o.type_bills

    from orders o
    left join order_items i using (order_id)

)

select * from final
