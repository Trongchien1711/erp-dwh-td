-- ============================================================
-- dim_customer_segmentation
-- Domain  : Sales
-- Grain   : One row per customer (customer_key)
-- Purpose : RFM-based customer segmentation for sales analytics.
--           Combines customer master data with lifetime buying
--           behaviour to drive segment-level reporting.
--
-- Segments (rfm_segment):
--   Champions         — bought recently, buy often, spend most
--   Loyal             — buy regularly, good spend
--   At Risk           — used to buy regularly, not recently
--   Lost              — haven't bought in a long time
--   New               — first or very few purchases
--   Promising         — recent buyer, low frequency so far
--   Potential Loyalist — recent, mid-frequency, growing spend
--
-- RFM scores (1–5 scale, higher = better):
--   recency_score    — how recently they bought (5 = very recent)
--   frequency_score  — how many orders (5 = most orders)
--   monetary_score   — how much revenue  (5 = highest spend)
-- ============================================================

with orders as (

    select
        customer_key,
        order_date,
        order_id,
        grand_total

    from {{ ref('int_orders_enriched') }}
    where is_cancel = 0

),

customers as (

    select
        customer_key,
        customer_code,
        fullname        as customer_name,
        company,
        city,
        type_client,
        price_group_name,
        vip_rating,
        is_active,
        debt_limit,
        debt_limit_day,
        datecreated     as customer_since

    from {{ ref('stg_customers') }}

),

-- ── raw RFM aggregates per customer ───────────────────────────
rfm_raw as (

    select
        customer_key,
        max(order_date)                                   as last_order_date,
        count(distinct order_id)                          as frequency,
        sum(grand_total)                                  as monetary,
        avg(grand_total)                                  as avg_order_value,
        min(order_date)                                   as first_order_date,
        count(distinct order_id)                          as total_orders,
        current_date - max(order_date)                    as days_since_last_order

    from orders
    group by customer_key

),

-- ── percentile-based RFM scoring (1–5) ────────────────────────
rfm_scored as (

    select
        customer_key,
        last_order_date,
        first_order_date,
        frequency,
        monetary,
        avg_order_value,
        total_orders,
        days_since_last_order,

        -- recency: fewer days since order = higher score
        ntile(5) over (order by days_since_last_order desc) as recency_score,
        ntile(5) over (order by frequency asc)              as frequency_score,
        ntile(5) over (order by monetary asc)               as monetary_score

    from rfm_raw

),

-- ── composite RFM score and segment label ─────────────────────
rfm_segmented as (

    select
        *,
        recency_score + frequency_score + monetary_score    as rfm_total_score,
        case
            when recency_score >= 4 and frequency_score >= 4 and monetary_score >= 4
                then 'Champions'
            when recency_score >= 3 and frequency_score >= 3
                then 'Loyal'
            when recency_score >= 4 and frequency_score <= 2
                then 'Promising'
            when recency_score >= 3 and frequency_score >= 2 and monetary_score >= 3
                then 'Potential Loyalist'
            when recency_score <= 2 and frequency_score >= 3
                then 'At Risk'
            when recency_score <= 2 and frequency_score <= 2 and monetary_score <= 2
                then 'Lost'
            when frequency_score = 1
                then 'New'
            else 'Others'
        end                                                 as rfm_segment

    from rfm_scored

),

final as (

    select
        -- ── identity ───────────────────────────────────────────
        c.customer_key,
        c.customer_code,
        c.customer_name,
        c.company,
        c.city,
        c.type_client,
        c.price_group_name,
        c.vip_rating,
        c.is_active,
        c.customer_since,
        c.debt_limit,
        c.debt_limit_day,

        -- ── activity summary ───────────────────────────────────
        coalesce(r.total_orders, 0)       as total_orders,
        coalesce(r.monetary, 0)           as lifetime_revenue,
        coalesce(r.avg_order_value, 0)    as avg_order_value,
        r.first_order_date,
        r.last_order_date,
        r.days_since_last_order,

        -- ── RFM scores ─────────────────────────────────────────
        coalesce(r.recency_score, 1)      as recency_score,
        coalesce(r.frequency_score, 1)    as frequency_score,
        coalesce(r.monetary_score, 1)     as monetary_score,
        coalesce(r.rfm_total_score, 3)    as rfm_total_score,
        coalesce(r.rfm_segment, 'New')    as rfm_segment

    from customers c
    left join rfm_segmented r using (customer_key)

)

select * from final
