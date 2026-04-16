-- ============================================================
-- dim_customer_credit
-- Domain  : Finance
-- Grain   : One row per customer (customer_key)
-- Purpose : Customer credit & AR status for finance monitoring.
--           Combines customer master (debt_limit, debt_limit_day)
--           with live outstanding AR from fct_revenue to show
--           credit utilisation and flag at-risk accounts.
--
-- Key metrics:
--   debt_limit          — maximum credit extended (from CRM)
--   debt_limit_day      — payment terms in days
--   outstanding_ar      — total unpaid invoices (= lifetime_revenue; payment module not active)
--   credit_utilisation  — lifetime_revenue / debt_limit × 100
--   is_over_limit       — 1 when AR > debt_limit
--   credit_headroom     — debt_limit − lifetime_revenue
-- ============================================================

with customers as (

    select
        customer_key,
        customer_code,
        fullname            as customer_name,
        company,
        city,
        type_client,
        price_group_name,
        vip_rating,
        is_active,
        debt_limit,
        debt_limit_day,
        time_payment,
        discount,
        datecreated         as customer_since,
        phonenumber,
        email

    from {{ ref('stg_customers') }}

),

-- aggregate outstanding AR per customer from fct_revenue
-- (uses the mart model since it already excludes cancelled orders)
ar_summary as (

    select
        customer_key,
        sum(revenue)                    as lifetime_revenue,
        count(distinct order_date_key)  as active_days,
        max(order_date)                 as last_order_date

    from {{ ref('fct_revenue') }}
    group by customer_key

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
        c.phonenumber,
        c.email,

        -- ── credit terms (from ERP master) ─────────────────────
        coalesce(c.debt_limit, 0)        as debt_limit,
        c.debt_limit_day,
        c.time_payment,
        c.discount,

        -- ── AR position ────────────────────────────────────────────────
        -- lifetime_revenue is used as proxy for outstanding AR
        -- (payment module not configured in ERP; collected = 0 always)
        coalesce(ar.lifetime_revenue, 0)     as lifetime_revenue,
        ar.last_order_date,
        coalesce(ar.active_days, 0)          as active_days,

        -- ── credit utilisation ─────────────────────────────────
        round(
            coalesce(ar.lifetime_revenue, 0)
            / nullif(c.debt_limit, 0) * 100,
            2
        )                                    as credit_utilisation_pct,

        coalesce(c.debt_limit, 0)
            - coalesce(ar.lifetime_revenue, 0) as credit_headroom,

        -- ── risk flags ─────────────────────────────────────────
        case
            when coalesce(c.debt_limit, 0) = 0            then null   -- no limit set
            when coalesce(ar.lifetime_revenue, 0)
                 > coalesce(c.debt_limit, 0)               then 1
            else 0
        end                                  as is_over_limit,

        case
            when coalesce(c.debt_limit, 0) = 0
                then 'No Limit Set'
            when coalesce(ar.lifetime_revenue, 0)
                 >= coalesce(c.debt_limit, 0)
                then 'Over Limit'
            when coalesce(ar.lifetime_revenue, 0)
                 >= coalesce(c.debt_limit, 0) * 0.8
                then 'Near Limit'
            when coalesce(ar.lifetime_revenue, 0) > 0
                then 'Within Limit'
            else 'Clear'
        end                                  as credit_status

    from customers c
    left join ar_summary ar using (customer_key)

)

select * from final
