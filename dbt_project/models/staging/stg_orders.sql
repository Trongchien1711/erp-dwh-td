-- ============================================================
-- stg_orders
-- Source  : core.fact_orders  (partitioned by order_date_key)
-- Purpose : Thin staging wrapper over fact_orders.
--           No filtering — marts handle business-rule filters
--           (e.g. is_cancel = 0).
-- ============================================================

with source as (

    select * from {{ source('core', 'fact_orders') }}

),

renamed as (

    select
        -- primary key
        order_key,
        order_id,
        reference_no,

        -- foreign keys → dimension tables
        customer_key,
        employee_key,
        order_date_key,         -- INT YYYYMMDD → join stg_date.date_key
        id_branch,
        warehouse_id,
        currencies,

        -- header measures
        count_items,
        total_quantity,

        -- item-level subtotals (pre-aggregated from order items)
        total_amount_items,
        total_tax_items,
        total_discount_percent_items,
        total_discount_direct_items,
        grand_total_items,

        -- order-level charges
        total_tax,
        total_discount_percent,
        total_discount_direct,
        cost_delivery,

        -- totals
        grand_total,            -- revenue (incl. tax)
        total_cost,             -- COGS
        total_profit,           -- gross profit = grand_total - total_cost
        total_payment,          -- cash actually collected

        -- status flags
        status,
        status_payment,
        status_orders,
        type_orders,
        type_bills,
        is_cancel,              -- 1 = cancelled order
        is_end,                 -- 1 = fully completed

        -- timestamps
        date_created,
        date_updated,
        etl_loaded_at

    from source

)

select * from renamed
