-- ============================================================
-- stg_order_items
-- Source  : core.fact_order_items  (partitioned by order_date_key)
-- Purpose : Thin staging wrapper over fact_order_items.
--           One row = one product line on one order.
-- ============================================================

with source as (

    select * from {{ source('core', 'fact_order_items') }}

),

renamed as (

    select
        -- primary key
        order_item_key,
        order_item_id,
        order_id,               -- join back to stg_orders

        -- foreign keys
        customer_key,
        product_key,
        unit_id,
        order_date_key,         -- INT YYYYMMDD → join stg_date.date_key

        -- measures
        quantity,
        price,                  -- unit price on the order
        amount,                 -- quantity * price
        tax_rate_item,
        tax_amount_item,
        discount_percent_item,
        discount_percent_amount_item,
        discount_direct_amount_item,
        total_amount,           -- net amount after discounts + tax

        -- fulfilment
        quantity_delivery,
        quantity_not_delivery,
        quantity_returned,

        -- cost / profit (populated during transform)
        cost,
        profit,

        -- cost using temporary capital
        cost_temporary_capital,
        profit_temporary_capital,

        -- item metadata
        type_item,
        item_code,
        type_gift,
        active,

        -- metadata
        etl_loaded_at

    from source

)

select * from renamed
