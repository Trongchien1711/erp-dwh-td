-- ============================================================
-- fct_inbound_outbound
-- Domain  : Inventory
-- Grain   : One row per (date × product × warehouse × movement_type)
-- Purpose : Daily stock movement summary — inbound receipts vs
--           outbound deliveries per warehouse and product.
--           Feeds Days-on-Hand, stock velocity, and reorder monitoring.
--
-- Movement types:
--   INBOUND   — stock received from suppliers (fact_purchase_product_items)
--   OUTBOUND  — stock issued to customers     (fact_delivery_items)
--
-- Key metrics:
--   quantity_in       — units received into warehouse
--   quantity_out      — units shipped out of warehouse
--   net_movement      — quantity_in − quantity_out
--   value_in          — 0: purchase receipt prices not populated in ERP source
--   value_out         — outbound revenue value (total_amount from delivery)
-- ============================================================

with inbound as (

    select
        ppi.import_date_key                         as movement_date_key,
        ppi.product_key,
        ppi.warehouse_key,
        'INBOUND'                                   as movement_type,
        sum(ppi.quantity)                           as quantity_in,
        0::numeric                                  as quantity_out,
        sum(ppi.amount)                             as value_in,
        0::numeric                                  as value_out,
        count(*)                                    as transaction_count

    from {{ source('core', 'fact_purchase_product_items') }} ppi
    group by
        ppi.import_date_key,
        ppi.product_key,
        ppi.warehouse_key

),

outbound as (

    select
        di.delivery_date_key                        as movement_date_key,
        di.product_key,
        di.warehouse_key,
        'OUTBOUND'                                  as movement_type,
        0::numeric                                  as quantity_in,
        sum(di.quantity)                            as quantity_out,
        0::numeric                                  as value_in,
        sum(di.total_amount)                        as value_out,
        count(*)                                    as transaction_count

    from {{ source('core', 'fact_delivery_items') }} di
    group by
        di.delivery_date_key,
        di.product_key,
        di.warehouse_key

),

combined as (

    select * from inbound
    union all
    select * from outbound

),

with_dims as (

    select
        -- ── time ───────────────────────────────────────────────
        c.movement_date_key,
        d.full_date          as movement_date,
        d.year,
        d.quarter,
        d.month_num,
        d.month_name,

        -- ── product ────────────────────────────────────────────
        c.product_key,
        p.product_code,
        p.product_name,
        p.type_products,
        p.brand,

        -- ── warehouse ──────────────────────────────────────────
        c.warehouse_key,
        w.warehouse_code,
        w.warehouse_name,
        w.id_branch,

        -- ── movement ───────────────────────────────────────────
        c.movement_type,
        c.quantity_in,
        c.quantity_out,
        c.value_in,
        c.value_out,
        c.transaction_count

    from combined c
    left join {{ ref('stg_date') }}       d on d.date_key = c.movement_date_key
    left join {{ ref('stg_products') }}   p using (product_key)
    left join {{ ref('stg_warehouses') }} w using (warehouse_key)

),

final as (

    select
        movement_date_key,
        movement_date,
        year,
        quarter,
        month_num,
        month_name,
        product_key,
        product_code,
        product_name,
        type_products,
        brand,
        warehouse_key,
        warehouse_code,
        warehouse_name,
        id_branch,
        movement_type,
        quantity_in,
        quantity_out,
        quantity_in - quantity_out      as net_movement,
        value_in,
        value_out,
        transaction_count

    from with_dims

)

select * from final
