-- ============================================================
-- fct_inbound_outbound
-- Domain  : Inventory
-- Grain   : One row per (date × product × warehouse × movement_type × movement_subtype)
-- Purpose : Daily stock movement summary — inbound receipts vs
--           ALL outbound movements per warehouse and product.
--           Feeds Days-on-Hand, stock velocity, and reorder monitoring.
--
-- Movement types:
--   INBOUND   — ALL stock received into warehouse (fact_warehouse_stock.quantity)
--               covers purchase receipts + production output + all inbound types
--               Uses import_date_key (physical receive date)
--   OUTBOUND  — ALL outbound movements with exact export date (fact_warehouse_export)
--               covers customer delivery + production export + transfer + loss
--
-- Why fact_warehouse_stock for INBOUND (not fact_purchase_product_items)?
--   purchase_product_items = 17,806M = 95.9% of total inbound only.
--   The remaining 4.1% (~768M) is production output entering warehouse.
--   fact_warehouse_stock.quantity is the COMPLETE nhap kho ledger — all sources.
--   Using it gives nhap > xuat every year as expected (building stock).
--
-- Why NOT fact_delivery_items for outbound?
--   delivery = 6.1% of total outbound only. The remaining 93.9% is
--   production consumption, internal transfers, and losses.
--
-- Key metrics:
--   quantity_in       — units received into warehouse (all inbound types)
--   quantity_out      — units exported from warehouse (all types, exact export date)
--   net_movement      — quantity_in − quantity_out
--   value_in          — 0: no consistent inbound value in stock ledger
--   value_out         — 0: no consistent outbound value in ERP export table
-- ============================================================

with inbound as (

    -- INBOUND: fact_warehouse_stock — complete nhap kho ledger, all sources
    -- (purchases + production output). Grain: product × warehouse × import_date.
    select
        ws.import_date_key                          as movement_date_key,
        ws.product_key,
        ws.warehouse_key,
        'INBOUND'                                   as movement_type,
        coalesce(nullif(trim(ws.type_items), ''), 'UNKNOWN')
                                                    as movement_subtype,
        sum(ws.quantity)                            as quantity_in,
        0::numeric                                  as quantity_out,
        0::numeric                                  as value_in,
        0::numeric                                  as value_out,
        count(*)                                    as transaction_count

    from {{ source('core', 'fact_warehouse_stock') }} ws
    group by
        ws.import_date_key,
        ws.product_key,
        ws.warehouse_key,
        coalesce(nullif(trim(ws.type_items), ''), 'UNKNOWN')

),

-- OUTBOUND: fact_warehouse_export — all outbound movements recorded per transaction
-- with exact export date. Covers customer delivery, production consumption,
-- warehouse transfers, and losses. ETL: staging.tblwarehouse_export → core (transform_core.py).
outbound as (

    select
        we.export_date_key                          as movement_date_key,
        we.product_key,
        we.warehouse_key,
        'OUTBOUND'                                  as movement_type,
        coalesce(nullif(trim(we.type_export), ''), 'UNKNOWN')
                                                    as movement_subtype,
        0::numeric                                  as quantity_in,
        sum(coalesce(we.quantity, 0))               as quantity_out,
        0::numeric                                  as value_in,
        0::numeric                                  as value_out,
        count(*)                                    as transaction_count

    from {{ source('core', 'fact_warehouse_export') }} we
    group by
        we.export_date_key,
        we.product_key,
        we.warehouse_key,
        coalesce(nullif(trim(we.type_export), ''), 'UNKNOWN')

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
        c.movement_subtype,
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
        movement_subtype,
        quantity_in,
        quantity_out,
        quantity_in - quantity_out      as net_movement,
        value_in,
        value_out,
        transaction_count

    from with_dims

)

select * from final
