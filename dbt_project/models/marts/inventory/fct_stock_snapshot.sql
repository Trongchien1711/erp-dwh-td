-- ============================================================
-- fct_stock_snapshot
-- Domain  : Inventory
-- Grain   : One row per stock lot (product × warehouse × lot_code)
-- Purpose : Current stock levels with product and warehouse
--           attributes for inventory reporting.
--
-- Key metrics:
--   quantity_left       — remaining units in stock
--   quantity_exported   — units consumed / issued
--   lot_code            — traceability identifier (expiry dates)
-- ============================================================

with stock as (

    select * from {{ source('core', 'fact_warehouse_stock') }}

),

products as (

    select
        product_key,
        product_code,
        product_name,
        type_products,
        category_id,
        brand,
        unit_id,
        price_import,
        price_sell

    from {{ ref('stg_products') }}

),

warehouses as (

    select
        warehouse_key,
        warehouse_code,
        warehouse_name,
        id_branch

    from {{ ref('stg_warehouses') }}

),

final as (

    select
        -- ── keys ───────────────────────────────────────────────
        s.stock_key,
        s.stock_id,

        -- ── product ────────────────────────────────────────────
        s.product_key,
        p.product_code,
        p.product_name,
        p.type_products,
        p.brand,
        p.unit_id,

        -- ── warehouse ──────────────────────────────────────────
        s.warehouse_key,
        w.warehouse_code,
        w.warehouse_name,
        w.id_branch,
        s.location_key,

        -- ── lot traceability ───────────────────────────────────
        s.lot_code,
        s.date_sx,              -- manufacturing date
        s.date_sd,              -- expiry date

        -- ── stock measures ─────────────────────────────────────
        s.quantity,             -- original received quantity
        s.quantity_left,        -- remaining after exports
        s.quantity_export       as quantity_exported,

        -- ── secondary unit conversions ─────────────────────────
        s.product_quantity_unit,
        s.product_quantity_unit_left,
        s.product_quantity_unit_export,

        -- ── value ──────────────────────────────────────────────
        -- NOTE: s.price is the TOTAL lot import price (tong tien lo nhap), not
        --       unit price. Unit price = price / quantity.
        --       Non-3D products are capped at 50,000 VND/unit per business rule.
        s.price                                            as lot_price,
        round(
            s.price / nullif(s.quantity, 0), 2
        )                                                  as unit_price,
        round(
            case
                when p.product_code like '3D%'
                    then s.price / nullif(s.quantity, 0)
                else least(s.price / nullif(s.quantity, 0), 50000)
            end, 2
        )                                                  as unit_price_capped,
        round(
            s.quantity_left * case
                when p.product_code like '3D%'
                    then s.price / nullif(s.quantity, 0)
                else least(s.price / nullif(s.quantity, 0), 50000)
            end, 2
        )                                                  as stock_value,

        -- ── import date reference ──────────────────────────────
        s.import_date_key,

        -- ── item metadata ──────────────────────────────────────
        s.type_items,
        s.type_export,
        s.type_transfer,
        s.series,

        -- ── metadata ───────────────────────────────────────────
        s.etl_loaded_at

    from stock s
    left join products  p using (product_key)
    left join warehouses w using (warehouse_key)

)

select * from final
