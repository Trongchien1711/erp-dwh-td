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
--
-- Price logic (unit_price):
--   1. lot_price / quantity from fact_warehouse_stock (when populated)
--   2. Fallback: latest price_suppliers from fact_purchase_order_items
--      (ERP does not consistently populate lot import price in tblwarehouse_product)
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

-- Latest purchase order price per product (fallback when lot price missing)
-- NOTE: fact_purchase_order_items contains ONLY outsourced finished goods
-- (products / semi_products purchased from external suppliers).
-- Raw materials (NPL) are NOT in this table — their receipt prices are
-- not recorded in the ERP source (tbl_purchase_products.price = 0).
-- po_fallback is therefore only valid for products/semi_products that were
-- legitimately purchased externally AND lack a lot-level price.
latest_po_price as (

    select distinct on (product_key)
        product_key,
        price_suppliers as po_unit_price
    from {{ source('core', 'fact_purchase_order_items') }}
    where price_suppliers > 0
    order by product_key, po_date_key desc

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
        -- lot_price: total lot import price from warehouse stock (tong tien lo nhap)
        -- unit_price: lot_price / quantity; fallback to latest PO price if missing
        -- unit_price_capped: non-3D products capped at 50,000 VND/unit
        s.price                                            as lot_price,

        -- price source flag for transparency
        case
            when s.price > 0 then 'lot_price'
            when po.po_unit_price is not null then 'po_fallback'
            else 'no_price'
        end                                                as price_source,

        round(
            coalesce(
                nullif(s.price / nullif(s.quantity, 0), 0),
                po.po_unit_price
            ), 2
        )                                                  as unit_price,

        -- unit_price_capped: cap non-3D at 50,000 VND; NULL stays NULL (not capped)
        -- NOTE: PostgreSQL LEAST(NULL, n) = n, so must guard with CASE
        round(
            case
                when coalesce(
                        nullif(s.price / nullif(s.quantity, 0), 0),
                        po.po_unit_price
                     ) is null then null
                when p.product_code like '3D%'
                    then coalesce(
                            nullif(s.price / nullif(s.quantity, 0), 0),
                            po.po_unit_price
                         )
                else least(
                        coalesce(
                            nullif(s.price / nullif(s.quantity, 0), 0),
                            po.po_unit_price
                        ), 50000
                     )
            end, 2
        )                                                  as unit_price_capped,

        round(
            s.quantity_left * case
                when coalesce(
                        nullif(s.price / nullif(s.quantity, 0), 0),
                        po.po_unit_price
                     ) is null then null
                when p.product_code like '3D%'
                    then coalesce(
                            nullif(s.price / nullif(s.quantity, 0), 0),
                            po.po_unit_price
                         )
                else least(
                        coalesce(
                            nullif(s.price / nullif(s.quantity, 0), 0),
                            po.po_unit_price
                        ), 50000
                     )
            end, 2
        )                                                  as stock_value,

        -- ── valuation quality flags for PBI filtering ─────────
        -- is_valued: TRUE only when price comes from actual lot receipt
        -- value_note: explains why stock_value may be NULL or estimated
        case
            when s.price > 0 then true
            else false
        end                                                as is_valued,

        case
            when s.price > 0
                then 'lot_price'
            when po.po_unit_price is not null and p.type_products in ('products', 'semi_products')
                then 'po_fallback_outsourced'
            when po.po_unit_price is not null
                then 'po_fallback_material'
            else 'unpriced_no_eln_receipt'
        end                                                as value_note,

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
    left join products      p   using (product_key)
    left join warehouses    w   using (warehouse_key)
    left join latest_po_price po using (product_key)

)

select * from final
