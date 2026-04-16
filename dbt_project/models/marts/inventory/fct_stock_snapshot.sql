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
        id_branch,
        is_virtual

    from {{ ref('stg_warehouses') }}

),

-- Latest purchase order price per product (fallback when lot price missing)
-- NOTE: fact_purchase_order_items contains BOTH:
--   type != 'nvl': outsourced finished goods (product_key set)
--   type =  'nvl': raw materials purchased (material_key set, price_suppliers populated)
-- Two separate CTEs to avoid cross-joining the two key spaces.
latest_po_price as (

    select distinct on (product_key)
        product_key,
        price_suppliers as po_unit_price
    from {{ source('core', 'fact_purchase_order_items') }}
    where price_suppliers > 0
      and product_key is not null
    order by product_key, po_date_key desc

),

latest_npl_po_price as (

    select distinct on (material_key)
        material_key,
        price_suppliers as po_unit_price
    from {{ source('core', 'fact_purchase_order_items') }}
    where price_suppliers > 0
      and material_key is not null
    order by material_key, po_date_key desc

),

materials as (

    select
        material_key,
        material_code,
        material_name,
        unit_id      as material_unit_id

    from {{ ref('stg_materials') }}

),

final as (

    select
        -- ── keys ───────────────────────────────────────────────
        s.stock_key,
        s.stock_id,

        -- ── item (product OR nvl material) ─────────────────────
        s.product_key,
        s.material_key,
        -- unified display columns
        coalesce(p.product_code, m.material_code) as item_code,
        coalesce(p.product_name, m.material_name) as item_name,
        case
            when s.product_key  is not null then 'product'
            when s.material_key is not null then 'nvl'
            else 'UNKNOWN'
        end                                        as item_type,
        -- product-specific columns (NULL for NVL rows)
        p.product_code,
        p.product_name,
        p.type_products,
        p.brand,
        coalesce(p.unit_id, m.material_unit_id)   as unit_id,
        -- material-specific columns (NULL for product rows)
        m.material_code,
        m.material_name,

        -- ── warehouse ──────────────────────────────────────────
        s.warehouse_key,
        w.warehouse_code,
        w.warehouse_name,
        w.id_branch,
        w.is_virtual,
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
        s.price                                            as lot_price,

        -- effective_po: product rows → latest_po_price; nvl rows → latest_npl_po_price
        coalesce(po.po_unit_price, npl_po.po_unit_price)   as po_fallback_price,

        case
            when s.price > 0 then 'lot_price'
            when po.po_unit_price     is not null then 'po_fallback'
            when npl_po.po_unit_price is not null then 'npl_po_fallback'
            else 'no_price'
        end                                                as price_source,

        round(
            coalesce(
                nullif(s.price / nullif(s.quantity, 0), 0),
                po.po_unit_price,
                npl_po.po_unit_price,
                p.price_import
            ), 2
        )                                                  as unit_price,

        -- unit_price_capped: NVL items not capped; non-3D products capped at 50,000
        round(
            case
                when coalesce(
                        nullif(s.price / nullif(s.quantity, 0), 0),
                        po.po_unit_price,
                        npl_po.po_unit_price,
                        p.price_import
                     ) is null then null
                when s.type_items = 'nvl'
                    then coalesce(
                            nullif(s.price / nullif(s.quantity, 0), 0),
                            npl_po.po_unit_price
                         )
                when p.product_code like '3D%'
                    then coalesce(
                            nullif(s.price / nullif(s.quantity, 0), 0),
                            po.po_unit_price,
                            p.price_import
                         )
                else least(
                        coalesce(
                            nullif(s.price / nullif(s.quantity, 0), 0),
                            po.po_unit_price,
                            p.price_import
                        ), 50000
                     )
            end, 2
        )                                                  as unit_price_capped,

        round(
            s.quantity_left * case
                when coalesce(
                        nullif(s.price / nullif(s.quantity, 0), 0),
                        po.po_unit_price,
                        npl_po.po_unit_price,
                        p.price_import
                     ) is null then null
                when s.type_items = 'nvl'
                    then coalesce(
                            nullif(s.price / nullif(s.quantity, 0), 0),
                            npl_po.po_unit_price
                         )
                when p.product_code like '3D%'
                    then coalesce(
                            nullif(s.price / nullif(s.quantity, 0), 0),
                            po.po_unit_price,
                            p.price_import
                         )
                else least(
                        coalesce(
                            nullif(s.price / nullif(s.quantity, 0), 0),
                            po.po_unit_price,
                            p.price_import
                        ), 50000
                     )
            end, 2
        )                                                  as stock_value,

        case
            when s.price > 0 then true
            else false
        end                                                as is_valued,

        case
            when s.price > 0
                then 'lot_price'
            when npl_po.po_unit_price is not null and s.type_items = 'nvl'
                then 'npl_po_fallback'
            when po.po_unit_price is not null and p.type_products in ('products', 'semi_products')
                then 'po_fallback_outsourced'
            when po.po_unit_price is not null
                then 'po_fallback_material'
            when p.price_import > 0
                then 'product_master_fallback'
            else 'unpriced_no_eln_receipt'
        end                                                as value_note,

        -- ── import date reference ──────────────────────────────
        s.import_date_key,

        -- ── item metadata ──────────────────────────────────────
        s.type_items,
        s.type_export,
        s.type_transfer,
        s.series

    from stock s
    left join products            p      using (product_key)
    left join warehouses          w      using (warehouse_key)
    left join latest_po_price     po     using (product_key)
    left join latest_npl_po_price npl_po using (material_key)
    left join materials           m      using (material_key)

)

select * from final
