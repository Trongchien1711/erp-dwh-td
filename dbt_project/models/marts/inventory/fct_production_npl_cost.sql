-- ============================================================
-- fct_production_npl_cost
-- Domain  : Inventory / Production
-- Grain   : One row per (production plan × plan item × BOM material line)
-- Purpose : NPL raw material cost per production plan, combining BOM
--           quantities with latest purchase prices (from PO history).
--
-- Source tables (staging schema — raw MySQL extracts):
--   tbl_productions_plan        : plan headers, options1_id = source order IDs
--   tbl_productions_plan_items  : finished products to produce per plan
--   tbl_productions_plan_bom    : actual BOM (NPL requirements)
--   tblpurchase_order_items     : purchase history for unit price lookup
--   tblcurrencies               : exchange rates (USD→VND etc.)
--
-- NPL classification (item_type in plan_bom):
--   'materials'     → raw / bought-in NPL items (labels, hangtags, paper, etc.)
--   'semi_products' → sub-assemblies (BTP), excluded here
--   'element'       → grouping rows, excluded here
--
-- Revenue link:
--   tbl_productions_plan.options1_id = comma-separated tbl_orders.id values.
--   Revenue is resolved in reporting by joining to fact_orders/fact_order_items.
--
-- Key metrics:
--   qty_single        : quantity_single (qty NPL per 1 kho giay / run unit = "gia tri quy doi")
--   number_children_size : so SP tren 1 kho giay (N in formula = "so luong con tren kho giay")
--   qty_per_sp        : true qty NPL per 1 SP = (1/N) x qty_single  [regular materials]
--                       For Kem: qty_bom / finished_qty  (fixed integer qty per order)
--   qty_bom           : total NPL qty for the whole order = qty_single x (finished_qty/N)
--   qty_compensation  : waste qty -- fixed per order (does not scale with SP count)
--   qty_total         : qty_bom + qty_compensation  (actual consumed)
--   unit_price_vnd    : latest NPL purchase price (from PO history)
--   npl_cost_vnd      : qty_bom x price  (base cost, no waste, full order)
--   waste_cost_per_order_vnd : qty_compensation x price  ("Tien bu hao tren 1 Lenh")
--   npl_cost_with_waste_vnd  : qty_total x price  (full order, with waste)
--   npl_cost_per_sp_vnd : (1/N) x qty_single x price  [= NPL cost per 1 finished SP]
--   is_zinc           : TRUE = Kem in (zinc offset plate) row
--   is_zinc_injected  : TRUE = Kem row auto-added because BOM had no zinc line
--
-- Kem rule:
--   Each offset print order needs at least 1 zinc plate (normally 1-4, integer).
--   If a plan_item BOM has no Kem line (is_zinc=1), the model auto-injects
--   1 default Kem plate (item_id=8530, VTSX-KEMIN-2, qty=1, comp=0).
--
-- Coverage note:
--   ~92% of plan BOM materials have item_id that matches tbl_products.
--   The remaining ~8% are discontinued raw materials (paper, bags, ink)
--   no longer in the product master — npl_cost_vnd = 0 for those rows.
--   ~84% of material rows have quantity_compensation > 0.
--
-- NPL/Revenue benchmark: ~23-32% (validated on 2024 full-coverage plans).
-- ============================================================

with

-- ── Latest VND unit price per product from PO history ─────────
-- Uses total_suppliers / quantity_unit as effective unit price.
-- Picks the single most-recent PO line per product.
latest_price as (

    select distinct on (poi.product_id)
        poi.product_id,
        round(
            cast(poi.total_suppliers as numeric)
            / nullif(cast(poi.quantity_unit as numeric), 0),
        4)                                              as unit_price_vnd,
        po.date::date                                   as last_po_date

    from {{ source('staging', 'tblpurchase_order_items') }} poi
    join {{ source('staging', 'tblpurchase_order') }}       po
        on po.id = poi.id_purchase_order
    where poi.price_suppliers > 0
      and poi.quantity_unit   > 0
      and poi.total_suppliers > 0

    order by poi.product_id, po.date desc

),

-- ── BOM materials from production plans ───────────────────────
bom as (

    select
        ppb.id                                          as bom_line_id,
        ppb.productions_plan_id,
        ppb.productions_plan_items_id,
        ppi.product_id                                  as finished_product_id,
        ppi.quantity_total_details                      as finished_qty_planned,
        ppb.item_type,
        ppb.item_id                                     as npl_product_id,
        ppb.item_code                                   as npl_code,
        ppb.item_name                                   as npl_name,
        ppb.unit_id,
        -- quantity_single = qty NPL per 1 kho giay (gia tri quy doi)
        -- NOT yet per SP -- divide by number_children_size to get per SP
        cast(ppb.quantity_single       as numeric)      as qty_single,
        cast(ppb.quantity              as numeric)      as qty_bom,
        cast(coalesce(ppb.quantity_compensation, 0)
                                       as numeric)      as qty_compensation,
        -- So luong con tren kho giay (= N in formula)
        cast(nullif(ppb.number_children_size, 0)
                                       as numeric)      as number_children_size,
        -- is_zinc = 1: Kem in offset plate (fixed integer qty: 1,2,3,4 per order)
        cast(coalesce(ppb.is_zinc, 0)  as int)          as is_zinc,
        false                                           as is_zinc_injected,
        pp.date::date                                   as plan_date,
        pp.options1_id                                  as source_order_ids,
        pp.status                                       as plan_status

    from {{ source('staging', 'tbl_productions_plan_bom') }}   ppb
    join {{ source('staging', 'tbl_productions_plan_items') }}  ppi
        on ppi.id = ppb.productions_plan_items_id
    join {{ source('staging', 'tbl_productions_plan') }}        pp
        on pp.id = ppb.productions_plan_id

    where ppb.item_type = 'materials'
      and ppb.item_id   > 0
      and ppb.quantity  > 0
      and ppi.quantity_total_details > 0

),

-- ── Auto-inject 1 Kem for plan items that have none ────────────
-- Kem (zinc plate) is required for every offset print run.
-- If the ERP BOM is missing the Kem line, default to qty=1.
-- Reference product: item_id=8530 (VTSX-KEMIN-2, standard Kodak Elecxd plate).
injected_zinc as (

    select
        null::bigint                                    as bom_line_id,
        pp.id                                           as productions_plan_id,
        ppi.id                                          as productions_plan_items_id,
        ppi.product_id                                  as finished_product_id,
        cast(ppi.quantity_total_details as numeric)     as finished_qty_planned,
        'materials'                                     as item_type,
        8530                                            as npl_product_id,
        'VTSX-KEMIN-2'                                  as npl_code,
        'VTSX_Kem In_Offset Kodak Elecxd QC 80x103 cm'  as npl_name,
        null::bigint                                    as unit_id,
        cast(1 as numeric)                              as qty_single,
        cast(1 as numeric)                              as qty_bom,       -- 1 zinc plate for the whole order
        cast(0 as numeric)                              as qty_compensation,
        null::numeric                                   as number_children_size,
        1                                               as is_zinc,
        true                                            as is_zinc_injected,
        pp.date::date                                   as plan_date,
        pp.options1_id                                  as source_order_ids,
        pp.status                                       as plan_status

    from {{ source('staging', 'tbl_productions_plan_items') }}  ppi
    join {{ source('staging', 'tbl_productions_plan') }}        pp
        on pp.id = ppi.productions_plan_id

    where ppi.quantity_total_details > 0
      -- only inject for plan_items that have no Kem (zinc) row
      and not exists (
          select 1
          from {{ source('staging', 'tbl_productions_plan_bom') }} z
          where z.productions_plan_items_id = ppi.id
            and z.is_zinc   = 1
            and z.item_type = 'materials'
      )

),

-- ── Merge real BOM rows with injected Kem rows ─────────────────
bom_all as (
    select * from bom
    union all
    select * from injected_zinc
),

-- ── Enrich BOM with price + product flag ──────────────────────
enriched as (

    select
        b.bom_line_id,
        b.productions_plan_id,
        b.productions_plan_items_id,
        b.finished_product_id,
        b.finished_qty_planned,
        b.npl_product_id,
        b.npl_code,
        b.npl_name,
        b.unit_id,
        b.is_zinc,
        b.is_zinc_injected,
        b.number_children_size,

        -- qty_single = quantity_single from BOM (= NPL qty per 1 kho giay / run unit)
        -- qty_per_sp = correct formula:
        --   regular materials: (1 / number_children_size) x qty_single
        --   Kem (is_zinc=1)  : qty_bom / finished_qty  (fixed integer per order)
        b.qty_single,
        case
            when b.is_zinc = 1
                then b.qty_bom / nullif(b.finished_qty_planned, 0)
            else
                b.qty_single / nullif(b.number_children_size, 0)
        end                                             as qty_per_sp,

        b.qty_bom,
        b.qty_compensation,
        (b.qty_bom + b.qty_compensation)                as qty_total,
        b.plan_date,
        b.source_order_ids,
        b.plan_status,

        -- Product master match flag
        case when dp.product_key is not null
            then true else false
        end                                             as material_matched,
        dp.product_name                                 as product_master_name,
        dp.product_code                                 as product_master_code,

        -- Latest purchase price
        lp.unit_price_vnd,
        lp.last_po_date,
        case when lp.unit_price_vnd is not null
            then true else false
        end                                             as has_price,

        -- NPL cost for the whole order (order-level)
        -- npl_cost_vnd      : qty_bom x price  (no waste)
        -- npl_cost_with_waste_vnd : (qty_bom + qty_compensation) x price
        --   = (1/N) x qty_single x finished_qty x price  +  qty_compensation x price
        round(
            b.qty_bom * coalesce(lp.unit_price_vnd, 0),
        0)                                              as npl_cost_vnd,

        round(
            (b.qty_bom + b.qty_compensation)
            * coalesce(lp.unit_price_vnd, 0),
        0)                                              as npl_cost_with_waste_vnd,

        -- Tien bu hao tren 1 Lenh (waste cost per order)
        -- Fixed per production order -- not prorated per SP
        round(
            b.qty_compensation * coalesce(lp.unit_price_vnd, 0),
        0)                                              as waste_cost_per_order_vnd,

        -- NPL cost per SP (VND) -- formula: (1/N) x qty_single x price
        -- Kem: qty_bom x price / finished_qty
        round(
            cast(
                case
                    when b.is_zinc = 1
                        then b.qty_bom
                             / nullif(b.finished_qty_planned, 0)
                             * coalesce(lp.unit_price_vnd, 0)
                    else
                        b.qty_single
                        / nullif(b.number_children_size, 0)
                        * coalesce(lp.unit_price_vnd, 0)
                end
            as numeric),
        4)                                              as npl_cost_per_sp_vnd

    from bom_all b
    left join {{ source('core', 'dim_product') }} dp
        on dp.product_id = b.npl_product_id
    left join latest_price lp
        on lp.product_id = b.npl_product_id

)

select
    e.bom_line_id,
    e.productions_plan_id,
    e.productions_plan_items_id,
    e.finished_product_id,
    e.finished_qty_planned,
    e.plan_date,
    dd.year,
    dd.month_num                                        as month,
    dd.quarter,
    e.source_order_ids,
    e.plan_status,

    -- NPL material identification
    e.npl_product_id,
    e.npl_code,
    e.npl_name,
    e.unit_id,
    e.material_matched,
    e.product_master_name,
    e.product_master_code,

    -- Zinc / Kem flags
    e.is_zinc,
    e.is_zinc_injected,     -- TRUE = Kem row auto-injected (BOM had no zinc line)

    -- Quantities
    -- qty_single    : quantity_single from BOM (NPL qty per 1 kho giay / run unit)
    -- number_children_size : so SP tren 1 kho giay (N in formula)
    -- qty_per_sp    : (1/N) x qty_single  [= true NPL qty per 1 finished SP]
    --                 Kem: qty_bom / finished_qty  (plates / total SP)
    e.qty_single,
    e.number_children_size,
    e.qty_per_sp,
    e.qty_bom,
    e.qty_compensation,
    e.qty_total,

    -- Price info
    e.unit_price_vnd,
    e.last_po_date,
    e.has_price,

    -- Cost for the whole order (VND)
    -- npl_cost_vnd              : qty_bom x price  (no waste)
    -- waste_cost_per_order_vnd  : qty_compensation x price  (= "Tien bu hao tren 1 Lenh")
    -- npl_cost_with_waste_vnd   : (qty_bom + qty_compensation) x price
    e.npl_cost_vnd,
    e.waste_cost_per_order_vnd,
    e.npl_cost_with_waste_vnd,
    (e.npl_cost_with_waste_vnd - e.npl_cost_vnd)       as waste_cost_vnd,

    -- Cost per SP (VND) -- formula: (1/N) x qty_single x price
    -- Kem: qty_bom x price / finished_qty
    e.npl_cost_per_sp_vnd

from enriched e
left join {{ source('core', 'dim_date') }} dd
    on dd.full_date = e.plan_date
