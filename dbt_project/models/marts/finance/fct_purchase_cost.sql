-- ============================================================
-- fct_purchase_cost
-- Domain  : Finance
-- Grain   : One row per (po_date × supplier × product)
-- Purpose : Purchase cost summary — tracks procurement spending
--           by supplier, product, and period.
--           Feeds supplier performance, cost trend, and
--           COGS vs purchase-price variance dashboards.
--
-- Key metrics:
--   qty_ordered       — total units ordered to supplier
--   expected_cost     — budgeted cost (price_expected × quantity)
--   actual_cost       — invoiced cost  (total_suppliers)
--   price_variance    — actual_cost − expected_cost
--   unit_cost         — average unit cost across lines
--   po_count          — number of distinct purchase orders
-- ============================================================

with po_items as (

    select * from {{ source('core', 'fact_purchase_order_items') }}

),

suppliers as (

    select
        supplier_key,
        supplier_code,
        company         as supplier_name,
        city,
        groups_in,
        is_active

    from {{ ref('stg_suppliers') }}

),

products as (

    select
        product_key,
        product_code,
        product_name,
        type_products,
        category_id,
        brand,
        price_import

    from {{ ref('stg_products') }}

),

dates as (

    select
        date_key,
        full_date,
        year,
        quarter,
        month_num,
        month_name

    from {{ ref('stg_date') }}

),

final as (

    select
        -- ── time ───────────────────────────────────────────────
        p.po_date_key,
        d.full_date         as po_date,
        d.year,
        d.quarter,
        d.month_num,
        d.month_name,

        -- ── supplier ───────────────────────────────────────────
        p.supplier_key,
        s.supplier_code,
        s.supplier_name,
        s.city              as supplier_city,
        s.groups_in         as supplier_group,

        -- ── product ────────────────────────────────────────────
        p.product_key,
        pr.product_code,
        pr.product_name,
        pr.type_products,
        pr.category_id,
        pr.brand,
        pr.price_import     as std_import_price,

        -- ── purchase measures ──────────────────────────────────
        count(distinct p.po_id)             as po_count,
        count(*)                            as line_count,
        sum(p.quantity)                     as qty_ordered,
        sum(p.quantity_suppliers)           as qty_received,

        -- costs
        sum(p.total_expected)               as expected_cost,
        sum(p.total_suppliers)              as actual_cost,
        sum(p.total_suppliers)
            - sum(p.total_expected)         as price_variance,

        -- unit cost (weighted average)
        round(
            sum(p.total_suppliers)
            / nullif(sum(p.quantity_suppliers), 0),
            4
        )                                   as avg_unit_cost,

        -- tax
        sum(p.subtotal)                     as subtotal,
        sum(p.total_expected)
            - sum(p.subtotal)               as tax_amount,

        -- fulfilment gap
        sum(p.quantity)
            - sum(p.quantity_suppliers)     as qty_gap

    from po_items p
    left join suppliers s using (supplier_key)
    left join products  pr using (product_key)
    left join dates     d on d.date_key = p.po_date_key
    group by
        p.po_date_key,
        d.full_date,
        d.year,
        d.quarter,
        d.month_num,
        d.month_name,
        p.supplier_key,
        s.supplier_code,
        s.supplier_name,
        s.city,
        s.groups_in,
        p.product_key,
        pr.product_code,
        pr.product_name,
        pr.type_products,
        pr.category_id,
        pr.brand,
        pr.price_import

)

select * from final
