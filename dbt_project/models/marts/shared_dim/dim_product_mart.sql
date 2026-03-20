-- ============================================================
-- dim_product_mart
-- Domain  : Shared / All domains
-- Grain   : One row per product SKU
-- Purpose : Clean product dimension for BI tools.
-- ============================================================

with products as (

    select * from {{ ref('stg_products') }}

)

select
    -- ── keys ───────────────────────────────────────────────────
    product_key,
    product_id,
    product_code,

    -- ── names ──────────────────────────────────────────────────
    product_name,
    product_name_customer,

    -- ── classification ─────────────────────────────────────────
    type_products,
    category_id,
    brand,
    brand_id,
    unit_id,
    species,

    -- ── pricing ────────────────────────────────────────────────
    price_import,
    price_sell,
    price_processing,

    -- ── physical attributes ─────────────────────────────────────
    longs,
    wide,
    height,
    warranty,

    -- ── manufacturing ──────────────────────────────────────────
    bom_id,
    versions,
    versions_stage,
    loss,
    conversion_unit,
    conversion_quantity_unit,

    -- ── status ─────────────────────────────────────────────────
    status,
    id_branch,
    is_no_stock,
    is_active,

    -- ── dates ──────────────────────────────────────────────────
    date_created                as product_created_at

from products
