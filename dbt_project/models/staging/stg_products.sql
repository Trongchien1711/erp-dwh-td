-- ============================================================
-- stg_products
-- Source  : core.dim_product  (built by Python ELT)
-- Purpose : Thin staging wrapper over dim_product.
-- ============================================================

with source as (

    select * from {{ source('core', 'dim_product') }}

),

renamed as (

    select
        -- surrogate key
        product_key,

        -- natural key from ERP
        product_id,
        product_code,

        -- names
        product_name,
        product_name_customer,

        -- classification
        type_products,
        category_id,
        unit_id,
        species,
        brand,
        brand_id,

        -- pricing
        price_import,
        price_sell,
        price_processing,
        loss,

        -- manufacturing
        bom_id,
        versions,
        versions_stage,

        -- physical dimensions
        longs,
        wide,
        height,

        -- other
        warranty,
        status,
        id_branch,
        is_no_stock,
        conversion_unit,
        conversion_quantity_unit,
        is_active,

        -- metadata
        date_created,
        etl_loaded_at

    from source

)

select * from renamed
