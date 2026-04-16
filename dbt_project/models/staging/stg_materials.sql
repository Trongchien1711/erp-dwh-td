-- ============================================================
-- stg_materials
-- Source  : core.dim_material  (built by Python ELT from tbl_materials)
-- Purpose : Thin staging wrapper over dim_material.
--           One row per NPL (nguyen phu lieu) item.
-- ============================================================

with source as (

    select * from {{ source('core', 'dim_material') }}

),

renamed as (

    select
        -- surrogate key
        material_key,

        -- natural key from ERP
        material_id,
        material_code,

        -- names
        material_name,
        material_name_supplier,

        -- classification
        category_id,
        unit_id,
        species,
        is_zinc,            -- True = Kem in (zinc offset plate)
        mode_id,

        -- pricing
        price_import,

        -- physical dimensions
        longs,
        wide,
        height,

        -- flags
        is_single_use,
        is_active,

        -- metadata
        id_branch,
        date_created,
        date_updated,
        etl_loaded_at

    from source

)

select * from renamed
