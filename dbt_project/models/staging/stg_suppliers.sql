-- ============================================================
-- stg_suppliers
-- Source  : core.dim_supplier  (built by Python ELT)
-- Purpose : Thin staging wrapper over dim_supplier.
-- ============================================================

with source as (

    select * from {{ source('core', 'dim_supplier') }}

),

renamed as (

    select
        -- surrogate key
        supplier_key,

        -- natural key from ERP
        supplier_id,
        supplier_code,
        supplier_prefix,

        -- identity
        company,
        abbreviation,
        representative,
        phone,
        email,
        vat,

        -- geography
        address,
        city,
        district,
        ward,
        country,

        -- classification
        groups_in,
        is_active,

        -- metadata
        etl_loaded_at

    from source

)

select * from renamed
