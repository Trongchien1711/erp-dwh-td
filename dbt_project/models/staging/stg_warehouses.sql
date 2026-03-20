-- ============================================================
-- stg_warehouses
-- Source  : core.dim_warehouse  (built by Python ELT)
-- Purpose : Thin staging wrapper over dim_warehouse.
-- ============================================================

with source as (

    select * from {{ source('core', 'dim_warehouse') }}

),

renamed as (

    select
        -- surrogate key
        warehouse_key,

        -- natural key from ERP
        warehouse_id,
        warehouse_code,
        warehouse_name,

        -- location & grouping
        address,
        id_group_warehouse,
        id_branch,
        supplier_id,

        -- metadata
        etl_loaded_at

    from source

)

select * from renamed
