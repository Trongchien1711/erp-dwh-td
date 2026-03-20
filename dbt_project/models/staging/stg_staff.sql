-- ============================================================
-- stg_staff
-- Source  : core.dim_staff  (built by Python ELT)
-- Purpose : Thin staging wrapper over dim_staff.
--
-- Note    : "role" is renamed to staff_role to avoid collision
--           with the PostgreSQL reserved word ROLE.
-- ============================================================

with source as (

    select * from {{ source('core', 'dim_staff') }}

),

renamed as (

    select
        -- surrogate key
        staff_key,

        -- natural key from ERP
        staff_id,
        staff_code,

        -- identity
        firstname,
        lastname,
        fullname,
        email,
        phonenumber,
        gender,

        -- employment
        birthday,
        day_in,
        status_work,
        role            as staff_role,   -- renamed from reserved word
        admin           as is_admin,     -- clearer boolean name
        id_branch,
        department_id,
        is_active,

        -- metadata
        etl_loaded_at

    from source

)

select * from renamed
