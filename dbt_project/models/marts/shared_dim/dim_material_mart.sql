-- ============================================================
-- dim_material_mart
-- Domain  : Shared / Inventory / Production
-- Grain   : One row per NPL (nguyen phu lieu) item
-- Purpose : Analysis-ready NPL dimension for BI tools.
--           Strips ELT metadata, renames technical fields,
--           adds category labels and NPL type flags.
-- ============================================================

with materials as (

    select * from {{ ref('stg_materials') }}

),

final as (

    select
        -- ── keys ───────────────────────────────────────────────
        material_key,
        material_id,

        -- ── identification ─────────────────────────────────────
        material_code,
        material_name,
        material_name_supplier,

        -- ── classification ─────────────────────────────────────
        category_id,
        unit_id,
        species,
        mode_id,

        -- ── NPL type flags ─────────────────────────────────────
        -- is_zinc: Kem in offset (zinc plate) — fixed integer qty per order
        is_zinc,

        -- is_giay: Giay cac loai (paper-family materials)
        case
            when material_name ilike 'Gi%y %'             then true
            when material_name ilike '%(kh%ng%d%ng)Gi%y%' then true
            when material_name ilike 'Decal Gi%y%'        then true
            when material_name ilike 'D%y B%ng Gi%y%'     then true
            else false
        end                                                     as is_giay,

        -- npl_type: high-level grouping for BI slicers
        case
            when is_zinc                                   then 'Kem in'
            when material_name ilike 'Gi%y %'             then 'Giay'
            when material_name ilike 'Decal%'              then 'Decal'
            when material_name ilike 'Muc%' or material_name ilike 'UV%'
                                                           then 'Muc / UV'
            when material_name ilike 'Bao bi%' or material_name ilike 'Tui%'
                                                           then 'Bao bi'
            when material_name ilike 'Bang keo%' or material_name ilike 'Bang d%n%'
                                                           then 'Bang keo / Dan'
            when material_name ilike 'Hop%' or material_name ilike 'Carton%'
                or material_name ilike 'VTDG%'            then 'Vat tu dong goi'
            else 'Khac'
        end                                                     as npl_type,

        -- ── pricing ────────────────────────────────────────────
        price_import,

        -- ── physical dimensions ────────────────────────────────
        longs,
        wide,
        height,

        -- ── flags ──────────────────────────────────────────────
        is_single_use,
        is_active,
        id_branch

    from materials

)

select * from final
