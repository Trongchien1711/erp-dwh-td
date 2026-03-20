-- ============================================================
-- dim_customer_mart
-- Domain  : Shared / All domains
-- Grain   : One row per customer
-- Purpose : Clean, analysis-ready customer dimension for BI tools.
--           Includes only columns relevant to reporting — strips
--           ELT metadata and renames technical columns for clarity.
-- ============================================================

with customers as (

    select * from {{ ref('stg_customers') }}

)

select
    -- ── keys ───────────────────────────────────────────────────
    customer_key,
    customer_id,
    customer_code,

    -- ── identity ───────────────────────────────────────────────
    prefix_client,
    company,
    company_short,
    representative,
    fullname                    as customer_name,
    phonenumber,
    email,

    -- ── classification ─────────────────────────────────────────
    type_client,
    vip_rating,
    price_group_code,
    price_group_name,
    status_clients,
    is_active,

    -- ── geography ──────────────────────────────────────────────
    city,
    district,
    address,

    -- ── financials ─────────────────────────────────────────────
    debt_limit,
    debt_limit_day,
    discount,
    time_payment,

    -- ── dates ──────────────────────────────────────────────────
    datecreated                 as customer_created_at

from customers
