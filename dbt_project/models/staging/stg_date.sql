-- ============================================================
-- stg_date
-- Source  : core.dim_date  (generated once via 03_dim_date.sql)
-- Purpose : Expose the date spine to downstream models.
--           All date-related mart columns should join on date_key.
-- ============================================================

with source as (

    select * from {{ source('core', 'dim_date') }}

)

select
    date_key,           -- INT YYYYMMDD  e.g. 20240115
    full_date,          -- DATE          e.g. 2024-01-15
    day_of_week,        -- 0 = Sunday, 6 = Saturday
    day_name,           -- 'Monday', 'Tuesday', ...
    day_of_month,
    day_of_year,
    week_of_year,
    month_num,          -- 1–12
    month_name,         -- 'January', ...
    quarter,            -- 1–4
    year,
    is_weekend,
    is_holiday

from source
