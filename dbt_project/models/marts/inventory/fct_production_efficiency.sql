-- ============================================================
-- fct_production_efficiency
-- Domain  : Inventory / Production
-- Grain   : One row per (production order × product × stage date)
-- Purpose : Production performance — plan vs actual quantity,
--           worker productivity, and stage-level time tracking.
--           Feeds production KPIs: efficiency, utilisation,
--           and output-per-worker-hour dashboards.
--
-- Key metrics:
--   qty_planned       — planned production quantity (order items)
--   qty_produced      — actual output (sum of stage output)
--   efficiency_pct    — qty_produced / qty_planned × 100
--   total_workers     — headcount assigned to this order-date
--   total_hours       — total worker-hours logged
--   output_per_hour   — qty_produced / total_hours
--
-- ⚠ WARNING — do NOT SUM(qty_planned) across rows without filtering.
--   Because grain includes stage_date, each production order × product
--   appears once per distinct stage date. qty_planned repeats for every
--   stage date of the same order → SUM over-counts planned quantities.
--   Use MAX(qty_planned) or filter to a single stage_date when
--   aggregating planned totals.
-- ============================================================

with prod_items as (

    select
        pi.prod_item_key,
        pi.prod_item_id,
        pi.productions_orders_id,
        pi.product_key,
        pi.prod_date_key,
        pi.quantity                     as qty_planned,
        pi.type_items,
        pi.versions_bom,
        pi.versions_stage

    from {{ source('core', 'fact_production_order_items') }} pi

),

prod_stages as (

    select
        ps.productions_orders_id,
        ps.stage_date_key,
        ps.staff_key,
        sum(ps.number)                  as total_workers,
        sum(ps.number_hours)            as total_hours,
        sum(ps.total_time)              as total_time,
        sum(ps.number_face)             as total_face_output,
        sum(ps.number_operations)       as total_operations,
        count(*)                        as stage_count

    from {{ source('core', 'fact_production_stages') }} ps
    where ps.active = 1
    group by
        ps.productions_orders_id,
        ps.stage_date_key,
        ps.staff_key

),

-- aggregate stages up to order level for efficiency calc
order_stage_totals as (

    select
        productions_orders_id,
        stage_date_key,
        sum(total_workers)              as total_workers,
        sum(total_hours)                as total_hours,
        sum(total_time)                 as total_time,
        sum(total_face_output)          as qty_produced,
        sum(total_operations)           as total_operations,
        count(distinct staff_key)       as distinct_workers

    from prod_stages
    group by productions_orders_id, stage_date_key

),

joined as (

    select
        pi.prod_item_key,
        pi.prod_item_id,
        pi.productions_orders_id,
        pi.product_key,
        pi.prod_date_key,
        pi.qty_planned,
        pi.type_items,
        pi.versions_bom,
        pi.versions_stage,

        -- stage activity on each date
        os.stage_date_key,
        coalesce(os.total_workers, 0)   as total_workers,
        coalesce(os.distinct_workers, 0) as distinct_workers,
        coalesce(os.total_hours, 0)     as total_hours,
        coalesce(os.total_time, 0)      as total_time,
        coalesce(os.qty_produced, 0)    as qty_produced,
        coalesce(os.total_operations, 0) as total_operations

    from prod_items pi
    left join order_stage_totals os
        using (productions_orders_id)

),

final as (

    select
        -- ── identifiers ────────────────────────────────────────
        j.prod_item_key,
        j.prod_item_id,
        j.productions_orders_id,

        -- ── time ───────────────────────────────────────────────
        j.prod_date_key,
        pd.full_date                                as prod_date,
        pd.year,
        pd.quarter,
        pd.month_num,
        pd.month_name,

        -- stage activity may be on a different date
        j.stage_date_key,
        sd.full_date                                as stage_date,

        -- ── product ────────────────────────────────────────────
        j.product_key,
        p.product_code,
        p.product_name,
        p.type_products,
        p.brand,
        j.type_items,
        j.versions_bom,
        j.versions_stage,

        -- ── production measures ────────────────────────────────
        j.qty_planned,
        j.qty_produced,

        round(
            j.qty_produced / nullif(j.qty_planned, 0) * 100,
            2
        )                                           as efficiency_pct,

        -- ── workforce metrics ──────────────────────────────────
        j.total_workers,
        j.distinct_workers,
        j.total_hours,
        j.total_time,
        j.total_operations,

        -- output per worker-hour (labour productivity)
        round(
            j.qty_produced / nullif(j.total_hours, 0),
            4
        )                                           as output_per_hour

    from joined j
    left join {{ ref('stg_date') }} pd on pd.date_key = j.prod_date_key
    left join {{ ref('stg_date') }} sd on sd.date_key = j.stage_date_key
    left join {{ ref('stg_products') }} p using (product_key)

)

select * from final
