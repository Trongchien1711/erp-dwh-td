-- ============================================================
-- fct_production_sales_reconciliation
-- Domain  : Inventory / Production
-- Grain   : One row per (date_key × product_key)
-- Purpose : Direct comparison of factory output vs sales.
--           Provides the "True Inventory Turnover" by filtering
--           out internal transfers and focusing on finished goods.
-- ============================================================

{{ config(materialized="table") }}

with production as (

    -- Actual production output from the floor logs (Nhật ký sản xuất)
    select
        ps.stage_date_key as date_key,
        poi.product_key,
        sum(ps.number_face) as qty_produced,
        count(distinct ps.productions_orders_id) as prod_order_count

    from {{ source('core', 'fact_production_stages') }} ps
    inner join {{ source('core', 'fact_production_order_items') }} poi
        on ps.productions_orders_items_id = poi.prod_item_id
    where ps.active = 1
    group by 1, 2

),

sales as (

    -- Actual sales output from delivery line items
    select
        delivery_date_key as date_key,
        product_key,
        sum(quantity) as qty_sold,
        count(distinct delivery_id) as delivery_count

    from {{ source('core', 'fact_delivery_items') }}
    group by 1, 2

),

calendar as (

    select distinct
        date_key,
        full_date,
        year,
        month_num,
        month_name,
        (date_trunc('month', full_date) + interval '1 month - 1 day')::date as month_end
    from {{ ref('stg_date') }}
    where full_date >= '2023-01-01'
      and full_date <= current_date

),

products as (

    select
        product_key,
        product_code,
        product_name,
        type_products,
        brand
    from {{ ref('stg_products') }}

),

-- Monthly average stock for turnover calculation
monthly_avg_stock as (

    select
        month_end,
        product_key,
        avg(est_qty_end_month) as avg_monthly_qty
    from {{ ref('fct_stock_monthly_snapshot') }}
    where item_type = 'product'
    group by 1, 2

),

joined as (

    select
        c.date_key,
        c.month_end,
        p.product_key,
        coalesce(pr.qty_produced, 0) as qty_produced,
        coalesce(s.qty_sold, 0) as qty_sold,
        pr.prod_order_count,
        s.delivery_count

    from calendar c
    cross join products p
    left join production pr
        on c.date_key = pr.date_key
        and p.product_key = pr.product_key
    left join sales s
        on c.date_key = s.date_key
        and p.product_key = s.product_key
    where pr.qty_produced > 0 or s.qty_sold > 0

),

final as (

    select
        j.date_key,
        c.full_date,
        c.year,
        c.month_num,
        c.month_name,
        j.product_key,
        p.product_code,
        p.product_name,
        p.type_products,
        p.brand,

        -- Measures
        j.qty_produced,
        j.qty_sold,
        j.qty_produced - j.qty_sold as production_sales_variance,
        
        -- Rolling 12-month sales for turnover granularity
        sum(j.qty_sold) over (
            partition by j.product_key 
            order by j.date_key 
            rows between 364 preceding and current row
        ) as rolling_12m_sales,

        -- Average stock from the monthly snapshot
        mas.avg_monthly_qty as avg_stock_level,

        -- Inventory Turnover (Targeting 0.6 - 0.83 range)
        -- Formula: Rolling 12M Sales / Current month average stock
        case 
            when coalesce(mas.avg_monthly_qty, 0) > 0 
            then round(
                (sum(j.qty_sold) over (
                    partition by j.product_key 
                    order by j.date_key 
                    rows between 364 preceding and current row
                ))::numeric / mas.avg_monthly_qty, 
                2
            )
            else null 
        end as inventory_turnover,

        j.prod_order_count,
        j.delivery_count

    from joined j
    left join calendar c on j.date_key = c.date_key
    left join products p on j.product_key = p.product_key
    left join monthly_avg_stock mas 
        on j.month_end = mas.month_end 
        and j.product_key = mas.product_key

)

select * from final
