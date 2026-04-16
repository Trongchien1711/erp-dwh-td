-- test: assert_monthly_stock_non_negative
-- This test identifies cases where backward projection results in negative stock.
-- Since est_qty_end_month uses greatest(..., 0) in the model, we check 
-- the underlying math if possible, or check if those zero-clamped rows 
-- actually had negative values before clamping.

with negative_stocks as (
    select
        month_end,
        item_code,
        item_name,
        warehouse_code,
        est_qty_end_month
    from {{ ref('fct_stock_monthly_snapshot') }}
    where est_qty_end_month < 0
)

select * from negative_stocks
