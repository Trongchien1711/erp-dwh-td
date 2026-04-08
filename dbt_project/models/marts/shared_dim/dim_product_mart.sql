-- ============================================================
-- dim_product_mart
-- Domain  : Shared / All domains
-- Grain   : One row per product SKU
-- Purpose : Clean product dimension for BI tools.
-- ============================================================

with products as (

    select * from {{ ref('stg_products') }}

),

actual_prices as (

    -- Giá bán thực tế tính ngược từ toàn bộ lịch sử giao dịch.
    -- Dùng unit_price_adjusted (total_amount / qty, đã cap spike >3x) thay vì
    -- price_sell từ ERP vốn không được populate.
    select
        product_key,
        round(
            sum(unit_price_adjusted * quantity)
            / nullif(sum(quantity), 0),
            2
        )                                                           as avg_sell_price_actual,
        round(
            cast(
                percentile_cont(0.5) within group (
                    order by unit_price_adjusted
                ) as numeric
            ),
            2
        )                                                           as median_sell_price_actual,
        round(
            cast(
                (array_agg(unit_price_adjusted order by order_date desc))[1]
            as numeric),
            2
        )                                                           as last_sell_price_actual,
        count(distinct order_id)                                    as sold_order_count_12m

    from {{ ref('fct_order_items_detail') }}
    where unit_price_adjusted > 0
    group by product_key

),

product_price_thresholds as (

    -- Nguong p33/p67 tinh tren trung binh gia ban tung SKU.
    -- Khac voi fct_order_items_detail (tinh tren tung dong hang),
    -- day phan tang o cap san pham de dung trong dim.
    select
        percentile_cont(0.33) within group (
            order by avg_sell_price_actual
        ) as p33,
        percentile_cont(0.67) within group (
            order by avg_sell_price_actual
        ) as p67
    from actual_prices
    where avg_sell_price_actual > 0

)

select
    -- ── keys ───────────────────────────────────────────────────
    product_key,
    product_id,
    product_code,

    -- ── names ──────────────────────────────────────────────────
    product_name,
    product_name_customer,

    -- ── classification ─────────────────────────────────────────
    type_products,
    category_id,
    brand,
    brand_id,
    unit_id,
    species,

    -- ── pricing (ERP static — gần như = 0, không populate trong ERP) ──
    price_import,
    price_sell,
    price_processing,

    -- ── pricing (thực tế từ toàn bộ lịch sử giao dịch) ───────
    ap.avg_sell_price_actual,
    ap.median_sell_price_actual,
    ap.last_sell_price_actual,
    ap.sold_order_count_12m,

    -- ── price tier (phan tang theo gia ban trung binh SKU) ─────
    -- 'Low'  : avg_sell_price_actual <= p33 (SKU gia re)
    -- 'Mid'  : p33 < avg <= p67
    -- 'High' : avg_sell_price_actual > p67  (SKU gia cao)
    -- NULL   : chua tung co giao dich
    case
        when ap.avg_sell_price_actual is null
          or ap.avg_sell_price_actual  = 0    then null
        when ap.avg_sell_price_actual <= pt.p33 then 'Low'
        when ap.avg_sell_price_actual <= pt.p67 then 'Mid'
        else                                         'High'
    end                                             as price_tier,

    -- ── physical attributes ─────────────────────────────────────
    longs,
    wide,
    height,
    warranty,

    -- ── manufacturing ──────────────────────────────────────────
    bom_id,
    versions,
    versions_stage,
    loss,
    conversion_unit,
    conversion_quantity_unit,

    -- ── status ─────────────────────────────────────────────────
    status,
    id_branch,
    is_no_stock,
    is_active,

    -- ── dates ──────────────────────────────────────────────────
    date_created                as product_created_at

from products
left join actual_prices             ap using (product_key)
cross join product_price_thresholds pt
