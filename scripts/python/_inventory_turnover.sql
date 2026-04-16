-- Vong quay ton kho (Inventory Turnover) — 12 thang gan nhat
-- NVL: COGS proxy = outbound qty x weighted avg unit_price
-- Product: chi tinh theo so luong (ERP khong co gia lo san pham)

WITH
nvl_price AS (
    SELECT material_key,
           round(sum(stock_value) / nullif(sum(quantity_left),0), 4) AS unit_price
    FROM mart.fct_stock_snapshot
    WHERE item_type = 'nvl' AND quantity_left > 0 AND stock_value IS NOT NULL
    GROUP BY material_key
),
nvl_cogs AS (
    SELECT
        round(sum(io.quantity_out * np.unit_price)) AS cogs_12m,
        round(sum(io.quantity_out))                  AS qty_consumed_12m
    FROM mart.fct_inbound_outbound io
    LEFT JOIN nvl_price np USING (material_key)
    WHERE io.item_type        = 'nvl'
      AND io.movement_type    = 'OUTBOUND'
      AND io.movement_subtype != '17'   -- loai chuyen kho noi bo
      AND io.movement_date >= date_trunc('month', current_date) - interval '12 months'
      AND io.movement_date <  date_trunc('month', current_date)
),
nvl_avg_inventory AS (
    SELECT round(avg(monthly_val)) AS avg_inventory_value
    FROM (
        SELECT month_end, sum(est_value_end_month) AS monthly_val
        FROM mart.fct_stock_monthly_snapshot
        WHERE item_type = 'nvl'
          AND is_valued  = TRUE
          AND month_end >= date_trunc('month', current_date) - interval '12 months'
          AND month_end <  date_trunc('month', current_date)
        GROUP BY month_end
    ) t
),
product_qty AS (
    SELECT round(sum(io.quantity_out)) AS qty_consumed_12m
    FROM mart.fct_inbound_outbound io
    WHERE io.item_type        = 'product'
      AND io.movement_type    = 'OUTBOUND'
      AND io.movement_subtype != '17'   -- loai chuyen kho noi bo
      AND io.movement_date >= date_trunc('month', current_date) - interval '12 months'
      AND io.movement_date <  date_trunc('month', current_date)
),
product_avg_stock AS (
    SELECT round(avg(monthly_qty)) AS avg_stock_qty
    FROM (
        SELECT month_end, sum(est_qty_end_month) AS monthly_qty
        FROM mart.fct_stock_monthly_snapshot
        WHERE item_type = 'product'
          AND month_end >= date_trunc('month', current_date) - interval '12 months'
          AND month_end <  date_trunc('month', current_date)
        GROUP BY month_end
    ) t
)
SELECT
    'NVL'                    AS item_type,
    nc.cogs_12m              AS cogs_vnd_12m,
    ai.avg_inventory_value   AS avg_inventory_vnd,
    round(nc.cogs_12m::numeric / nullif(ai.avg_inventory_value,0), 2)          AS inventory_turnover,
    round(365 / nullif(nc.cogs_12m::numeric / nullif(ai.avg_inventory_value,0), 0), 0) AS days_on_hand,
    nc.qty_consumed_12m      AS qty_consumed,
    NULL::bigint             AS avg_stock_qty
FROM nvl_cogs nc, nvl_avg_inventory ai
UNION ALL
SELECT
    'Product (qty basis)'    AS item_type,
    NULL, NULL,
    round(pq.qty_consumed_12m::numeric / nullif(pas.avg_stock_qty,0), 2)          AS inventory_turnover,
    round(365 / nullif(pq.qty_consumed_12m::numeric / nullif(pas.avg_stock_qty,0), 0), 0) AS days_on_hand,
    pq.qty_consumed_12m,
    pas.avg_stock_qty
FROM product_qty pq, product_avg_stock pas;
