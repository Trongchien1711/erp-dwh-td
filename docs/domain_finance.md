# Domain: Finance

## Mô tả
Domain Finance theo dõi chi phí mua hàng, công nợ khách hàng, lợi nhuận.

## Nguồn MySQL

| Bảng | Mô tả | Loại load |
|------|---------|----------|
| `tblpurchase_order` | Header đơn đặt mua | Incremental (date_create) |
| `tblpurchase_order_items` | Chi tiết đơn đặt mua | Full load |
| `tblsuppliers` | Danh mục nhà cung cấp | Full load |
| `tblclients` | Khách hàng (có debt_limit) | Full load |

## Core Tables

| Table | Grain | Liên kết Finance |
|-------|-------|-------------------|
| `core.fact_purchase_order_items` | 1 item đơn mua | Chi phí nhập hàng |
| `core.fact_orders` | 1 đơn bán | Doanh thu, lợi nhuận |
| `core.dim_customer` | Khách hàng | Hạn mức công nợ |

## Dimensions dùng
- `dim_supplier` — nhà cung cấp
- `dim_customer` — khách hàng (debt_limit, debt_limit_day)
- `dim_product` — sản phẩm (price_import, price_sell)
- `dim_date`

## KPIs quan trọng
- Tổng giá trị nhập hàng theo nhà cung cấp / tháng
- Chi phí vốn hàng bán (COGS = `total_cost`)
- Lợi nhuận gộp (`total_profit` = `grand_total` - `total_cost`)
- Tặng COGS so với doanh thu (%)
- Khách hàng gần vượt hạn mức công nợ

## dbt Mart Target
`mart.finance` — models: `fct_purchase_cost`, `fct_gross_profit`, `dim_customer_credit`

