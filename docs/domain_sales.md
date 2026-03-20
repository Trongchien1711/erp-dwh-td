# Domain: Sales

## Mô tả
Domain Sales theo dõi toàn bộ vòng đời đơn hàng bán: tạo đơn → xử lý → giao hàng → thanh toán.

## Nguồn MySQL

| Bảng | Mô tả | Loại load |
|------|---------|----------|
| `tbl_orders` | Header đơn hàng | Incremental (date) |
| `tbl_order_items` | Chi tiết sản phẩm trong đơn | Full load |
| `tbl_order_items_stages` | Trạng thái từng item | Full load |
| `tbl_deliveries` | Header phiếu giao hàng | Incremental (date) |
| `tbl_delivery_items` | Chi tiết giao hàng | Full load |

## Core Tables

| Table | Grain |
|-------|-------|
| `core.fact_orders` | 1 dòng = 1 đơn hàng |
| `core.fact_order_items` | 1 dòng = 1 sản phẩm trong đơn |
| `core.fact_delivery_items` | 1 dòng = 1 sản phẩm giao hàng |

## Dimensions dùng
- `dim_customer` — khách hàng
- `dim_product` — sản phẩm
- `dim_staff` — nhân viên bán hàng
- `dim_date` — ngày đơn

## KPIs quan trọng
- Doanh thu (`grand_total`) theo ngày / tháng / quý
- Số đơn hàng, giá trị trung bình / đơn
- Lợi nhuận (`total_profit`) theo sản phẩm / khách hàng
- Tỷ lệ giao hàng đú́ng hạn
- Tồn đọ́ng (`quantity_not_delivery`)

## dbt Mart Target
`mart.sales` — models: `fct_revenue`, `fct_order_performance`, `dim_customer_segmentation`

