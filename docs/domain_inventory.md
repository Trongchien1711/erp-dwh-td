# Domain: Inventory

## Mô tả
Domain Inventory theo dõi hàng tồn kho, nhập xuất kho, sản xuất.

## Nguồn MySQL

| Bảng | Mô tả | Loại load |
|------|---------|----------|
| `tblwarehouse` | Danh mục kho | Full load |
| `tbllocaltion_warehouses` | Vị trí trong kho | Full load |
| `tblwarehouse_product` | Tồn kho từng lô | Incremental (date_warehouse) |
| `tblwarehouse_export` | Xuất kho | Incremental (date_warehouse) |
| `tbltransfer_warehouse_detail` | Chuyển kho | Full load |
| `tbl_purchase_products` | Phiếu nhập hàng | Incremental (date) |
| `tbl_purchase_product_items` | Chi tiết nhập hàng | Full load |
| `tbl_productions_orders` | Lệnh sản xuất | Full load |
| `tbl_productions_orders_items` | Chi tiết sản xuất | Full load |
| `tbl_productions_orders_items_stages` | Công đoạn sx | Incremental (date_active) |
| `tbl_manufactures` | Đơn vị sản xuất | Incremental (date) |
| `tbl_productions_plan` | Kế hoạch sản xuất (lệnh plan) | Full load |
| `tbl_productions_plan_items` | Chi tiết kế hoạch SX (sản phẩm) | Full load |
| `tbl_productions_plan_bom` | BOM thực tế của kế hoạch SX | Full load |

## Core Tables

| Table | Grain |
|-------|-------|
| `core.fact_warehouse_stock` | 1 dòng = 1 lô hàng trong kho |
| `core.fact_purchase_product_items` | 1 dòng = 1 item trong phiếu nhập |
| `core.fact_transfer_warehouse` | 1 dòng = 1 item chuyển kho |
| `core.fact_production_order_items` | 1 dòng = 1 item lệnh sx |
| `core.fact_production_stages` | 1 dòng = 1 công đoạn sx |

## Dimensions dùng
- `dim_product` — sản phẩm
- `dim_warehouse` — kho
- `dim_warehouse_location` — vị trí trong kho
- `dim_supplier` — nhà cung cấp
- `dim_staff` — công nhân sx
- `dim_manufacture` — đơn vị sx
- `dim_date` — ngày

## KPIs quan trọng
- Tồn kho hiện tại (`quantity_left`) theo kho / sản phẩm
- Giá trị tồn kho (`quantity_left * price`)
- Số ngày tồn (Days on Hand)
- Hiệu suất sản xuất (actual vs plan quantity)
- Công nhân năng suất so sánh (`number_hours` / `number`)

## Data Quality Notes
- `fact_transfer_warehouse.product_key`: ~3.03% NULL — sản phẩm bị xoá khỏi ERP trước khi DWH tồn tại (irrecoverable)
- `fact_warehouse_stock.product_key`: ~1.01% NULL — tương tự, irrecoverable
- `fact_production_stages.total_hours`: luôn = 0 — field `number_hours` không được nhập liệu trong ERP
- `fact_warehouse_stock.location_key`: đã backfill đầy đủ (845,078 rows, commit e4ec364)

## dbt Mart Target
`mart.inventory` — models:

| Model | Grain | Rows (~) | Mô tả |
|-------|-------|----------|--------|
| `fct_stock_snapshot` | lô hàng × kho | 845,079 | Tồn kho chi tiết theo lô |
| `fct_inbound_outbound` | ngày × SP × kho × loại | 197,980 | Luồng nhập/xuất kho |
| `fct_production_efficiency` | lệnh SX × ngày công đoạn | 196,146 | KH vs thực tế sản xuất |
| `fct_production_npl_cost` | plan × plan_item × vật liệu BOM | 189,466 | Chi phí NPL từng dòng BOM. Giá = PO gần nhất. Waste multiplier 5%. |
| `fct_order_npl_cost` | 1 đơn hàng | 48,109 | NPL cost phân bổ về đơn hàng theo số lượng (BC_SP formula). Weighted NPL%  ~27% (2025). |

## NPL Cost Logic

Phân bổ chi phí NPL (nguyên phụ liệu) về từng đơn hàng:

```
fct_production_npl_cost  →  plan BOM cost per material line
                                 │
                                 ▼
fct_order_npl_cost       ←  phân bổ theo tỉ lệ số lượng:
  alloc_ratio = order_product_qty / SUM(order_product_qty trong plan)
  (Fallback: revenue share → equal split)
```

Khớp với công thức BC_SP: `variable_cost = (1/N) × conversion_value × price × order_qty`

Data quality flags: `normal` (≤150%) / `high_cost` (150-500%) / `suspect_data` (>500%) / `no_revenue`

