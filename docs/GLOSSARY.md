# Data Glossary

Tài liệu giải thích ý nghĩa các trường hay bị nhầm lẫn, quy ước đặt tên, và mapping giữa MySQL (ERP nguồn) → PostgreSQL (DWH).

## Mục lục

1. [Kiến trúc layer](#1-kiến-trúc-layer)
2. [Quy ước khóa](#2-quy-ước-khóa)
3. [Cột thường bị nhầm lẫn](#3-cột-thường-bị-nhầm-lẫn)
4. [Mapping MySQL → Core Schema](#4-mapping-mysql--core-schema)
5. [Trạng thái (Status codes)](#5-trạng-thái-status-codes)
6. [Các trường nullable hệ thống](#6-các-trường-nullable-hệ-thống)

---

## 1. Kiến trúc layer

| Layer | Schema PostgreSQL | Vai trò | Refresh |
|-------|-------------------|---------|---------|
| Staging | `staging` | Bản sao thô từ MySQL | TRUNCATE + COPY hàng ngày |
| Core | `core` | Star schema chuẩn hóa | UPSERT / INSERT incremental |
| Mart (dbt) | `mart` | Aggregated, BI-ready | Rebuild hàng ngày sau core |
| Staging dbt | `staging_dbt` | dbt views trên core | Phụ thuộc core |

---

## 2. Quy ước khóa

### Surrogate Key (khóa thay thế)
- **Kiểu**: `INTEGER` auto-increment (`SERIAL` hoặc sequence)
- **Quy tắc tên**: `{entity}_key` — ví dụ `customer_key`, `product_key`
- **Mục đích**: Khóa kỹ thuật trong DWH, không xuất hiện trong ERP nguồn

### Natural Key (khóa tự nhiên)
- **Kiểu**: `INTEGER` hoặc `VARCHAR` đến từ MySQL
- **Quy tắc tên**: `{entity}_id` — ví dụ `customer_id`, `product_id`
- **Mục đích**: ID gốc từ ERP, dùng để JOIN và lookup

### Date Key
- **Kiểu**: `INTEGER`
- **Định dạng**: `YYYYMMDD` — ví dụ `20260320` = ngày 20/03/2026
- **Giá trị NULL**: `19000101` (ngày 01/01/1900) — đại diện cho giá trị NULL
- **Quy tắc tên**: `{context}_date_key` — ví dụ `order_date_key`, `delivery_date_key`

---

## 3. Cột thường bị nhầm lẫn

### Doanh thu và lợi nhuận

| Trường | Bảng | Mô tả | Lưu ý |
|--------|------|-------|-------|
| `grand_total` | fact_orders / tbl_orders | Tổng tiền đơn hàng sau thuế, phí ship | Giá trị tin cậy |
| `total_amount` | fact_order_items | Tổng tiền từng dòng sản phẩm | = `quantity × price - discount` |
| `total_cost` | fact_orders | Giá vốn hàng bán (COGS) | ~99% NULL trong ERP vì module COGS chưa cấu hình |
| `price_import` | dim_product | Giá nhập kho tiêu chuẩn | Hầu hết = 0 trong ERP thực tế |
| `total_profit` | fact_orders | Lợi nhuận gộp = grand_total - total_cost | NULL khi total_cost NULL |
| `profit_temporary_capital` | fact_order_items | Lợi nhuận ước tính dòng sản phẩm | Tạm tính, không chính xác |

> **Kết luận**: Không dùng `total_cost` / `total_profit` để phân tích COGS — module chưa được cấu hình trong ERP này.

---

### Thanh toán

| Trường | Bảng | Mô tả | Lưu ý |
|--------|------|-------|-------|
| `status_payment` | tbl_orders | Trạng thái thanh toán (0/1/2) | Định nghĩa: 0=Chưa TT, 1=TT một phần, 2=Đã TT |
| `total_payment` | tbl_orders | Số tiền đã thu | ~100% = 0 — module AR chưa kết nối |
| `debt_limit` | tblclients | Hạn mức tín dụng khách hàng | Hầu hết = 0 — module credit không dùng |

---

### Kho và số lượng

| Trường | Bảng | Mô tả |
|--------|------|-------|
| `quantity` | tbl_delivery_items / tblwarehouse_product | Số lượng theo đơn vị giao dịch |
| `quantity_unit` | tbl_delivery_items | Số lượng quy đổi về đơn vị mặc định sản phẩm |
| `quantity_stock` | tbl_delivery_items | Số lượng tính theo đơn vị tồn kho |
| `quantity_payment` | tbl_delivery_items | Số lượng tính theo đơn vị thanh toán |
| `quantity_left` | tblwarehouse_product | Tồn kho hiện tại (sau xuất nhập) |
| `quantity_loss` | tblwarehouse_product | Số lượng hàng hao hụt/hỏng |

---

### Ngày tháng

| Trường | Ý nghĩa |
|--------|---------|
| `date` | Ngày lập đơn/phiếu |
| `date_created` | Ngày tạo bản ghi trong hệ thống |
| `date_updated` | Ngày cập nhật lần cuối |
| `date_status` | Ngày thay đổi trạng thái |
| `date_warehouseman` | Ngày thủ kho xác nhận |
| `etl_loaded_at` | Timestamp pipeline load vào staging |

---

## 4. Mapping MySQL → Core Schema

### Bảng Dimension

| MySQL (ERP nguồn) | Core Schema | Loại Surrogate Key | Load Type |
|-------------------|------------|-------------------|-----------|
| `tblclients` | `core.dim_customer` | `customer_key` SERIAL | Full |
| `tbl_products` | `core.dim_product` | `product_key` SERIAL | Full |
| `tblstaff` | `core.dim_staff` | `staff_key` SERIAL | Full |
| `tbldepartments` | `core.dim_department` | `department_key` SERIAL | Full |
| `tblsuppliers` | `core.dim_supplier` | `supplier_key` SERIAL | Full |
| `tblwarehouse` | `core.dim_warehouse` | `warehouse_key` SERIAL | Full |
| `tbllocaltion_warehouses` | `core.dim_warehouse_location` | `location_key` SERIAL | Full |
| `tbl_manufactures` | `core.dim_manufacture` | `manufacture_key` SERIAL | Full |
| *(generated)* | `core.dim_date` | `date_key` INT YYYYMMDD | Static |

### Bảng Fact

| MySQL (ERP nguồn) | Core Schema | Natural Key | Watermark |
|-------------------|------------|-------------|-----------|
| `tbl_orders` | `core.fact_orders` | `order_id` | `date_updated` |
| `tbl_order_items` | `core.fact_order_items` | `order_item_id` | `id` (incremental) |
| `tbl_deliveries` + `tbl_delivery_items` | `core.fact_delivery_items` | `delivery_item_id` | `id` (incremental) |
| `tblwarehouse_product` | `core.fact_warehouse_stock` | `stock_id` | `date_warehouse` |
| `tblpurchase_order` + `tblpurchase_order_items` | `core.fact_purchase_order_items` | `po_item_id` | `date` |
| `tbl_purchase_products` + `tbl_purchase_product_items` | `core.fact_purchase_product_items` | `pp_item_id` | `id` |
| `tbltransfer_warehouse_detail` | `core.fact_transfer_warehouse` | `transfer_detail_id` | `id` |
| `tbl_productions_orders_items` | `core.fact_production_order_items` | `prod_item_id` | `id` |
| `tbl_productions_orders_items_stages` | `core.fact_production_stages` | `prod_stage_id` | `id` |

---

## 5. Trạng thái (Status codes)

### tbl_orders.status
| Giá trị | Ý nghĩa |
|---------|---------|
| `"new"` | Đơn hàng mới tạo |
| `"approved"` | Đã phê duyệt |
| `"completed"` | Hoàn thành |
| `"cancelled"` | Đã hủy |

### tbl_orders.status_payment
| Giá trị | Ý nghĩa |
|---------|---------|
| `0` | Chưa thanh toán |
| `1` | Thanh toán một phần |
| `2` | Đã thanh toán đủ |

### tbl_deliveries.status
| Giá trị | Ý nghĩa |
|---------|---------|
| `"new"` | Phiếu xuất mới |
| `"approved"` | Đã duyệt |
| `"completed"` | Đã xuất kho |

### tblwarehouse_product.type_export
| Giá trị | Ý nghĩa |
|---------|---------|
| `1` | Xuất bán |
| `2` | Xuất trả nhà cung cấp |
| `3` | Xuất điều chuyển |
| `4` | Xuất hỏng/hao hụt |

---

## 6. Các trường nullable hệ thống

Những trường này thường NULL do module ERP chưa được cấu hình, **không nên dùng cho phân tích chính thức**:

| Trường | Bảng | Lý do NULL |
|--------|------|-----------|
| `total_cost` | fact_orders | Module tính COGS không cấu hình |
| `total_profit` | fact_orders | Phụ thuộc total_cost |
| `cost_temporary_capital` | fact_order_items | Module tính giá vốn tạm |
| `profit_temporary_capital` | fact_order_items | Module tính lợi nhuận tạm |
| `total_payment` | tbl_orders | Module AR (Accounts Receivable) không dùng |
| `debt_limit` / `debt_limit_day` | tblclients | Module credit không cấu hình |
| `price_import` | tbl_products | Giá nhập thường để 0 trong ERP này |

> Muốn phân tích lợi nhuận thực: cần bổ sung dữ liệu COGS ngoài ERP hoặc bật module tính giá vốn.
