# Grain Definition

Grain = mỗi hàng trong fact table đại diện cho **điều gì**.
Xác định grain trước khi thiết kế schema là bước quan trọng nhất.

---

## Fact Tables

### `fact_orders`
- **Grain**: 1 đơn hàng bán (header)
- **Natural key**: `order_id`
- **Date key**: `order_date_key` (ngày tạo đơn)
- **Dimensions**: customer, employee (staff)
- **Mặc định**: aggregate mắc độ order — JOIN với `fact_order_items` để lấy chi tiết

### `fact_order_items`
- **Grain**: 1 dòng sản phẩm trong 1 đơn hàng
- **Natural key**: `order_item_id`
- **Date key**: `order_date_key` (ngày của đơn)
- **Dimensions**: customer, product
- **Measures**: quantity, price, amount, discount, tax, profit

### `fact_delivery_items`
- **Grain**: 1 dòng sản phẩm trong 1 phiếu giao hàng
- **Natural key**: `delivery_item_id`
- **Date key**: `delivery_date_key`
- **Dimensions**: customer, product, warehouse, warehouse_location
- **Measures**: quantity, price, amount

### `fact_warehouse_stock`
- **Grain**: 1 lô hàng trong kho tại một thời điểm nhập
- **Natural key**: `stock_id`
- **Date key**: `import_date_key` (ngày nhập kho)
- **Dimensions**: product, warehouse
- **Measures**: quantity (nhập), quantity_left (tồn), quantity_export (xuất)

### `fact_purchase_order_items`
- **Grain**: 1 dòng sản phẩm trong 1 đơn đặt mua
- **Natural key**: `po_item_id`
- **Date key**: `po_date_key`
- **Dimensions**: product, supplier
- **Measures**: quantity, unit_cost, total_expected, total_suppliers

### `fact_production_order_items`
- **Grain**: 1 dòng sản phẩm trong 1 lệnh sản xuất
- **Natural key**: `prod_item_id`
- **Date key**: `prod_date_key`
- **Dimensions**: product
- **Measures**: quantity

### `fact_production_stages`
- **Grain**: 1 công đoạn sản xuất của 1 nhân viên
- **Natural key**: `prod_stage_id`
- **Date key**: `stage_date_key`
- **Dimensions**: staff
- **Measures**: number, number_hours, total_time

### `fact_purchase_product_items`
- **Grain**: 1 dòng hàng trong 1 phiếu nhập kho (từ NCC)
- **Natural key**: `pp_item_id`
- **Date key**: `import_date_key`
- **Dimensions**: product, warehouse, warehouse_location
- **Measures**: quantity, price, amount

### `fact_transfer_warehouse`
- **Grain**: 1 dòng hàng trong 1 phiếu chuyển kho
- **Natural key**: `transfer_detail_id`
- **Dimensions**: product, warehouse_from, warehouse_to, location_from, location_to
- **Measures**: quantity, quantity_net, price, amount

---

## Date Key Convention

Tất cả date key đều dùng format **`INT YYYYMMDD`**:
- `20260320` = 2026-03-20
- `19000101` = fallback khi NULL
- Tại sao INT? JOIN nhanh hơn DATE, dễ filter `WHERE date_key BETWEEN 20260101 AND 20261231`

