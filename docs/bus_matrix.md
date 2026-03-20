# Enterprise Bus Matrix

Bus Matrix cho biết fact nào chia sẻ dimension nào — đây là nền tảng để thiết kế conformed dimensions.

| Fact Table | Date | Customer | Product | Staff | Supplier | Warehouse | Location | Manufacture | Price Group |
|------------|:----:|:--------:|:-------:|:-----:|:--------:|:---------:|:--------:|:-----------:|:-----------:|
| `fact_orders` | ✅ | ✅ | | ✅ | | | | | |
| `fact_order_items` | ✅ | ✅ | ✅ | | | | | | |
| `fact_delivery_items` | ✅ | ✅ | ✅ | | | ✅ | ✅ | | |
| `fact_warehouse_stock` | ✅ | | ✅ | | | ✅ | | | |
| `fact_purchase_order_items` | ✅ | | ✅ | | ✅ | | | | |
| `fact_purchase_product_items` | ✅ | | ✅ | | | ✅ | ✅ | | |
| `fact_transfer_warehouse` | | | ✅ | | | ✅ (from+to) | ✅ (from+to) | | |
| `fact_production_order_items` | ✅ | | ✅ | | | | | | |
| `fact_production_stages` | ✅ | | | ✅ | | | | | |

---

## Dimensions

| Dimension | Source Table | Natural Key | Domain |
|-----------|-------------|-------------|--------|
| `dim_date` | generated | date_key (YYYYMMDD INT) | Shared |
| `dim_customer` | tblclients | customer_id | Sales, Finance |
| `dim_product` | tbl_products | product_id | All |
| `dim_staff` | tblstaff | staff_id | Sales, Production |
| `dim_department` | tbldepartments | department_id | Shared |
| `dim_supplier` | tblsuppliers | supplier_id | Finance |
| `dim_warehouse` | tblwarehouse | warehouse_id | Inventory |
| `dim_warehouse_location` | tbllocaltion_warehouses | location_id | Inventory |
| `dim_manufacture` | tbl_manufactures | manufacture_id | Production |
| `dim_price_group` | tblcustomers_groups | price_group_id | Sales |

---

## Domain → Fact → Mart Mapping

| Domain | Core Fact Tables | dbt Mart |
|--------|-----------------|----------|
| **Sales** | fact_orders, fact_order_items, fact_delivery_items | mart.sales |
| **Inventory** | fact_warehouse_stock, fact_purchase_product_items, fact_transfer_warehouse, fact_production_order_items, fact_production_stages | mart.inventory |
| **Finance** | fact_purchase_order_items, fact_orders (profit cols) | mart.finance |
