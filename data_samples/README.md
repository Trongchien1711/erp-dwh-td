# Data Samples

Sample CSV files for local testing and development. Each file mirrors the schema of the corresponding MySQL source table and contains a small representative dataset.

---

## Files

| File | Source Table | Rows | Description |
|------|-------------|------|-------------|
| `sample_customers.csv` | `tblclients` | 10 | B2B clients with various types, debt limits, and active flags |
| `sample_products.csv` | `tbl_products` | 20 | Mix of finished goods (category 10–13) and raw materials (category 20–22) |
| `sample_orders.csv` | `tbl_orders` | 15 | Sales orders covering all status codes (new / approved / completed / cancelled) |
| `sample_order_items.csv` | `tbl_order_items` | 20 | Line items referencing orders and products in the two sample sets above |
| `sample_suppliers.csv` | `tblsuppliers` | 7 | Supplier master records with type and payment-term columns |
| `sample_warehouse_stock.csv` | `tblwarehouse_product` | 15 | Lot-level stock snapshot across 2 warehouses and 4 locations |
| `sample_staff.csv` | `tblstaff` | 8 | Staff accounts with roles, branch assignment, and active status |
| `sample_npl_order_2025.csv` | `mart.fct_order_npl_cost` | ~190 | 2025 orders with allocated NPL cost, NPL%, and quality flag (qty-based allocation) |
| `sample_npl_bom_detail_2025.csv` | `mart.fct_production_npl_cost` | ~1,000 | 2025 BOM cost detail per plan × material line — used for NPL model validation |
| `top20_npl_product_2025.csv` | `mart.fct_order_npl_cost` | 20 | Top-20 finished products by total allocated NPL cost in 2025 |

---

## Column Reference

### `sample_customers.csv` → `tblclients`

| Column | Type | Notes |
|--------|------|-------|
| `userid` | integer | PK |
| `code_client` | varchar | Business-assigned client code |
| `prefix_client` | varchar | Display prefix (e.g. "Cty", "Ong") |
| `company` | varchar | Legal company name |
| `representative` | varchar | Primary contact name |
| `phonenumber` | varchar | Contact phone |
| `email_client` | varchar | Contact email |
| `type_client` | integer | Client classification (1 = enterprise, 2 = retail) |
| `debt_limit` | numeric | Maximum allowed outstanding balance |
| `active` | boolean | 1 = active, 0 = inactive |

### `sample_products.csv` → `tbl_products`

| Column | Type | Notes |
|--------|------|-------|
| `id` | integer | PK |
| `category_id` | integer | Links to product category tree |
| `type_products` | integer | 1 = finished good, 2 = raw material |
| `code` | varchar | SKU / product code |
| `name` | varchar | Product display name |
| `price_import` | numeric | Standard purchase cost |
| `price_sell` | numeric | Standard selling price |
| `unit_id` | integer | Unit of measure FK |
| `status` | integer | 1 = active, 0 = discontinued |
| `date_created` | timestamp | Record creation date |

### `sample_orders.csv` → `tbl_orders`

| Column | Type | Notes |
|--------|------|-------|
| `id` | integer | PK |
| `code_order` | varchar | Human-readable order reference |
| `date_order` | date | Order date |
| `client_id` | integer | FK → `tblclients.userid` |
| `staff_id` | integer | FK → `tblstaff.staffid` (sales rep) |
| `warehouse_id` | integer | Fulfilling warehouse |
| `grand_total` | numeric | Total order value (sum of line items) |
| `discount` | numeric | Order-level discount amount |
| `status` | integer | 1=new, 2=approved, 3=completed, 4=cancelled |
| `status_payment` | integer | 0=unpaid, 1=partial, 2=paid |
| `total_payment` | numeric | Amount received so far |
| `note` | text | Free-text notes |

### `sample_order_items.csv` → `tbl_order_items`

| Column | Type | Notes |
|--------|------|-------|
| `id` | integer | PK |
| `order_id` | integer | FK → `tbl_orders.id` |
| `product_id` | integer | FK → `tbl_products.id` |
| `type_items` | integer | 1 = product, 2 = material |
| `quantity_order` | integer | Ordered quantity |
| `quantity_export` | integer | Quantity shipped / exported |
| `price_sell` | numeric | Unit price at time of order |
| `discount` | numeric | Line-level discount |
| `total_cost` | numeric | `price_sell × quantity_order − discount` |

### `sample_suppliers.csv` → `tblsuppliers`

| Column | Type | Notes |
|--------|------|-------|
| `id` | integer | PK |
| `prefix` | varchar | Display prefix |
| `code` | varchar | Supplier code |
| `company` | varchar | Supplier name |
| `representative` | varchar | Contact person |
| `phone` | varchar | Contact phone |
| `groups_in` | integer | Supplier group FK |
| `type` | integer | 1 = goods, 2 = services |
| `type_suppliers` | integer | Classification within type |
| `time_payment` | integer | Standard payment terms (days) |

### `sample_warehouse_stock.csv` → `tblwarehouse_product`

| Column | Type | Notes |
|--------|------|-------|
| `id` | integer | PK |
| `warehouse_id` | integer | FK → warehouse master |
| `location_id` | integer | Bin / shelf location |
| `product_id` | integer | FK → `tbl_products.id` |
| `lot_code` | varchar | Lot / batch identifier |
| `date_in` | date | Receipt date |
| `date_expired` | date | Expiry date (nullable) |
| `quantity` | integer | Original quantity received |
| `quantity_left` | integer | Current on-hand after exports |
| `quantity_export` | integer | Total exported from this lot |

### `sample_staff.csv` → `tblstaff`

| Column | Type | Notes |
|--------|------|-------|
| `staffid` | integer | PK |
| `email` | varchar | Login email |
| `firstname` | varchar | Given name |
| `lastname` | varchar | Family name |
| `phonenumber` | varchar | Contact phone |
| `gender` | varchar | Male / Female |
| `birthday` | date | Date of birth |
| `day_in` | date | Start date at company |
| `status_work` | integer | 1 = active, 0 = inactive |
| `role` | integer | 1=admin, 2=sales, 3=warehouse, 4=purchase, 5=manager, 6=production |
| `admin` | boolean | 1 = has admin privileges |
| `active` | boolean | 1 = can log in |
| `id_branch` | integer | FK → branch master |
| `role_level_id` | integer | Fine-grained permission level |
| `date_update` | timestamp | Last update time |

---

## Loading Samples into PostgreSQL (for testing)

```sql
-- Example: load sample orders into the staging table
COPY staging.tbl_orders
FROM '/path/to/data_samples/sample_orders.csv'
WITH (FORMAT csv, HEADER true, NULL '');
```

Or via psql:

```powershell
psql -h localhost -U dwh_admin -d erp_dwh `
  -c "\copy staging.tbl_orders FROM 'data_samples/sample_orders.csv' CSV HEADER"
```

> The sample data is designed so that foreign-key relationships are self-consistent:
> `sample_order_items` references order IDs 1–15 and product IDs 1–20, both of which exist in the
> corresponding sample files.
