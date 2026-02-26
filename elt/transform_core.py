# ============================================
# transform_core.py
# Transform từ staging → core (dimensions + facts)
# Chạy sau khi staging đã được load xong
# ============================================

from sqlalchemy import text
from loguru import logger


# ============================================
# DIMENSIONS
# ============================================

SQL_DIM_CUSTOMER = """
INSERT INTO core.dim_customer (
    customer_id, customer_code, prefix_client, company, company_short,
    representative, fullname, phonenumber, email,
    type_client, country, city, district, ward, address, vat,
    debt_limit, debt_limit_day, discount, table_price_id,
    status_clients, vip_rating, time_payment, is_active, datecreated,
    etl_loaded_at, etl_source
)
SELECT
    userid, code_client, prefix_client, company, company_short,
    representative, fullname, phonenumber, email_client,
    type_client, country, city, district, ward, address, vat,
    debt_limit, debt_limit_day, discount, table_price_id,
    status_clients, vip_rating, time_payment,
    (active = 1),
    datecreated,
    NOW(), 'tblclients'
FROM staging.tblclients AS src
WHERE NOT EXISTS (
    SELECT 1 FROM core.dim_customer d WHERE d.customer_id = src.userid
)
ON CONFLICT DO NOTHING;
"""

SQL_DIM_PRODUCT = """
INSERT INTO core.dim_product (
    product_id, product_code, product_name, product_name_customer,
    type_products, category_id, unit_id, species, brand, brand_id,
    price_import, price_sell, price_processing, loss, bom_id,
    versions, versions_stage, longs, wide, height, warranty,
    is_active, id_branch, etl_loaded_at, etl_source
)
SELECT
    id, code, name, name_customer,
    type_products, category_id, unit_id, species, brand, brand_id,
    price_import, price_sell, price_processing, loss, bom_id,
    versions, versions_stage, longs, wide, height, warranty,
    (status = 1),
    id_branch, NOW(), 'tbl_products'
FROM staging.tbl_products AS src
WHERE NOT EXISTS (
    SELECT 1 FROM core.dim_product d WHERE d.product_id = src.id
)
ON CONFLICT DO NOTHING;
"""

SQL_DIM_STAFF = """
INSERT INTO core.dim_staff (
    staff_id, firstname, lastname,
    fullname, email, phonenumber, gender, birthday,
    day_in, status_work, role, admin, id_branch,
    is_active, etl_loaded_at, etl_source
)
SELECT
    staffid, firstname, lastname,
    CONCAT(COALESCE(firstname,''), ' ', COALESCE(lastname,'')),
    email, phonenumber, gender, birthday,
    day_in, status_work, role, admin, id_branch,
    (active = 1), NOW(), 'tblstaff'
FROM staging.tblstaff AS src
WHERE NOT EXISTS (
    SELECT 1 FROM core.dim_staff d WHERE d.staff_id = src.staffid
)
ON CONFLICT DO NOTHING;
"""

SQL_DIM_DEPARTMENT = """
INSERT INTO core.dim_department (
    department_id, department_code, department_name, type, is_active,
    etl_loaded_at, etl_source
)
SELECT
    departmentid, code, name, type,
    (active_departments = 1), NOW(), 'tbldepartments'
FROM staging.tbldepartments AS src
WHERE NOT EXISTS (
    SELECT 1 FROM core.dim_department d WHERE d.department_id = src.departmentid
)
ON CONFLICT DO NOTHING;
"""

SQL_DIM_WAREHOUSE = """
INSERT INTO core.dim_warehouse (
    warehouse_id, warehouse_code, warehouse_name, address,
    supplier_id, id_branch, etl_loaded_at, etl_source
)
SELECT
    id, code, name, address, supplier_id, id_branch,
    NOW(), 'tblwarehouse'
FROM staging.tblwarehouse AS src
WHERE NOT EXISTS (
    SELECT 1 FROM core.dim_warehouse d WHERE d.warehouse_id = src.id
)
ON CONFLICT DO NOTHING;
"""

SQL_DIM_SUPPLIER = """
INSERT INTO core.dim_supplier (
    supplier_id, supplier_code, company, representative,
    phone, email, vat, address, type_suppliers,
    debt_limit, is_active, datecreated, etl_loaded_at, etl_source
)
SELECT
    id, code, company, representative,
    phone, email, vat, address, type_suppliers,
    debt_limit, (active = 1), datecreated, NOW(), 'tblsuppliers'
FROM staging.tblsuppliers AS src
WHERE NOT EXISTS (
    SELECT 1 FROM core.dim_supplier d WHERE d.supplier_id = src.id
)
ON CONFLICT DO NOTHING;
"""

# ============================================
# FACT TABLES
# ============================================

SQL_FACT_ORDERS = """
INSERT INTO core.fact_orders (
    order_id, reference_no, customer_key, employee_key, order_date_key,
    count_items, total_quantity, total_amount_items, total_tax_items,
    total_discount_percent_items, total_discount_direct_items, grand_total_items,
    total_tax, total_discount_percent, total_discount_direct,
    cost_delivery, grand_total, total_cost, total_profit, total_payment,
    status, status_payment, status_orders, type_orders, type_bills,
    is_cancel, is_end, id_branch, warehouse_id, currencies,
    date_created, date_updated, etl_loaded_at, etl_source
)
SELECT
    o.id,
    o.reference_no,
    dc.customer_key,
    ds.staff_key,
    TO_CHAR(o.date, 'YYYYMMDD')::INT,
    o.count_items, o.total_quantity, o.total_amount_items, o.total_tax_items,
    o.total_discount_percent_items, o.total_discount_direct_items, o.grand_total_items,
    o.total_tax, o.total_discount_percent, o.total_discount_direct,
    o.cost_delivery, o.grand_total, o.total_cost, o.total_profit, o.total_payment,
    o.status, o.status_payment, o.status_orders, o.type_orders, o.type_bills,
    o.is_cancel, o.is_end, o.id_branch, o.warehouse_id, o.currencies,
    o.date_created, o.date_updated, NOW(), 'tbl_orders'
FROM staging.tbl_orders o
LEFT JOIN core.dim_customer dc ON dc.customer_id = o.customer_id
LEFT JOIN core.dim_staff    ds ON ds.staff_id    = o.employee_id
WHERE NOT EXISTS (
    SELECT 1 FROM core.fact_orders f WHERE f.order_id = o.id
);
"""

SQL_FACT_ORDER_ITEMS = """
INSERT INTO core.fact_order_items (
    order_item_id, order_id, customer_key, product_key, order_date_key,
    quantity, price, amount,
    tax_rate_item, tax_amount_item,
    discount_percent_item, discount_percent_amount_item, discount_direct_amount_item,
    total_amount, quantity_delivery, quantity_not_delivery,
    cost, profit, cost_temporary_capital, profit_temporary_capital,
    quantity_returned, type_item, item_code, type_gift, active, unit_id,
    etl_loaded_at, etl_source
)
SELECT
    oi.id, oi.order_id,
    dc.customer_key,
    dp.product_key,
    TO_CHAR(o.date, 'YYYYMMDD')::INT,
    oi.quantity, oi.price, oi.amount,
    oi.tax_rate_item, oi.tax_amount_item,
    oi.discount_percent_item, oi.discount_percent_amount_item, oi.discount_direct_amount_item,
    oi.total_amount, oi.quantity_delivery, oi.quantity_not_delivery,
    oi.cost, oi.profit, oi.cost_temporary_capital, oi.profit_temporary_capital,
    oi.quantity_returned, oi.type_item, oi.item_code, oi.type_gift, oi.active, oi.unit_id,
    NOW(), 'tbl_order_items'
FROM staging.tbl_order_items oi
JOIN  staging.tbl_orders      o  ON o.id  = oi.order_id
LEFT JOIN core.dim_customer  dc ON dc.customer_id = o.customer_id
LEFT JOIN core.dim_product   dp ON dp.product_id  = oi.item_id
WHERE NOT EXISTS (
    SELECT 1 FROM core.fact_order_items f WHERE f.order_item_id = oi.id
);
"""

SQL_FACT_DELIVERY_ITEMS = """
INSERT INTO core.fact_delivery_items (
    delivery_item_id, delivery_id, order_item_id,
    customer_key, product_key, warehouse_key, location_key,
    delivery_date_key,
    quantity, quantity_loss, quantity_sample,
    price, amount, tax_rate_item, tax_amount_item,
    discount_percent_item, discount_percent_amount_item, discount_direct_amount_item,
    total_amount, quantity_unit, quantity_stock, quantity_payment,
    type_item, item_code, lot_code, date_sx, date_sd, unit_id,
    etl_loaded_at, etl_source
)
SELECT
    di.id, di.delivery_id, di.order_item_id,
    dc.customer_key, dp.product_key, dw.warehouse_key, NULL,
    TO_CHAR(d.date, 'YYYYMMDD')::INT,
    di.quantity, di.quantity_loss, di.quantity_sample,
    di.price, di.amount, di.tax_rate_item, di.tax_amount_item,
    di.discount_percent_item, di.discount_percent_amount_item, di.discount_direct_amount_item,
    di.total_amount, di.quantity_unit, di.quantity_stock, di.quantity_payment,
    di.type_item, di.item_code, di.lot_code, di.date_sx, di.date_sd, di.unit_id,
    NOW(), 'tbl_delivery_items'
FROM staging.tbl_delivery_items di
JOIN  staging.tbl_deliveries   d  ON d.id  = di.delivery_id
LEFT JOIN core.dim_customer   dc ON dc.customer_id  = d.customer_id
LEFT JOIN core.dim_product    dp ON dp.product_id   = di.item_id
LEFT JOIN core.dim_warehouse  dw ON dw.warehouse_id = di.warehouse_id
WHERE NOT EXISTS (
    SELECT 1 FROM core.fact_delivery_items f WHERE f.delivery_item_id = di.id
);
"""

SQL_FACT_WH_STOCK = """
INSERT INTO core.fact_warehouse_stock (
    stock_id, product_key, warehouse_key, location_key, import_date_key,
    quantity, quantity_left, quantity_export,
    quantity_exchange, quantity_exchange_left, quantity_exchange_export,
    product_quantity_unit, product_quantity_unit_export, product_quantity_unit_left,
    product_quantity_payment, product_quantity_payment_export, product_quantity_payment_left,
    price, type_items, type_export, type_transfer, lot_code, date_sx, date_sd, series,
    etl_loaded_at, etl_source
)
SELECT
    wp.id, dp.product_key, dw.warehouse_key, NULL,
    TO_CHAR(wp.date_import, 'YYYYMMDD')::INT,
    wp.quantity, wp.quantity_left, wp.quantity_export,
    wp.quantity_exchange, wp.quantity_exchange_left, wp.quantity_exchange_export,
    wp.product_quantity_unit, wp.product_quantity_unit_export, wp.product_quantity_unit_left,
    wp.product_quantity_payment, wp.product_quantity_payment_export, wp.product_quantity_payment_left,
    wp.price, wp.type_items, wp.type_export, wp.type_transfer,
    wp.lot_code, wp.date_sx, wp.date_sd, wp.series,
    NOW(), 'tblwarehouse_product'
FROM staging.tblwarehouse_product wp
LEFT JOIN core.dim_product   dp ON dp.product_id   = wp.product_id
LEFT JOIN core.dim_warehouse dw ON dw.warehouse_id = wp.warehouse_id
WHERE NOT EXISTS (
    SELECT 1 FROM core.fact_warehouse_stock f WHERE f.stock_id = wp.id
);
"""

SQL_FACT_PO_ITEMS = """
INSERT INTO core.fact_purchase_order_items (
    po_item_id, po_id, product_key, supplier_key, po_date_key,
    quantity, quantity_suppliers, unit_cost, price_expected, price_suppliers,
    promotion_expected, total_expected, total_suppliers, subtotal, tax_rate,
    quantity_unit, quantity_stock, quantity_payment, type,
    etl_loaded_at, etl_source
)
SELECT
    poi.id, poi.id_purchase_order,
    dp.product_key, ds.supplier_key,
    TO_CHAR(po.date, 'YYYYMMDD')::INT,
    poi.quantity, poi.quantity_suppliers, poi.unit_cost,
    poi.price_expected, poi.price_suppliers,
    poi.promotion_expected, poi.total_expected, poi.total_suppliers,
    poi.subtotal, poi.tax_rate,
    poi.quantity_unit, poi.quantity_stock, poi.quantity_payment, poi.type,
    NOW(), 'tblpurchase_order_items'
FROM staging.tblpurchase_order_items poi
JOIN  staging.tblpurchase_order  po ON po.id = poi.id_purchase_order
LEFT JOIN core.dim_product  dp ON dp.product_id  = poi.product_id
LEFT JOIN core.dim_supplier ds ON ds.supplier_id = po.suppliers_id
WHERE NOT EXISTS (
    SELECT 1 FROM core.fact_purchase_order_items f WHERE f.po_item_id = poi.id
);
"""


# ============================================
# Runner
# ============================================

TRANSFORM_STEPS = [
    ("dim_customer",          SQL_DIM_CUSTOMER),
    ("dim_product",           SQL_DIM_PRODUCT),
    ("dim_staff",             SQL_DIM_STAFF),
    ("dim_department",        SQL_DIM_DEPARTMENT),
    ("dim_warehouse",         SQL_DIM_WAREHOUSE),
    ("dim_supplier",          SQL_DIM_SUPPLIER),
    ("fact_orders",           SQL_FACT_ORDERS),
    ("fact_order_items",      SQL_FACT_ORDER_ITEMS),
    ("fact_delivery_items",   SQL_FACT_DELIVERY_ITEMS),
    ("fact_warehouse_stock",  SQL_FACT_WH_STOCK),
    ("fact_purchase_order_items", SQL_FACT_PO_ITEMS),
]


def run_transforms(pg_engine):
    """Chạy toàn bộ bước transform staging → core."""
    for step_name, sql in TRANSFORM_STEPS:
        try:
            with pg_engine.begin() as conn:
                result = conn.execute(text(sql))
            logger.success(
                f"[Transform] {step_name} → {result.rowcount:,} rows inserted."
            )
        except Exception as e:
            logger.error(f"[Transform] {step_name} FAILED: {e}")
            raise
