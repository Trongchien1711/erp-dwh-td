
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
WITH updated AS (
    UPDATE core.dim_customer d
    SET
        customer_code      = src.code_client,
        prefix_client      = src.prefix_client,
        company            = src.company,
        company_short      = src.company_short,
        representative     = src.representative,
        fullname           = src.fullname,
        phonenumber        = src.phonenumber,
        email              = src.email_client,
        type_client        = src.type_client,
        country            = src.country,
        city               = src.city,
        district           = src.district,
        ward               = src.ward,
        address            = src.address,
        vat                = src.vat,
        debt_limit         = src.debt_limit,
        debt_limit_day     = src.debt_limit_day,
        discount           = src.discount,
        table_price_id     = src.table_price_id,
        status_clients     = src.status_clients,
        vip_rating         = src.vip_rating,
        time_payment       = src.time_payment,
        is_active          = (src.active = 1),
        datecreated        = src.datecreated,
        etl_loaded_at      = NOW()
    FROM staging.tblclients src
    WHERE d.customer_id = src.userid
    RETURNING d.customer_id
)
INSERT INTO core.dim_customer (
    customer_id, customer_code, prefix_client, company, company_short,
    representative, fullname, phonenumber, email,
    type_client, country, city, district, ward, address, vat,
    debt_limit, debt_limit_day, discount, table_price_id,
    status_clients, vip_rating, time_payment, is_active, datecreated,
    etl_loaded_at, etl_source
)
SELECT
    src.userid, src.code_client, src.prefix_client, src.company, src.company_short,
    src.representative, src.fullname, src.phonenumber, src.email_client,
    src.type_client, src.country, src.city, src.district, src.ward, src.address, src.vat,
    src.debt_limit, src.debt_limit_day, src.discount, src.table_price_id,
    src.status_clients, src.vip_rating, src.time_payment,
    (src.active = 1), src.datecreated,
    NOW(), 'tblclients'
FROM staging.tblclients src
WHERE NOT EXISTS (SELECT 1 FROM updated u WHERE u.customer_id = src.userid);
"""

SQL_DIM_PRODUCT = """
WITH updated AS (
    UPDATE core.dim_product d
    SET
        product_code          = src.code,
        product_name          = src.name,
        product_name_customer = src.name_customer,
        type_products         = src.type_products,
        category_id           = src.category_id,
        unit_id               = src.unit_id,
        species               = src.species,
        brand                 = src.brand,
        brand_id              = src.brand_id,
        price_import          = src.price_import,
        price_sell            = src.price_sell,
        price_processing      = src.price_processing,
        loss                  = src.loss,
        bom_id                = src.bom_id,
        versions              = src.versions,
        versions_stage        = src.versions_stage,
        longs                 = src.longs,
        wide                  = src.wide,
        height                = src.height,
        warranty              = src.warranty,
        is_active             = (src.status = 1),
        id_branch             = src.id_branch,
        etl_loaded_at         = NOW()
    FROM staging.tbl_products src
    WHERE d.product_id = src.id
    RETURNING d.product_id
)
INSERT INTO core.dim_product (
    product_id, product_code, product_name, product_name_customer,
    type_products, category_id, unit_id, species, brand, brand_id,
    price_import, price_sell, price_processing, loss, bom_id,
    versions, versions_stage, longs, wide, height, warranty,
    is_active, id_branch, etl_loaded_at, etl_source
)
SELECT
    src.id, src.code, src.name, src.name_customer,
    src.type_products, src.category_id, src.unit_id, src.species, src.brand, src.brand_id,
    src.price_import, src.price_sell, src.price_processing, src.loss, src.bom_id,
    src.versions, src.versions_stage, src.longs, src.wide, src.height, src.warranty,
    (src.status = 1), src.id_branch, NOW(), 'tbl_products'
FROM staging.tbl_products src
WHERE NOT EXISTS (SELECT 1 FROM updated u WHERE u.product_id = src.id);
"""

SQL_DIM_STAFF = """
WITH updated AS (
    UPDATE core.dim_staff d
    SET
        firstname     = src.firstname,
        lastname      = src.lastname,
        fullname      = CONCAT(COALESCE(src.firstname,''), ' ', COALESCE(src.lastname,'')),
        email         = src.email,
        phonenumber   = src.phonenumber,
        gender        = src.gender,
        birthday      = src.birthday,
        day_in        = src.day_in,
        status_work   = src.status_work,
        role          = src.role,
        admin         = src.admin,
        id_branch     = src.id_branch,
        is_active     = (src.active = 1),
        etl_loaded_at = NOW()
    FROM staging.tblstaff src
    WHERE d.staff_id = src.staffid
    RETURNING d.staff_id
)
INSERT INTO core.dim_staff (
    staff_id, firstname, lastname,
    fullname, email, phonenumber, gender, birthday,
    day_in, status_work, role, admin, id_branch,
    is_active, etl_loaded_at, etl_source
)
SELECT
    src.staffid, src.firstname, src.lastname,
    CONCAT(COALESCE(src.firstname,''), ' ', COALESCE(src.lastname,'')),
    src.email, src.phonenumber, src.gender, src.birthday,
    src.day_in, src.status_work, src.role, src.admin, src.id_branch,
    (src.active = 1), NOW(), 'tblstaff'
FROM staging.tblstaff src
WHERE NOT EXISTS (SELECT 1 FROM updated u WHERE u.staff_id = src.staffid);
"""

SQL_DIM_DEPARTMENT = """
WITH updated AS (
    UPDATE core.dim_department d
    SET
        department_code = src.code,
        department_name = src.name,
        type            = src.type,
        is_active       = (src.active_departments = 1),
        etl_loaded_at   = NOW()
    FROM staging.tbldepartments src
    WHERE d.department_id = src.departmentid
    RETURNING d.department_id
)
INSERT INTO core.dim_department (
    department_id, department_code, department_name, type, is_active,
    etl_loaded_at, etl_source
)
SELECT
    src.departmentid, src.code, src.name, src.type,
    (src.active_departments = 1), NOW(), 'tbldepartments'
FROM staging.tbldepartments src
WHERE NOT EXISTS (SELECT 1 FROM updated u WHERE u.department_id = src.departmentid);
"""

SQL_DIM_PRICE_GROUP = """
WITH updated AS (
    UPDATE core.dim_price_group d
    SET
        price_group_name = src.name,
        is_active        = TRUE,
        etl_loaded_at    = NOW()
    FROM staging.tblcustomers_groups src
    WHERE d.price_group_id = src.id
    RETURNING d.price_group_id
)
INSERT INTO core.dim_price_group (
    price_group_id, price_group_name, is_active,
    etl_loaded_at, etl_source
)
SELECT
    src.id, src.name, TRUE,
    NOW(), 'tblcustomers_groups'
FROM staging.tblcustomers_groups src
WHERE NOT EXISTS (SELECT 1 FROM updated u WHERE u.price_group_id = src.id);
"""

SQL_UPDATE_DIM_CUSTOMER_PRICE_GROUP = """
UPDATE core.dim_customer dc
SET
    price_group_key  = dpg.price_group_key,
    price_group_code = (SELECT code FROM staging.tblcustomers_groups WHERE id = dpg.price_group_id),
    price_group_name = dpg.price_group_name
FROM staging.tblcustomer_groups tcg
JOIN core.dim_price_group dpg ON dpg.price_group_id = tcg.groupid
WHERE dc.customer_id = tcg.customer_id
  AND dc.price_group_key IS DISTINCT FROM dpg.price_group_key;
"""



SQL_DIM_WAREHOUSE = """
WITH updated AS (
    UPDATE core.dim_warehouse d
    SET
        warehouse_code = src.code,
        warehouse_name = src.name,
        address        = src.address,
        supplier_id    = src.supplier_id,
        id_branch      = src.id_branch,
        etl_loaded_at  = NOW()
    FROM staging.tblwarehouse src
    WHERE d.warehouse_id = src.id
    RETURNING d.warehouse_id
)
INSERT INTO core.dim_warehouse (
    warehouse_id, warehouse_code, warehouse_name, address,
    supplier_id, id_branch, etl_loaded_at, etl_source
)
SELECT
    src.id, src.code, src.name, src.address, src.supplier_id, src.id_branch,
    NOW(), 'tblwarehouse'
FROM staging.tblwarehouse src
WHERE NOT EXISTS (SELECT 1 FROM updated u WHERE u.warehouse_id = src.id);
"""

SQL_DIM_SUPPLIER = """
WITH updated AS (
    UPDATE core.dim_supplier d
    SET
        supplier_code  = src.code,
        company        = src.company,
        representative = src.representative,
        phone          = src.phone,
        email          = src.email,
        vat            = src.vat,
        address        = src.address,
        type_suppliers = src.type_suppliers,
        groups_in      = src.groups_in,
        debt_limit     = src.debt_limit,
        is_active      = (src.active = 1),
        datecreated    = src.datecreated,
        etl_loaded_at  = NOW()
    FROM staging.tblsuppliers src
    WHERE d.supplier_id = src.id
    RETURNING d.supplier_id
)
INSERT INTO core.dim_supplier (
    supplier_id, supplier_code, company, representative,
    phone, email, vat, address, type_suppliers, groups_in,
    debt_limit, is_active, datecreated, etl_loaded_at, etl_source
)
SELECT
    src.id, src.code, src.company, src.representative,
    src.phone, src.email, src.vat, src.address, src.type_suppliers, src.groups_in,
    src.debt_limit, (src.active = 1), src.datecreated, NOW(), 'tblsuppliers'
FROM staging.tblsuppliers src
WHERE NOT EXISTS (SELECT 1 FROM updated u WHERE u.supplier_id = src.id);
"""

SQL_DIM_WAREHOUSE_LOCATION = """
WITH updated AS (
    UPDATE core.dim_warehouse_location d
    SET
        location_name = src.name,
        location_code = src.code,
        warehouse_id  = src.warehouse,
        id_parent     = src.id_parent,
        name_parent   = src.name_parent,
        lever         = src.lever,
        child         = src.child,
        status        = src.status,
        stage_id      = src.stage_id,
        pod_id        = src.pod_id,
        etl_loaded_at = NOW()
    FROM staging.tbllocaltion_warehouses src
    WHERE d.location_id = src.id
    RETURNING d.location_id
)
INSERT INTO core.dim_warehouse_location (
    location_id, location_name, location_code, warehouse_id,
    id_parent, name_parent, lever, child, status, stage_id, pod_id,
    etl_loaded_at, etl_source
)
SELECT
    src.id, src.name, src.code, src.warehouse,
    src.id_parent, src.name_parent, src.lever, src.child,
    src.status, src.stage_id, src.pod_id,
    NOW(), 'tbllocaltion_warehouses'
FROM staging.tbllocaltion_warehouses src
WHERE NOT EXISTS (SELECT 1 FROM updated u WHERE u.location_id = src.id);
"""

SQL_DIM_MANUFACTURE = """
WITH updated AS (
    UPDATE core.dim_manufacture d
    SET
        reference_no         = src.reference_no,
        id_production_detail = src.id_production_detail,
        status               = src.status,
        status_manufactures  = src.status_manufactures,
        id_branch            = src.id_branch,
        date_created         = src.date_created,
        etl_loaded_at        = NOW()
    FROM staging.tbl_manufactures src
    WHERE d.manufacture_id = src.id
    RETURNING d.manufacture_id
)
INSERT INTO core.dim_manufacture (
    manufacture_id, reference_no, id_production_detail,
    status, status_manufactures, id_branch, date_created,
    etl_loaded_at, etl_source
)
SELECT
    src.id, src.reference_no, src.id_production_detail,
    src.status, src.status_manufactures, src.id_branch, src.date_created,
    NOW(), 'tbl_manufactures'
FROM staging.tbl_manufactures src
WHERE NOT EXISTS (SELECT 1 FROM updated u WHERE u.manufacture_id = src.id);
"""

# ============================================
# FACT TABLES
# ============================================

SQL_FACT_ORDERS = """
-- Step 1: patch any existing rows where dim keys resolved late
WITH fix_keys AS (
    UPDATE core.fact_orders f
    SET
        customer_key = COALESCE(dc.customer_key, f.customer_key),
        employee_key = COALESCE(ds.staff_key,    f.employee_key)
    FROM staging.tbl_orders o
    LEFT JOIN core.dim_customer dc ON dc.customer_id = o.customer_id
    LEFT JOIN core.dim_staff    ds ON ds.staff_id    = o.employee_id
    WHERE f.order_id = o.id
      AND (f.customer_key IS NULL OR f.employee_key IS NULL)
    RETURNING 1
),
-- Step 2: insert new rows only
new_rows AS (
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
        o.id, o.reference_no,
        dc.customer_key, ds.staff_key,
        TO_CHAR(o.date::DATE, 'YYYYMMDD')::INT,
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
    )
    RETURNING 1
)
SELECT
    (SELECT count(*) FROM fix_keys)  AS keys_fixed,
    (SELECT count(*) FROM new_rows)  AS rows_inserted;
"""

SQL_FACT_ORDER_ITEMS = """
WITH fix_keys AS (
    UPDATE core.fact_order_items fi
    SET customer_key = COALESCE(dc.customer_key, fi.customer_key)
    FROM staging.tbl_order_items oi
    JOIN  staging.tbl_orders     o  ON o.id = oi.order_id
    LEFT JOIN core.dim_customer  dc ON dc.customer_id = o.customer_id
    WHERE fi.order_item_id = oi.id
      AND fi.customer_key IS NULL
    RETURNING 1
),
new_rows AS (
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
        dc.customer_key, dp.product_key,
        TO_CHAR(o.date::DATE, 'YYYYMMDD')::INT,
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
    )
    RETURNING 1
)
SELECT
    (SELECT count(*) FROM fix_keys)  AS keys_fixed,
    (SELECT count(*) FROM new_rows)  AS rows_inserted;
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
    TO_CHAR(d.date::DATE, 'YYYYMMDD')::INT,
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
WITH fix_keys AS (
    -- Fix NULL product_key + backfill NULL location_key for existing rows
    UPDATE core.fact_warehouse_stock f
    SET
        product_key  = COALESCE(dp.product_key,  f.product_key),
        location_key = COALESCE(dl.location_key, f.location_key)
    FROM staging.tblwarehouse_product wp
    LEFT JOIN core.dim_product            dp ON dp.product_id   = wp.product_id
    LEFT JOIN core.dim_warehouse_location dl ON dl.location_id  = wp.localtion
    WHERE f.stock_id = wp.id
      AND (f.product_key IS NULL OR f.location_key IS NULL)
    RETURNING 1
),
new_rows AS (
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
        wp.id, dp.product_key, dw.warehouse_key, dl.location_key,
        TO_CHAR(wp.date_import::DATE, 'YYYYMMDD')::INT,
        wp.quantity, wp.quantity_left, wp.quantity_export,
        wp.quantity_exchange, wp.quantity_exchange_left, wp.quantity_exchange_export,
        wp.product_quantity_unit, wp.product_quantity_unit_export, wp.product_quantity_unit_left,
        wp.product_quantity_payment, wp.product_quantity_payment_export, wp.product_quantity_payment_left,
        wp.price, wp.type_items, wp.type_export, wp.type_transfer,
        wp.lot_code, wp.date_sx, wp.date_sd, wp.series,
        NOW(), 'tblwarehouse_product'
    FROM staging.tblwarehouse_product wp
    LEFT JOIN core.dim_product            dp ON dp.product_id   = wp.product_id
    LEFT JOIN core.dim_warehouse          dw ON dw.warehouse_id = wp.warehouse_id
    LEFT JOIN core.dim_warehouse_location dl ON dl.location_id  = wp.localtion
    WHERE NOT EXISTS (
        SELECT 1 FROM core.fact_warehouse_stock f WHERE f.stock_id = wp.id
    )
    RETURNING 1
)
SELECT
    (SELECT count(*) FROM fix_keys)  AS keys_fixed,
    (SELECT count(*) FROM new_rows)  AS rows_inserted;
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
    TO_CHAR(po.date::DATE, 'YYYYMMDD')::INT,
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


SQL_FACT_PRODUCTION_ORDER_ITEMS = """
INSERT INTO core.fact_production_order_items (
    prod_item_id, productions_orders_id, product_key, prod_date_key,
    quantity, type_items, items_code, versions_bom, versions_stage,
    etl_loaded_at, etl_source
)
SELECT
    poi.id, poi.productions_orders_id,
    dp.product_key,
    COALESCE(TO_CHAR(po.date::DATE, 'YYYYMMDD')::INT, 19000101),
    poi.quantity, poi.type_items, poi.items_code,
    poi.versions_bom, poi.versions_stage,
    NOW(), 'tbl_productions_orders_items'
FROM staging.tbl_productions_orders_items poi
LEFT JOIN staging.tbl_productions_orders  po ON po.id = poi.productions_orders_id
LEFT JOIN core.dim_product               dp ON dp.product_id = poi.items_id
WHERE NOT EXISTS (
    SELECT 1 FROM core.fact_production_order_items f WHERE f.prod_item_id = poi.id
);
"""

SQL_FACT_PRODUCTION_STAGES = """
INSERT INTO core.fact_production_stages (
    prod_stage_id, productions_orders_id, productions_orders_items_id,
    staff_key, stage_date_key,
    number, number_hours, total_time,
    number_face, number_operations, number_cutting,
    quota_time_f1, quota_time_f2,
    stage_id, machines_id, final_stage, active, type,
    etl_loaded_at, etl_source
)
SELECT
    s.id, s.productions_orders_id, s.productions_orders_items_id,
    ds.staff_key,
    COALESCE(TO_CHAR(s.date_active::DATE, 'YYYYMMDD')::INT, 19000101),
    s.number, s.number_hours, s.total_time,
    s.number_face, s.number_operations, s.number_cutting,
    s.quota_time_f1, s.quota_time_f2,
    s.stage_id, s.machines_id, s.final_stage, s.active, s.type,
    NOW(), 'tbl_productions_orders_items_stages'
FROM staging.tbl_productions_orders_items_stages s
LEFT JOIN core.dim_staff ds ON ds.staff_id = s.staff_active
WHERE NOT EXISTS (
    SELECT 1 FROM core.fact_production_stages f WHERE f.prod_stage_id = s.id
);
"""

SQL_FACT_PURCHASE_PRODUCT_ITEMS = """
INSERT INTO core.fact_purchase_product_items (
    pp_item_id, purchase_product_id,
    product_key, warehouse_key, location_key, import_date_key,
    quantity, quantity_exchange, quantity_single, quantity_semi_product,
    price, amount, quantity_unit, quantity_stock, quantity_payment,
    type_item, item_code, type_order, unit_id,
    etl_loaded_at, etl_source
)
SELECT
    ppi.id, ppi.purchase_product_id,
    dp.product_key,
    dw.warehouse_key,
    dwl.location_key,
    COALESCE(TO_CHAR(pp.date::DATE, 'YYYYMMDD')::INT, 19000101),
    ppi.quantity, ppi.quantity_exchange, ppi.quantity_single, ppi.quantity_semi_product,
    ppi.price, ppi.amount, ppi.quantity_unit, ppi.quantity_stock, ppi.quantity_payment,
    ppi.type_item, ppi.item_code, ppi.type_order, ppi.unit_id,
    NOW(), 'tbl_purchase_product_items'
FROM staging.tbl_purchase_product_items      ppi
JOIN  staging.tbl_purchase_products          pp  ON pp.id  = ppi.purchase_product_id
LEFT JOIN core.dim_product                   dp  ON dp.product_id   = ppi.item_id
LEFT JOIN core.dim_warehouse                 dw  ON dw.warehouse_id = pp.warehouse_id
LEFT JOIN core.dim_warehouse_location        dwl ON dwl.location_id = ppi.location_id
WHERE NOT EXISTS (
    SELECT 1 FROM core.fact_purchase_product_items f WHERE f.pp_item_id = ppi.id
);
"""

SQL_FACT_WH_EXPORT = """
WITH deleted AS (
    -- Remove existing rows for ids being loaded (idempotent re-insert)
    DELETE FROM core.fact_warehouse_export
    WHERE export_id IN (SELECT id FROM staging.tblwarehouse_export)
)
INSERT INTO core.fact_warehouse_export (
    export_id, product_key, warehouse_key, location_key,
    export_date_key,
    quantity, product_quantity_unit, product_quantity_payment,
    type_items, type_export, lot_code, date_sx, date_sd,
    etl_loaded_at, etl_source
)
SELECT
    we.id,
    dp.product_key,
    dw.warehouse_key,
    dwl.location_key,
    COALESCE(TO_CHAR(we.date_export::DATE, 'YYYYMMDD')::INT, 19000101),
    we.quantity,
    we.product_quantity_unit,
    we.product_quantity_payment,
    we.type_items,
    we.type_export::VARCHAR,
    we.lot_code,
    we.date_sx,
    we.date_sd,
    NOW(), 'tblwarehouse_export'
FROM staging.tblwarehouse_export        we
LEFT JOIN core.dim_product            dp  ON dp.product_id   = we.product_id
LEFT JOIN core.dim_warehouse          dw  ON dw.warehouse_id = we.warehouse_id
LEFT JOIN core.dim_warehouse_location dwl ON dwl.location_id = we.localtion;
"""

SQL_FACT_TRANSFER_WAREHOUSE = """
INSERT INTO core.fact_transfer_warehouse (
    transfer_detail_id, transfer_id,
    product_key, warehouse_from_key, warehouse_to_key,
    location_from_key, location_to_key,
    quantity, quantity_net, price, amount,
    quantity_unit, quantity_stock, quantity_payment,
    type, lot_code, date_sx, date_sd, unit_id,
    etl_loaded_at, etl_source
)
SELECT
    t.id, t.id_transfer,
    dp.product_key,
    dwf.warehouse_key,
    dwt.warehouse_key,
    dwlf.location_key,
    dwlt.location_key,
    t.quantity, t.quantity_net, t.price, t.amount,
    t.quantity_unit, t.quantity_stock, t.quantity_payment,
    t.type, t.lot_code, t.date_sx, t.date_sd, t.unit_id,
    NOW(), 'tbltransfer_warehouse_detail'
FROM staging.tbltransfer_warehouse_detail          t
LEFT JOIN core.dim_product              dp   ON dp.product_id   = t.id_items
LEFT JOIN core.dim_warehouse            dwf  ON dwf.warehouse_id = t.warehouses_id
LEFT JOIN core.dim_warehouse            dwt  ON dwt.warehouse_id = t.warehouses_to
LEFT JOIN core.dim_warehouse_location   dwlf ON dwlf.location_id = t.localtion_id
LEFT JOIN core.dim_warehouse_location   dwlt ON dwlt.location_id = t.localtion_to
WHERE NOT EXISTS (
    SELECT 1 FROM core.fact_transfer_warehouse f WHERE f.transfer_detail_id = t.id
);
"""

# ============================================
# Runner
# ============================================

TRANSFORM_STEPS = [
    # Dimensions (phải chạy trước facts)
    ("dim_customer",              SQL_DIM_CUSTOMER),
    ("dim_product",               SQL_DIM_PRODUCT),
    ("dim_staff",                 SQL_DIM_STAFF),
    ("dim_department",            SQL_DIM_DEPARTMENT),
    ("dim_warehouse",             SQL_DIM_WAREHOUSE),
    ("dim_supplier",              SQL_DIM_SUPPLIER),
    ("dim_warehouse_location",    SQL_DIM_WAREHOUSE_LOCATION),
    ("dim_manufacture",           SQL_DIM_MANUFACTURE),
    ("dim_price_group",           SQL_DIM_PRICE_GROUP),
    # Update dim_customer voi price_group_key (can ALTER truoc bang init_schema.py)
    ("dim_customer [UPDATE price_group_key]", SQL_UPDATE_DIM_CUSTOMER_PRICE_GROUP),
    # Facts
    ("fact_orders",               SQL_FACT_ORDERS),
    ("fact_order_items",          SQL_FACT_ORDER_ITEMS),
    ("fact_delivery_items",       SQL_FACT_DELIVERY_ITEMS),
    ("fact_warehouse_stock",      SQL_FACT_WH_STOCK),
    ("fact_purchase_order_items", SQL_FACT_PO_ITEMS),
    ("fact_production_order_items",  SQL_FACT_PRODUCTION_ORDER_ITEMS),
    ("fact_production_stages",       SQL_FACT_PRODUCTION_STAGES),
    ("fact_purchase_product_items",  SQL_FACT_PURCHASE_PRODUCT_ITEMS),
    ("fact_warehouse_export",         SQL_FACT_WH_EXPORT),
    ("fact_transfer_warehouse",      SQL_FACT_TRANSFER_WAREHOUSE),
]


# Steps that are allowed to fail silently (e.g. column already exists, or missing column)
_SOFT_STEPS = {"dim_customer [UPDATE price_group_key]"}


def run_transforms(pg_engine):
    """Chay toan bo buoc transform staging -> core."""
    for step_name, sql in TRANSFORM_STEPS:
        try:
            with pg_engine.begin() as conn:
                result = conn.execute(text(sql))
            # CTE steps (fact_orders, fact_order_items, fact_warehouse_stock) end with a
            # SELECT returning (keys_fixed, rows_inserted) — log both counts.
            if result.returns_rows:
                row = result.fetchone()
                keys_fixed    = int(row[0]) if row else 0
                rows_inserted = int(row[1]) if row else 0
                logger.success(
                    f"[Transform] {step_name} -> "
                    f"{keys_fixed:,} keys fixed, {rows_inserted:,} new rows inserted."
                )
            else:
                logger.success(
                    f"[Transform] {step_name} -> {result.rowcount:,} rows affected."
                )
        except Exception as e:
            if step_name in _SOFT_STEPS:
                short = str(e).split('\n')[0]
                logger.warning(f"[Transform] {step_name} SKIPPED (column missing? run init_schema.py first): {short}")
            else:
                logger.error(f"[Transform] {step_name} FAILED: {e}")
                raise
