-- ============================================
-- BƯỚC 6: STAGING TABLES
-- ============================================

-- staging.stg_staff
CREATE TABLE staging.stg_staff AS SELECT * FROM core.dim_staff WHERE 1=0;
ALTER TABLE staging.stg_staff ADD COLUMN stg_loaded_at TIMESTAMP DEFAULT NOW();
ALTER TABLE staging.stg_staff ADD COLUMN stg_checksum  TEXT;

-- staging.stg_department
CREATE TABLE staging.stg_department AS SELECT * FROM core.dim_department WHERE 1=0;
ALTER TABLE staging.stg_department ADD COLUMN stg_loaded_at TIMESTAMP DEFAULT NOW();
ALTER TABLE staging.stg_department ADD COLUMN stg_checksum  TEXT;

-- staging.stg_customer
CREATE TABLE staging.stg_customer AS SELECT * FROM core.dim_customer WHERE 1=0;
ALTER TABLE staging.stg_customer ADD COLUMN stg_loaded_at TIMESTAMP DEFAULT NOW();
ALTER TABLE staging.stg_customer ADD COLUMN stg_checksum  TEXT;

-- staging.stg_product
CREATE TABLE staging.stg_product AS SELECT * FROM core.dim_product WHERE 1=0;
ALTER TABLE staging.stg_product ADD COLUMN stg_loaded_at TIMESTAMP DEFAULT NOW();
ALTER TABLE staging.stg_product ADD COLUMN stg_checksum  TEXT;

-- staging.stg_price_group
CREATE TABLE staging.stg_price_group AS SELECT * FROM core.dim_price_group WHERE 1=0;
ALTER TABLE staging.stg_price_group ADD COLUMN stg_loaded_at TIMESTAMP DEFAULT NOW();
ALTER TABLE staging.stg_price_group ADD COLUMN stg_checksum  TEXT;

-- staging.stg_warehouse
CREATE TABLE staging.stg_warehouse AS SELECT * FROM core.dim_warehouse WHERE 1=0;
ALTER TABLE staging.stg_warehouse ADD COLUMN stg_loaded_at TIMESTAMP DEFAULT NOW();
ALTER TABLE staging.stg_warehouse ADD COLUMN stg_checksum  TEXT;

-- staging.stg_warehouse_location
CREATE TABLE staging.stg_warehouse_location AS SELECT * FROM core.dim_warehouse_location WHERE 1=0;
ALTER TABLE staging.stg_warehouse_location ADD COLUMN stg_loaded_at TIMESTAMP DEFAULT NOW();
ALTER TABLE staging.stg_warehouse_location ADD COLUMN stg_checksum  TEXT;

-- staging.stg_supplier
CREATE TABLE staging.stg_supplier AS SELECT * FROM core.dim_supplier WHERE 1=0;
ALTER TABLE staging.stg_supplier ADD COLUMN stg_loaded_at TIMESTAMP DEFAULT NOW();
ALTER TABLE staging.stg_supplier ADD COLUMN stg_checksum  TEXT;

-- staging.stg_orders
CREATE TABLE staging.stg_orders AS SELECT * FROM core.fact_orders WHERE 1=0;
ALTER TABLE staging.stg_orders ADD COLUMN stg_loaded_at TIMESTAMP DEFAULT NOW();
ALTER TABLE staging.stg_orders ADD COLUMN stg_checksum  TEXT;

-- staging.stg_order_items
CREATE TABLE staging.stg_order_items AS SELECT * FROM core.fact_order_items WHERE 1=0;
ALTER TABLE staging.stg_order_items ADD COLUMN stg_loaded_at TIMESTAMP DEFAULT NOW();
ALTER TABLE staging.stg_order_items ADD COLUMN stg_checksum  TEXT;

-- staging.stg_delivery_items
CREATE TABLE staging.stg_delivery_items AS SELECT * FROM core.fact_delivery_items WHERE 1=0;
ALTER TABLE staging.stg_delivery_items ADD COLUMN stg_loaded_at TIMESTAMP DEFAULT NOW();
ALTER TABLE staging.stg_delivery_items ADD COLUMN stg_checksum  TEXT;

-- staging.stg_warehouse_stock
CREATE TABLE staging.stg_warehouse_stock AS SELECT * FROM core.fact_warehouse_stock WHERE 1=0;
ALTER TABLE staging.stg_warehouse_stock ADD COLUMN stg_loaded_at TIMESTAMP DEFAULT NOW();
ALTER TABLE staging.stg_warehouse_stock ADD COLUMN stg_checksum  TEXT;

-- staging.stg_purchase_order_items
CREATE TABLE staging.stg_purchase_order_items AS SELECT * FROM core.fact_purchase_order_items WHERE 1=0;
ALTER TABLE staging.stg_purchase_order_items ADD COLUMN stg_loaded_at TIMESTAMP DEFAULT NOW();
ALTER TABLE staging.stg_purchase_order_items ADD COLUMN stg_checksum  TEXT;

-- staging.stg_purchase_product_items
CREATE TABLE staging.stg_purchase_product_items AS SELECT * FROM core.fact_purchase_product_items WHERE 1=0;
ALTER TABLE staging.stg_purchase_product_items ADD COLUMN stg_loaded_at TIMESTAMP DEFAULT NOW();
ALTER TABLE staging.stg_purchase_product_items ADD COLUMN stg_checksum  TEXT;

-- staging.stg_production_order_items
CREATE TABLE staging.stg_production_order_items AS SELECT * FROM core.fact_production_order_items WHERE 1=0;
ALTER TABLE staging.stg_production_order_items ADD COLUMN stg_loaded_at TIMESTAMP DEFAULT NOW();
ALTER TABLE staging.stg_production_order_items ADD COLUMN stg_checksum  TEXT;

-- staging.stg_production_stages
CREATE TABLE staging.stg_production_stages AS SELECT * FROM core.fact_production_stages WHERE 1=0;
ALTER TABLE staging.stg_production_stages ADD COLUMN stg_loaded_at TIMESTAMP DEFAULT NOW();
ALTER TABLE staging.stg_production_stages ADD COLUMN stg_checksum  TEXT;

-- ETL log table
CREATE TABLE staging.etl_log (
    log_id          BIGSERIAL PRIMARY KEY,
    table_name      VARCHAR(100),
    rows_extracted  INT,
    rows_loaded     INT,
    rows_skipped    INT,
    status          VARCHAR(20),
    error_message   TEXT,
    started_at      TIMESTAMP,
    finished_at     TIMESTAMP DEFAULT NOW()
);
