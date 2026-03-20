-- ============================================
-- BƯỚC 4: CORE DIMENSIONS
-- ============================================

-- --------------------------------------------
-- dim_staff
-- --------------------------------------------
CREATE TABLE core.dim_staff (
    staff_key           SERIAL PRIMARY KEY,
    staff_id            INT          NOT NULL,
    staff_code          TEXT,
    firstname           VARCHAR(50),
    lastname            VARCHAR(50),
    fullname            VARCHAR(101),
    email               VARCHAR(100),
    phonenumber         VARCHAR(30),
    gender              CHAR(10),
    birthday            DATE,
    day_in              DATE,
    status_work         SMALLINT,
    role                INT,
    admin               SMALLINT,
    id_branch           INT,
    department_id       INT,
    is_active           BOOLEAN,
    etl_loaded_at       TIMESTAMP DEFAULT NOW(),
    etl_source          VARCHAR(50) DEFAULT 'tblstaff'
);
CREATE INDEX idx_dim_staff_id     ON core.dim_staff(staff_id);
CREATE INDEX idx_dim_staff_branch ON core.dim_staff(id_branch);

-- --------------------------------------------
-- dim_department
-- --------------------------------------------
CREATE TABLE core.dim_department (
    department_key      SERIAL PRIMARY KEY,
    department_id       INT          NOT NULL,
    department_code     VARCHAR(250),
    department_name     VARCHAR(100),
    type                INT,
    is_active           BOOLEAN,
    etl_loaded_at       TIMESTAMP DEFAULT NOW(),
    etl_source          VARCHAR(50) DEFAULT 'tbldepartments'
);
CREATE INDEX idx_dim_dept_id ON core.dim_department(department_id);

-- --------------------------------------------
-- dim_customer
-- --------------------------------------------
CREATE TABLE core.dim_customer (
    customer_key        SERIAL PRIMARY KEY,
    customer_id         INT          NOT NULL,
    customer_code       VARCHAR(250),
    prefix_client       VARCHAR(250),
    company             VARCHAR(191),
    company_short       VARCHAR(500),
    representative      VARCHAR(255),
    fullname            VARCHAR(500),
    phonenumber         VARCHAR(30),
    email               VARCHAR(250),
    type_client         INT,
    country             INT,
    city                VARCHAR(100),
    district            INT,
    ward                INT,
    address             VARCHAR(100),
    vat                 VARCHAR(50),
    debt_limit          NUMERIC(22,4),
    debt_limit_day      NUMERIC(22,4),
    discount            NUMERIC(22,4),
    table_price_id      INT,
    status_clients      INT,
    vip_rating          INT,
    time_payment        NUMERIC(12,4),
    is_active           BOOLEAN,
    datecreated         TIMESTAMP,
    -- price_group (added via setup.py if not present)
    price_group_key     INTEGER,
    price_group_code    VARCHAR,
    price_group_name    VARCHAR,
    etl_loaded_at       TIMESTAMP DEFAULT NOW(),
    etl_source          VARCHAR(50) DEFAULT 'tblclients'
);
CREATE INDEX idx_dim_customer_id   ON core.dim_customer(customer_id);
CREATE INDEX idx_dim_customer_code ON core.dim_customer(customer_code);

-- --------------------------------------------
-- dim_product
-- --------------------------------------------
CREATE TABLE core.dim_product (
    product_key              SERIAL PRIMARY KEY,
    product_id               INT          NOT NULL,
    product_code             VARCHAR(255),
    product_name             VARCHAR(1000),
    product_name_customer    VARCHAR(1000),
    type_products            VARCHAR(255),
    category_id              INT,
    unit_id                  INT,
    species                  INT,
    brand                    VARCHAR(255),
    brand_id                 INT,
    price_import             NUMERIC(22,4),
    price_sell               NUMERIC(22,4),
    price_processing         NUMERIC(22,4),
    loss                     NUMERIC(12,4),
    bom_id                   INT,
    versions                 VARCHAR(255),
    versions_stage           VARCHAR(255),
    longs                    NUMERIC(12,4),
    wide                     NUMERIC(12,4),
    height                   NUMERIC(12,4),
    warranty                 INT,
    status                   SMALLINT,
    id_branch                INT,
    is_no_stock              INT,
    conversion_unit          INT,
    conversion_quantity_unit NUMERIC(12,4),
    is_active                BOOLEAN,
    date_created             TIMESTAMP,
    etl_loaded_at            TIMESTAMP DEFAULT NOW(),
    etl_source               VARCHAR(50) DEFAULT 'tbl_products'
);
CREATE INDEX idx_dim_product_id   ON core.dim_product(product_id);
CREATE INDEX idx_dim_product_code ON core.dim_product(product_code);
CREATE INDEX idx_dim_product_type ON core.dim_product(type_products);
CREATE INDEX idx_dim_product_cat  ON core.dim_product(category_id);

-- --------------------------------------------
-- dim_price_group
-- --------------------------------------------
CREATE TABLE core.dim_price_group (
    price_group_key  SERIAL PRIMARY KEY,
    price_group_id   INT          NOT NULL,
    price_group_name VARCHAR(255),
    is_active        BOOLEAN,
    etl_loaded_at    TIMESTAMP DEFAULT NOW(),
    etl_source       VARCHAR(50) DEFAULT 'tblgroup_price'
);
CREATE INDEX idx_dim_pricegrp_id ON core.dim_price_group(price_group_id);

-- --------------------------------------------
-- dim_warehouse
-- --------------------------------------------
CREATE TABLE core.dim_warehouse (
    warehouse_key       SERIAL PRIMARY KEY,
    warehouse_id        INT          NOT NULL,
    warehouse_code      VARCHAR(255),
    warehouse_name      VARCHAR(255),
    address             VARCHAR(255),
    id_group_warehouse  INT,
    id_branch           INT,
    supplier_id         INT,
    etl_loaded_at       TIMESTAMP DEFAULT NOW(),
    etl_source          VARCHAR(50) DEFAULT 'tblwarehouse'
);
CREATE INDEX idx_dim_wh_id     ON core.dim_warehouse(warehouse_id);
CREATE INDEX idx_dim_wh_branch ON core.dim_warehouse(id_branch);

-- --------------------------------------------
-- dim_warehouse_location
-- --------------------------------------------
CREATE TABLE core.dim_warehouse_location (
    location_key        SERIAL PRIMARY KEY,
    location_id         INT          NOT NULL,
    location_name       VARCHAR(250),
    location_code       VARCHAR(250),
    warehouse_id        INT,
    id_parent           INT,
    name_parent         VARCHAR(255),
    lever               INT,
    child               INT,
    status              INT,
    stage_id            INT,
    pod_id              INT,
    etl_loaded_at       TIMESTAMP DEFAULT NOW(),
    etl_source          VARCHAR(50) DEFAULT 'tbllocaltion_warehouses'
);
CREATE INDEX idx_dim_whloc_id ON core.dim_warehouse_location(location_id);
CREATE INDEX idx_dim_whloc_wh ON core.dim_warehouse_location(warehouse_id);

-- --------------------------------------------
-- dim_supplier
-- --------------------------------------------
CREATE TABLE core.dim_supplier (
    supplier_key      SERIAL PRIMARY KEY,
    supplier_id       INT          NOT NULL,
    supplier_code     VARCHAR(55),
    supplier_prefix   VARCHAR(55),
    company           VARCHAR(255),
    abbreviation      VARCHAR(150),
    representative    VARCHAR(255),
    phone             VARCHAR(55),
    email             VARCHAR(255),
    vat               VARCHAR(55),
    address           TEXT,
    city              INT,
    district          INT,
    ward              INT,
    country           INT,
    groups_in         INT,
    type              INT,
    type_suppliers    INT,
    debt_limit        NUMERIC(22,4),
    debt_begin        NUMERIC(22,4),
    time_payment      NUMERIC(12,4),
    deadline_contract DATE,
    is_active         BOOLEAN,
    datecreated       TIMESTAMP,
    etl_loaded_at     TIMESTAMP DEFAULT NOW(),
    etl_source          VARCHAR(50) DEFAULT 'tblsuppliers'
);
CREATE INDEX idx_dim_sup_id   ON core.dim_supplier(supplier_id);
CREATE INDEX idx_dim_sup_code ON core.dim_supplier(supplier_code);

-- --------------------------------------------
-- dim_manufacture
-- --------------------------------------------
CREATE TABLE core.dim_manufacture (
    manufacture_key      SERIAL PRIMARY KEY,
    manufacture_id       INT          NOT NULL,
    reference_no         VARCHAR(255),
    id_production_detail INT,
    status               SMALLINT,
    status_manufactures  INT,
    id_branch            INT,
    date_created         TIMESTAMP,
    etl_loaded_at        TIMESTAMP DEFAULT NOW(),
    etl_source           VARCHAR(50) DEFAULT 'tbl_manufactures'
);
CREATE INDEX idx_dim_mfg_id  ON core.dim_manufacture(manufacture_id);
CREATE INDEX idx_dim_mfg_ref ON core.dim_manufacture(reference_no);
