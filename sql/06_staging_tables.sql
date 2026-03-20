-- ============================================
-- BƯỚC 6: STAGING TABLES
-- ============================================


-- --------------------------------------------
-- staging.tbl_orders
-- --------------------------------------------
CREATE TABLE staging.tbl_orders (
    id                              INT,
    date                            TIMESTAMP,
    reference_no                    VARCHAR(255),
    reference_no_customer           VARCHAR(255),
    customer_id                     INT,
    customer_name                   VARCHAR(255),
    address_delivery_id             INT,
    employee_id                     INT,
    note                            TEXT,
    count_items                     INT,
    total_quantity                  NUMERIC(12,4),
    total_amount_items              NUMERIC(22,4),
    total_tax_items                 NUMERIC(22,4),
    total_discount_percent_items    NUMERIC(22,4),
    total_discount_direct_items     NUMERIC(22,4),
    grand_total_items               NUMERIC(22,4),
    tax_id                          INT,
    tax_name                        VARCHAR(255),
    tax_rate                        NUMERIC(12,4),
    total_tax                       NUMERIC(22,4),
    discount_percent                NUMERIC(12,4),
    total_discount_percent          NUMERIC(22,4),
    total_discount_direct           NUMERIC(22,4),
    cost_delivery                   NUMERIC(22,4),
    grand_total                     NUMERIC(22,4),
    status                          VARCHAR(255),
    user_status                     INT,
    date_status                     TIMESTAMP,
    created_by                      INT,
    date_created                    TIMESTAMP,
    updated_by                      INT,
    date_updated                    TIMESTAMP,
    quotes_id                       INT,
    count_delivery                  INT,
    total_quantity_had_delivery     NUMERIC(22,4),
    total_quantity_not_delivery     NUMERIC(22,4),
    table_price_id                  INT,
    productions_plan_id             INT,
    status_payment                  INT,
    total_payment                   NUMERIC(25,4),
    price_other_expenses            NUMERIC(22,4),
    status_custom                   VARCHAR(255),
    type_bills                      INT,
    payment_mode                    INT,
    warehouse_id                    INT,
    person_contact_id               INT,
    contract_id                     INT,
    total_cost_temporary_capital    NUMERIC(22,4),
    total_profit_temporary_capital  NUMERIC(22,4),
    total_cost                      NUMERIC(22,4),
    total_profit                    NUMERIC(22,4),
    id_branch                       INT,
    status_productions_orders       INT,
    currencies                      INT,
    amount_to_vnd                   NUMERIC(12,4),
    type_orders                     INT,
    status_orders                   INT,
    grand_total_quantity            NUMERIC(22,4),
    is_cancel                       SMALLINT,
    date_cancel                     TIMESTAMP,
    user_cancel                     INT,
    is_end                          SMALLINT,
    etl_loaded_at                   TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_stg_orders_id       ON staging.tbl_orders(id);
CREATE INDEX idx_stg_orders_customer ON staging.tbl_orders(customer_id);
CREATE INDEX idx_stg_orders_date     ON staging.tbl_orders(date);


-- --------------------------------------------
-- staging.tbl_order_items
-- --------------------------------------------
CREATE TABLE staging.tbl_order_items (
    id                              INT,
    order_id                        INT,
    type_item                       VARCHAR(255),
    item_id                         INT,
    item_code                       VARCHAR(255),
    item_name                       VARCHAR(255),
    quantity                        NUMERIC(22,4),
    price                           NUMERIC(22,4),
    amount                          NUMERIC(22,4),
    tax_id_item                     INT,
    tax_name_item                   VARCHAR(255),
    tax_rate_item                   INT,
    tax_amount_item                 NUMERIC(22,4),
    discount_percent_item           NUMERIC(12,4),
    discount_percent_amount_item    NUMERIC(22,4),
    discount_direct_amount_item     NUMERIC(22,4),
    total_amount                    NUMERIC(22,4),
    quantity_delivery               NUMERIC(22,4),
    quantity_not_delivery           NUMERIC(22,4),
    quantity_purchase               NUMERIC(22,4),
    type_gift                       INT,
    cost_temporary_capital          NUMERIC(22,4),
    profit_temporary_capital        NUMERIC(22,4),
    cost                            NUMERIC(22,4),
    profit                          NUMERIC(22,4),
    quantity_returned               NUMERIC(22,4),
    versions_stage                  VARCHAR(255),
    active                          SMALLINT,
    staff_active                    INT,
    date_active                     TIMESTAMP,
    unit_id                         INT,
    conversion_quantity_unit        NUMERIC(12,4),
    etl_loaded_at                   TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_stg_oi_id    ON staging.tbl_order_items(id);
CREATE INDEX idx_stg_oi_order ON staging.tbl_order_items(order_id);
CREATE INDEX idx_stg_oi_item  ON staging.tbl_order_items(item_id);


-- --------------------------------------------
-- staging.tbl_order_items_stages
-- --------------------------------------------
CREATE TABLE staging.tbl_order_items_stages (
    id              INT,
    order_id        INT,
    order_item_id   INT,
    stage_id        INT,
    number          INT,
    number_hours    NUMERIC(12,4),
    final_stage     SMALLINT,
    active          INT,
    staff_active    INT,
    date_active     TIMESTAMP,
    etl_loaded_at   TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_stg_ois_id    ON staging.tbl_order_items_stages(id);
CREATE INDEX idx_stg_ois_order ON staging.tbl_order_items_stages(order_id);


-- --------------------------------------------
-- staging.tbl_deliveries
-- --------------------------------------------
CREATE TABLE staging.tbl_deliveries (
    id                              INT,
    date                            TIMESTAMP,
    reference_no                    VARCHAR(255),
    customer_id                     INT,
    customer_name                   VARCHAR(255),
    address_delivery_id             INT,
    employee_id                     INT,
    note                            TEXT,
    count_items                     INT,
    total_quantity                  NUMERIC(12,4),
    total_amount_items              NUMERIC(22,4),
    total_tax_items                 NUMERIC(22,4),
    grand_total_items               NUMERIC(22,4),
    total_tax                       NUMERIC(22,4),
    total_discount_percent          NUMERIC(22,4),
    total_discount_direct           NUMERIC(22,4),
    grand_total                     NUMERIC(22,4),
    status                          VARCHAR(255),
    user_status                     INT,
    date_status                     TIMESTAMP,
    created_by                      INT,
    date_created                    TIMESTAMP,
    updated_by                      INT,
    date_updated                    TIMESTAMP,
    order_id                        INT,
    warehouseman_id                 INT,
    date_warehouseman               TIMESTAMP,
    type_bills                      SMALLINT,
    received_certificate            INT,
    id_branch                       INT,
    additional_costs                NUMERIC(22,4),
    etl_loaded_at                   TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_stg_del_id       ON staging.tbl_deliveries(id);
CREATE INDEX idx_stg_del_customer ON staging.tbl_deliveries(customer_id);
CREATE INDEX idx_stg_del_order    ON staging.tbl_deliveries(order_id);
CREATE INDEX idx_stg_del_date     ON staging.tbl_deliveries(date);


-- --------------------------------------------
-- staging.tbl_delivery_items
-- --------------------------------------------
CREATE TABLE staging.tbl_delivery_items (
    id                              INT,
    delivery_id                     INT,
    order_item_id                   INT,
    warehouse_id                    INT,
    location_id                     INT,
    type_item                       VARCHAR(255),
    item_id                         INT,
    item_code                       VARCHAR(255),
    item_name                       VARCHAR(255),
    quantity                        NUMERIC(22,4),
    quantity_loss                   NUMERIC(22,4),
    quantity_sample                 NUMERIC(22,4),
    price                           NUMERIC(22,4),
    amount                          NUMERIC(22,4),
    tax_id_item                     INT,
    tax_name_item                   VARCHAR(255),
    tax_rate_item                   INT,
    tax_amount_item                 NUMERIC(22,4),
    discount_percent_item           NUMERIC(12,4),
    discount_percent_amount_item    NUMERIC(22,4),
    discount_direct_amount_item     NUMERIC(22,4),
    total_amount                    NUMERIC(22,4),
    lot_code                        VARCHAR(255),
    date_sx                         DATE,
    date_sd                         DATE,
    quantity_unit                   NUMERIC(22,4),
    quantity_stock                  NUMERIC(22,4),
    quantity_payment                NUMERIC(22,4),
    unit_id                         INT,
    conversion_quantity_unit        NUMERIC(12,4),
    etl_loaded_at                   TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_stg_di_id       ON staging.tbl_delivery_items(id);
CREATE INDEX idx_stg_di_delivery ON staging.tbl_delivery_items(delivery_id);
CREATE INDEX idx_stg_di_item     ON staging.tbl_delivery_items(item_id);


-- --------------------------------------------
-- staging.tblclients
-- --------------------------------------------
CREATE TABLE staging.tblclients (
    userid              INT,
    code_client         VARCHAR(250),
    prefix_client       VARCHAR(250),
    company             VARCHAR(191),
    company_short       VARCHAR(500),
    representative      VARCHAR(255),
    fullname            VARCHAR(500),
    phonenumber         VARCHAR(30),
    email_client        VARCHAR(250),
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
    active              INT,
    datecreated         TIMESTAMP,
    date_update         TIMESTAMP,
    etl_loaded_at       TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_stg_clients_id   ON staging.tblclients(userid);
CREATE INDEX idx_stg_clients_code ON staging.tblclients(code_client);


-- --------------------------------------------
-- staging.tbl_products
-- --------------------------------------------
CREATE TABLE staging.tbl_products (
    id                       INT,
    category_id              INT,
    type_products            VARCHAR(255),
    code                     VARCHAR(255),
    name                     VARCHAR(1000),
    name_customer            VARCHAR(1000),
    price_import             NUMERIC(22,4),
    price_sell               NUMERIC(22,4),
    price_processing         NUMERIC(22,4),
    unit_id                  INT,
    bom_id                   INT,
    versions                 VARCHAR(255),
    versions_stage           VARCHAR(255),
    species                  INT,
    brand                    VARCHAR(255),
    brand_id                 INT,
    longs                    NUMERIC(12,4),
    wide                     NUMERIC(12,4),
    height                   NUMERIC(12,4),
    warranty                 INT,
    status                   SMALLINT,
    loss                     NUMERIC(12,4),
    id_branch                INT,
    is_no_stock              INT,
    conversion_unit          INT,
    conversion_quantity_unit NUMERIC(12,4),
    date_created             TIMESTAMP,
    date_updated             TIMESTAMP,
    etl_loaded_at            TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_stg_products_id   ON staging.tbl_products(id);
CREATE INDEX idx_stg_products_code ON staging.tbl_products(code);


-- --------------------------------------------
-- staging.tblwarehouse
-- --------------------------------------------
CREATE TABLE staging.tblwarehouse (
    id                  INT,
    id_group_warehouse  INT,
    code                VARCHAR(255),
    name                VARCHAR(255),
    address             VARCHAR(255),
    note                VARCHAR(255),
    supplier_id         INT,
    id_branch           INT,
    etl_loaded_at       TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_stg_wh_id ON staging.tblwarehouse(id);


-- --------------------------------------------
-- staging.tbllocaltion_warehouses
-- --------------------------------------------
CREATE TABLE staging.tbllocaltion_warehouses (
    id              INT,
    name            VARCHAR(250),
    code            VARCHAR(250),
    warehouse       INT,
    id_parent       INT,
    name_parent     VARCHAR(255),
    child           INT,
    create_by       INT,
    date_create     TIMESTAMP,
    status          INT,
    lever           INT,
    stage_id        INT,
    pod_id          INT,
    order_id        INT,
    order_item_id   INT,
    etl_loaded_at   TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_stg_whloc_id ON staging.tbllocaltion_warehouses(id);
CREATE INDEX idx_stg_whloc_wh ON staging.tbllocaltion_warehouses(warehouse);


-- --------------------------------------------
-- staging.tblwarehouse_product
-- --------------------------------------------
CREATE TABLE staging.tblwarehouse_product (
    id                              INT,
    warehouse_id                    INT,
    localtion                       INT,
    import_id                       INT,
    product_id                      INT,
    type_items                      VARCHAR(100),
    date_import                     DATE,
    date_warehouse                  TIMESTAMP,
    quantity                        NUMERIC(22,4),
    quantity_left                   NUMERIC(22,4),
    quantity_export                 NUMERIC(22,4),
    type_export                     INT,
    price                           NUMERIC(22,4),
    type_transfer                   INT,
    series                          INT,
    quantity_exchange               NUMERIC(22,4),
    quantity_exchange_left          NUMERIC(22,4),
    quantity_exchange_export        NUMERIC(22,4),
    lot_code                        VARCHAR(255),
    date_sx                         DATE,
    date_sd                         DATE,
    product_quantity_unit           NUMERIC(22,4),
    product_quantity_unit_export    NUMERIC(22,4),
    product_quantity_unit_left      NUMERIC(22,4),
    product_quantity_payment        NUMERIC(22,4),
    product_quantity_payment_export NUMERIC(22,4),
    product_quantity_payment_left   NUMERIC(22,4),
    etl_loaded_at                   TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_stg_whp_id      ON staging.tblwarehouse_product(id);
CREATE INDEX idx_stg_whp_product ON staging.tblwarehouse_product(product_id);
CREATE INDEX idx_stg_whp_wh      ON staging.tblwarehouse_product(warehouse_id);


-- --------------------------------------------
-- staging.tblwarehouse_export
-- --------------------------------------------
CREATE TABLE staging.tblwarehouse_export (
    id                       INT,
    warehouse_id             INT,
    localtion                INT,
    export_id                INT,
    product_id               INT,
    type_items               VARCHAR(100),
    date_export              DATE,
    date_warehouse           TIMESTAMP,
    quantity                 NUMERIC(22,4),
    type_export              VARCHAR(100),
    lot_code                 VARCHAR(255),
    date_sx                  DATE,
    date_sd                  DATE,
    product_quantity_unit    NUMERIC(22,4),
    product_quantity_payment NUMERIC(22,4),
    etl_loaded_at            TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_stg_we_id      ON staging.tblwarehouse_export(id);
CREATE INDEX idx_stg_we_product ON staging.tblwarehouse_export(product_id);
CREATE INDEX idx_stg_we_wh      ON staging.tblwarehouse_export(warehouse_id);
CREATE INDEX idx_stg_we_date    ON staging.tblwarehouse_export(date_export);


-- --------------------------------------------
-- staging.tbltransfer_warehouse_detail
-- --------------------------------------------
CREATE TABLE staging.tbltransfer_warehouse_detail (
    id                  INT,
    id_transfer         INT,
    id_items            INT,
    quantity            NUMERIC(22,4),
    quantity_net        NUMERIC(22,4),
    price               NUMERIC(22,4),
    localtion_id        INT,
    warehouses_id       INT,
    warehouses_to       INT,
    localtion_to        INT,
    amount              NUMERIC(22,4),
    type                VARCHAR(100),
    lot_code            VARCHAR(255),
    date_sx             DATE,
    date_sd             DATE,
    order_id_item       INT,
    quantity_unit       NUMERIC(22,4),
    quantity_stock      NUMERIC(22,4),
    quantity_payment    NUMERIC(22,4),
    unit_id             INT,
    etl_loaded_at       TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_stg_twd_id       ON staging.tbltransfer_warehouse_detail(id);
CREATE INDEX idx_stg_twd_transfer ON staging.tbltransfer_warehouse_detail(id_transfer);
CREATE INDEX idx_stg_twd_item     ON staging.tbltransfer_warehouse_detail(id_items);


-- --------------------------------------------
-- staging.tblsuppliers
-- --------------------------------------------
CREATE TABLE staging.tblsuppliers (
    id                  INT,
    prefix              VARCHAR(55),
    code                VARCHAR(55),
    company             VARCHAR(255),
    abbreviation        VARCHAR(150),
    representative      VARCHAR(255),
    phone               VARCHAR(55),
    email               VARCHAR(255),
    vat                 VARCHAR(55),
    address             TEXT,
    city                INT,
    district            INT,
    ward                INT,
    country             INT,
    groups_in           INT,
    type                INT,
    type_suppliers      INT,
    debt_limit          NUMERIC(22,4),
    debt_begin          NUMERIC(22,4),
    time_payment        NUMERIC(12,4),
    deadline_contract   DATE,
    active              INT,
    datecreated         TIMESTAMP,
    date_update         TIMESTAMP,
    etl_loaded_at       TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_stg_sup_id   ON staging.tblsuppliers(id);
CREATE INDEX idx_stg_sup_code ON staging.tblsuppliers(code);


-- --------------------------------------------
-- staging.tblpurchase_order
-- --------------------------------------------
CREATE TABLE staging.tblpurchase_order (
    id                      INT,
    prefix                  VARCHAR(100),
    code                    VARCHAR(100),
    staff_create            INT,
    date_create             TIMESTAMP,
    date                    DATE,
    status                  INT,
    cancel                  VARCHAR(255),
    total                   NUMERIC(22,4),
    totalAll_expected       NUMERIC(22,4),
    price_expected          NUMERIC(22,4),
    totalAll_suppliers      NUMERIC(22,4),
    price_suppliers         NUMERIC(22,4),
    suppliers_id            INT,
    delivery_date           DATE,
    status_pay              INT,
    amount_paid             NUMERIC(22,4),
    order_id                INT,
    currency                INT,
    id_branch               INT,
    is_end                  SMALLINT,
    date_end                TIMESTAMP,
    etl_loaded_at           TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_stg_po_id       ON staging.tblpurchase_order(id);
CREATE INDEX idx_stg_po_supplier ON staging.tblpurchase_order(suppliers_id);
CREATE INDEX idx_stg_po_date     ON staging.tblpurchase_order(date);


-- --------------------------------------------
-- staging.tblpurchase_order_items
-- --------------------------------------------
CREATE TABLE staging.tblpurchase_order_items (
    id                      INT,
    id_purchase_order       INT,
    product_id              INT,
    quantity                NUMERIC(22,4),
    tax_id                  INT,
    tax_rate                INT,
    unit_cost               NUMERIC(22,4),
    subtotal                NUMERIC(22,4),
    type                    VARCHAR(100),
    quantity_suppliers      NUMERIC(22,4),
    price_expected          NUMERIC(22,4),
    price_suppliers         NUMERIC(22,4),
    promotion_expected      NUMERIC(22,4),
    total_expected          NUMERIC(22,4),
    total_suppliers         NUMERIC(22,4),
    quantity_unit           NUMERIC(22,4),
    quantity_stock          NUMERIC(22,4),
    quantity_payment        NUMERIC(22,4),
    etl_loaded_at           TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_stg_poi_id      ON staging.tblpurchase_order_items(id);
CREATE INDEX idx_stg_poi_po      ON staging.tblpurchase_order_items(id_purchase_order);
CREATE INDEX idx_stg_poi_product ON staging.tblpurchase_order_items(product_id);


-- --------------------------------------------
-- staging.tbl_purchase_products
-- --------------------------------------------
CREATE TABLE staging.tbl_purchase_products (
    id                      INT,
    reference_no            VARCHAR(255),
    date                    TIMESTAMP,
    warehouse_id            INT,
    count_items             INT,
    total_quantity          NUMERIC(22,4),
    grand_total             NUMERIC(22,4),
    status                  VARCHAR(255),
    date_status             TIMESTAMP,
    user_status             INT,
    warehouseman_id         INT,
    date_warehouseman       TIMESTAMP,
    type                    INT,
    pois_id                 INT,
    final_stage             INT,
    is_errors               SMALLINT,
    branch_id               INT,
    etl_loaded_at           TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_stg_pp_id   ON staging.tbl_purchase_products(id);
CREATE INDEX idx_stg_pp_wh   ON staging.tbl_purchase_products(warehouse_id);
CREATE INDEX idx_stg_pp_date ON staging.tbl_purchase_products(date);


-- --------------------------------------------
-- staging.tbl_purchase_product_items
-- --------------------------------------------
CREATE TABLE staging.tbl_purchase_product_items (
    id                          INT,
    purchase_product_id         INT,
    type_item                   VARCHAR(255),
    item_id                     INT,
    location_id                 INT,
    item_code                   VARCHAR(255),
    item_name                   VARCHAR(255),
    quantity                    NUMERIC(22,4),
    price                       NUMERIC(22,4),
    amount                      NUMERIC(22,4),
    quantity_exchange           NUMERIC(22,4),
    quantity_single             NUMERIC(22,4),
    quantity_semi_product       NUMERIC(22,4),
    type_order                  VARCHAR(10),
    quantity_unit               NUMERIC(22,4),
    quantity_stock              NUMERIC(22,4),
    quantity_payment            NUMERIC(22,4),
    unit_id                     INT,
    conversion_quantity_unit    NUMERIC(12,4),
    etl_loaded_at               TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_stg_ppi_id   ON staging.tbl_purchase_product_items(id);
CREATE INDEX idx_stg_ppi_pp   ON staging.tbl_purchase_product_items(purchase_product_id);
CREATE INDEX idx_stg_ppi_item ON staging.tbl_purchase_product_items(item_id);


-- --------------------------------------------
-- staging.tbl_productions_orders
-- --------------------------------------------
CREATE TABLE staging.tbl_productions_orders (
    id                              INT,
    reference_no                    VARCHAR(255),
    location_id                     INT,
    date                            TIMESTAMP,
    note                            TEXT,
    status                          VARCHAR(255),
    user_status                     INT,
    date_status                     TIMESTAMP,
    total_quantity                  NUMERIC(22,4),
    count_items                     INT,
    created_by                      INT,
    date_created                    TIMESTAMP,
    updated_by                      INT,
    date_updated                    TIMESTAMP,
    status_details                  INT,
    status_orders                   SMALLINT,
    staff_orders                    INT,
    date_orders                     TIMESTAMP,
    is_ptm                          SMALLINT,
    etl_loaded_at                   TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_stg_pro_id  ON staging.tbl_productions_orders(id);
CREATE INDEX idx_stg_pro_ref ON staging.tbl_productions_orders(reference_no);


-- --------------------------------------------
-- staging.tbl_productions_orders_items
-- --------------------------------------------
CREATE TABLE staging.tbl_productions_orders_items (
    id                          INT,
    productions_orders_id       INT,
    production_plan_item_id     INT,
    type_items                  VARCHAR(255),
    items_id                    INT,
    items_code                  VARCHAR(255),
    items_name                  VARCHAR(255),
    quantity                    NUMERIC(22,4),
    versions_bom                VARCHAR(255),
    versions_stage              VARCHAR(255),
    etl_loaded_at               TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_stg_proi_id    ON staging.tbl_productions_orders_items(id);
CREATE INDEX idx_stg_proi_order ON staging.tbl_productions_orders_items(productions_orders_id);
CREATE INDEX idx_stg_proi_item  ON staging.tbl_productions_orders_items(items_id);


-- --------------------------------------------
-- staging.tbl_productions_orders_items_stages
-- --------------------------------------------
CREATE TABLE staging.tbl_productions_orders_items_stages (
    id                              INT,
    productions_orders_id           INT,
    productions_orders_items_id     INT,
    stage_id                        INT,
    number                          INT,
    number_hours                    NUMERIC(12,4),
    final_stage                     INT,
    total_time                      NUMERIC(12,4),
    active                          SMALLINT,
    staff_active                    INT,
    date_active                     TIMESTAMP,
    machines_id                     INT,
    date_start                      TIMESTAMP,
    date_end                        TIMESTAMP,
    type                            SMALLINT,
    number_face                     NUMERIC(12,4),
    number_operations               NUMERIC(12,4),
    number_cutting                  NUMERIC(12,4),
    quota_time_f1                   NUMERIC(12,4),
    quota_time_f2                   NUMERIC(12,4),
    etl_loaded_at                   TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_stg_ps_id    ON staging.tbl_productions_orders_items_stages(id);
CREATE INDEX idx_stg_ps_order ON staging.tbl_productions_orders_items_stages(productions_orders_id);
CREATE INDEX idx_stg_ps_stage ON staging.tbl_productions_orders_items_stages(stage_id);


-- --------------------------------------------
-- staging.tbl_manufactures
-- --------------------------------------------
CREATE TABLE staging.tbl_manufactures (
    id                      INT,
    date                    TIMESTAMP,
    reference_no            VARCHAR(255),
    id_production_detail    INT,
    count_items             INT,
    total_quantity          NUMERIC(22,4),
    status                  SMALLINT,
    user_status             INT,
    date_status             TIMESTAMP,
    created_by              INT,
    date_created            TIMESTAMP,
    updated_by              INT,
    date_updated            TIMESTAMP,
    status_manufactures     INT,
    date_manufactures       TIMESTAMP,
    id_branch               INT,
    etl_loaded_at           TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_stg_mfg_id  ON staging.tbl_manufactures(id);
CREATE INDEX idx_stg_mfg_ref ON staging.tbl_manufactures(reference_no);


-- --------------------------------------------
-- staging.tblstaff
-- --------------------------------------------
CREATE TABLE staging.tblstaff (
    staffid         INT,
    email           VARCHAR(100),
    firstname       VARCHAR(50),
    lastname        VARCHAR(50),
    phonenumber     VARCHAR(30),
    gender          CHAR(10),
    birthday        DATE,
    day_in          DATE,
    status_work     INT,
    role            INT,
    admin           INT,
    active          INT,
    id_branch       INT,
    role_level_id   INT,
    date_update     TIMESTAMP,
    etl_loaded_at   TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_stg_staff_id ON staging.tblstaff(staffid);


-- --------------------------------------------
-- staging.tbldepartments
-- --------------------------------------------
CREATE TABLE staging.tbldepartments (
    departmentid        INT,
    code                VARCHAR(250),
    name                VARCHAR(100),
    type                INT,
    active_departments  INT,
    etl_loaded_at       TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_stg_dept_id ON staging.tbldepartments(departmentid);
