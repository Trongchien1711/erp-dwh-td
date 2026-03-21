-- ============================================
-- BƯỚC 5: CORE FACT TABLES
-- ============================================

-- --------------------------------------------
-- fact_orders
-- --------------------------------------------
CREATE TABLE core.fact_orders (
    order_key                        BIGSERIAL,
    order_id                         INT          NOT NULL,
    reference_no                     VARCHAR(255),
    customer_key                     INT,
    employee_key                     INT,
    order_date_key                   INT          NOT NULL,
    count_items                      INT,
    total_quantity                   NUMERIC(12,4),
    total_amount_items               NUMERIC(22,4),
    total_tax_items                  NUMERIC(22,4),
    total_discount_percent_items     NUMERIC(22,4),
    total_discount_direct_items      NUMERIC(22,4),
    grand_total_items                NUMERIC(22,4),
    total_tax                        NUMERIC(22,4),
    total_discount_percent           NUMERIC(22,4),
    total_discount_direct            NUMERIC(22,4),
    cost_delivery                    NUMERIC(22,4),
    grand_total                      NUMERIC(22,4),
    total_cost                       NUMERIC(22,4),
    total_profit                     NUMERIC(22,4),
    total_payment                    NUMERIC(25,4),
    status                           VARCHAR(255),
    status_payment                   INT,
    status_orders                    INT,
    type_orders                      INT,
    type_bills                       INT,
    is_cancel                        SMALLINT,
    is_end                           SMALLINT,
    id_branch                        INT,
    warehouse_id                     INT,
    currencies                       INT,
    date_created                     TIMESTAMP,
    date_updated                     TIMESTAMP,
    etl_loaded_at                    TIMESTAMP DEFAULT NOW(),
    etl_source                       VARCHAR(50) DEFAULT 'tbl_orders',
    PRIMARY KEY (order_key, order_date_key)
) PARTITION BY RANGE (order_date_key);

CREATE TABLE core.fact_orders_2022    PARTITION OF core.fact_orders FOR VALUES FROM (20220101) TO (20230101);
CREATE TABLE core.fact_orders_2023    PARTITION OF core.fact_orders FOR VALUES FROM (20230101) TO (20240101);
CREATE TABLE core.fact_orders_2024    PARTITION OF core.fact_orders FOR VALUES FROM (20240101) TO (20250101);
CREATE TABLE core.fact_orders_2025    PARTITION OF core.fact_orders FOR VALUES FROM (20250101) TO (20260101);
CREATE TABLE core.fact_orders_2026    PARTITION OF core.fact_orders FOR VALUES FROM (20260101) TO (20270101);
CREATE TABLE core.fact_orders_2027    PARTITION OF core.fact_orders FOR VALUES FROM (20270101) TO (20280101);
CREATE TABLE core.fact_orders_default PARTITION OF core.fact_orders DEFAULT;

CREATE INDEX idx_fact_orders_id       ON core.fact_orders(order_id);
CREATE INDEX idx_fact_orders_customer ON core.fact_orders(customer_key);
CREATE INDEX idx_fact_orders_date     ON core.fact_orders(order_date_key);
CREATE INDEX idx_fact_orders_status   ON core.fact_orders(status);
CREATE INDEX idx_fact_orders_branch   ON core.fact_orders(id_branch);

-- --------------------------------------------
-- fact_order_items
-- --------------------------------------------
CREATE TABLE core.fact_order_items (
    order_item_key               BIGSERIAL,
    order_item_id                INT         NOT NULL,
    order_id                     INT         NOT NULL,
    customer_key                 INT,
    product_key                  INT,
    order_date_key               INT         NOT NULL,
    quantity                     NUMERIC(22,4),
    price                        NUMERIC(22,4),
    amount                       NUMERIC(22,4),
    tax_rate_item                INT,
    tax_amount_item              NUMERIC(22,4),
    discount_percent_item        NUMERIC(12,4),
    discount_percent_amount_item NUMERIC(22,4),
    discount_direct_amount_item  NUMERIC(22,4),
    total_amount                 NUMERIC(22,4),
    quantity_delivery            NUMERIC(22,4),
    quantity_not_delivery        NUMERIC(22,4),
    cost                         NUMERIC(22,4),
    profit                       NUMERIC(22,4),
    cost_temporary_capital       NUMERIC(22,4),
    profit_temporary_capital     NUMERIC(22,4),
    quantity_returned            NUMERIC(22,4),
    type_item                    VARCHAR(255),
    item_code                    VARCHAR(255),
    type_gift                    INT,
    active                       SMALLINT,
    unit_id                      INT,
    etl_loaded_at                TIMESTAMP DEFAULT NOW(),
    etl_source                   VARCHAR(50) DEFAULT 'tbl_order_items',
    PRIMARY KEY (order_item_key, order_date_key)
) PARTITION BY RANGE (order_date_key);

CREATE TABLE core.fact_order_items_2022    PARTITION OF core.fact_order_items FOR VALUES FROM (20220101) TO (20230101);
CREATE TABLE core.fact_order_items_2023    PARTITION OF core.fact_order_items FOR VALUES FROM (20230101) TO (20240101);
CREATE TABLE core.fact_order_items_2024    PARTITION OF core.fact_order_items FOR VALUES FROM (20240101) TO (20250101);
CREATE TABLE core.fact_order_items_2025    PARTITION OF core.fact_order_items FOR VALUES FROM (20250101) TO (20260101);
CREATE TABLE core.fact_order_items_2026    PARTITION OF core.fact_order_items FOR VALUES FROM (20260101) TO (20270101);
CREATE TABLE core.fact_order_items_2027    PARTITION OF core.fact_order_items FOR VALUES FROM (20270101) TO (20280101);
CREATE TABLE core.fact_order_items_default PARTITION OF core.fact_order_items DEFAULT;

CREATE INDEX idx_fact_oi_id       ON core.fact_order_items(order_item_id);
CREATE INDEX idx_fact_oi_order    ON core.fact_order_items(order_id);
CREATE INDEX idx_fact_oi_product  ON core.fact_order_items(product_key);
CREATE INDEX idx_fact_oi_customer ON core.fact_order_items(customer_key);
CREATE INDEX idx_fact_oi_date     ON core.fact_order_items(order_date_key);

-- --------------------------------------------
-- fact_delivery_items
-- --------------------------------------------
CREATE TABLE core.fact_delivery_items (
    delivery_item_key            BIGSERIAL,
    delivery_item_id             INT         NOT NULL,
    delivery_id                  INT         NOT NULL,
    order_item_id                INT,
    customer_key                 INT,
    product_key                  INT,
    warehouse_key                INT,
    location_key                 INT,
    delivery_date_key            INT         NOT NULL,
    quantity                     NUMERIC(22,4),
    quantity_loss                NUMERIC(22,4),
    quantity_sample              NUMERIC(22,4),
    price                        NUMERIC(22,4),
    amount                       NUMERIC(22,4),
    tax_rate_item                INT,
    tax_amount_item              NUMERIC(22,4),
    discount_percent_item        NUMERIC(12,4),
    discount_percent_amount_item NUMERIC(22,4),
    discount_direct_amount_item  NUMERIC(22,4),
    total_amount                 NUMERIC(22,4),
    quantity_unit                NUMERIC(22,4),
    quantity_stock               NUMERIC(22,4),
    quantity_payment             NUMERIC(22,4),
    type_item                    VARCHAR(255),
    item_code                    VARCHAR(255),
    lot_code                     VARCHAR(255),
    date_sx                      DATE,
    date_sd                      DATE,
    unit_id                      INT,
    etl_loaded_at                TIMESTAMP DEFAULT NOW(),
    etl_source                   VARCHAR(50) DEFAULT 'tbl_delivery_items',
    PRIMARY KEY (delivery_item_key, delivery_date_key)
) PARTITION BY RANGE (delivery_date_key);

CREATE TABLE core.fact_delivery_items_2022    PARTITION OF core.fact_delivery_items FOR VALUES FROM (20220101) TO (20230101);
CREATE TABLE core.fact_delivery_items_2023    PARTITION OF core.fact_delivery_items FOR VALUES FROM (20230101) TO (20240101);
CREATE TABLE core.fact_delivery_items_2024    PARTITION OF core.fact_delivery_items FOR VALUES FROM (20240101) TO (20250101);
CREATE TABLE core.fact_delivery_items_2025    PARTITION OF core.fact_delivery_items FOR VALUES FROM (20250101) TO (20260101);
CREATE TABLE core.fact_delivery_items_2026    PARTITION OF core.fact_delivery_items FOR VALUES FROM (20260101) TO (20270101);
CREATE TABLE core.fact_delivery_items_2027    PARTITION OF core.fact_delivery_items FOR VALUES FROM (20270101) TO (20280101);
CREATE TABLE core.fact_delivery_items_default PARTITION OF core.fact_delivery_items DEFAULT;

CREATE INDEX idx_fact_di_id       ON core.fact_delivery_items(delivery_item_id);
CREATE INDEX idx_fact_di_delivery ON core.fact_delivery_items(delivery_id);
CREATE INDEX idx_fact_di_product  ON core.fact_delivery_items(product_key);
CREATE INDEX idx_fact_di_customer ON core.fact_delivery_items(customer_key);
CREATE INDEX idx_fact_di_date     ON core.fact_delivery_items(delivery_date_key);
CREATE INDEX idx_fact_di_wh       ON core.fact_delivery_items(warehouse_key);

-- --------------------------------------------
-- fact_warehouse_stock (không partition)
-- --------------------------------------------
CREATE TABLE core.fact_warehouse_stock (
    stock_key                       BIGSERIAL    PRIMARY KEY,
    stock_id                        INT          NOT NULL,
    product_key                     INT,
    warehouse_key                   INT,
    location_key                    INT,
    import_date_key                 INT,
    quantity                        NUMERIC(22,4),
    quantity_left                   NUMERIC(22,4),
    quantity_export                 NUMERIC(22,4),
    quantity_exchange               NUMERIC(22,4),
    quantity_exchange_left          NUMERIC(22,4),
    quantity_exchange_export        NUMERIC(22,4),
    product_quantity_unit           NUMERIC(22,4),
    product_quantity_unit_export    NUMERIC(22,4),
    product_quantity_unit_left      NUMERIC(22,4),
    product_quantity_payment        NUMERIC(22,4),
    product_quantity_payment_export NUMERIC(22,4),
    product_quantity_payment_left   NUMERIC(22,4),
    price                           NUMERIC(22,4),
    type_items                      VARCHAR(100),
    type_export                     INT,
    type_transfer                   INT,
    lot_code                        VARCHAR(255),
    date_sx                         DATE,
    date_sd                         DATE,
    series                          INT,
    etl_loaded_at                   TIMESTAMP    DEFAULT NOW(),
    etl_source                      VARCHAR(50)  DEFAULT 'tblwarehouse_product'
);

CREATE INDEX idx_fact_whs_id      ON core.fact_warehouse_stock(stock_id);
CREATE INDEX idx_fact_whs_product ON core.fact_warehouse_stock(product_key);
CREATE INDEX idx_fact_whs_wh      ON core.fact_warehouse_stock(warehouse_key);
CREATE INDEX idx_fact_whs_date    ON core.fact_warehouse_stock(import_date_key);
CREATE INDEX idx_fact_whs_lot     ON core.fact_warehouse_stock(lot_code);

-- --------------------------------------------
-- fact_warehouse_export
-- --------------------------------------------
CREATE TABLE core.fact_warehouse_export (
    export_key               BIGSERIAL,
    export_id                INT         NOT NULL,
    product_key              INT,
    warehouse_key            INT,
    location_key             INT,
    export_date_key          INT         NOT NULL,
    quantity                 NUMERIC(22,4),
    product_quantity_unit    NUMERIC(22,4),
    product_quantity_payment NUMERIC(22,4),
    type_items               VARCHAR(100),
    type_export              VARCHAR(100),
    lot_code                 VARCHAR(255),
    date_sx                  DATE,
    date_sd                  DATE,
    etl_loaded_at            TIMESTAMP DEFAULT NOW(),
    etl_source               VARCHAR(50) DEFAULT 'tblwarehouse_export',
    PRIMARY KEY (export_key, export_date_key)
) PARTITION BY RANGE (export_date_key);

CREATE TABLE core.fact_warehouse_export_2022    PARTITION OF core.fact_warehouse_export FOR VALUES FROM (20220101) TO (20230101);
CREATE TABLE core.fact_warehouse_export_2023    PARTITION OF core.fact_warehouse_export FOR VALUES FROM (20230101) TO (20240101);
CREATE TABLE core.fact_warehouse_export_2024    PARTITION OF core.fact_warehouse_export FOR VALUES FROM (20240101) TO (20250101);
CREATE TABLE core.fact_warehouse_export_2025    PARTITION OF core.fact_warehouse_export FOR VALUES FROM (20250101) TO (20260101);
CREATE TABLE core.fact_warehouse_export_2026    PARTITION OF core.fact_warehouse_export FOR VALUES FROM (20260101) TO (20270101);
CREATE TABLE core.fact_warehouse_export_2027    PARTITION OF core.fact_warehouse_export FOR VALUES FROM (20270101) TO (20280101);
CREATE TABLE core.fact_warehouse_export_default PARTITION OF core.fact_warehouse_export DEFAULT;

CREATE INDEX idx_fact_we_id      ON core.fact_warehouse_export(export_id);
CREATE INDEX idx_fact_we_product ON core.fact_warehouse_export(product_key);
CREATE INDEX idx_fact_we_wh      ON core.fact_warehouse_export(warehouse_key);
CREATE INDEX idx_fact_we_date    ON core.fact_warehouse_export(export_date_key);

-- --------------------------------------------
-- fact_transfer_warehouse (không partition)
-- --------------------------------------------
CREATE TABLE core.fact_transfer_warehouse (
    transfer_key            BIGSERIAL    PRIMARY KEY,
    transfer_detail_id      INT          NOT NULL,
    transfer_id             INT          NOT NULL,
    product_key             INT,
    warehouse_from_key      INT,
    warehouse_to_key        INT,
    location_from_key       INT,
    location_to_key         INT,
    quantity                NUMERIC(22,4),
    quantity_net            NUMERIC(22,4),
    price                   NUMERIC(22,4),
    amount                  NUMERIC(22,4),
    quantity_unit           NUMERIC(22,4),
    quantity_stock          NUMERIC(22,4),
    quantity_payment        NUMERIC(22,4),
    type                    VARCHAR(100),
    lot_code                VARCHAR(255),
    date_sx                 DATE,
    date_sd                 DATE,
    unit_id                 INT,
    etl_loaded_at           TIMESTAMP    DEFAULT NOW(),
    etl_source              VARCHAR(50)  DEFAULT 'tbltransfer_warehouse_detail'
);

CREATE INDEX idx_fact_tw_id       ON core.fact_transfer_warehouse(transfer_detail_id);
CREATE INDEX idx_fact_tw_transfer ON core.fact_transfer_warehouse(transfer_id);
CREATE INDEX idx_fact_tw_product  ON core.fact_transfer_warehouse(product_key);
CREATE INDEX idx_fact_tw_wh_from  ON core.fact_transfer_warehouse(warehouse_from_key);
CREATE INDEX idx_fact_tw_wh_to    ON core.fact_transfer_warehouse(warehouse_to_key);

-- --------------------------------------------
-- fact_purchase_order_items
-- --------------------------------------------
CREATE TABLE core.fact_purchase_order_items (
    po_item_key             BIGSERIAL,
    po_item_id              INT         NOT NULL,
    po_id                   INT         NOT NULL,
    product_key             INT,
    supplier_key            INT,
    po_date_key             INT         NOT NULL,
    quantity                NUMERIC(22,4),
    quantity_suppliers      NUMERIC(22,4),
    unit_cost               NUMERIC(22,4),
    price_expected          NUMERIC(22,4),
    price_suppliers         NUMERIC(22,4),
    promotion_expected      NUMERIC(22,4),
    total_expected          NUMERIC(22,4),
    total_suppliers         NUMERIC(22,4),
    subtotal                NUMERIC(22,4),
    tax_rate                NUMERIC(22,4),
    quantity_unit           NUMERIC(22,4),
    quantity_stock          NUMERIC(22,4),
    quantity_payment        NUMERIC(22,4),
    type                    VARCHAR(100),
    etl_loaded_at           TIMESTAMP DEFAULT NOW(),
    etl_source              VARCHAR(50) DEFAULT 'tblpurchase_order_items',
    PRIMARY KEY (po_item_key, po_date_key)
) PARTITION BY RANGE (po_date_key);

CREATE TABLE core.fact_purchase_order_items_2022    PARTITION OF core.fact_purchase_order_items FOR VALUES FROM (20220101) TO (20230101);
CREATE TABLE core.fact_purchase_order_items_2023    PARTITION OF core.fact_purchase_order_items FOR VALUES FROM (20230101) TO (20240101);
CREATE TABLE core.fact_purchase_order_items_2024    PARTITION OF core.fact_purchase_order_items FOR VALUES FROM (20240101) TO (20250101);
CREATE TABLE core.fact_purchase_order_items_2025    PARTITION OF core.fact_purchase_order_items FOR VALUES FROM (20250101) TO (20260101);
CREATE TABLE core.fact_purchase_order_items_2026    PARTITION OF core.fact_purchase_order_items FOR VALUES FROM (20260101) TO (20270101);
CREATE TABLE core.fact_purchase_order_items_2027    PARTITION OF core.fact_purchase_order_items FOR VALUES FROM (20270101) TO (20280101);
CREATE TABLE core.fact_purchase_order_items_default PARTITION OF core.fact_purchase_order_items DEFAULT;

CREATE INDEX idx_fact_poi_id       ON core.fact_purchase_order_items(po_item_id);
CREATE INDEX idx_fact_poi_po       ON core.fact_purchase_order_items(po_id);
CREATE INDEX idx_fact_poi_product  ON core.fact_purchase_order_items(product_key);
CREATE INDEX idx_fact_poi_supplier ON core.fact_purchase_order_items(supplier_key);
CREATE INDEX idx_fact_poi_date     ON core.fact_purchase_order_items(po_date_key);

-- --------------------------------------------
-- fact_purchase_product_items
-- --------------------------------------------
CREATE TABLE core.fact_purchase_product_items (
    pp_item_key             BIGSERIAL,
    pp_item_id              INT         NOT NULL,
    purchase_product_id     INT         NOT NULL,
    product_key             INT,
    warehouse_key           INT,
    location_key            INT,
    import_date_key         INT         NOT NULL,
    quantity                NUMERIC(22,4),
    quantity_exchange       NUMERIC(22,4),
    quantity_single         NUMERIC(22,4),
    quantity_semi_product   NUMERIC(22,4),
    price                   NUMERIC(22,4),
    amount                  NUMERIC(22,4),
    quantity_unit           NUMERIC(22,4),
    quantity_stock          NUMERIC(22,4),
    quantity_payment        NUMERIC(22,4),
    type_item               VARCHAR(255),
    item_code               VARCHAR(255),
    type_order              VARCHAR(10),
    unit_id                 INT,
    etl_loaded_at           TIMESTAMP DEFAULT NOW(),
    etl_source              VARCHAR(50) DEFAULT 'tbl_purchase_product_items',
    PRIMARY KEY (pp_item_key, import_date_key)
) PARTITION BY RANGE (import_date_key);

CREATE TABLE core.fact_purchase_product_items_2022    PARTITION OF core.fact_purchase_product_items FOR VALUES FROM (20220101) TO (20230101);
CREATE TABLE core.fact_purchase_product_items_2023    PARTITION OF core.fact_purchase_product_items FOR VALUES FROM (20230101) TO (20240101);
CREATE TABLE core.fact_purchase_product_items_2024    PARTITION OF core.fact_purchase_product_items FOR VALUES FROM (20240101) TO (20250101);
CREATE TABLE core.fact_purchase_product_items_2025    PARTITION OF core.fact_purchase_product_items FOR VALUES FROM (20250101) TO (20260101);
CREATE TABLE core.fact_purchase_product_items_2026    PARTITION OF core.fact_purchase_product_items FOR VALUES FROM (20260101) TO (20270101);
CREATE TABLE core.fact_purchase_product_items_2027    PARTITION OF core.fact_purchase_product_items FOR VALUES FROM (20270101) TO (20280101);
CREATE TABLE core.fact_purchase_product_items_default PARTITION OF core.fact_purchase_product_items DEFAULT;

CREATE INDEX idx_fact_ppi_id      ON core.fact_purchase_product_items(pp_item_id);
CREATE INDEX idx_fact_ppi_product ON core.fact_purchase_product_items(product_key);
CREATE INDEX idx_fact_ppi_wh      ON core.fact_purchase_product_items(warehouse_key);
CREATE INDEX idx_fact_ppi_date    ON core.fact_purchase_product_items(import_date_key);

-- --------------------------------------------
-- fact_production_order_items
-- --------------------------------------------
CREATE TABLE core.fact_production_order_items (
    prod_item_key           BIGSERIAL,
    prod_item_id            INT         NOT NULL,
    productions_orders_id   INT         NOT NULL,
    product_key             INT,
    prod_date_key           INT         NOT NULL,
    quantity                NUMERIC(22,4),
    type_items              VARCHAR(255),
    items_code              VARCHAR(255),
    versions_bom            VARCHAR(255),
    versions_stage          VARCHAR(255),
    etl_loaded_at           TIMESTAMP DEFAULT NOW(),
    etl_source              VARCHAR(50) DEFAULT 'tbl_productions_orders_items',
    PRIMARY KEY (prod_item_key, prod_date_key)
) PARTITION BY RANGE (prod_date_key);

CREATE TABLE core.fact_production_order_items_2022    PARTITION OF core.fact_production_order_items FOR VALUES FROM (20220101) TO (20230101);
CREATE TABLE core.fact_production_order_items_2023    PARTITION OF core.fact_production_order_items FOR VALUES FROM (20230101) TO (20240101);
CREATE TABLE core.fact_production_order_items_2024    PARTITION OF core.fact_production_order_items FOR VALUES FROM (20240101) TO (20250101);
CREATE TABLE core.fact_production_order_items_2025    PARTITION OF core.fact_production_order_items FOR VALUES FROM (20250101) TO (20260101);
CREATE TABLE core.fact_production_order_items_2026    PARTITION OF core.fact_production_order_items FOR VALUES FROM (20260101) TO (20270101);
CREATE TABLE core.fact_production_order_items_2027    PARTITION OF core.fact_production_order_items FOR VALUES FROM (20270101) TO (20280101);
CREATE TABLE core.fact_production_order_items_default PARTITION OF core.fact_production_order_items DEFAULT;

CREATE INDEX idx_fact_proi_id      ON core.fact_production_order_items(prod_item_id);
CREATE INDEX idx_fact_proi_order   ON core.fact_production_order_items(productions_orders_id);
CREATE INDEX idx_fact_proi_product ON core.fact_production_order_items(product_key);
CREATE INDEX idx_fact_proi_date    ON core.fact_production_order_items(prod_date_key);

-- --------------------------------------------
-- fact_production_stages
-- --------------------------------------------
CREATE TABLE core.fact_production_stages (
    prod_stage_key              BIGSERIAL,
    prod_stage_id               INT         NOT NULL,
    productions_orders_id       INT         NOT NULL,
    productions_orders_items_id INT         NOT NULL,
    staff_key                   INT,
    stage_date_key              INT         NOT NULL,
    number                      INT,
    number_hours                NUMERIC(12,4),
    total_time                  NUMERIC(12,4),
    number_face                 NUMERIC(12,4),
    number_operations           NUMERIC(12,4),
    number_cutting              NUMERIC(12,4),
    quota_time_f1               NUMERIC(12,4),
    quota_time_f2               NUMERIC(12,4),
    stage_id                    INT,
    machines_id                 INT,
    final_stage                 INT,
    active                      SMALLINT,
    type                        SMALLINT,
    etl_loaded_at               TIMESTAMP DEFAULT NOW(),
    etl_source                  VARCHAR(50) DEFAULT 'tbl_productions_orders_items_stages',
    PRIMARY KEY (prod_stage_key, stage_date_key)
) PARTITION BY RANGE (stage_date_key);

CREATE TABLE core.fact_production_stages_2022    PARTITION OF core.fact_production_stages FOR VALUES FROM (20220101) TO (20230101);
CREATE TABLE core.fact_production_stages_2023    PARTITION OF core.fact_production_stages FOR VALUES FROM (20230101) TO (20240101);
CREATE TABLE core.fact_production_stages_2024    PARTITION OF core.fact_production_stages FOR VALUES FROM (20240101) TO (20250101);
CREATE TABLE core.fact_production_stages_2025    PARTITION OF core.fact_production_stages FOR VALUES FROM (20250101) TO (20260101);
CREATE TABLE core.fact_production_stages_2026    PARTITION OF core.fact_production_stages FOR VALUES FROM (20260101) TO (20270101);
CREATE TABLE core.fact_production_stages_2027    PARTITION OF core.fact_production_stages FOR VALUES FROM (20270101) TO (20280101);
CREATE TABLE core.fact_production_stages_default PARTITION OF core.fact_production_stages DEFAULT;

CREATE INDEX idx_fact_ps_id    ON core.fact_production_stages(prod_stage_id);
CREATE INDEX idx_fact_ps_order ON core.fact_production_stages(productions_orders_id);
CREATE INDEX idx_fact_ps_staff ON core.fact_production_stages(staff_key);
CREATE INDEX idx_fact_ps_date  ON core.fact_production_stages(stage_date_key);

-- ─────────────────────────────────────────────────────────────────────────────
-- ADD PARTITIONS FOR EXISTING DATABASES
-- Run this block manually each year (e.g. in Jan 2027 for year 2028).
-- ─────────────────────────────────────────────────────────────────────────────
-- Example for 2028:
-- CREATE TABLE IF NOT EXISTS core.fact_orders_2028 PARTITION OF core.fact_orders FOR VALUES FROM (20280101) TO (20290101);
-- CREATE TABLE IF NOT EXISTS core.fact_order_items_2028 PARTITION OF core.fact_order_items FOR VALUES FROM (20280101) TO (20290101);
-- CREATE TABLE IF NOT EXISTS core.fact_delivery_items_2028 PARTITION OF core.fact_delivery_items FOR VALUES FROM (20280101) TO (20290101);
-- CREATE TABLE IF NOT EXISTS core.fact_warehouse_export_2028 PARTITION OF core.fact_warehouse_export FOR VALUES FROM (20280101) TO (20290101);
-- CREATE TABLE IF NOT EXISTS core.fact_purchase_order_items_2028 PARTITION OF core.fact_purchase_order_items FOR VALUES FROM (20280101) TO (20290101);
-- CREATE TABLE IF NOT EXISTS core.fact_purchase_product_items_2028 PARTITION OF core.fact_purchase_product_items FOR VALUES FROM (20280101) TO (20290101);
-- CREATE TABLE IF NOT EXISTS core.fact_production_order_items_2028 PARTITION OF core.fact_production_order_items FOR VALUES FROM (20280101) TO (20290101);
-- CREATE TABLE IF NOT EXISTS core.fact_production_stages_2028 PARTITION OF core.fact_production_stages FOR VALUES FROM (20280101) TO (20290101);
