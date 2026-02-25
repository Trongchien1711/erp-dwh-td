-- ============================================
-- BƯỚC 7: FIX STAGING TABLES
-- Thêm cột còn thiếu so với MySQL thực tế
-- Tạo bảng mới chưa có trong staging
-- ============================================


-- --------------------------------------------
-- 1. staging.tbl_orders
-- --------------------------------------------
ALTER TABLE staging.tbl_orders
    ADD COLUMN IF NOT EXISTS referenceId_api                VARCHAR(250),
    ADD COLUMN IF NOT EXISTS id_order_api                   VARCHAR(250),
    ADD COLUMN IF NOT EXISTS pos                            INT,
    ADD COLUMN IF NOT EXISTS table_discount_id              INT,
    ADD COLUMN IF NOT EXISTS gift                           INT,
    ADD COLUMN IF NOT EXISTS status_payment_orders          INT,
    ADD COLUMN IF NOT EXISTS staff_coupon                   INT,
    ADD COLUMN IF NOT EXISTS transporter_id                 INT,
    ADD COLUMN IF NOT EXISTS charge_party                   VARCHAR(255),
    ADD COLUMN IF NOT EXISTS price_other_expenses_delivery  NUMERIC(22,4),
    ADD COLUMN IF NOT EXISTS total_quantity_had_outsource   NUMERIC(12,4),
    ADD COLUMN IF NOT EXISTS count_outsource                INT,
    ADD COLUMN IF NOT EXISTS hold_the_goods                 INT,
    ADD COLUMN IF NOT EXISTS so                             VARCHAR(255),
    ADD COLUMN IF NOT EXISTS pi                             VARCHAR(255),
    ADD COLUMN IF NOT EXISTS po_style                       VARCHAR(255),
    ADD COLUMN IF NOT EXISTS type_items                     INT,
    ADD COLUMN IF NOT EXISTS item_code                      VARCHAR(255),
    ADD COLUMN IF NOT EXISTS ptm                            SMALLINT,
    ADD COLUMN IF NOT EXISTS note_cancel                    TEXT;


-- --------------------------------------------
-- 2. staging.tbl_order_items
-- --------------------------------------------
ALTER TABLE staging.tbl_order_items
    ADD COLUMN IF NOT EXISTS promotion_item_gift_id             INT,
    ADD COLUMN IF NOT EXISTS promotion_item_id                  INT,
    ADD COLUMN IF NOT EXISTS quantity_bs                        INT,
    ADD COLUMN IF NOT EXISTS quantity_condition                 NUMERIC(22,4),
    ADD COLUMN IF NOT EXISTS quantity_outsource                 NUMERIC(12,4),
    ADD COLUMN IF NOT EXISTS quantity_plan                      NUMERIC(22,4),
    ADD COLUMN IF NOT EXISTS quantity_productions_orders        NUMERIC(22,4),
    ADD COLUMN IF NOT EXISTS quantity_child_sheet               NUMERIC(12,4),
    ADD COLUMN IF NOT EXISTS quantity_sheet_bale                NUMERIC(12,4),
    ADD COLUMN IF NOT EXISTS order_code                         VARCHAR(255),
    ADD COLUMN IF NOT EXISTS command                            VARCHAR(255),
    ADD COLUMN IF NOT EXISTS quantity_loss                      NUMERIC(22,4),
    ADD COLUMN IF NOT EXISTS sample_quantity                    NUMERIC(22,4),
    ADD COLUMN IF NOT EXISTS total_quantity_item                NUMERIC(22,4),
    ADD COLUMN IF NOT EXISTS ct_counter_item                    INT,
    ADD COLUMN IF NOT EXISTS hand_input_price                   SMALLINT,
    ADD COLUMN IF NOT EXISTS loss                               NUMERIC(12,4),
    ADD COLUMN IF NOT EXISTS product_name_customer              VARCHAR(1000),
    ADD COLUMN IF NOT EXISTS conversion_quantity_unit_default   NUMERIC(12,4),
    ADD COLUMN IF NOT EXISTS check_delivery                     INT,
    ADD COLUMN IF NOT EXISTS is_updateprice                     INT,
    ADD COLUMN IF NOT EXISTS is_lot                             SMALLINT;


-- --------------------------------------------
-- 3. staging.tbl_deliveries
-- --------------------------------------------
ALTER TABLE staging.tbl_deliveries
    ADD COLUMN IF NOT EXISTS total_discount_percent_items   NUMERIC(22,4),
    ADD COLUMN IF NOT EXISTS total_discount_direct_items    NUMERIC(22,4),
    ADD COLUMN IF NOT EXISTS tax_id                         INT,
    ADD COLUMN IF NOT EXISTS tax_name                       VARCHAR(255),
    ADD COLUMN IF NOT EXISTS tax_rate                       NUMERIC(12,4),
    ADD COLUMN IF NOT EXISTS discount_percent               NUMERIC(12,4),
    ADD COLUMN IF NOT EXISTS type_id                        INT,
    ADD COLUMN IF NOT EXISTS count_export_warehouse         INT,
    ADD COLUMN IF NOT EXISTS person_contact_id              INT,
    ADD COLUMN IF NOT EXISTS code_custom                    VARCHAR(255),
    ADD COLUMN IF NOT EXISTS date_custom                    DATE;


-- --------------------------------------------
-- 4. staging.tbl_delivery_items
-- --------------------------------------------
ALTER TABLE staging.tbl_delivery_items
    ADD COLUMN IF NOT EXISTS id_import              VARCHAR(255),
    ADD COLUMN IF NOT EXISTS date_use               NUMERIC(22,4),
    ADD COLUMN IF NOT EXISTS quantity_unit_loss     NUMERIC(22,4),
    ADD COLUMN IF NOT EXISTS quantity_stock_loss    NUMERIC(22,4),
    ADD COLUMN IF NOT EXISTS quantity_payment_loss  NUMERIC(22,4),
    ADD COLUMN IF NOT EXISTS quantity_unit_sample   NUMERIC(22,4),
    ADD COLUMN IF NOT EXISTS quantity_stock_sample  NUMERIC(22,4),
    ADD COLUMN IF NOT EXISTS quantity_payment_sample NUMERIC(22,4),
    ADD COLUMN IF NOT EXISTS check_loss             INT;


-- --------------------------------------------
-- 5. staging.tbl_manufactures
-- --------------------------------------------
ALTER TABLE staging.tbl_manufactures
    ADD COLUMN IF NOT EXISTS warehouseman_id    INT,
    ADD COLUMN IF NOT EXISTS warehouseman_date  TIMESTAMP,
    ADD COLUMN IF NOT EXISTS user_manufactures  INT;


-- --------------------------------------------
-- 6. staging.tbl_productions_orders
-- --------------------------------------------
ALTER TABLE staging.tbl_productions_orders
    ADD COLUMN IF NOT EXISTS productions_plan_id            TEXT,
    ADD COLUMN IF NOT EXISTS productions_plan_reference_no  TEXT,
    ADD COLUMN IF NOT EXISTS options1                       INT,
    ADD COLUMN IF NOT EXISTS options2                       INT,
    ADD COLUMN IF NOT EXISTS status_gdsx                    SMALLINT,
    ADD COLUMN IF NOT EXISTS user_gdsx                      INT,
    ADD COLUMN IF NOT EXISTS date_gdsx                      TIMESTAMP,
    ADD COLUMN IF NOT EXISTS is_color                       SMALLINT,
    ADD COLUMN IF NOT EXISTS is_layout                      SMALLINT,
    ADD COLUMN IF NOT EXISTS is_sewing                      SMALLINT,
    ADD COLUMN IF NOT EXISTS is_npl                         SMALLINT,
    ADD COLUMN IF NOT EXISTS is_material                    SMALLINT,
    ADD COLUMN IF NOT EXISTS is_cutting                     SMALLINT,
    ADD COLUMN IF NOT EXISTS date_npl                       DATE,
    ADD COLUMN IF NOT EXISTS is_number_printed              SMALLINT,
    ADD COLUMN IF NOT EXISTS is_export_npl                  SMALLINT,
    ADD COLUMN IF NOT EXISTS is_export_vtsx                 SMALLINT;


-- --------------------------------------------
-- 7. staging.tbl_productions_orders_items
-- --------------------------------------------
ALTER TABLE staging.tbl_productions_orders_items
    ADD COLUMN IF NOT EXISTS productions_capacity_items_id  INT,
    ADD COLUMN IF NOT EXISTS plan_item_id                   INT,
    ADD COLUMN IF NOT EXISTS object_item_type               VARCHAR(20),
    ADD COLUMN IF NOT EXISTS plan_id                        INT;


-- --------------------------------------------
-- 8. staging.tbl_productions_orders_items_stages
-- --------------------------------------------
ALTER TABLE staging.tbl_productions_orders_items_stages
    ADD COLUMN IF NOT EXISTS machines           VARCHAR(255),
    ADD COLUMN IF NOT EXISTS object_type        VARCHAR(20),
    ADD COLUMN IF NOT EXISTS object_id          INT,
    ADD COLUMN IF NOT EXISTS object_item_id     INT,
    ADD COLUMN IF NOT EXISTS begin_productions  SMALLINT,
    ADD COLUMN IF NOT EXISTS date_productions   TIMESTAMP,
    ADD COLUMN IF NOT EXISTS staff_productions  INT,
    ADD COLUMN IF NOT EXISTS date_machines      TIMESTAMP,
    ADD COLUMN IF NOT EXISTS user_machines      INT,
    ADD COLUMN IF NOT EXISTS pois_id            INT,
    ADD COLUMN IF NOT EXISTS face               SMALLINT,
    ADD COLUMN IF NOT EXISTS face_after         SMALLINT;


-- --------------------------------------------
-- 9. staging.tbl_products
-- --------------------------------------------
ALTER TABLE staging.tbl_products
    ADD COLUMN IF NOT EXISTS name_supplier                  VARCHAR(1000),
    ADD COLUMN IF NOT EXISTS quantity_minimum               NUMERIC(22,4),
    ADD COLUMN IF NOT EXISTS quantity_max                   NUMERIC(22,4),
    ADD COLUMN IF NOT EXISTS calculated_on_sales            INT,
    ADD COLUMN IF NOT EXISTS size                           INT,
    ADD COLUMN IF NOT EXISTS number_day                     NUMERIC(12,4),
    ADD COLUMN IF NOT EXISTS hand_input_code                SMALLINT,
    ADD COLUMN IF NOT EXISTS customer                       INT,
    ADD COLUMN IF NOT EXISTS product_code_customer          VARCHAR(500),
    ADD COLUMN IF NOT EXISTS product_name_customer          VARCHAR(500),
    ADD COLUMN IF NOT EXISTS quantity_child_sheet           NUMERIC(12,4),
    ADD COLUMN IF NOT EXISTS quantity_sheet_bale            NUMERIC(12,4),
    ADD COLUMN IF NOT EXISTS type_print                     INT,
    ADD COLUMN IF NOT EXISTS columns_id                     INT,
    ADD COLUMN IF NOT EXISTS sample_cover_code              VARCHAR(255),
    ADD COLUMN IF NOT EXISTS mold_code                      VARCHAR(255),
    ADD COLUMN IF NOT EXISTS color_size                     VARCHAR(255),
    ADD COLUMN IF NOT EXISTS gw                             VARCHAR(255),
    ADD COLUMN IF NOT EXISTS carton_size                    VARCHAR(255),
    ADD COLUMN IF NOT EXISTS code_bom                       VARCHAR(255),
    ADD COLUMN IF NOT EXISTS quantity_child_molds           NUMERIC(22,4),
    ADD COLUMN IF NOT EXISTS quantity_child_molds_offset    NUMERIC(12,4),
    ADD COLUMN IF NOT EXISTS quantity_child_molds_flexo     NUMERIC(12,4),
    ADD COLUMN IF NOT EXISTS is_zinc                        SMALLINT,
    ADD COLUMN IF NOT EXISTS classify                       VARCHAR(255),
    ADD COLUMN IF NOT EXISTS unit_measure                   INT,
    ADD COLUMN IF NOT EXISTS delivery_norms                 NUMERIC(12,4),
    ADD COLUMN IF NOT EXISTS id_standard_carry              INT,
    ADD COLUMN IF NOT EXISTS id_standard_sample_cover       INT,
    ADD COLUMN IF NOT EXISTS id_standard_smooth_shine       INT,
    ADD COLUMN IF NOT EXISTS id_standard_fsc                INT,
    ADD COLUMN IF NOT EXISTS id_standard_delivery_package   INT,
    ADD COLUMN IF NOT EXISTS id_standard_membrane           INT,
    ADD COLUMN IF NOT EXISTS id_standard_template           INT,
    ADD COLUMN IF NOT EXISTS id_standard_condition_color    INT,
    ADD COLUMN IF NOT EXISTS id_standard_color              INT,
    ADD COLUMN IF NOT EXISTS id_standard_bin_carton         INT,
    ADD COLUMN IF NOT EXISTS id_standard_trame              INT,
    ADD COLUMN IF NOT EXISTS id_standard_sample_code        INT,
    ADD COLUMN IF NOT EXISTS id_standard_methods            INT,
    ADD COLUMN IF NOT EXISTS id_standard_quality_standards  INT,
    ADD COLUMN IF NOT EXISTS allowable                      NUMERIC(12,4),
    ADD COLUMN IF NOT EXISTS quota                          NUMERIC(12,4),
    ADD COLUMN IF NOT EXISTS barrel_size                    NUMERIC(12,4),
    ADD COLUMN IF NOT EXISTS max_price                      NUMERIC(12,4),
    ADD COLUMN IF NOT EXISTS total_business_plan            NUMERIC(22,4),
    ADD COLUMN IF NOT EXISTS total_transfer_business        NUMERIC(22,4);


-- --------------------------------------------
-- 10. staging.tblclients
-- --------------------------------------------
ALTER TABLE staging.tblclients
    ADD COLUMN IF NOT EXISTS zcode              VARCHAR(250),
    ADD COLUMN IF NOT EXISTS debt_begin         NUMERIC(25,4),
    ADD COLUMN IF NOT EXISTS code_system        VARCHAR(250),
    ADD COLUMN IF NOT EXISTS code_type          VARCHAR(50),
    ADD COLUMN IF NOT EXISTS area               VARCHAR(500),
    ADD COLUMN IF NOT EXISTS id_discount_client INT,
    ADD COLUMN IF NOT EXISTS allowed_vat        INT,
    ADD COLUMN IF NOT EXISTS tm_ck              SMALLINT,
    ADD COLUMN IF NOT EXISTS colors             INT,
    ADD COLUMN IF NOT EXISTS declare_customs    INT,
    ADD COLUMN IF NOT EXISTS code_xnk           VARCHAR(255),
    ADD COLUMN IF NOT EXISTS vat_id             INT,
    ADD COLUMN IF NOT EXISTS currency           INT,
    ADD COLUMN IF NOT EXISTS type_contract      VARCHAR(255),
    ADD COLUMN IF NOT EXISTS date_renewal       DATE,
    ADD COLUMN IF NOT EXISTS discount_id        INT,
    ADD COLUMN IF NOT EXISTS deadline_contract  DATE,
    ADD COLUMN IF NOT EXISTS date_accounting    DATE,
    ADD COLUMN IF NOT EXISTS status_activity    DATE,
    ADD COLUMN IF NOT EXISTS bank_account       VARCHAR(1000),
    ADD COLUMN IF NOT EXISTS name_account       VARCHAR(1000),
    ADD COLUMN IF NOT EXISTS contract_number    VARCHAR(1000);


-- --------------------------------------------
-- 11. staging.tbldepartments
-- --------------------------------------------
ALTER TABLE staging.tbldepartments
    ADD COLUMN IF NOT EXISTS room_id INT;


-- --------------------------------------------
-- 12. staging.tbllocaltion_warehouses
-- --------------------------------------------
ALTER TABLE staging.tbllocaltion_warehouses
    ADD COLUMN IF NOT EXISTS type_excel                     INT,
    ADD COLUMN IF NOT EXISTS productions_plan_id            INT,
    ADD COLUMN IF NOT EXISTS stage_id_import_outsource      INT,
    ADD COLUMN IF NOT EXISTS tranfer_business_id            INT;


-- --------------------------------------------
-- 13. staging.tblpurchase_order
-- --------------------------------------------
ALTER TABLE staging.tblpurchase_order
    ADD COLUMN IF NOT EXISTS history_status                 VARCHAR(100),
    ADD COLUMN IF NOT EXISTS valtype_check_expected         INT,
    ADD COLUMN IF NOT EXISTS valtype_check_suppliers        INT,
    ADD COLUMN IF NOT EXISTS discount_percent_expected      NUMERIC(22,4),
    ADD COLUMN IF NOT EXISTS discount_percent_suppliers     NUMERIC(22,4),
    ADD COLUMN IF NOT EXISTS total_novat                    NUMERIC(22,4),
    ADD COLUMN IF NOT EXISTS promotion_expected             NUMERIC(22,4),
    ADD COLUMN IF NOT EXISTS id_purchases                   VARCHAR(255),
    ADD COLUMN IF NOT EXISTS id_quotes                      INT,
    ADD COLUMN IF NOT EXISTS type_items                     VARCHAR(255),
    ADD COLUMN IF NOT EXISTS check_purchase_all             INT,
    ADD COLUMN IF NOT EXISTS red_invoice                    INT,
    ADD COLUMN IF NOT EXISTS price_other_expenses           NUMERIC(22,4),
    ADD COLUMN IF NOT EXISTS money_arises                   NUMERIC(22,4),
    ADD COLUMN IF NOT EXISTS amount_paid_debt               NUMERIC(22,4),
    ADD COLUMN IF NOT EXISTS type_plan                      INT,
    ADD COLUMN IF NOT EXISTS id_purchase_proce              INT,
    ADD COLUMN IF NOT EXISTS delivery_cost                  NUMERIC(22,4),
    ADD COLUMN IF NOT EXISTS reduce_cost                    NUMERIC(22,4),
    ADD COLUMN IF NOT EXISTS tax_all                        INT,
    ADD COLUMN IF NOT EXISTS total_dqd                      NUMERIC(22,4),
    ADD COLUMN IF NOT EXISTS plan_id                        TEXT,
    ADD COLUMN IF NOT EXISTS amount_to_vnd                  NUMERIC(22,4),
    ADD COLUMN IF NOT EXISTS total_cqd                      NUMERIC(22,4),
    ADD COLUMN IF NOT EXISTS amount_paid_qd                 NUMERIC(22,4),
    ADD COLUMN IF NOT EXISTS type_order                     INT,
    ADD COLUMN IF NOT EXISTS id_internal_proposal           INT,
    ADD COLUMN IF NOT EXISTS is_du                          INT,
    ADD COLUMN IF NOT EXISTS user_end                       INT;


-- --------------------------------------------
-- 14. staging.tblpurchase_order_items
-- Fix type: tax_rate INT → NUMERIC(22,4)
-- Thêm cột còn thiếu
-- --------------------------------------------
ALTER TABLE staging.tblpurchase_order_items
    ALTER COLUMN tax_rate TYPE NUMERIC(22,4) USING tax_rate::NUMERIC;

ALTER TABLE staging.tblpurchase_order_items
    ADD COLUMN IF NOT EXISTS plan_id                                    INT,
    ADD COLUMN IF NOT EXISTS exchange_unit                              NUMERIC(22,4),
    ADD COLUMN IF NOT EXISTS exchange_stock                             NUMERIC(22,4),
    ADD COLUMN IF NOT EXISTS exchange_payment                           NUMERIC(22,4),
    ADD COLUMN IF NOT EXISTS id_internal_proposal_purchase_items        INT,
    ADD COLUMN IF NOT EXISTS purchase_items_id                          INT;


-- --------------------------------------------
-- 15. staging.tbl_purchase_products
-- --------------------------------------------
ALTER TABLE staging.tbl_purchase_products
    ADD COLUMN IF NOT EXISTS productions_orders_details_id  INT,
    ADD COLUMN IF NOT EXISTS task_id                        INT,
    ADD COLUMN IF NOT EXISTS save_and_warehouse             INT,
    ADD COLUMN IF NOT EXISTS sp_type                        INT,
    ADD COLUMN IF NOT EXISTS cqi_id                         INT,
    ADD COLUMN IF NOT EXISTS parent_id                      INT,
    ADD COLUMN IF NOT EXISTS po_id                          INT,
    ADD COLUMN IF NOT EXISTS type_business_plan             SMALLINT,
    ADD COLUMN IF NOT EXISTS is_pass                        SMALLINT;


-- --------------------------------------------
-- 16. staging.tbl_purchase_product_items
-- --------------------------------------------
ALTER TABLE staging.tbl_purchase_product_items
    ADD COLUMN IF NOT EXISTS productions_orders_details_id INT;


-- --------------------------------------------
-- 17. staging.tblstaff
-- --------------------------------------------
ALTER TABLE staging.tblstaff
    ADD COLUMN IF NOT EXISTS code               TEXT,
    ADD COLUMN IF NOT EXISTS status_overtime    INT,
    ADD COLUMN IF NOT EXISTS date_status_work   DATE;


-- --------------------------------------------
-- 18. staging.tblsuppliers
-- --------------------------------------------
ALTER TABLE staging.tblsuppliers
    ADD COLUMN IF NOT EXISTS default_currency       INT,
    ADD COLUMN IF NOT EXISTS default_language       INT,
    ADD COLUMN IF NOT EXISTS datecreated            TIMESTAMP,
    ADD COLUMN IF NOT EXISTS addedfrom              INT,
    ADD COLUMN IF NOT EXISTS id_supplier_classify   INT,
    ADD COLUMN IF NOT EXISTS debt_begin             NUMERIC(22,4),
    ADD COLUMN IF NOT EXISTS renewal_date           DATE,
    ADD COLUMN IF NOT EXISTS barcode                VARCHAR(255),
    ADD COLUMN IF NOT EXISTS discount_id            INT,
    ADD COLUMN IF NOT EXISTS date_begin             DATE,
    ADD COLUMN IF NOT EXISTS package_specifications VARCHAR(100),
    ADD COLUMN IF NOT EXISTS cost_id                INT,
    ADD COLUMN IF NOT EXISTS number_contract        VARCHAR(100),
    ADD COLUMN IF NOT EXISTS address_delivery       VARCHAR(255),
    ADD COLUMN IF NOT EXISTS code_nxk               VARCHAR(100);


-- --------------------------------------------
-- 19. staging.tbltransfer_warehouse_detail
-- --------------------------------------------
ALTER TABLE staging.tbltransfer_warehouse_detail
    ADD COLUMN IF NOT EXISTS id_export              VARCHAR(255),
    ADD COLUMN IF NOT EXISTS id_import              VARCHAR(255),
    ADD COLUMN IF NOT EXISTS date_use               NUMERIC(22,4),
    ADD COLUMN IF NOT EXISTS exchange_unit          NUMERIC(22,4),
    ADD COLUMN IF NOT EXISTS exchange_stock         NUMERIC(22,4),
    ADD COLUMN IF NOT EXISTS exchange_payment       NUMERIC(22,4),
    ADD COLUMN IF NOT EXISTS tranfer_business_item_id INT,
    ADD COLUMN IF NOT EXISTS tranfer_business_id    INT;


-- --------------------------------------------
-- 20. staging.tblwarehouse_export
-- --------------------------------------------
ALTER TABLE staging.tblwarehouse_export
    ADD COLUMN IF NOT EXISTS date_use NUMERIC(22,4);


-- --------------------------------------------
-- 21. staging.tblwarehouse_product
-- --------------------------------------------
ALTER TABLE staging.tblwarehouse_product
    ADD COLUMN IF NOT EXISTS id_export  VARCHAR(255),
    ADD COLUMN IF NOT EXISTS price_old  NUMERIC(22,4),
    ADD COLUMN IF NOT EXISTS id_plan    INT,
    ADD COLUMN IF NOT EXISTS date_use   NUMERIC(22,4);


-- ============================================
-- TẠO BẢNG MỚI CHƯA CÓ TRONG STAGING
-- ============================================

-- --------------------------------------------
-- 22. staging.tbl_products_colors  (MỚI)
-- --------------------------------------------
CREATE TABLE IF NOT EXISTS staging.tbl_products_colors (
    id            INT,
    product_id    INT,
    color_id      INT,
    etl_loaded_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_stg_pcol_id         ON staging.tbl_products_colors(id);
CREATE INDEX IF NOT EXISTS idx_stg_pcol_product     ON staging.tbl_products_colors(product_id);


-- --------------------------------------------
-- 23. staging.tblpurchases_items  (MỚI)
-- --------------------------------------------
CREATE TABLE IF NOT EXISTS staging.tblpurchases_items (
    id                  INT,
    purchases_id        INT,
    product_id          INT,
    quantity            NUMERIC(22,4),
    quantity_net        NUMERIC(22,4),
    type                VARCHAR(100),
    quantity_create_all NUMERIC(22,4),
    quantity_create     NUMERIC(22,4),
    order_item_id       INT,
    id_plan             INT,
    etl_loaded_at       TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_stg_puritems_id      ON staging.tblpurchases_items(id);
CREATE INDEX IF NOT EXISTS idx_stg_puritems_pur     ON staging.tblpurchases_items(purchases_id);
CREATE INDEX IF NOT EXISTS idx_stg_puritems_product ON staging.tblpurchases_items(product_id);


-- --------------------------------------------
-- 24. staging.tblwarehouse_items  (MỚI)
-- --------------------------------------------
CREATE TABLE IF NOT EXISTS staging.tblwarehouse_items (
    id                       INT,
    id_items                 INT,
    warehouse_id             INT,
    localtion                INT,
    product_quantity         NUMERIC(22,4),
    product_quantity_unit    NUMERIC(22,4),
    product_quantity_payment NUMERIC(22,4),
    quantity_exchange        NUMERIC(22,4),
    type_items               VARCHAR(100),
    series                   INT,
    id_plan                  INT,
    lot_code                 VARCHAR(255),
    date_sx                  DATE,
    date_sd                  DATE,
    date_use                 NUMERIC(22,4),
    etl_loaded_at            TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_stg_whi_id      ON staging.tblwarehouse_items(id);
CREATE INDEX IF NOT EXISTS idx_stg_whi_item    ON staging.tblwarehouse_items(id_items);
CREATE INDEX IF NOT EXISTS idx_stg_whi_wh      ON staging.tblwarehouse_items(warehouse_id);
CREATE INDEX IF NOT EXISTS idx_stg_whi_lot     ON staging.tblwarehouse_items(lot_code);