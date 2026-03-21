-- ============================================================
-- BƯỚC 8: KIỂM TRA CẤU TRÚC DWH (Structure Validation Tests)
-- Chạy toàn bộ file sau khi setup xong BƯỚC 1-7
-- Mỗi test trả về: test_name | status | detail
-- ============================================================

-- ============================================================
-- SECTION 1: KIỂM TRA SCHEMA TỒN TẠI
-- ============================================================

SELECT
    'TEST-S01' AS test_id,
    'Schema: staging tồn tại' AS test_name,
    CASE WHEN EXISTS (
        SELECT 1 FROM information_schema.schemata WHERE schema_name = 'staging'
    ) THEN 'PASS ✅' ELSE 'FAIL ❌' END AS status;

SELECT
    'TEST-S02' AS test_id,
    'Schema: core tồn tại' AS test_name,
    CASE WHEN EXISTS (
        SELECT 1 FROM information_schema.schemata WHERE schema_name = 'core'
    ) THEN 'PASS ✅' ELSE 'FAIL ❌' END AS status;

SELECT
    'TEST-S03' AS test_id,
    'Schema: mart tồn tại' AS test_name,
    CASE WHEN EXISTS (
        SELECT 1 FROM information_schema.schemata WHERE schema_name = 'mart'
    ) THEN 'PASS ✅' ELSE 'FAIL ❌' END AS status;


-- ============================================================
-- SECTION 2: KIỂM TRA EXTENSIONS TỒN TẠI
-- ============================================================

SELECT
    'TEST-EXT01' AS test_id,
    'Extension: pg_stat_statements' AS test_name,
    CASE WHEN EXISTS (
        SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'
    ) THEN 'PASS ✅' ELSE 'FAIL ❌' END AS status;

SELECT
    'TEST-EXT02' AS test_id,
    'Extension: btree_gin' AS test_name,
    CASE WHEN EXISTS (
        SELECT 1 FROM pg_extension WHERE extname = 'btree_gin'
    ) THEN 'PASS ✅' ELSE 'FAIL ❌' END AS status;

SELECT
    'TEST-EXT03' AS test_id,
    'Extension: pg_trgm' AS test_name,
    CASE WHEN EXISTS (
        SELECT 1 FROM pg_extension WHERE extname = 'pg_trgm'
    ) THEN 'PASS ✅' ELSE 'FAIL ❌' END AS status;

SELECT
    'TEST-EXT04' AS test_id,
    'Extension: tablefunc' AS test_name,
    CASE WHEN EXISTS (
        SELECT 1 FROM pg_extension WHERE extname = 'tablefunc'
    ) THEN 'PASS ✅' ELSE 'FAIL ❌' END AS status;


-- ============================================================
-- SECTION 3: KIỂM TRA BẢNG CORE DIMENSION TỒN TẠI
-- ============================================================

SELECT
    'TEST-D01' AS test_id,
    'Table: core.dim_date tồn tại' AS test_name,
    CASE WHEN EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'core' AND table_name = 'dim_date'
    ) THEN 'PASS ✅' ELSE 'FAIL ❌' END AS status;

SELECT
    'TEST-D02' AS test_id,
    'Table: core.dim_staff tồn tại' AS test_name,
    CASE WHEN EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'core' AND table_name = 'dim_staff'
    ) THEN 'PASS ✅' ELSE 'FAIL ❌' END AS status;

SELECT
    'TEST-D03' AS test_id,
    'Table: core.dim_department tồn tại' AS test_name,
    CASE WHEN EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'core' AND table_name = 'dim_department'
    ) THEN 'PASS ✅' ELSE 'FAIL ❌' END AS status;

SELECT
    'TEST-D04' AS test_id,
    'Table: core.dim_customer tồn tại' AS test_name,
    CASE WHEN EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'core' AND table_name = 'dim_customer'
    ) THEN 'PASS ✅' ELSE 'FAIL ❌' END AS status;

SELECT
    'TEST-D05' AS test_id,
    'Table: core.dim_product tồn tại' AS test_name,
    CASE WHEN EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'core' AND table_name = 'dim_product'
    ) THEN 'PASS ✅' ELSE 'FAIL ❌' END AS status;


-- ============================================================
-- SECTION 4: KIỂM TRA BẢNG CORE FACT TỒN TẠI
-- ============================================================

SELECT
    'TEST-F01' AS test_id,
    'Table: core.fact_orders tồn tại (partitioned)' AS test_name,
    CASE WHEN EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'core' AND table_name = 'fact_orders'
    ) THEN 'PASS ✅' ELSE 'FAIL ❌' END AS status;

SELECT
    'TEST-F02' AS test_id,
    'Table: core.fact_order_items tồn tại' AS test_name,
    CASE WHEN EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'core' AND table_name = 'fact_order_items'
    ) THEN 'PASS ✅' ELSE 'FAIL ❌' END AS status;

-- Kiểm tra partitions của fact_orders
SELECT
    'TEST-F03' AS test_id,
    'Partition: core.fact_orders_2022 tồn tại' AS test_name,
    CASE WHEN EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'core' AND table_name = 'fact_orders_2022'
    ) THEN 'PASS ✅' ELSE 'FAIL ❌' END AS status;

SELECT
    'TEST-F04' AS test_id,
    'Partition: core.fact_orders_2025 tồn tại' AS test_name,
    CASE WHEN EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'core' AND table_name = 'fact_orders_2025'
    ) THEN 'PASS ✅' ELSE 'FAIL ❌' END AS status;

SELECT
    'TEST-F05' AS test_id,
    'Partition: core.fact_orders_2026 tồn tại' AS test_name,
    CASE WHEN EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'core' AND table_name = 'fact_orders_2026'
    ) THEN 'PASS ✅' ELSE 'FAIL ❌' END AS status;

SELECT
    'TEST-F06' AS test_id,
    'Partition: core.fact_orders_default tồn tại' AS test_name,
    CASE WHEN EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'core' AND table_name = 'fact_orders_default'
    ) THEN 'PASS ✅' ELSE 'FAIL ❌' END AS status;


-- ============================================================
-- SECTION 5: KIỂM TRA BẢNG STAGING TỒN TẠI
-- ============================================================

SELECT
    'TEST-STG01' AS test_id,
    'Table: staging.tbl_orders tồn tại' AS test_name,
    CASE WHEN EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'staging' AND table_name = 'tbl_orders'
    ) THEN 'PASS ✅' ELSE 'FAIL ❌' END AS status;

SELECT
    'TEST-STG02' AS test_id,
    'Table: staging.tbl_order_items tồn tại' AS test_name,
    CASE WHEN EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'staging' AND table_name = 'tbl_order_items'
    ) THEN 'PASS ✅' ELSE 'FAIL ❌' END AS status;

SELECT
    'TEST-STG03' AS test_id,
    'Table: staging.tbl_deliveries tồn tại' AS test_name,
    CASE WHEN EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'staging' AND table_name = 'tbl_deliveries'
    ) THEN 'PASS ✅' ELSE 'FAIL ❌' END AS status;

SELECT
    'TEST-STG04' AS test_id,
    'Table: staging.tbl_delivery_items tồn tại' AS test_name,
    CASE WHEN EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'staging' AND table_name = 'tbl_delivery_items'
    ) THEN 'PASS ✅' ELSE 'FAIL ❌' END AS status;

SELECT
    'TEST-STG05' AS test_id,
    'Table: staging.tbl_manufactures tồn tại' AS test_name,
    CASE WHEN EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'staging' AND table_name = 'tbl_manufactures'
    ) THEN 'PASS ✅' ELSE 'FAIL ❌' END AS status;

SELECT
    'TEST-STG06' AS test_id,
    'Table: staging.tbl_productions_orders tồn tại' AS test_name,
    CASE WHEN EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'staging' AND table_name = 'tbl_productions_orders'
    ) THEN 'PASS ✅' ELSE 'FAIL ❌' END AS status;

SELECT
    'TEST-STG07' AS test_id,
    'Table: staging.tbl_products tồn tại' AS test_name,
    CASE WHEN EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'staging' AND table_name = 'tbl_products'
    ) THEN 'PASS ✅' ELSE 'FAIL ❌' END AS status;

SELECT
    'TEST-STG08' AS test_id,
    'Table: staging.tblclients tồn tại' AS test_name,
    CASE WHEN EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'staging' AND table_name = 'tblclients'
    ) THEN 'PASS ✅' ELSE 'FAIL ❌' END AS status;

SELECT
    'TEST-STG09' AS test_id,
    'Table: staging.tblstaff tồn tại' AS test_name,
    CASE WHEN EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'staging' AND table_name = 'tblstaff'
    ) THEN 'PASS ✅' ELSE 'FAIL ❌' END AS status;

SELECT
    'TEST-STG10' AS test_id,
    'Table: staging.tblsuppliers tồn tại' AS test_name,
    CASE WHEN EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'staging' AND table_name = 'tblsuppliers'
    ) THEN 'PASS ✅' ELSE 'FAIL ❌' END AS status;

SELECT
    'TEST-STG11' AS test_id,
    'Table: staging.tblpurchase_order tồn tại' AS test_name,
    CASE WHEN EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'staging' AND table_name = 'tblpurchase_order'
    ) THEN 'PASS ✅' ELSE 'FAIL ❌' END AS status;

-- Kiểm tra 3 bảng mới tạo ở BƯỚC 7
SELECT
    'TEST-STG12' AS test_id,
    'Table: staging.tbl_products_colors tồn tại (BƯỚC 7)' AS test_name,
    CASE WHEN EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'staging' AND table_name = 'tbl_products_colors'
    ) THEN 'PASS ✅' ELSE 'FAIL ❌' END AS status;

SELECT
    'TEST-STG13' AS test_id,
    'Table: staging.tblpurchases_items tồn tại (BƯỚC 7)' AS test_name,
    CASE WHEN EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'staging' AND table_name = 'tblpurchases_items'
    ) THEN 'PASS ✅' ELSE 'FAIL ❌' END AS status;

SELECT
    'TEST-STG14' AS test_id,
    'Table: staging.tblwarehouse_items tồn tại (BƯỚC 7)' AS test_name,
    CASE WHEN EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'staging' AND table_name = 'tblwarehouse_items'
    ) THEN 'PASS ✅' ELSE 'FAIL ❌' END AS status;


-- ============================================================
-- SECTION 6: KIỂM TRA CỘT QUAN TRỌNG TỒN TẠI (Column Existence)
-- ============================================================

-- core.dim_date: kiểm tra cột date_key là PRIMARY KEY
SELECT
    'TEST-COL01' AS test_id,
    'Column: core.dim_date.date_key tồn tại' AS test_name,
    CASE WHEN EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'core' AND table_name = 'dim_date' AND column_name = 'date_key'
    ) THEN 'PASS ✅' ELSE 'FAIL ❌' END AS status;

-- staging.tbl_orders: kiểm tra cột etl_loaded_at (ETL tracking)
SELECT
    'TEST-COL02' AS test_id,
    'Column: staging.tbl_orders.etl_loaded_at tồn tại' AS test_name,
    CASE WHEN EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'staging' AND table_name = 'tbl_orders' AND column_name = 'etl_loaded_at'
    ) THEN 'PASS ✅' ELSE 'FAIL ❌' END AS status;

-- staging.tbl_orders: kiểm tra cột bổ sung từ BƯỚC 7
SELECT
    'TEST-COL03' AS test_id,
    'Column: staging.tbl_orders.referenceId_api tồn tại (BƯỚC 7)' AS test_name,
    CASE WHEN EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'staging' AND table_name = 'tbl_orders' AND column_name = 'referenceId_api'
    ) THEN 'PASS ✅' ELSE 'FAIL ❌' END AS status;

-- staging.tblpurchase_order_items: kiểm tra column type fix (BƯỚC 7)
SELECT
    'TEST-COL04' AS test_id,
    'Column: staging.tblpurchase_order_items.tax_rate là NUMERIC (fix BƯỚC 7)' AS test_name,
    CASE WHEN EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'staging'
          AND table_name  = 'tblpurchase_order_items'
          AND column_name = 'tax_rate'
          AND data_type   = 'numeric'
    ) THEN 'PASS ✅' ELSE 'FAIL ❌' END AS status;

-- core.fact_orders: kiểm tra cột order_date_key (dùng partition)
SELECT
    'TEST-COL05' AS test_id,
    'Column: core.fact_orders.order_date_key tồn tại' AS test_name,
    CASE WHEN EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'core' AND table_name = 'fact_orders' AND column_name = 'order_date_key'
    ) THEN 'PASS ✅' ELSE 'FAIL ❌' END AS status;

-- core.dim_staff: kiểm tra cột etl_source
SELECT
    'TEST-COL06' AS test_id,
    'Column: core.dim_staff.etl_source tồn tại' AS test_name,
    CASE WHEN EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'core' AND table_name = 'dim_staff' AND column_name = 'etl_source'
    ) THEN 'PASS ✅' ELSE 'FAIL ❌' END AS status;


-- ============================================================
-- SECTION 7: KIỂM TRA INDEX TỒN TẠI
-- ============================================================

SELECT
    'TEST-IDX01' AS test_id,
    'Index: idx_stg_orders_id tồn tại' AS test_name,
    CASE WHEN EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE schemaname = 'staging' AND tablename = 'tbl_orders' AND indexname = 'idx_stg_orders_id'
    ) THEN 'PASS ✅' ELSE 'FAIL ❌' END AS status;

SELECT
    'TEST-IDX02' AS test_id,
    'Index: idx_fact_orders_customer tồn tại' AS test_name,
    CASE WHEN EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE schemaname = 'core' AND tablename = 'fact_orders' AND indexname = 'idx_fact_orders_customer'
    ) THEN 'PASS ✅' ELSE 'FAIL ❌' END AS status;

SELECT
    'TEST-IDX03' AS test_id,
    'Index: idx_dim_customer_id tồn tại' AS test_name,
    CASE WHEN EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE schemaname = 'core' AND tablename = 'dim_customer' AND indexname = 'idx_dim_customer_id'
    ) THEN 'PASS ✅' ELSE 'FAIL ❌' END AS status;

SELECT
    'TEST-IDX04' AS test_id,
    'Index: idx_dim_staff_id tồn tại' AS test_name,
    CASE WHEN EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE schemaname = 'core' AND tablename = 'dim_staff' AND indexname = 'idx_dim_staff_id'
    ) THEN 'PASS ✅' ELSE 'FAIL ❌' END AS status;

SELECT
    'TEST-IDX05' AS test_id,
    'Index: idx_stg_whi_lot (tblwarehouse_items) tồn tại (BƯỚC 7)' AS test_name,
    CASE WHEN EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE schemaname = 'staging' AND tablename = 'tblwarehouse_items' AND indexname = 'idx_stg_whi_lot'
    ) THEN 'PASS ✅' ELSE 'FAIL ❌' END AS status;


-- ============================================================
-- SECTION 8: KIỂM TRA DỮ LIỆU core.dim_date
-- ============================================================

-- Kiểm tra số lượng row (2020-2030 = 11 năm ~ 4018 ngày)
SELECT
    'TEST-DAT01' AS test_id,
    'Data: core.dim_date có đủ rows (2020-2030)' AS test_name,
    CASE WHEN (SELECT COUNT(*) FROM core.dim_date) = 4018
        THEN 'PASS ✅ (' || (SELECT COUNT(*) FROM core.dim_date) || ' rows)'
        ELSE 'FAIL ❌ (expected 4018, got ' || (SELECT COUNT(*) FROM core.dim_date) || ')'
    END AS status;

-- Kiểm tra ngày bắt đầu
SELECT
    'TEST-DAT02' AS test_id,
    'Data: core.dim_date bắt đầu từ 2020-01-01' AS test_name,
    CASE WHEN (SELECT MIN(full_date) FROM core.dim_date) = '2020-01-01'
        THEN 'PASS ✅'
        ELSE 'FAIL ❌ (min date = ' || (SELECT MIN(full_date)::TEXT FROM core.dim_date) || ')'
    END AS status;

-- Kiểm tra ngày kết thúc
SELECT
    'TEST-DAT03' AS test_id,
    'Data: core.dim_date kết thúc tại 2030-12-31' AS test_name,
    CASE WHEN (SELECT MAX(full_date) FROM core.dim_date) = '2030-12-31'
        THEN 'PASS ✅'
        ELSE 'FAIL ❌ (max date = ' || (SELECT MAX(full_date)::TEXT FROM core.dim_date) || ')'
    END AS status;

-- Kiểm tra không có duplicate date_key
SELECT
    'TEST-DAT04' AS test_id,
    'Data: core.dim_date không có duplicate date_key' AS test_name,
    CASE WHEN (
        SELECT COUNT(*) FROM (
            SELECT date_key, COUNT(*) FROM core.dim_date GROUP BY date_key HAVING COUNT(*) > 1
        ) dup
    ) = 0
        THEN 'PASS ✅'
        ELSE 'FAIL ❌ (có duplicate date_key)'
    END AS status;

-- Kiểm tra is_weekend đúng với thứ 7 / chủ nhật
SELECT
    'TEST-DAT05' AS test_id,
    'Data: core.dim_date.is_weekend đúng logic (Sat/Sun)' AS test_name,
    CASE WHEN NOT EXISTS (
        SELECT 1 FROM core.dim_date
        WHERE is_weekend = TRUE AND day_of_week NOT IN (0, 6)
    ) AND NOT EXISTS (
        SELECT 1 FROM core.dim_date
        WHERE is_weekend = FALSE AND day_of_week IN (0, 6)
    )
        THEN 'PASS ✅'
        ELSE 'FAIL ❌ (is_weekend không khớp day_of_week)'
    END AS status;

-- Kiểm tra date_key format YYYYMMDD
SELECT
    'TEST-DAT06' AS test_id,
    'Data: core.dim_date.date_key khớp format YYYYMMDD với full_date' AS test_name,
    CASE WHEN NOT EXISTS (
        SELECT 1 FROM core.dim_date
        WHERE date_key <> TO_CHAR(full_date, 'YYYYMMDD')::INT
    )
        THEN 'PASS ✅'
        ELSE 'FAIL ❌ (date_key không khớp full_date)'
    END AS status;


-- ============================================================
-- SECTION 9: KIỂM TRA STAGING TABLE RỖNG (chưa có dữ liệu thật)
-- ============================================================

SELECT
    'TEST-EMPTY01' AS test_id,
    'Data: staging.tbl_orders hiện tại rỗng (chưa ETL)' AS test_name,
    CASE WHEN (SELECT COUNT(*) FROM staging.tbl_orders) = 0
        THEN 'PASS ✅ (0 rows - chờ ETL)'
        ELSE 'INFO ℹ️ (' || (SELECT COUNT(*) FROM staging.tbl_orders) || ' rows đã có)'
    END AS status;

SELECT
    'TEST-EMPTY02' AS test_id,
    'Data: core.dim_customer hiện tại rỗng (chưa ETL)' AS test_name,
    CASE WHEN (SELECT COUNT(*) FROM core.dim_customer) = 0
        THEN 'PASS ✅ (0 rows - chờ ETL)'
        ELSE 'INFO ℹ️ (' || (SELECT COUNT(*) FROM core.dim_customer) || ' rows đã có)'
    END AS status;

SELECT
    'TEST-EMPTY03' AS test_id,
    'Data: core.fact_orders hiện tại rỗng (chưa ETL)' AS test_name,
    CASE WHEN (SELECT COUNT(*) FROM core.fact_orders) = 0
        THEN 'PASS ✅ (0 rows - chờ ETL)'
        ELSE 'INFO ℹ️ (' || (SELECT COUNT(*) FROM core.fact_orders) || ' rows đã có)'
    END AS status;


-- ============================================================
-- SECTION 10: TỔNG HỢP SỐ LƯỢNG BẢNG THEO SCHEMA
-- ============================================================

SELECT
    'SUMMARY' AS test_id,
    'Tổng số bảng theo schema' AS test_name,
    table_schema AS schema_name,
    COUNT(*) AS total_tables
FROM information_schema.tables
WHERE table_schema IN ('staging', 'core', 'mart')
  AND table_type = 'BASE TABLE'
GROUP BY table_schema
ORDER BY table_schema;

-- ============================================================
-- SECTION 11: KIỂM TRA PHÂN QUYỀN USER
-- ============================================================

-- Kiểm tra user bi_reader tồn tại
SELECT
    'TEST-SEC01' AS test_id,
    'Security: user bi_reader tồn tại' AS test_name,
    CASE WHEN EXISTS (
        SELECT 1 FROM pg_roles WHERE rolname = 'bi_reader'
    ) THEN 'PASS ✅' ELSE 'FAIL ❌' END AS status;

-- Kiểm tra user dwh_admin tồn tại
SELECT
    'TEST-SEC02' AS test_id,
    'Security: user dwh_admin tồn tại' AS test_name,
    CASE WHEN EXISTS (
        SELECT 1 FROM pg_roles WHERE rolname = 'dwh_admin'
    ) THEN 'PASS ✅' ELSE 'FAIL ❌' END AS status;

-- ============================================================
-- KẾT THÚC: DWH Structure Tests
-- ============================================================
SELECT '=============================' AS separator,
       'ALL TESTS COMPLETED' AS message,
       NOW() AS run_at;