-- ============================================================
-- BƯỚC 8: BỔ SUNG INDEXES CÒN THIẾU
-- Chạy với user postgres (superuser) vì tables do postgres tạo.
--
-- Cách chạy:
--   psql -U postgres -d erp_dwh -f sql/08_add_missing_indexes.sql
--
-- Mục đích:
--   1. UNIQUE index trên natural key của các dim tables
--      → Ngăn duplicate rows khi ETL chạy lại
--   2. Index employee_key trên fact_orders
--      → Tăng tốc JOIN dim_staff trong dbt và BI queries
-- ============================================================

-- ─── 1. UNIQUE indexes: dim natural keys ──────────────────────
-- Đảm bảo mỗi ERP entity chỉ có 1 row trong dim table
CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_staff_id
    ON core.dim_staff(staff_id);

CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_dept_id
    ON core.dim_department(department_id);

CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_customer_id
    ON core.dim_customer(customer_id);

CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_product_id
    ON core.dim_product(product_id);

CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_wh_id
    ON core.dim_warehouse(warehouse_id);

CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_sup_id
    ON core.dim_supplier(supplier_id);

-- ─── 2. Indexes còn thiếu trên fact tables ────────────────────
-- fact_orders.employee_key: thường dùng để join dim_staff
CREATE INDEX IF NOT EXISTS idx_fact_orders_employee
    ON core.fact_orders(employee_key);

-- ─── Verify ───────────────────────────────────────────────────
SELECT
    schemaname,
    tablename,
    indexname
FROM pg_indexes
WHERE schemaname = 'core'
  AND indexname LIKE 'uq_%'
ORDER BY tablename, indexname;
