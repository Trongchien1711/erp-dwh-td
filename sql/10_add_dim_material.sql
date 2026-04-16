-- ============================================================
-- BUOC 10: THEM dim_material
-- NPL master dimension (tbl_materials from MySQL ERP)
--
-- Cach chay (can superuser neu tables do postgres tao):
--   psql -U postgres -d erp_dwh -f sql/10_add_dim_material.sql
--
-- Hoac chay voi dwh_admin neu co quyen CREATE:
--   psql -U dwh_admin -d erp_dwh -f sql/10_add_dim_material.sql
-- ============================================================

-- --------------------------------------------
-- dim_material
-- Source: tbl_materials (MySQL ERP)
-- Grain : 1 row per NPL (nguyen phu lieu) item
-- --------------------------------------------
CREATE TABLE IF NOT EXISTS core.dim_material (
    material_key            SERIAL PRIMARY KEY,
    material_id             INT             NOT NULL,   -- tbl_materials.id
    material_code           VARCHAR(100),               -- code (e.g. VTDG-CAR3L1X-1)
    material_name           TEXT,                       -- name (full NPL name)
    material_name_supplier  TEXT,                       -- name_supplier
    category_id             INT,                        -- nhom NPL
    unit_id                 INT,                        -- don vi tinh
    price_import            NUMERIC(22, 4),             -- don gia nhap
    species                 INT,                        -- loai species (paper type etc.)
    is_zinc                 BOOLEAN DEFAULT FALSE,      -- True = Kem in (zinc offset plate)
    mode_id                 INT,                        -- che do su dung
    longs                   NUMERIC(10, 4),             -- chieu dai
    wide                    NUMERIC(10, 4),             -- chieu rong
    height                  NUMERIC(10, 4),             -- chieu cao
    is_single_use           BOOLEAN DEFAULT FALSE,      -- su dung 1 lan
    is_active               BOOLEAN DEFAULT TRUE,       -- status=1
    id_branch               INT,
    date_created            TIMESTAMP,
    date_updated            TIMESTAMP,
    etl_loaded_at           TIMESTAMP DEFAULT NOW(),
    etl_source              VARCHAR(50) DEFAULT 'tbl_materials'
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_material_id
    ON core.dim_material(material_id);
CREATE INDEX IF NOT EXISTS idx_dim_material_code
    ON core.dim_material(material_code);
CREATE INDEX IF NOT EXISTS idx_dim_material_is_zinc
    ON core.dim_material(is_zinc);
CREATE INDEX IF NOT EXISTS idx_dim_material_category
    ON core.dim_material(category_id);
