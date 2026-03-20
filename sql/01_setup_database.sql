-- ============================================
-- BƯỚC 1: SETUP DATABASE, USER, SCHEMA
-- Chạy trên database: postgres (mặc định)
-- ============================================

-- Tạo user
CREATE USER dwh_admin WITH PASSWORD 'your_strong_password_here';
CREATE USER bi_reader WITH PASSWORD 'bi_reader_password';

-- Tạo database
CREATE DATABASE erp_dwh
    WITH OWNER     = dwh_admin
    ENCODING       = 'UTF8'
    LC_COLLATE     = 'en_US.UTF-8'
    LC_CTYPE       = 'en_US.UTF-8'
    TEMPLATE       = template0;

-- ============================================
-- Chạy trên database: erp_dwh
-- ============================================

-- Tạo schema
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS mart;

-- Phân quyền dwh_admin
GRANT ALL ON SCHEMA staging TO dwh_admin;
GRANT ALL ON SCHEMA core    TO dwh_admin;
GRANT ALL ON SCHEMA mart    TO dwh_admin;

-- Phân quyền bi_reader
GRANT CONNECT  ON DATABASE erp_dwh TO bi_reader;
GRANT USAGE    ON SCHEMA core       TO bi_reader;
GRANT USAGE    ON SCHEMA mart       TO bi_reader;

GRANT SELECT ON ALL TABLES IN SCHEMA core TO bi_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA mart TO bi_reader;

ALTER DEFAULT PRIVILEGES IN SCHEMA core
    GRANT SELECT ON TABLES TO bi_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA mart
    GRANT SELECT ON TABLES TO bi_reader;