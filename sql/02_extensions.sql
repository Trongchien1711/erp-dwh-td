-- ============================================
-- BƯỚC 2: EXTENSIONS
-- ============================================
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
CREATE EXTENSION IF NOT EXISTS btree_gin;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS tablefunc;

-- Kiểm tra
SELECT extname, extversion
FROM pg_extension
ORDER BY extname;
