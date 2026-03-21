# ============================================
# watermark.py
# Quản lý watermark (incremental load)
# Lưu last_loaded_at cho từng bảng
# ============================================

from sqlalchemy import text
from loguru import logger


WATERMARK_DDL = """
CREATE TABLE IF NOT EXISTS staging.etl_watermark (
    table_name      VARCHAR(100) PRIMARY KEY,
    last_loaded_at  TEXT         NOT NULL DEFAULT '2020-01-01 00:00:00',
    updated_at      TIMESTAMP    NOT NULL DEFAULT NOW()
);
"""

# Migration: convert existing TIMESTAMP column to TEXT so both datetime strings
# ('2026-03-12 14:13:14') and integer strings ('725264') can be stored.
_MIGRATE_DDL = """
ALTER TABLE staging.etl_watermark
    ALTER COLUMN last_loaded_at TYPE TEXT
    USING last_loaded_at::TEXT;
"""


def init_watermark_table(pg_engine):
    """Tạo bảng etl_watermark nếu chưa có; migrate cột TIMESTAMP → TEXT nếu cần."""
    with pg_engine.begin() as conn:
        conn.execute(text(WATERMARK_DDL))
        # Check current column type and migrate if still TIMESTAMP
        col_type = conn.execute(text(
            "SELECT data_type FROM information_schema.columns "
            "WHERE table_schema='staging' AND table_name='etl_watermark' "
            "AND column_name='last_loaded_at'"
        )).scalar()
        if col_type and col_type.lower() not in ('text', 'character varying'):
            conn.execute(text(_MIGRATE_DDL))
            logger.info("[Watermark] Migrated last_loaded_at column: TIMESTAMP -> TEXT")
    logger.info("[Watermark] Table ready.")


def get_watermark(pg_engine, table_name: str, default: str = '2020-01-01 00:00:00') -> str:
    """
    Lấy watermark (last_loaded_at) của bảng.
    default: giá trị trả về khi bảng chưa có watermark.
      - Dùng '2020-01-01 00:00:00' cho cột timestamp/datetime (mặc định)
      - Dùng '0' cho cột integer ID (tránh MySQL cast '2020-01-01' → 2020)
    """
    sql = """
        SELECT COALESCE(
            (SELECT last_loaded_at FROM staging.etl_watermark WHERE table_name = :tbl),
            :default
        ) AS wm
    """
    with pg_engine.connect() as conn:
        result = conn.execute(text(sql), {"tbl": table_name, "default": default}).fetchone()
    wm = str(result[0])
    logger.info(f"[Watermark] {table_name} -> last_loaded_at = {wm}")
    return wm


def set_watermark(pg_engine, table_name: str, new_watermark: str):
    """Cập nhật watermark sau khi load xong."""
    sql = """
        INSERT INTO staging.etl_watermark (table_name, last_loaded_at, updated_at)
        VALUES (:tbl, :wm, NOW())
        ON CONFLICT (table_name)
        DO UPDATE SET last_loaded_at = EXCLUDED.last_loaded_at,
                      updated_at     = NOW();
    """
    with pg_engine.begin() as conn:
        conn.execute(text(sql), {"tbl": table_name, "wm": new_watermark})
    logger.info(f"[Watermark] {table_name} -> updated to {new_watermark}")