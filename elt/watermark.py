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
    last_loaded_at  TIMESTAMP    NOT NULL DEFAULT '2020-01-01 00:00:00',
    updated_at      TIMESTAMP    NOT NULL DEFAULT NOW()
);
"""


def init_watermark_table(pg_engine):
    """Tạo bảng etl_watermark nếu chưa có."""
    with pg_engine.begin() as conn:
        conn.execute(text(WATERMARK_DDL))
    logger.info("[Watermark] Table ready.")


def get_watermark(pg_engine, table_name: str) -> str:
    """Lấy watermark (last_loaded_at) của bảng."""
    sql = """
        SELECT COALESCE(
            (SELECT last_loaded_at FROM staging.etl_watermark WHERE table_name = :tbl),
            '2020-01-01 00:00:00'::TIMESTAMP
        ) AS wm
    """
    with pg_engine.connect() as conn:
        result = conn.execute(text(sql), {"tbl": table_name}).fetchone()
    wm = str(result[0])
    logger.info(f"[Watermark] {table_name} → last_loaded_at = {wm}")
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
    logger.info(f"[Watermark] {table_name} → updated to {new_watermark}")
