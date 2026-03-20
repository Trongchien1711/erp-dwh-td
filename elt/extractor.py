# ============================================
# extractor.py
# Extract dữ liệu từ MySQL theo watermark
# ============================================

from typing import Optional

import pandas as pd
from sqlalchemy import text
from loguru import logger


# ============================================
# Danh sách bảng cần extract
# Format: {
#   "source_table":  "tên bảng trong MySQL",
#   "watermark_col": "cột dùng để incremental (None = full load)",
# }
# ============================================
TABLE_CONFIG = [
    {
        "source_table":  "tbl_orders",
        "watermark_col": "date",
    },
    {
        "source_table":  "tbl_order_items",
        "watermark_col": None,          # full load (do không có cột timestamp nào phù hợp)
    },
    {
        "source_table":  "tbl_order_items_stages",
        "watermark_col": None,
    },
    {
        "source_table":  "tbl_deliveries",
        "watermark_col": "date",
    },
    {
        "source_table":  "tbl_delivery_items",
        "watermark_col": None,          # full load
    },
    {
        "source_table":  "tblclients",
        "watermark_col": None,          # date_update NULL/cũ trước 2020 → full load
    },
    {
        "source_table":  "tbl_products",
        "watermark_col": "date_updated",
    },
    {
        "source_table":  "tblwarehouse",
        "watermark_col": None,
    },
    {
        "source_table":  "tbllocaltion_warehouses",
        "watermark_col": None,
    },
    {
        "source_table":  "tblwarehouse_product",
        "watermark_col": "date_warehouse",
    },
    {
        "source_table":  "tblwarehouse_export",
        "watermark_col": "date_warehouse",
    },
    {
        "source_table":  "tbltransfer_warehouse_detail",
        "watermark_col": None,
    },
    {
        "source_table":  "tblsuppliers",
        "watermark_col": None,          # date_update NULL/cũ trước 2020 → full load
    },
    {
        "source_table":  "tblpurchase_order",
        "watermark_col": "date_create",
    },
    {
        "source_table":  "tblpurchase_order_items",
        "watermark_col": None,
    },
    {
        "source_table":  "tbl_purchase_products",
        "watermark_col": "date",
    },
    {
        "source_table":  "tbl_purchase_product_items",
        "watermark_col": None,
    },
    {
        "source_table":  "tbl_productions_orders",
        "watermark_col": None,   # date filter bị thiếu lệnh cũ -> full load
    },
    {
        "source_table":  "tbl_productions_orders_items",
        "watermark_col": None,
    },
    {
        "source_table":  "tbl_productions_orders_items_stages",
        "watermark_col": "date_active",
    },
    {
        "source_table":  "tbl_manufactures",
        "watermark_col": "date",
    },
    {
        "source_table":  "tblstaff",
        "watermark_col": None,          # datecreated thường NULL hoặc trước 2020 → full load
    },
    {
        "source_table":  "tbldepartments",
        "watermark_col": None,
    },
    {
        "source_table":  "tblcustomers_groups",
        "watermark_col": None,   # nhom khach hang (master: ADIDAS, RPAC, ...)
    },
    {
        "source_table":  "tblcustomer_groups",
        "watermark_col": None,   # bang phan cong khach hang -> nhom
    },
]

CHUNK_SIZE = 5000


def extract_table(
    mysql_engine,
    source_table: str,
    watermark_col: Optional[str],
    last_watermark: str,
) -> pd.DataFrame:
    """
    Extract 1 bảng từ MySQL.
    - Nếu watermark_col có → incremental theo watermark_col >= last_watermark
    - Nếu watermark_col = None → full load (truncate + reload)
    """
    if watermark_col:
        query = f"""
            SELECT *
            FROM `{source_table}`
            WHERE  COALESCE(`{watermark_col}`, '2000-01-01') >= :wm
            ORDER BY `{watermark_col}` ASC
        """
        params = {"wm": last_watermark}
        logger.info(f"[Extract] {source_table} | incremental from {last_watermark}")
    else:
        query = f"SELECT * FROM `{source_table}`"
        params = {}
        logger.info(f"[Extract] {source_table} | full load")

    chunks = []
    with mysql_engine.connect() as conn:
        for chunk in pd.read_sql(
            text(query),
            conn,
            params=params if params else None,
            chunksize=CHUNK_SIZE,
        ):
            chunks.append(chunk)

    if chunks:
        df = pd.concat(chunks, ignore_index=True)
    else:
        df = pd.DataFrame()

    logger.info(f"[Extract] {source_table} -> {len(df):,} rows")
    return df