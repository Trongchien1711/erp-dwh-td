# ============================================
# extractor.py
# Extract dữ liệu từ MySQL theo watermark
# ============================================

import pandas as pd
from sqlalchemy import text
from loguru import logger


# ============================================
# Danh sách bảng cần extract
# Format: {
#   "staging_table": "source_table",
#   "watermark_col": "cột dùng để incremental",
# }
# ============================================
TABLE_CONFIG = [
    {
        "source_table":   "tbl_orders",
        "staging_table":  "tbl_orders",
        "watermark_col":  "date_updated",
    },
    {
        "source_table":   "tbl_order_items",
        "staging_table":  "tbl_order_items",
        "watermark_col":  "date_active",
    },
    {
        "source_table":   "tbl_order_items_stages",
        "staging_table":  "tbl_order_items_stages",
        "watermark_col":  "date_active",
    },
    {
        "source_table":   "tbl_deliveries",
        "staging_table":  "tbl_deliveries",
        "watermark_col":  "date_updated",
    },
    {
        "source_table":   "tbl_delivery_items",
        "staging_table":  "tbl_delivery_items",
        "watermark_col":  None,          # full load
    },
    {
        "source_table":   "tblclients",
        "staging_table":  "tblclients",
        "watermark_col":  "date_update",
    },
    {
        "source_table":   "tbl_products",
        "staging_table":  "tbl_products",
        "watermark_col":  "date_updated",
    },
    {
        "source_table":   "tblwarehouse",
        "staging_table":  "tblwarehouse",
        "watermark_col":  None,
    },
    {
        "source_table":   "tbllocaltion_warehouses",
        "staging_table":  "tbllocaltion_warehouses",
        "watermark_col":  None,
    },
    {
        "source_table":   "tblwarehouse_product",
        "staging_table":  "tblwarehouse_product",
        "watermark_col":  "date_warehouse",
    },
    {
        "source_table":   "tblwarehouse_export",
        "staging_table":  "tblwarehouse_export",
        "watermark_col":  "date_warehouse",
    },
    {
        "source_table":   "tbltransfer_warehouse_detail",
        "staging_table":  "tbltransfer_warehouse_detail",
        "watermark_col":  None,
    },
    {
        "source_table":   "tblsuppliers",
        "staging_table":  "tblsuppliers",
        "watermark_col":  "date_update",
    },
    {
        "source_table":   "tblpurchase_order",
        "staging_table":  "tblpurchase_order",
        "watermark_col":  "date_create",
    },
    {
        "source_table":   "tblpurchase_order_items",
        "staging_table":  "tblpurchase_order_items",
        "watermark_col":  None,
    },
    {
        "source_table":   "tbl_purchase_products",
        "staging_table":  "tbl_purchase_products",
        "watermark_col":  "date",
    },
    {
        "source_table":   "tbl_purchase_product_items",
        "staging_table":  "tbl_purchase_product_items",
        "watermark_col":  None,
    },
    {
        "source_table":   "tbl_productions_orders",
        "staging_table":  "tbl_productions_orders",
        "watermark_col":  "date_updated",
    },
    {
        "source_table":   "tbl_productions_orders_items",
        "staging_table":  "tbl_productions_orders_items",
        "watermark_col":  None,
    },
    {
        "source_table":   "tbl_productions_orders_items_stages",
        "staging_table":  "tbl_productions_orders_items_stages",
        "watermark_col":  "date_active",
    },
    {
        "source_table":   "tbl_manufactures",
        "staging_table":  "tbl_manufactures",
        "watermark_col":  "date_updated",
    },
    {
        "source_table":   "tblstaff",
        "staging_table":  "tblstaff",
        "watermark_col":  "date_update",
    },
    {
        "source_table":   "tbldepartments",
        "staging_table":  "tbldepartments",
        "watermark_col":  None,
    },
]

CHUNK_SIZE = 5000


def extract_table(
    mysql_engine,
    source_table: str,
    watermark_col: str | None,
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
            WHERE `{watermark_col}` >= %(wm)s
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
            text(query) if not params else query,
            conn,
            params=params if params else None,
            chunksize=CHUNK_SIZE,
        ):
            chunks.append(chunk)

    if chunks:
        df = pd.concat(chunks, ignore_index=True)
    else:
        df = pd.DataFrame()

    logger.info(f"[Extract] {source_table} → {len(df):,} rows")
    return df
