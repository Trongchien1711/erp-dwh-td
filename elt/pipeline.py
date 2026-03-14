# ============================================
# pipeline.py
# Main ETL pipeline: Extract → Load → Transform
# Usage:
#   python pipeline.py              # chạy toàn bộ
#   python pipeline.py --stage extract   # chỉ extract + load staging
#   python pipeline.py --stage transform # chỉ transform staging → core
#   python pipeline.py --table tbl_orders  # chỉ chạy 1 bảng
# ============================================

import argparse
import sys
from datetime import datetime

from loguru import logger

from connections    import get_mysql_engine, get_pg_engine
from watermark      import init_watermark_table, get_watermark, set_watermark
from extractor      import TABLE_CONFIG, extract_table
from loader         import load_table
from transform_core import run_transforms


# ============================================
# Logger config
# ============================================
logger.remove()
logger.add(sys.stdout, level="INFO", colorize=True,
           format="<green>{time:HH:mm:ss}</green> | <level>{level}</level> | {message}")
logger.add("logs/pipeline_{time:YYYY-MM-DD}.log", level="DEBUG", rotation="1 day", retention="30 days")

def run_extract_load(mysql_engine, pg_engine, table_filter: str = None):
    """Extract từ MySQL và load vào staging PostgreSQL."""
    init_watermark_table(pg_engine)

    configs = TABLE_CONFIG
    if table_filter:
        configs = [c for c in TABLE_CONFIG if c["source_table"] == table_filter]
        if not configs:
            logger.error(f"Table '{table_filter}' not found in TABLE_CONFIG.")
            return

    for cfg in configs:
        source    = cfg["source_table"]
        staging   = cfg["staging_table"]
        wm_col    = cfg["watermark_col"]

        try:
            last_wm = get_watermark(pg_engine, source)
            df      = extract_table(mysql_engine, source, wm_col, last_wm,
                                    allow_null_watermark=cfg.get("allow_null_watermark", False))

            if df.empty:
                logger.info(f"[Pipeline] {source} → no new data, skip load.")
                continue

            load_table(pg_engine, df, staging, wm_col)

            # Cập nhật watermark
            if wm_col and wm_col in df.columns:
                new_wm = str(df[wm_col].max())
            else:
                new_wm = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            set_watermark(pg_engine, source, new_wm)

        except Exception as e:
            logger.error(f"[Pipeline] {source} FAILED: {e}")
            # Tiếp tục bảng tiếp theo thay vì dừng toàn bộ pipeline
            continue


def main():
    parser = argparse.ArgumentParser(description="ERP DWH ETL Pipeline")
    parser.add_argument(
        "--stage",
        choices=["extract", "transform", "all"],
        default="all",
        help="Chọn stage cần chạy (default: all)",
    )
    parser.add_argument(
        "--table",
        default=None,
        help="Chỉ chạy 1 bảng cụ thể (chỉ dùng với stage=extract)",
    )
    args = parser.parse_args()

    start = datetime.now()
    logger.info(f"{'='*50}")
    logger.info(f"Pipeline started at {start.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Stage: {args.stage} | Table: {args.table or 'ALL'}")
    logger.info(f"{'='*50}")

    mysql_engine = get_mysql_engine()
    pg_engine    = get_pg_engine()

    if args.stage in ("extract", "all"):
        logger.info(">>> STAGE 1: Extract + Load to Staging")
        run_extract_load(mysql_engine, pg_engine, table_filter=args.table)

    if args.stage in ("transform", "all"):
        logger.info(">>> STAGE 2: Transform Staging → Core")
        run_transforms(pg_engine)

    elapsed = (datetime.now() - start).total_seconds()
    logger.success(f"Pipeline finished in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
