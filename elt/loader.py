# ============================================
# loader.py
# Load DataFrame vào staging schema (PostgreSQL)
# ============================================

import pandas as pd
from sqlalchemy import text
from loguru import logger

def load_table(
    pg_engine,
    df: pd.DataFrame,
    staging_table: str,
    watermark_col: str | None,
    chunksize: int = 2000,
):
    """
    Load DataFrame vào staging.{staging_table}.

    Strategy:
    - watermark_col = None  → TRUNCATE rồi INSERT (full load)
    - watermark_col có giá trị → INSERT rows mới (incremental)
      Nếu bảng có cột `id`, dùng DELETE + INSERT để tránh duplicate.
    """
    if df.empty:
        logger.warning(f"[Load] {staging_table} → no data, skip.")
        return

    full_table = f"staging.{staging_table}"

    # Thêm etl_loaded_at nếu chưa có
    if "etl_loaded_at" not in df.columns:
        df = df.copy()
        df["etl_loaded_at"] = pd.Timestamp.now()

    with pg_engine.begin() as conn:
        if watermark_col is None:
            # Full load: truncate → insert
            conn.execute(text(f"TRUNCATE TABLE {full_table}"))
            logger.info(f"[Load] {full_table} truncated.")
            df.to_sql(
                name=staging_table,
                schema="staging",
                con=conn,
                if_exists="append",
                index=False,
                chunksize=chunksize,
                method="multi",
            )
        else:
            # Incremental: nếu bảng có cột id → upsert bằng DELETE + INSERT
            if "id" in df.columns:
                ids = df["id"].dropna().astype(int).tolist()
                if ids:
                    id_list = ",".join(map(str, ids))
                    conn.execute(
                        text(f"DELETE FROM {full_table} WHERE id IN ({id_list})")
                    )
                    logger.info(
                        f"[Load] {full_table} deleted {len(ids):,} existing rows."
                    )
            df.to_sql(
                name=staging_table,
                schema="staging",
                con=conn,
                if_exists="append",
                index=False,
                chunksize=chunksize,
                method="multi",
            )

    logger.success(f"[Load] {full_table} ← {len(df):,} rows inserted.")
