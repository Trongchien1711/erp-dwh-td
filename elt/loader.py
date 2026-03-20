# ============================================
# loader.py
# Load DataFrame vao staging schema (PostgreSQL)
# ============================================

import io
import re
import pandas as pd
from sqlalchemy import text
from loguru import logger

# Map pandas dtype → PostgreSQL column type dùng khi ALTER TABLE
_DTYPE_MAP = {
    "int64":          "BIGINT",
    "int32":          "INTEGER",
    "float64":        "DOUBLE PRECISION",
    "float32":        "REAL",
    "bool":           "BOOLEAN",
    "datetime64[ns]": "TIMESTAMP",
    "object":         "TEXT",
}

_ZERO_DATE_RE = re.compile(r'^0{4}-0{2}-0{2}')

# Tên cột trùng với THỰC SỰ reserved keywords PostgreSQL — gây lỗi khi to_sql tạo DDL
# Lưu ý: date, type, time, timestamp, year... KHÔNG reserved trong PG — là type names thôi
_PG_RESERVED = {
    "concurrently", "order", "default", "user", "table", "where", "group",
    "select", "from", "index", "check", "unique", "primary", "end", "begin",
}


def _sanitize_columns(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    Rename cột nào trùng với PostgreSQL reserved keyword bằng cách thêm '_col' suffix.
    Trả về (df_renamed, rename_map).
    """
    rename = {c: f"{c}_col" for c in df.columns if c.lower() in _PG_RESERVED}
    if rename:
        df = df.rename(columns=rename)
        logger.warning(f"[Load] Renamed reserved keyword cols: {rename}")
    return df, rename


def _fix_zero_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Replace MySQL zero-dates ('0000-00-00', '0000-00-00 00:00:00') with NULL."""
    df = df.copy()
    for col in df.select_dtypes(include='object').columns:
        mask = df[col].astype(str).str.match(_ZERO_DATE_RE)
        if mask.any():
            df.loc[mask, col] = None
    return df


def _get_staging_columns(conn, staging_table: str) -> list:
    result = conn.execute(
        text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'staging' AND table_name = :tbl "
            "ORDER BY ordinal_position"
        ),
        {"tbl": staging_table},
    )
    return [r[0] for r in result.fetchall()]


def _pg_type_for(series: pd.Series) -> str:
    """Trả về PostgreSQL type phù hợp cho một pandas Series."""
    dtype_str = str(series.dtype)
    if dtype_str.startswith("datetime64"):
        # Xử lý cả timezone-aware (datetime64[ns, UTC], ...) và naive
        return "TIMESTAMPTZ" if getattr(series.dtype, "tz", None) else "TIMESTAMP"
    return _DTYPE_MAP.get(dtype_str, "TEXT")


def _add_missing_columns(pg_engine, full_table: str, staging_table: str,
                         df: pd.DataFrame, existing_cols: list) -> list:
    """
    ALTER TABLE để thêm các cột có trong df nhưng chưa có trong staging.
    Mỗi ALTER chạy trong connection riêng để tránh làm abort transaction chính.
    Trả về danh sách tên cột đã được thêm thành công.
    """
    new_cols = [c for c in df.columns if c not in existing_cols]
    added = []
    for col in new_cols:
        pg_type = _pg_type_for(df[col])
        alter_sql = f'ALTER TABLE {full_table} ADD COLUMN IF NOT EXISTS "{col}" {pg_type}'
        try:
            # Dùng connection riêng — nếu fail không ảnh hưởng transaction load chính
            with pg_engine.begin() as alter_conn:
                alter_conn.execute(text(alter_sql))
            added.append(col)
            logger.info(f"[Load] {full_table}: added column '{col}' ({pg_type})")
        except Exception as e:
            short_err = str(e).split('\n')[0]
            logger.warning(f"[Load] {full_table}: cannot add column '{col}': {short_err}")
    return added


def _copy_insert(pg_engine, df: pd.DataFrame, staging_table: str):
    """
    Dùng PostgreSQL COPY protocol để insert nhanh hơn to_sql ~30-50x.
    Chạy trên raw psycopg2 connection riêng (TRUNCATE + COPY trong 1 transaction).
    """
    # pandas lưu nullable integer dưới dạng float64 (vd. 0 → "0.0").
    # Chuyển về Int64 (nullable int) để CSV viết "0" thay vì "0.0".
    df = df.copy()
    for col in df.select_dtypes(include="float").columns:
        notna = df[col].dropna()
        if len(notna) > 0 and (notna % 1 == 0).all():
            df[col] = df[col].astype(pd.Int64Dtype())

    cols = ", ".join(f'"{c}"' for c in df.columns)
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, na_rep="")
    buf.seek(0)

    raw = pg_engine.raw_connection()
    try:
        with raw.cursor() as cur:
            cur.execute(f'TRUNCATE TABLE staging."{staging_table}"')
            cur.copy_expert(
                f'COPY staging."{staging_table}" ({cols}) FROM STDIN WITH (FORMAT CSV)',
                buf,
            )
        raw.commit()
    except Exception:
        raw.rollback()
        raise
    finally:
        raw.close()


def load_table(
    pg_engine,
    df: pd.DataFrame,
    staging_table: str,
    watermark_col,
):
    if df.empty:
        logger.warning(f"[Load] {staging_table} -> no data, skip.")
        return

    full_table = f"staging.{staging_table}"

    if "etl_loaded_at" not in df.columns:
        df = df.copy()
        df["etl_loaded_at"] = pd.Timestamp.now()

    df = _fix_zero_dates(df)
    df, _ = _sanitize_columns(df)

    # --- Bước 1: kiểm tra & ALTER TABLE (connection riêng, trước transaction load) ---
    with pg_engine.connect() as probe_conn:
        existing_cols = _get_staging_columns(probe_conn, staging_table)

    if existing_cols:
        new_mysql_cols = [c for c in df.columns if c not in existing_cols]
        if new_mysql_cols:
            added = _add_missing_columns(pg_engine, full_table, staging_table, df, existing_cols)
            skipped = [c for c in new_mysql_cols if c not in added]
            if skipped:
                logger.warning(
                    f"[Load] {full_table}: {len(skipped)} cols still skipped "
                    f"(no ALTER privilege): {skipped}"
                )
            # Refresh lại sau ALTER
            with pg_engine.connect() as probe_conn:
                existing_cols = _get_staging_columns(probe_conn, staging_table)

    # --- Bước 2: TRUNCATE + COPY (hoặc to_sql khi bảng chưa tồn tại) ---
    if existing_cols:
        df_to_load = df[[c for c in df.columns if c in existing_cols]]
        _copy_insert(pg_engine, df_to_load, staging_table)
    else:
        # Bảng chưa tồn tại: dùng to_sql để tạo DDL + insert lần đầu
        with pg_engine.begin() as conn:
            df.to_sql(
                name=staging_table, schema="staging", con=conn,
                if_exists="replace", index=False, chunksize=5000, method="multi",
            )

    logger.success(f"[Load] {full_table} <- {len(df):,} rows inserted.")
