# ============================================
# connections.py
# Quản lý kết nối MySQL và PostgreSQL
# ============================================

import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from loguru import logger
# Load biến môi trường từ file .env
load_dotenv()

def get_mysql_engine():
    """Tạo SQLAlchemy engine kết nối MySQL (ERP source)."""
    host     = os.getenv("MYSQL_HOST", "localhost")
    port     = os.getenv("MYSQL_PORT", "3306")
    user     = os.getenv("MYSQL_USER")
    password = os.getenv("MYSQL_PASSWORD")
    database = os.getenv("MYSQL_DATABASE")

    url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4"
    engine = create_engine(
        url,
        pool_pre_ping=True,
        pool_recycle=3600,          # Tái tạo connection sau 1h — tránh lỗi "MySQL server has gone away"
        pool_size=5,
        max_overflow=2,
        connect_args={"connect_timeout": 10},
    )
    logger.info(f"[MySQL] Connected to {host}:{port}/{database}")
    return engine

def get_pg_engine():
    """Tạo SQLAlchemy engine kết nối PostgreSQL (DWH target)."""
    host     = os.getenv("PG_HOST", "localhost")
    port     = os.getenv("PG_PORT", "5432")
    user     = os.getenv("PG_USER")
    password = os.getenv("PG_PASSWORD")
    database = os.getenv("PG_DATABASE")

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(
        url,
        pool_pre_ping=True,
        pool_recycle=3600,          # Tái tạo connection sau 1h
        pool_size=5,
        max_overflow=2,
        connect_args={"connect_timeout": 10},
    )
    logger.info(f"[PostgreSQL] Connected to {host}:{port}/{database}")
    return engine

def test_connections():
    """Kiểm tra kết nối cả 2 database."""
    try:
        mysql_engine = get_mysql_engine()
        with mysql_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.success("[MySQL] Connection OK")
    except Exception as e:
        logger.error(f"[MySQL] Connection FAILED: {e}")

    try:
        pg_engine = get_pg_engine()
        with pg_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.success("[PostgreSQL] Connection OK")
    except Exception as e:
        logger.error(f"[PostgreSQL] Connection FAILED: {e}")


if __name__ == "__main__":
    test_connections()
