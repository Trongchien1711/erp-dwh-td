"""
setup.py
========
Chạy MỘT LẦN DUY NHẤT với tài khoản superuser PostgreSQL (postgres) để:
  1. ALTER TABLE core.dim_customer — thêm các cột price_group
  2. Đổi owner staging.* → dwh_admin (cần để loader có thể ALTER TABLE)
  3. GRANT quyền đầy đủ trên schema staging và core cho dwh_admin

Yêu cầu trước khi chạy:
  - PostgreSQL đang chạy
  - Database erp_dwh và các schema staging, core đã tồn tại
  - Thêm vào .env:
        PG_SUPER_USER=postgres
        PG_SUPER_PASSWORD=<mật_khẩu_postgres>

Cách chạy:
  python setup.py

SQL tương đương (có thể chạy thủ công trong pgAdmin):
-------------------------------------------------------
  ALTER TABLE core.dim_customer
    ADD COLUMN IF NOT EXISTS price_group_key  INTEGER,
    ADD COLUMN IF NOT EXISTS price_group_code VARCHAR,
    ADD COLUMN IF NOT EXISTS price_group_name VARCHAR;

  -- Chạy cho từng bảng trong staging:
  ALTER TABLE staging.<tbl> OWNER TO dwh_admin;

  GRANT USAGE, CREATE ON SCHEMA staging TO dwh_admin;
  GRANT ALL ON ALL TABLES IN SCHEMA staging TO dwh_admin;
  ALTER DEFAULT PRIVILEGES IN SCHEMA staging GRANT ALL ON TABLES TO dwh_admin;

  GRANT USAGE ON SCHEMA core TO dwh_admin;
  GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA core TO dwh_admin;
  ALTER DEFAULT PRIVILEGES IN SCHEMA core GRANT SELECT, INSERT, UPDATE ON TABLES TO dwh_admin;
-------------------------------------------------------
"""
import os
import sys
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

host     = os.getenv("PG_HOST", "localhost")
port     = os.getenv("PG_PORT", "5432")
database = os.getenv("PG_DATABASE", "erp_dwh")
su_user  = os.getenv("PG_SUPER_USER", "postgres")
su_pass  = os.getenv("PG_SUPER_PASSWORD", "")
etl_user = os.getenv("PG_USER", "dwh_admin")

if not su_pass:
    print("ERROR: PG_SUPER_PASSWORD chưa được cấu hình trong .env")
    print("Thêm vào .env:  PG_SUPER_PASSWORD=<mật_khẩu_postgres>")
    sys.exit(1)

engine = create_engine(
    f"postgresql+psycopg2://{su_user}:{su_pass}@{host}:{port}/{database}",
    pool_pre_ping=True,
)

print(f"Kết nối với [{su_user}@{host}:{port}/{database}] ...")

with engine.begin() as conn:

    # ──────────────────────────────────────────────────────────────────────────
    # 1. Thêm cột price_group vào core.dim_customer
    # ──────────────────────────────────────────────────────────────────────────
    print("\n[1] ALTER TABLE core.dim_customer — thêm cột price_group ...")
    conn.execute(text("""
        ALTER TABLE core.dim_customer
          ADD COLUMN IF NOT EXISTS price_group_key  INTEGER,
          ADD COLUMN IF NOT EXISTS price_group_code VARCHAR,
          ADD COLUMN IF NOT EXISTS price_group_name VARCHAR
    """))
    print("    OK")

    # ──────────────────────────────────────────────────────────────────────────
    # 2. Đổi owner tất cả bảng trong schema staging → etl_user
    # ──────────────────────────────────────────────────────────────────────────
    print(f"\n[2] Đổi owner staging.* → {etl_user} ...")
    tables = conn.execute(
        text("SELECT tablename FROM pg_tables WHERE schemaname = 'staging' ORDER BY tablename")
    ).fetchall()
    print(f"    Tìm thấy {len(tables)} bảng trong staging.")
    for (tbl,) in tables:
        try:
            conn.execute(text(f'ALTER TABLE staging."{tbl}" OWNER TO {etl_user}'))
            print(f"    OK  staging.{tbl}")
        except Exception as e:
            print(f"    ERR staging.{tbl}: {e}")

    # ──────────────────────────────────────────────────────────────────────────
    # 3. GRANT quyền schema staging
    # ──────────────────────────────────────────────────────────────────────────
    print(f"\n[3] GRANT quyền schema staging → {etl_user} ...")
    conn.execute(text(f"GRANT USAGE, CREATE ON SCHEMA staging TO {etl_user}"))
    conn.execute(text(f"GRANT ALL ON ALL TABLES IN SCHEMA staging TO {etl_user}"))
    conn.execute(text(
        f"ALTER DEFAULT PRIVILEGES IN SCHEMA staging "
        f"GRANT ALL ON TABLES TO {etl_user}"
    ))
    print("    OK")

    # ──────────────────────────────────────────────────────────────────────────
    # 4. GRANT quyền schema core (SELECT, INSERT, UPDATE — không DROP/DELETE)
    # ──────────────────────────────────────────────────────────────────────────
    print(f"\n[4] GRANT quyền schema core → {etl_user} ...")
    conn.execute(text(f"GRANT USAGE ON SCHEMA core TO {etl_user}"))
    conn.execute(text(
        f"GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA core TO {etl_user}"
    ))
    conn.execute(text(
        f"ALTER DEFAULT PRIVILEGES IN SCHEMA core "
        f"GRANT SELECT, INSERT, UPDATE ON TABLES TO {etl_user}"
    ))
    print("    OK")

engine.dispose()

print("\n" + "=" * 50)
print("Setup hoàn tất!")
print("Bước tiếp theo: python pipeline.py --stage all")
