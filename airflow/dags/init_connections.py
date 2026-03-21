"""
init_airflow_connections.py
===========================
Tạo Airflow Connections cho MySQL ERP và PostgreSQL DWH.

📚 Airflow Connection:
   - Lưu thông tin kết nối (host, port, user, password, dbname) ở một chỗ.
   - DAG không cần hardcode credentials — chỉ cần gọi BaseHook.get_connection("conn_id").
   - Quản lý qua UI: Admin → Connections.

Chạy script này 1 lần sau khi `docker compose up airflow-init`:
   docker compose exec airflow-scheduler python /opt/airflow/dags/init_connections.py
"""

import os
import subprocess


def create_connection(conn_id, conn_type, host, port, schema, login, password, description=""):
    """Tạo hoặc cập nhật Airflow connection dùng CLI."""
    cmd = [
        "airflow", "connections", "add", conn_id,
        "--conn-type",     conn_type,
        "--conn-host",     host,
        "--conn-port",     str(port),
        "--conn-schema",   schema,
        "--conn-login",    login,
        "--conn-password", password,
        "--conn-description", description,
    ]
    # Xoá nếu đã tồn tại (idempotent)
    subprocess.run(["airflow", "connections", "delete", conn_id],
                   capture_output=True)

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode == 0:
        print(f"  ✓  Connection '{conn_id}' created")
    else:
        print(f"  ✗  Connection '{conn_id}' FAILED: {result.stderr}")


if __name__ == "__main__":
    print("Creating Airflow connections...\n")

    # ── MySQL ERP (nguồn) ──────────────────────────────────────
    create_connection(
        conn_id="mysql_erp",
        conn_type="mysql",
        host=os.getenv("MYSQL_HOST", "host.docker.internal"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        schema=os.getenv("MYSQL_DATABASE", "test"),
        login=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", "Chienvipso1"),
        description="MySQL ERP source database",
    )

    # ── PostgreSQL DWH (đích) ─────────────────────────────────
    create_connection(
        conn_id="postgres_dwh",
        conn_type="postgres",
        host=os.getenv("PG_HOST", "host.docker.internal"),
        port=int(os.getenv("PG_PORT", "5432")),
        schema=os.getenv("PG_DATABASE", "erp_dwh"),
        login=os.getenv("PG_USER", "dwh_admin"),
        password=os.getenv("PG_PASSWORD", "881686"),
        description="PostgreSQL Data Warehouse",
    )

    print("\nDone! Verify at http://localhost:8080 → Admin → Connections")
    print("\n📚 Cách dùng trong DAG:")
    print("""
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from airflow.providers.mysql.hooks.mysql import MySqlHook

    # Lấy connection engine
    pg_hook    = PostgresHook(postgres_conn_id="postgres_dwh")
    mysql_hook = MySqlHook(mysql_conn_id="mysql_erp")

    pg_engine    = pg_hook.get_sqlalchemy_engine()
    mysql_engine = mysql_hook.get_sqlalchemy_engine()
    """)
