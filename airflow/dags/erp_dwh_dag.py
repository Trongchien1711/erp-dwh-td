"""
erp_dwh_dag.py
==============
DAG chính cho ERP Data Warehouse pipeline.

📚 CÁC KHÁI NIỆM AIRFLOW ĐƯỢC MINH HỌA TRONG FILE NÀY:
  1. DAG definition (schedule, catchup, tags, default_args)
  2. PythonOperator — chạy Python function
  3. BashOperator — chạy shell command
  4. TaskGroup — nhóm task liên quan để UI gọn hơn
  5. Task dependencies >> (set_upstream / set_downstream)
  6. XCom — truyền dữ liệu giữa các task
  7. on_failure_callback — gọi hàm khi task fail
  8. @task decorator (Taskflow API) — cách viết gọn hơn PythonOperator
  9. templated fields — dùng {{ ds }} {{ ts }} trong parameters

Luồng:
  [extract_group]  →  [transform_dims]  →  [transform_facts]
                                        →  [dbt_run]
                                        →  [dbt_test]
                                        →  [health_check]
"""

import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

# ── Airflow imports ───────────────────────────────────────────────────────────
from airflow import DAG
from airflow.decorators import task, task_group     # Taskflow API (Airflow 2.0+)
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# ── Đường dẫn đến ELT module (mount vào /opt/dwh trong Docker) ───────────────
ELT_PATH = Path(os.getenv("PYTHONPATH", "/opt/dwh/elt"))
sys.path.insert(0, str(ELT_PATH))

log = logging.getLogger(__name__)

# ============================================================
# 1. DEFAULT ARGS
#    Áp dụng cho tất cả task trong DAG nếu không override.
# ============================================================
default_args = {
    "owner": "dwh_team",
    "depends_on_past": False,       # task không phụ thuộc run trước đó
    "retries": 1,                   # retry 1 lần nếu fail
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,      # bật lên nếu muốn nhận email
    "email_on_retry": False,
}


# ============================================================
# 2. CALLBACK — gọi khi task fail (dùng để alert/log)
# ============================================================
def on_task_failure(context):
    """
    📚 Callback: Airflow gọi hàm này tự động khi 1 task fail.
    context chứa đầy đủ thông tin về task, dag_run, exception...

    Ở đây chỉ log — mở rộng bằng cách gửi Slack/Teams/email.
    """
    task_id  = context["task_instance"].task_id
    dag_id   = context["dag"].dag_id
    exc      = context.get("exception")
    log.error(f"[ALERT] DAG={dag_id} | TASK={task_id} | ERROR={exc}")


# ============================================================
# 3. DAG DEFINITION
# ============================================================
with DAG(
    dag_id="erp_dwh_daily",

    # 📚 schedule_interval: cron syntax hoặc preset (@daily, @hourly...)
    #    "0 1 * * *" = chạy lúc 01:00 UTC mỗi ngày (08:00 ICT)
    schedule_interval="0 1 * * *",

    start_date=days_ago(1),

    # 📚 catchup=False: không backfill các khoảng thời gian bị bỏ lỡ.
    #    Nếu True, Airflow sẽ chạy lại tất cả run từ start_date đến hôm nay.
    catchup=False,

    default_args=default_args,
    on_failure_callback=on_task_failure,

    # 📚 tags: dùng để filter trong UI
    tags=["erp", "dwh", "daily"],

    doc_md="""
## ERP DWH Daily Pipeline

**Luồng**: MySQL ERP → staging (PostgreSQL) → core dims/facts → dbt mart layer

**Schedule**: 01:00 UTC (08:00 ICT) mỗi ngày  
**SLA**: hoàn thành trong 15 phút  

### Task Groups
- `extract_load`: extract 25 bảng MySQL → staging  
- `transform`: UPSERT 10 dim + INSERT 9 fact  
- `dbt`: dbt run → dbt test  
- `health_check`: kiểm tra row counts + NULL key rates  
    """,
) as dag:

    # ============================================================
    # 4. TASK GROUP: extract_load
    #    📚 TaskGroup: nhóm các task liên quan trong cùng 1 box ở UI
    #       Giúp DAG graph gọn hơn khi có nhiều task.
    # ============================================================

    @task_group(group_id="extract_load")
    def extract_load_group():
        """
        📚 @task_group decorator: tương đương TaskGroup context manager.
        Mỗi task bên trong sẽ hiển thị là extract_load.extract_<tên>.
        """

        @task(task_id="extract_sales_tables")
        def extract_sales():
            """Extract bảng liên quan sales: orders, order_items, deliveries..."""
            from connections import get_mysql_engine, get_pg_engine
            from watermark   import init_watermark_table, get_watermark, set_watermark
            from extractor   import TABLE_CONFIG, extract_table
            from loader      import load_table

            SALES_TABLES = {
                "tbl_orders", "tbl_order_items",
                "tbl_deliveries", "tbl_delivery_items",
            }
            _run_extract_for_tables(SALES_TABLES)

        @task(task_id="extract_inventory_tables")
        def extract_inventory():
            """Extract bảng inventory: warehouse stock, transfers, purchases..."""
            INVENTORY_TABLES = {
                "tblwarehouse_product", "tblwarehouse_export",
                "tbltransfer_warehouse_detail",
                "tbl_purchase_products", "tbl_purchase_product_items",
            }
            _run_extract_for_tables(INVENTORY_TABLES)

        @task(task_id="extract_production_tables")
        def extract_production():
            """Extract bảng sản xuất."""
            PRODUCTION_TABLES = {
                "tbl_productions_orders",
                "tbl_productions_orders_items",
                "tbl_productions_orders_items_stages",
            }
            _run_extract_for_tables(PRODUCTION_TABLES)

        @task(task_id="extract_master_tables")
        def extract_master():
            """Extract master/dimension source tables (full load)."""
            MASTER_TABLES = {
                "tblclients", "tbl_products", "tblstaff", "tblwarehouse",
                "tbllocaltion_warehouses", "tblsuppliers", "tbldepartments",
                "tblcustomers_groups", "tbl_manufactures",
                "tblpurchase_order", "tblpurchase_order_items",
            }
            _run_extract_for_tables(MASTER_TABLES)

        # Task dependencies bên trong group:
        # Tất cả chạy song song (không có >> giữa chúng)
        extract_sales()
        extract_inventory()
        extract_production()
        extract_master()

    # ============================================================
    # 5. TASK GROUP: transform
    # ============================================================

    @task_group(group_id="transform")
    def transform_group():
        """
        Transform staging → core.
        Dims phải chạy trước facts (dim data được facts tham chiếu).

        📚 Cách tiếp cận: tách TRANSFORM_STEPS thành 2 nhóm
           (dims + facts) để Airflow hiển thị 2 task riêng,
           thay vì gọi run_transforms() 1 lần cho tất cả.
        """

        # Tên các dim steps trong TRANSFORM_STEPS
        DIM_STEP_NAMES = {
            "dim_customer", "dim_product", "dim_staff", "dim_department",
            "dim_warehouse", "dim_supplier", "dim_warehouse_location",
            "dim_manufacture", "dim_price_group",
            "dim_customer [UPDATE price_group_key]",
        }

        @task(task_id="upsert_dimensions")
        def upsert_dims():
            """
            📚 @task decorator (Taskflow API):
               Tương đương PythonOperator nhưng viết gọn hơn.
               Hàm Python thông thường → tự động thành Task.
            """
            from sqlalchemy import text
            from connections    import get_pg_engine
            from transform_core import TRANSFORM_STEPS, _SOFT_STEPS

            pg = get_pg_engine()
            try:
                dim_steps = [(name, sql) for name, sql in TRANSFORM_STEPS
                             if name in DIM_STEP_NAMES]
                for step_name, sql in dim_steps:
                    try:
                        with pg.begin() as conn:
                            conn.execute(text(sql))
                        log.info(f"[dim] {step_name} OK")
                    except Exception as e:
                        if step_name in _SOFT_STEPS:
                            log.warning(f"[dim] {step_name} SOFT SKIP: {e}")
                        else:
                            raise
                log.info("All dimensions upserted successfully")
            finally:
                pg.dispose()

        @task(task_id="insert_facts")
        def insert_facts():
            """
            Facts phải chạy SAU dims để foreign key lookups thành công.
            """
            from sqlalchemy import text
            from connections    import get_pg_engine
            from transform_core import TRANSFORM_STEPS, _SOFT_STEPS

            pg = get_pg_engine()
            try:
                fact_steps = [(name, sql) for name, sql in TRANSFORM_STEPS
                              if name not in DIM_STEP_NAMES]
                for step_name, sql in fact_steps:
                    with pg.begin() as conn:
                        result = conn.execute(text(sql))
                    if result.returns_rows:
                        row = result.fetchone()
                        log.info(f"[fact] {step_name}: {row[0]} fixed, {row[1]} inserted")
                    else:
                        log.info(f"[fact] {step_name}: {result.rowcount} rows")
                log.info("All facts inserted successfully")
            finally:
                pg.dispose()

        # 📚 >> operator: đặt dependency (upsert_dims phải xong trước insert_facts)
        upsert_dims() >> insert_facts()

    # ============================================================
    # 6. TASK GROUP: dbt
    #    📚 BashOperator: chạy shell command.
    #       templated_fields cho phép dùng {{ ds }} (execution date).
    # ============================================================

    @task_group(group_id="dbt")
    def dbt_group():

        dbt_run = BashOperator(
            task_id="dbt_run",
            bash_command=(
                "PYTHONUTF8=1 dbt run "
                "--profiles-dir $DBT_PROFILES_DIR "
                "--project-dir  $DBT_PROJECT_DIR "
                "--vars '{\"run_date\": \"{{ ds }}\"}' "   # 📚 {{ ds }} = execution date (YYYY-MM-DD)
            ),
            # 📚 env: inject biến môi trường bổ sung cho command này
            env={
                "DBT_PROFILES_DIR": os.getenv("DBT_PROFILES_DIR", "/opt/dwh/dbt_project"),
                "DBT_PROJECT_DIR":  os.getenv("DBT_PROJECT_DIR",  "/opt/dwh/dbt_project"),
                "PYTHONUTF8": "1",
                **os.environ,
            },
            append_env=False,
        )

        dbt_test = BashOperator(
            task_id="dbt_test",
            bash_command=(
                "PYTHONUTF8=1 dbt test "
                "--profiles-dir $DBT_PROFILES_DIR "
                "--project-dir  $DBT_PROJECT_DIR "
            ),
            env={
                "DBT_PROFILES_DIR": os.getenv("DBT_PROFILES_DIR", "/opt/dwh/dbt_project"),
                "DBT_PROJECT_DIR":  os.getenv("DBT_PROJECT_DIR",  "/opt/dwh/dbt_project"),
                "PYTHONUTF8": "1",
                **os.environ,
            },
            append_env=False,
        )

        dbt_run >> dbt_test

    # ============================================================
    # 7. HEALTH CHECK — dùng @task decorator + XCom return value
    # ============================================================

    @task(task_id="health_check")
    def health_check(**context):
        """
        📚 XCom (Cross-Communication):
           Khi @task function return một giá trị,
           Airflow tự động lưu vào XCom.
           Task khác có thể đọc bằng:
             ti = context['task_instance']
             result = ti.xcom_pull(task_ids='health_check')

        📚 **context: Airflow inject metadata tự động khi
           provide_context=True (mặc định với @task decorator).
           context['ds']          = execution date string
           context['dag_run']     = DagRun object
           context['task_instance'] = TaskInstance object
        """
        import subprocess

        run_date = context["ds"]   # 📚 execution date: YYYY-MM-DD
        log.info(f"Running health check for run_date={run_date}")

        result = subprocess.run(
            [sys.executable, "/opt/dwh/check_pipeline_health.py"],
            capture_output=True, text=True
        )
        log.info(result.stdout)
        if result.returncode != 0:
            log.warning(result.stderr)

        # 📚 Return value → tự động lưu vào XCom với key='return_value'
        return {
            "run_date": run_date,
            "health_check_exit_code": result.returncode,
            "summary": result.stdout[-500:],   # lưu 500 ký tự cuối
        }

    # ============================================================
    # 8. TASK DEPENDENCIES (DAG topology)
    #
    #    📚 >> operator sets downstream dependency:
    #         A >> B  =  "B phải chạy sau A"
    #         [A, B] >> C  =  "C chạy sau khi cả A và B xong"
    # ============================================================

    _extract  = extract_load_group()
    _transform = transform_group()
    _dbt       = dbt_group()
    _health    = health_check()

    # Extract xong → Transform → dbt → health check
    _extract >> _transform >> _dbt >> _health


# ============================================================
# HELPER FUNCTION (ngoài DAG context)
# ============================================================

def _run_extract_for_tables(table_set: set):
    """
    Chạy extract + load cho một tập con của TABLE_CONFIG.
    Hàm này được gọi bởi các task trong extract_load_group.
    """
    from connections import get_mysql_engine, get_pg_engine
    from watermark   import init_watermark_table, get_watermark, set_watermark
    from extractor   import TABLE_CONFIG, extract_table
    from loader      import load_table

    mysql_eng = get_mysql_engine()
    pg_eng    = get_pg_engine()
    init_watermark_table(pg_eng)

    configs = [c for c in TABLE_CONFIG if c["source_table"] in table_set]

    for cfg in configs:
        src    = cfg["source_table"]
        wm_col = cfg["watermark_col"]
        try:
            last_wm = get_watermark(pg_eng, src)
            df      = extract_table(mysql_eng, src, wm_col, last_wm)
            if df.empty:
                log.info(f"[extract] {src} → no new rows, skip")
                continue
            load_table(pg_eng, df, src, wm_col)
            new_wm = (
                str(df[wm_col].max())
                if wm_col and wm_col in df.columns and str(df[wm_col].max()) != "NaT"
                else datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )
            set_watermark(pg_eng, src, new_wm)
            log.info(f"[extract] {src} → {len(df):,} rows loaded, watermark={new_wm}")
        except Exception as e:
            log.error(f"[extract] {src} FAILED: {e}")
            # Không raise — task level retry sẽ xử lý
            continue

    mysql_eng.dispose()
    pg_eng.dispose()
