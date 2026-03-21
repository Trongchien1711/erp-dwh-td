# Airflow — Hướng dẫn cài đặt và học

## Cấu trúc thư mục

```
airflow/
├── docker-compose.yml          # Airflow 2.9 + PostgreSQL metadata DB
├── dags/
│   ├── erp_dwh_dag.py          # DAG chính (có chú thích học)
│   └── init_connections.py     # Script tạo Airflow connections
└── README.md                   # File này
```

---

## Bước 1 — Cài Docker Desktop

Tải và cài: https://www.docker.com/products/docker-desktop/

Sau khi cài xong, mở Docker Desktop và chờ nó start.

---

## Bước 2 — Khởi tạo Airflow (chạy 1 lần)

```powershell
cd "d:\Data Warehouse\airflow"

# Tạo thư mục logs và plugins (mount vào container)
New-Item -ItemType Directory -Force logs, plugins | Out-Null

# Khởi tạo database + tạo user admin/admin
docker compose up airflow-init
# Chờ đến khi thấy: "Airflow initialized — open http://localhost:8080"
# Nhấn Ctrl+C để thoát
```

---

## Bước 3 — Chạy Airflow

```powershell
# Chạy nền (scheduler + webserver)
docker compose up -d

# Kiểm tra trạng thái
docker compose ps
```

Mở trình duyệt: **http://localhost:8080**  
Login: `admin` / `admin`

---

## Bước 4 — Tạo Connections

```powershell
# Chạy script tạo MySQL + PostgreSQL connections
docker compose exec airflow-scheduler python /opt/airflow/dags/init_connections.py
```

Kiểm tra trong UI: **Admin → Connections** → thấy `mysql_erp` và `postgres_dwh`

---

## Bước 5 — Kích hoạt DAG

1. Trong UI, tìm DAG `erp_dwh_daily`
2. Toggle sang **ON** (ở cột bên trái)
3. Nhấn nút ▶ (Trigger DAG) để chạy thử ngay

---

## Dừng Airflow

```powershell
cd "d:\Data Warehouse\airflow"
docker compose down
```

---

## Xem logs

```powershell
# Logs của scheduler (xem DAG scan, errors)
docker compose logs -f airflow-scheduler

# Logs của 1 task cụ thể → dùng UI: DAG → Run → Task → Logs
```

---

## Các khái niệm Airflow được minh họa trong `erp_dwh_dag.py`

| Khái niệm | Ở đâu trong file | Mô tả |
|---|---|---|
| **DAG** | `with DAG(...)` | Container chứa tất cả tasks + schedule |
| **schedule_interval** | `"0 1 * * *"` | Cron: chạy lúc 01:00 UTC mỗi ngày |
| **default_args** | `default_args = {...}` | Áp dụng cho tất cả task (retries, owner...) |
| **@task** | `@task def health_check()` | Taskflow API — cách viết ngắn gọn nhất |
| **@task_group** | `@task_group def extract_load_group()` | Nhóm task → UI gọn hơn |
| **BashOperator** | `dbt_run = BashOperator(...)` | Chạy shell command (dbt) |
| **>> dependency** | `_extract >> _transform` | Set thứ tự chạy |
| **XCom** | `return {...}` trong `health_check` | Truyền data giữa tasks |
| **{{ ds }}** | bash_command dbt | Execution date template |
| **on_failure_callback** | `def on_task_failure(context)` | Alert khi task fail |
| **context** | `def health_check(**context)` | Metadata của run hiện tại |

---

## Graph của DAG

```
extract_load/
├── extract_sales_tables   ─┐
├── extract_inventory_tables─┤ (chạy song song)
├── extract_production_tables┤
└── extract_master_tables  ─┘
          │
          ▼
     transform/
     ├── upsert_dimensions
     │         │
     └── insert_facts
          │
          ▼
        dbt/
        ├── dbt_run
        │      │
        └── dbt_test
          │
          ▼
     health_check
```

---

## Troubleshooting

**DAG không hiện trong UI:**
- Chờ scheduler scan (mặc định 30s)
- Kiểm tra lỗi syntax: `docker compose exec airflow-scheduler python -c "import dags.erp_dwh_dag"`

**Task fail "Module not found":**
- Kiểm tra volume mount trong docker-compose.yml: `d:/Data Warehouse:/opt/dwh:ro`
- Thử: `docker compose exec airflow-scheduler python -c "import sys; sys.path.insert(0,'/opt/dwh/elt'); from connections import get_pg_engine"`

**Không kết nối được MySQL/PostgreSQL:**
- Trên Windows/Mac, dùng `host.docker.internal` thay cho `localhost`
- Kiểm tra firewall: PostgreSQL phải cho phép kết nối từ `127.0.0.1`

**Reset hoàn toàn:**
```powershell
docker compose down -v   # xoá cả volumes (mất metadata Airflow)
docker compose up airflow-init
docker compose up -d
```
