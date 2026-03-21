#!/usr/bin/env bash
# =============================================================
# setup_airflow_wsl.sh
# Chạy trong WSL2 (Ubuntu) để cài Airflow + kết nối project
#
# Cách dùng:
#   1. Mở WSL2 terminal (Windows Terminal → Ubuntu)
#   2. bash /mnt/d/Data\ Warehouse/airflow/setup_airflow_wsl.sh
# =============================================================

set -e

PROJECT_WIN="d:/Data Warehouse"
PROJECT_WSL="/mnt/d/Data Warehouse"
AIRFLOW_HOME="$HOME/airflow-dwh"
PYTHON="python3.11"   # Airflow tương thích tốt nhất với 3.11

echo "=============================="
echo " ERP DWH — Airflow WSL Setup"
echo "=============================="

# ── 1. Cài Python 3.11 nếu chưa có ───────────────────────────
echo ""
echo "[1] Installing Python 3.11..."
sudo apt-get update -q
sudo apt-get install -y python3.11 python3.11-venv python3.11-dev \
    gcc libpq-dev default-libmysqlclient-dev pkg-config

# ── 2. Tạo virtual environment ────────────────────────────────
echo ""
echo "[2] Creating virtual environment at ~/airflow-venv..."
python3.11 -m venv ~/airflow-venv
source ~/airflow-venv/bin/activate

# ── 3. Cài Airflow + providers ────────────────────────────────
echo ""
echo "[3] Installing Airflow 2.9 + providers (takes 2-3 min)..."
pip install --quiet --upgrade pip

AIRFLOW_VERSION=2.9.3
PYTHON_VERSION=$(python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow==${AIRFLOW_VERSION}" \
    apache-airflow-providers-postgres \
    apache-airflow-providers-mysql \
    dbt-postgres==1.9.0 \
    pymysql sqlalchemy loguru python-dotenv \
    --constraint "${CONSTRAINT_URL}"

# ── 4. Cấu hình AIRFLOW_HOME ──────────────────────────────────
echo ""
echo "[4] Configuring AIRFLOW_HOME..."
mkdir -p "$AIRFLOW_HOME/dags"
export AIRFLOW_HOME="$AIRFLOW_HOME"

# Lưu vào .bashrc để tự động load mỗi lần mở terminal
if ! grep -q "AIRFLOW_HOME" ~/.bashrc; then
cat >> ~/.bashrc << EOF

# Airflow DWH
export AIRFLOW_HOME="$HOME/airflow-dwh"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DEFAULT_TIMEZONE=Asia/Ho_Chi_Minh
export PYTHONUTF8=1
export PYTHONPATH="/mnt/d/Data Warehouse/elt"
alias activate-airflow='source ~/airflow-venv/bin/activate'
alias airflow-start='cd "/mnt/d/Data Warehouse" && airflow standalone'
EOF
fi

# ── 5. Link DAG file ──────────────────────────────────────────
echo ""
echo "[5] Linking DAG file..."
ln -sf "/mnt/d/Data Warehouse/airflow/dags/erp_dwh_dag.py" \
       "$AIRFLOW_HOME/dags/erp_dwh_dag.py"
ln -sf "/mnt/d/Data Warehouse/airflow/dags/init_connections.py" \
       "$AIRFLOW_HOME/dags/init_connections.py"

# ── 6. Load .env credentials ─────────────────────────────────
echo ""
echo "[6] Setting up environment variables from .env..."
if [ -f "/mnt/d/Data Warehouse/.env" ]; then
    # Export variables từ .env vào shell
    set -a
    source "/mnt/d/Data Warehouse/.env"
    set +a
    echo "  .env loaded"
else
    echo "  WARNING: .env not found, set MySQL/PG credentials manually"
fi

# ── 7. Khởi tạo Airflow DB ────────────────────────────────────
echo ""
echo "[7] Initializing Airflow database (SQLite for dev)..."
airflow db migrate

echo ""
echo "=============================="
echo " Setup complete!"
echo ""
echo " Để chạy Airflow:"
echo "   source ~/airflow-venv/bin/activate"
echo "   export AIRFLOW_HOME=~/airflow-dwh"
echo "   export PYTHONPATH='/mnt/d/Data Warehouse/elt'"
echo "   airflow standalone"
echo ""
echo " → Mở http://localhost:8080"
echo " → Username/password hiển thị trong terminal khi chạy standalone"
echo "=============================="
