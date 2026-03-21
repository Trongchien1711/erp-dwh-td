#!/usr/bin/env bash
# .devcontainer/setup.sh
# Chạy tự động khi Codespace khởi tạo

set -e

AIRFLOW_HOME="/workspaces/erp-dwh-td/airflow_home"
WORKSPACE="/workspaces/erp-dwh-td"
export AIRFLOW_HOME
export AIRFLOW__CORE__LOAD_EXAMPLES=False

echo "[1/4] Installing Airflow + dependencies..."
pip3 install --quiet --upgrade pip
pip3 install \
    "apache-airflow==2.9.3" \
    pymysql sqlalchemy loguru python-dotenv \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt"

# Đảm bảo airflow có trong PATH
export PATH="$HOME/.local/bin:$PATH"

echo "[2/4] Setting up Airflow home..."
mkdir -p "$AIRFLOW_HOME/dags"

# Link DAG files
ln -sf "$WORKSPACE/airflow/dags/erp_dwh_dag.py" "$AIRFLOW_HOME/dags/"
ln -sf "$WORKSPACE/airflow/dags/init_connections.py" "$AIRFLOW_HOME/dags/"

echo "[3/4] Initializing Airflow DB..."
airflow db migrate

# Persist env vars cho mọi terminal session sau này
cat >> /home/vscode/.bashrc << 'BASHRC'
export AIRFLOW_HOME="/workspaces/erp-dwh-td/airflow_home"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DEFAULT_TIMEZONE=Asia/Ho_Chi_Minh
export PYTHONUTF8=1
export PYTHONPATH="/workspaces/erp-dwh-td/elt"
export PATH="$HOME/.local/bin:$PATH"
BASHRC

echo "[4/4] Done!"
echo ""
echo "============================="
echo " Airflow ready! Run:"
echo "   airflow standalone"
echo " Then open port 8080 (Ports tab)"
echo "============================="
