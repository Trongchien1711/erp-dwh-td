#!/usr/bin/env bash
# .devcontainer/setup.sh
# Chạy tự động khi Codespace khởi tạo

set -e

AIRFLOW_HOME="/workspaces/erp-dwh-td/airflow_home"
WORKSPACE="/workspaces/erp-dwh-td"

echo "Installing Airflow + dependencies..."
pip install --quiet \
    "apache-airflow==2.9.3" \
    apache-airflow-providers-postgres \
    apache-airflow-providers-mysql \
    dbt-postgres==1.8.0 \
    pymysql sqlalchemy loguru python-dotenv \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt"

echo "Setting up Airflow home..."
mkdir -p "$AIRFLOW_HOME/dags"

# Link DAG files
ln -sf "$WORKSPACE/airflow/dags/erp_dwh_dag.py" "$AIRFLOW_HOME/dags/"
ln -sf "$WORKSPACE/airflow/dags/init_connections.py" "$AIRFLOW_HOME/dags/"

echo "Initializing Airflow DB..."
airflow db migrate

echo ""
echo "Setup complete!"
echo "Run: airflow standalone"
echo "Then open port 8080 in the Ports tab"
