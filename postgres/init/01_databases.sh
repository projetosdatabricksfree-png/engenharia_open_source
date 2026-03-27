#!/bin/bash
# Cria os bancos de dados brasileirao e airflow
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    SELECT 'CREATE DATABASE brasileirao' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'brasileirao')\gexec
    SELECT 'CREATE DATABASE airflow'     WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec
    SELECT 'CREATE DATABASE superset'    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'superset')\gexec
EOSQL

echo "[OK] Bancos de dados criados: brasileirao, airflow"
