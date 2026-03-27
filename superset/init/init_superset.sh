#!/bin/bash
set -e

echo "[Superset] Migrando banco..."
superset db upgrade

echo "[Superset] Criando admin..."
superset fab create-admin \
    --username  "${SUPERSET_ADMIN_USER:-admin}" \
    --firstname "Admin" \
    --lastname  "Superset" \
    --email     "${SUPERSET_ADMIN_EMAIL:-admin@brasileirao.local}" \
    --password  "${SUPERSET_ADMIN_PASSWORD:-admin}" 2>/dev/null || true

echo "[Superset] Inicializando permissoes..."
superset init

echo "[Superset] Pronto."
