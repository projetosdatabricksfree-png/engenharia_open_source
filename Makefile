SHELL  := /bin/bash
NETWORK_NAME := stack-net

.PHONY: help network up down restart ps clean \
        logs-postgres logs-spark logs-airflow logs-superset \
        dbt-run dbt-test dbt-docs \
        api-up api-down api-logs logs-api \
        mobile-install mobile-start mobile-android mobile-ios

help:
	@echo ""
	@echo "  Stack: Brasileirao Previsao  (Spark + dbt + PostgreSQL + Superset + Mobile)"
	@echo ""
	@echo "  make up              Sobe toda a stack (ordem correta)"
	@echo "  make down            Para toda a stack"
	@echo "  make restart         Reinicia toda a stack"
	@echo "  make ps              Lista containers e status"
	@echo "  make clean           Remove containers, volumes e rede"
	@echo ""
	@echo "  make dbt-run         Executa todos os modelos dbt"
	@echo "  make dbt-test        Executa testes dbt"
	@echo "  make dbt-docs        Gera documentacao dbt"
	@echo ""
	@echo "  make api-up          Sobe a API REST (BrasileirãoPRO backend)"
	@echo "  make api-down        Para a API REST"
	@echo "  make api-logs        Logs da API"
	@echo ""
	@echo "  make mobile-install  Instala dependencias do app mobile"
	@echo "  make mobile-start    Inicia Expo (QR code para Android/iOS)"
	@echo "  make mobile-android  Abre no emulador Android"
	@echo "  make mobile-ios      Abre no simulador iOS"
	@echo ""
	@echo "  make logs-postgres | logs-spark | logs-airflow | logs-superset"
	@echo ""
	@echo "  Acessos:"
	@echo "    Airflow  >> http://localhost:8081  (admin/admin)"
	@echo "    Spark    >> http://localhost:9090"
	@echo "    Superset >> http://localhost:8088  (admin/admin)"
	@echo "    API REST >> http://localhost:8000  (docs: /docs)"
	@echo "    Postgres >> localhost:5433          (admin/admin)"
	@echo ""

network:
	@docker network inspect $(NETWORK_NAME) >/dev/null 2>&1 \
	  || (docker network create --driver bridge $(NETWORK_NAME) \
	      && echo "[OK] Rede '$(NETWORK_NAME)' criada.")

up: network
	@echo ""
	@echo "==> [1/4] PostgreSQL"
	@docker compose -f postgres/docker-compose.yml --env-file .env up -d
	@echo "    Aguardando PostgreSQL ficar saudavel..."
	@sleep 10

	@echo "==> [2/4] Apache Spark (master + 2 workers)"
	@docker compose -f spark/docker-compose.yml --env-file .env up -d

	@echo "==> [3/4] Apache Airflow"
	@docker compose -f airflow/docker-compose.yml --env-file .env up -d
	@echo "    Aguardando Airflow inicializar (45s)..."
	@sleep 45

	@echo "==> [4/4] Apache Superset"
	@docker compose -f superset/docker-compose.yml --env-file .env up -d

	@echo ""
	@echo "========================================================"
	@echo "  Stack ativa!"
	@echo "  Airflow  >> http://localhost:8081  (admin/admin)"
	@echo "  Spark    >> http://localhost:9090"
	@echo "  Superset >> http://localhost:8088  (admin/admin)"
	@echo "  Postgres >> localhost:5433          (admin/admin)"
	@echo "========================================================"
	@echo ""

down:
	@echo "Parando Superset..."
	@docker compose -f superset/docker-compose.yml  --env-file .env down 2>/dev/null || true
	@echo "Parando Airflow..."
	@docker compose -f airflow/docker-compose.yml   --env-file .env down 2>/dev/null || true
	@echo "Parando Spark..."
	@docker compose -f spark/docker-compose.yml     --env-file .env down 2>/dev/null || true
	@echo "Parando PostgreSQL..."
	@docker compose -f postgres/docker-compose.yml  --env-file .env down 2>/dev/null || true
	@echo "[OK] Stack parada."

restart: down up

ps:
	@docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

# ---------------------------------------------------------------
# dbt (executa via container efemero na rede stack-net)
# ---------------------------------------------------------------
dbt-run:
	@docker compose -f dbt/docker-compose.yml --env-file .env \
	  run --rm dbt dbt run --profiles-dir . --project-dir .

dbt-test:
	@docker compose -f dbt/docker-compose.yml --env-file .env \
	  run --rm dbt dbt test --profiles-dir . --project-dir .

dbt-docs:
	@docker compose -f dbt/docker-compose.yml --env-file .env \
	  run --rm dbt dbt docs generate --profiles-dir . --project-dir .

# ---------------------------------------------------------------
# Logs
# ---------------------------------------------------------------
logs-postgres:
	@docker compose -f postgres/docker-compose.yml --env-file .env logs -f

logs-spark:
	@docker compose -f spark/docker-compose.yml --env-file .env logs -f

logs-airflow:
	@docker compose -f airflow/docker-compose.yml --env-file .env logs -f

logs-superset:
	@docker compose -f superset/docker-compose.yml --env-file .env logs -f

# ---------------------------------------------------------------
# BrasileirãoPRO — REST API
# ---------------------------------------------------------------
api-up: network
	@echo "==> Subindo API BrasileirãoPRO..."
	@docker compose -f api/docker-compose.yml --env-file .env up -d --build
	@echo "    API disponivel em http://localhost:8000"
	@echo "    Swagger docs:    http://localhost:8000/docs"

api-down:
	@docker compose -f api/docker-compose.yml --env-file .env down 2>/dev/null || true

api-logs:
	@docker compose -f api/docker-compose.yml --env-file .env logs -f

logs-api: api-logs

# ---------------------------------------------------------------
# BrasileirãoPRO — Mobile App (Expo)
# ---------------------------------------------------------------
mobile-install:
	@echo "==> Instalando dependencias do app mobile..."
	@cd mobile && npm install --legacy-peer-deps
	@echo "[OK] Dependencias instaladas."

mobile-start:
	@echo "==> Iniciando Expo Dev Server..."
	@cd mobile && npx expo start

mobile-android:
	@cd mobile && npx expo start --android

mobile-ios:
	@cd mobile && npx expo start --ios

# ---------------------------------------------------------------
# Limpeza total (remove volumes!)
# ---------------------------------------------------------------
clean: down
	@echo "Removendo volumes..."
	@docker compose -f postgres/docker-compose.yml  --env-file .env down -v 2>/dev/null || true
	@docker compose -f spark/docker-compose.yml     --env-file .env down -v 2>/dev/null || true
	@docker compose -f airflow/docker-compose.yml   --env-file .env down -v 2>/dev/null || true
	@docker compose -f superset/docker-compose.yml  --env-file .env down -v 2>/dev/null || true
	@echo "Removendo rede..."
	@docker network rm $(NETWORK_NAME) 2>/dev/null || true
	@echo "[OK] Ambiente limpo."
