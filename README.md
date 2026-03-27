# Engenharia Open Source — Previsao Brasileirao Serie A

Stack de dados 100% open source para previsao de resultados do Campeonato Brasileiro.

## Arquitetura

```
API Cartola FC
      |
[Spark 3.5]  Bronze — ingestao bruta
      |
[dbt]        Silver — limpeza e normalizacao
[dbt]        Gold   — feature engineering (ELO, medias moveis)
      |
[Spark 3.5]  Diamond ML — treino e inferencia (RandomForest)
[dbt]        Diamond Marts — views finais para BI
      |
[Superset]   Dashboards e BI profissional
[PostgreSQL] Armazenamento (medalha: bronze/silver/gold/diamond)
[Airflow]    Orquestracao (schedule 18:08 BRT diario)
```

## Servicos

| Servico     | URL                        | Credencial   |
|-------------|----------------------------|--------------|
| Airflow     | http://localhost:8081      | admin/admin  |
| Spark UI    | http://localhost:9090      | —            |
| Superset    | http://localhost:8088      | admin/admin  |
| PostgreSQL  | localhost:5433             | admin/admin  |

## Como usar

```bash
# Subir toda a stack
make up

# Parar
make down

# Executar dbt manualmente
make dbt-run
make dbt-test

# Ver logs
make logs-airflow
make logs-spark
make logs-superset
```

## Estrutura

```
engenharia_open_source/
├── .env                    # Variaveis de ambiente (nao versionar em prod)
├── Makefile                # Comandos da stack
├── postgres/               # PostgreSQL — medallion schemas
│   └── init/               # SQL de inicializacao (bronze/silver/gold/diamond)
├── spark/                  # Apache Spark standalone
│   ├── Dockerfile
│   └── jobs/               # PySpark jobs por camada
├── dbt/                    # dbt models
│   └── models/
│       ├── silver/         # Limpeza e normalizacao
│       ├── gold/           # Feature engineering
│       └── diamond/        # Marts para BI
├── airflow/                # Apache Airflow
│   └── dags/               # DAGs de orquestracao
└── superset/               # Apache Superset BI
```

## Tecnologias

- **Apache Spark 3.5** — processamento distribuido
- **dbt-postgres 1.8** — transformacoes SQL com testes e lineage
- **Apache Airflow 2.9** — orquestracao e agendamento
- **Apache Superset 3.1** — BI e dashboards
- **PostgreSQL 15** — armazenamento com medallion architecture
- **Docker / Docker Compose** — containerizacao
