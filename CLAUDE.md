# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A 100% open-source data stack for predicting Campeonato Brasileiro Série A (Brazilian Championship) match results. Data is ingested from the Cartola FC API, processed through a medallion architecture, and surfaced via ML predictions and dashboards.

**Stack:** Apache Spark 3.5 · dbt 1.8 · Apache Airflow 2.9 · Apache Superset 3.1 · PostgreSQL 15 · scikit-learn

## Common Commands

All services are orchestrated via `make`. Run `make help` to see all targets.

```bash
make up          # Start entire stack (postgres → spark → airflow → superset)
make down        # Stop all services
make restart     # Restart entire stack
make ps          # Show container status
make clean       # Remove all containers, volumes, and network

make dbt-run     # Run all dbt transformations
make dbt-test    # Run dbt tests
make dbt-docs    # Generate dbt documentation

make logs-postgres
make logs-spark
make logs-airflow
make logs-superset
```

**Service URLs:**
| Service    | URL                      | Credentials  |
|------------|--------------------------|--------------|
| Airflow    | http://localhost:8080    | admin/admin  |
| Spark UI   | http://localhost:9090    | (read-only)  |
| Superset   | http://localhost:8088    | admin/admin  |
| PostgreSQL | localhost:5433           | admin/admin  |

## Architecture

### Medallion Layers (PostgreSQL `brasileirao` database)

| Schema   | Owner     | Purpose                                                              |
|----------|-----------|----------------------------------------------------------------------|
| `bronze` | Spark     | Raw API payloads, append-only                                        |
| `silver` | dbt       | Cleaned, deduplicated, normalized data                               |
| `gold`   | dbt       | Feature engineering: ELO ratings, 5-match moving averages, momentum |
| `diamond`| Spark+dbt | ML models, predictions, BI marts                                     |

### Pipeline Flow

```
Cartola FC API
    → Spark: 01_bronze_ingestao.py  (raw ingestion → bronze.*)
    → dbt silver models             (normalize → silver.*)
    → dbt gold models               (features → gold.feat_store_enhanced)
    → Spark: 04_diamond_training.py (RandomForest + GradientBoosting → diamond.modelos_registry)
    → Spark: 05_diamond_inference.py (predictions → diamond.previsoes_proximas)
    → dbt diamond marts             (BI views → diamond.mart_*)
    → Superset dashboards
```

### Airflow DAG

`previsao_brasileirao_spark` — runs daily at 18:08 BRT. Six sequential `BashOperator` tasks, each using `docker exec` to run `spark-submit` inside the Spark master container. Retries: 2, backoff: 10 min.

### Shared Spark Utilities (`spark/jobs/commons.py`)

Contains: JDBC connection helpers, logging setup, and data quality (DQ) check utilities. All Spark jobs import from here. DQ results are written to `diamond.data_quality_checks`; pipeline audit logs go to `diamond.pipeline_executions`.

### Docker Network

All services share the `stack-net` Docker network. Each component has its own `docker-compose.yml`; the root `Makefile` starts them in dependency order. Spark cluster: 1 master + 2 workers (2 GB RAM each). Airflow uses `LocalExecutor`.

## Key Files

- `spark/jobs/commons.py` — shared JDBC/logging/DQ utilities for all Spark jobs
- `dbt/models/gold/feat_store_enhanced.sql` — core feature engineering (ELO, moving averages, momentum)
- `airflow/dags/previsao_brasileirao_spark_dag.py` — full pipeline DAG definition
- `postgres/init/02_schemas.sql` — schema/table definitions for all four medallion layers
- `.env` — all credentials and service configuration (loaded by docker-compose files)
