"""
DAG: previsao_brasileirao_spark
Pipeline completo: Spark (via docker exec) + dbt + PostgreSQL.

Fluxo:
  [Spark] bronze_ingestao
       >> [dbt]  silver + gold
       >> [Spark] diamond_training
       >> [Spark] diamond_inference
       >> [dbt]  diamond marts
"""

import os
import pendulum
from datetime import timedelta
from airflow.decorators import dag
from airflow.operators.bash import BashOperator

SPARK_CONTAINER = "spark-master"
SPARK_SUBMIT    = "/opt/spark/bin/spark-submit"
SPARK_MASTER    = "spark://spark-master:7077"
JOBS_DIR        = "/opt/spark-jobs"
JDBC_JAR        = "/opt/spark/jars/postgresql-42.7.3.jar"
DBT_DIR         = "/opt/dbt"

PG_HOST = os.getenv("POSTGRES_HOST",     "postgres")
PG_PORT = os.getenv("POSTGRES_PORT",     "5433")
PG_DB   = os.getenv("POSTGRES_DB",       "brasileirao")
PG_USER = os.getenv("POSTGRES_USER",     "admin")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "admin")

PG_ENV_INLINE = (
    f"POSTGRES_HOST={PG_HOST} "
    f"POSTGRES_PORT=5432 "        # porta interna do container
    f"POSTGRES_DB={PG_DB} "
    f"POSTGRES_USER={PG_USER} "
    f"POSTGRES_PASSWORD={PG_PASS}"
)

def spark_submit_cmd(job_file: str, app_name: str) -> str:
    """Monta comando docker exec spark-submit."""
    return (
        f"docker exec {SPARK_CONTAINER} "
        f"env {PG_ENV_INLINE} "
        f"{SPARK_SUBMIT} "
        f"--master {SPARK_MASTER} "
        f"--deploy-mode client "
        f"--jars {JDBC_JAR} "
        f"--driver-memory 1g "
        f"--executor-memory 2g "
        f"--name {app_name} "
        f"{JOBS_DIR}/{job_file}"
    )

def dbt_run_cmd(select: str) -> str:
    return (
        f"docker exec -e POSTGRES_HOST={PG_HOST} "
        f"-e POSTGRES_PORT=5432 "
        f"-e POSTGRES_DB={PG_DB} "
        f"-e POSTGRES_USER={PG_USER} "
        f"-e POSTGRES_PASSWORD={PG_PASS} "
        f"dbt dbt run --select {select} "
        f"--profiles-dir . --project-dir . || true"
    )

default_args = {
    "owner":            "engenharia_dados",
    "retries":          2,
    "retry_delay":      timedelta(minutes=10),
    "email_on_failure": False,
    "depends_on_past":  False,
}


@dag(
    dag_id="previsao_brasileirao_spark",
    default_args=default_args,
    description="Pipeline Spark + dbt + PostgreSQL para previsao do Brasileirao",
    schedule="8 18 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    tags=["spark", "dbt", "postgres", "futebol", "ml"],
)
def previsao_brasileirao_spark_dag():

    bronze = BashOperator(
        task_id="01_bronze_ingestao",
        bash_command=spark_submit_cmd("01_bronze_ingestao.py", "bronze_ingestao"),
    )

    silver = BashOperator(
        task_id="02_silver_transform",
        bash_command=dbt_run_cmd("silver"),
    )

    gold = BashOperator(
        task_id="03_gold_features",
        bash_command=dbt_run_cmd("gold"),
    )

    training = BashOperator(
        task_id="04_diamond_training",
        bash_command=spark_submit_cmd("04_diamond_training.py", "diamond_training"),
    )

    inference = BashOperator(
        task_id="05_diamond_inference",
        bash_command=spark_submit_cmd("05_diamond_inference.py", "diamond_inference"),
    )

    marts = BashOperator(
        task_id="06_diamond_marts",
        bash_command=dbt_run_cmd("diamond"),
    )

    bronze >> silver >> gold >> training >> inference >> marts


previsao_brasileirao_spark_dag()
