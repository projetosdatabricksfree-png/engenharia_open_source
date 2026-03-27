"""
Utilitarios compartilhados entre os jobs Spark.
"""
import os
import logging
from datetime import datetime
from pyspark.sql import SparkSession
import psycopg2

# Configuracao de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("brasileirao")

# Configuracoes PostgreSQL via variaveis de ambiente
PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_DB   = os.getenv("POSTGRES_DB",   "brasileirao")
PG_USER = os.getenv("POSTGRES_USER", "postgres")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")

JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
JDBC_DRIVER = "org.postgresql.Driver"
JDBC_PROPS = {
    "user":       PG_USER,
    "password":   PG_PASS,
    "driver":     JDBC_DRIVER,
    "stringtype": "unspecified",  # permite cast automatico VARCHAR -> JSONB no PostgreSQL
}


def get_spark(app_name: str) -> SparkSession:
    """Cria e retorna uma SparkSession configurada para PostgreSQL."""
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars", "/opt/spark/jars/postgresql-42.7.3.jar")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def read_jdbc(spark: SparkSession, table: str):
    """Le uma tabela do PostgreSQL via JDBC."""
    return (
        spark.read
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", table)
        .options(**JDBC_PROPS)
        .load()
    )


def write_jdbc(df, table: str, mode: str = "overwrite"):
    """Escreve um DataFrame no PostgreSQL via JDBC.

    Quando mode='overwrite', usa truncate=true para preservar views e
    foreign keys dependentes (evita DROP TABLE).
    """
    writer = (
        df.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", table)
        .options(**JDBC_PROPS)
        .mode(mode)
    )
    if mode == "overwrite":
        writer = writer.option("truncate", "true")
    writer.save()
    logger.info(f"[OK] {df.count()} registros escritos em {table}")


def log_pipeline_execution(stage: str, status: str, records: int = 0, error: str = None):
    """Registra execucao do pipeline na tabela de auditoria."""
    try:
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, dbname=PG_DB,
            user=PG_USER, password=PG_PASS
        )
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO gold.pipeline_executions
                (pipeline_name, stage, status, started_at, finished_at, records_processed, error_message)
            VALUES (%s, %s, %s, NOW(), NOW(), %s, %s)
        """, ("previsao_brasileirao", stage, status, records, error))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.warning(f"Nao foi possivel registrar execucao do pipeline: {e}")


def log_dq_check(check_name: str, tabela: str, status: str,
                  checked: int = 0, failed: int = 0, detalhe: str = ""):
    """Registra resultado de data quality check."""
    try:
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, dbname=PG_DB,
            user=PG_USER, password=PG_PASS
        )
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO gold.data_quality_checks
                (check_name, tabela, status, records_checked, records_failed, detalhe)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (check_name, tabela, status, checked, failed, detalhe))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.warning(f"Nao foi possivel registrar DQ check: {e}")
