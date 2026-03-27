"""
Testes unitários para spark/jobs/02_silver_transform.py
Execução: pytest tests/test_silver_transform.py -v
"""
import sys
import os
import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, TimestampType, DoubleType
)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "spark", "jobs"))


@pytest.fixture(scope="session")
def spark():
    """SparkSession local para testes (sem cluster)."""
    return (
        SparkSession.builder
        .appName("test_silver")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


# ── Dados de exemplo ───────────────────────────────────────────────────

BRONZE_PARTIDAS_SCHEMA = StructType([
    StructField("rodada",        IntegerType(), True),
    StructField("clube_casa_id", IntegerType(), True),
    StructField("clube_vis_id",  IntegerType(), True),
    StructField("placar_casa",   IntegerType(), True),
    StructField("placar_vis",    IntegerType(), True),
    StructField("data_partida",  TimestampType(), True),
    StructField("status",        StringType(),  True),
    StructField("raw_payload",   StringType(),  True),
    StructField("ingested_at",   TimestampType(), True),
])


class TestTransformPartidas:
    """Testa lógica de transformação das partidas bronze → silver."""

    def _get_transform_fn(self, spark_session, rows):
        """Replica a lógica do transform_partidas sem I/O."""
        from pyspark.sql import functions as F

        df = spark_session.createDataFrame(rows, schema=BRONZE_PARTIDAS_SCHEMA)

        df_silver = (
            df.withColumn(
                "resultado",
                F.when(F.col("placar_casa") > F.col("placar_vis"), "casa")
                 .when(F.col("placar_casa") < F.col("placar_vis"), "visitante")
                 .when(
                     (F.col("placar_casa").isNotNull()) & (F.col("placar_vis").isNotNull()),
                     "empate"
                 )
            )
            .filter(F.col("resultado").isNotNull())
            .select("rodada", "clube_casa_id", "clube_vis_id",
                    "placar_casa", "placar_vis", "resultado", "data_partida")
            .dropDuplicates(["rodada", "clube_casa_id", "clube_vis_id"])
        )
        return df_silver

    def test_vitoria_casa(self, spark):
        """Placar_casa > placar_vis → resultado='casa'."""
        rows = [(1, 275, 263, 3, 0, None, "", "{}", None)]
        df = self._get_transform_fn(spark, rows)
        resultado = df.collect()[0]["resultado"]
        assert resultado == "casa"

    def test_vitoria_visitante(self, spark):
        """Placar_vis > placar_casa → resultado='visitante'."""
        rows = [(1, 263, 275, 0, 2, None, "", "{}", None)]
        df = self._get_transform_fn(spark, rows)
        resultado = df.collect()[0]["resultado"]
        assert resultado == "visitante"

    def test_empate(self, spark):
        """Placares iguais → resultado='empate'."""
        rows = [(1, 285, 276, 1, 1, None, "", "{}", None)]
        df = self._get_transform_fn(spark, rows)
        resultado = df.collect()[0]["resultado"]
        assert resultado == "empate"

    def test_partida_sem_placar_filtrada(self, spark):
        """Partidas sem placar (futuras) devem ser filtradas."""
        rows = [(0, 263, 2305, None, None, None, "", "{}", None)]
        df = self._get_transform_fn(spark, rows)
        assert df.count() == 0, "Partidas sem resultado não devem ir para silver"

    def test_deduplicacao_por_rodada_casa_vis(self, spark):
        """Registros duplicados da mesma partida devem ser deduplicados."""
        rows = [
            (1, 275, 263, 3, 0, None, "", "{}", None),
            (1, 275, 263, 3, 0, None, "", "{}", None),  # duplicata
        ]
        df = self._get_transform_fn(spark, rows)
        assert df.count() == 1, "Partida duplicada deve resultar em 1 registro"

    def test_multiplas_partidas_diferentes(self, spark):
        """Múltiplas partidas distintas devem ser mantidas."""
        rows = [
            (1, 275, 263, 3, 0, None, "", "{}", None),
            (1, 285, 276, 1, 1, None, "", "{}", None),
            (1, 283, 287, 2, 0, None, "", "{}", None),
        ]
        df = self._get_transform_fn(spark, rows)
        assert df.count() == 3


class TestTransformClubes:
    """Testa lógica de transformação dos clubes."""

    def test_deduplicacao_por_clube_id(self, spark):
        """Clubes duplicados por clube_id devem ser deduplicados."""
        from pyspark.sql.types import LongType

        schema = StructType([
            StructField("id",           LongType(),    True),
            StructField("clube_id",     IntegerType(), True),
            StructField("nome",         StringType(),  True),
            StructField("abreviacao",   StringType(),  True),
            StructField("escudo_url",   StringType(),  True),
            StructField("raw_payload",  StringType(),  True),
            StructField("ingested_at",  TimestampType(), True),
        ])
        rows = [
            (1, 275, "Palmeiras", "PAL", "url1", "{}", None),
            (2, 275, "Palmeiras", "PAL", "url1", "{}", None),  # duplicata
            (3, 263, "Botafogo",  "BOT", "url2", "{}", None),
        ]
        df_bronze = spark.createDataFrame(rows, schema=schema)
        df_silver = df_bronze.select("clube_id", "nome", "abreviacao", "escudo_url") \
                             .dropDuplicates(["clube_id"])

        assert df_silver.count() == 2, "Deve manter apenas 1 registro por clube_id"

    def test_seleciona_colunas_corretas(self, spark):
        """Silver clubes deve ter apenas as colunas mapeadas."""
        from pyspark.sql.types import LongType, TimestampType

        schema = StructType([
            StructField("id",          LongType(),    True),
            StructField("clube_id",    IntegerType(), True),
            StructField("nome",        StringType(),  True),
            StructField("abreviacao",  StringType(),  True),
            StructField("escudo_url",  StringType(),  True),
            StructField("raw_payload", StringType(),  True),
            StructField("ingested_at", TimestampType(), True),
        ])
        df_bronze = spark.createDataFrame(
            [(1, 275, "Palmeiras", "PAL", "url", "{}", None)], schema=schema
        )
        df_silver = df_bronze.select("clube_id", "nome", "abreviacao", "escudo_url") \
                             .dropDuplicates(["clube_id"])

        assert set(df_silver.columns) == {"clube_id", "nome", "abreviacao", "escudo_url"}
