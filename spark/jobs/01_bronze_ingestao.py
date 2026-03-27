"""
Stage: BRONZE
Ingestao de dados brutos da API Cartola FC para PostgreSQL.
Escreve nas tabelas bronze.* com payload JSON original preservado.
"""
import sys
import json
import requests
import logging
from datetime import datetime
from pyspark.sql import Row
from pyspark.sql.functions import lit, current_timestamp, to_json, struct
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, LongType
)

sys.path.insert(0, "/opt/spark-jobs")
from commons import get_spark, write_jdbc, log_pipeline_execution, log_dq_check, logger

CARTOLA_BASE_URL = "https://api.cartola.globo.com"


def fetch_json(endpoint: str):
    url = f"{CARTOLA_BASE_URL}/{endpoint}"
    logger.info(f"Chamando API: {url}")
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()


def ingest_partidas(spark):
    logger.info("Ingestao de partidas...")
    data = fetch_json("partidas")
    partidas = data.get("partidas", [])
    if not partidas:
        logger.warning("Nenhuma partida retornada pela API.")
        return 0

    schema = StructType([
        StructField("rodada",        IntegerType(), True),
        StructField("clube_casa_id", IntegerType(), True),
        StructField("clube_vis_id",  IntegerType(), True),
        StructField("placar_casa",   IntegerType(), True),
        StructField("placar_vis",    IntegerType(), True),
        StructField("data_partida",  StringType(),  True),   # cast no write_jdbc
        StructField("status",        StringType(),  True),
        StructField("raw_payload",   StringType(),  True),
    ])

    rows = []
    for p in partidas:
        pc = p.get("placar_oficial_mandante")
        pv = p.get("placar_oficial_visitante")
        rows.append((
            int(p.get("rodada", 0)),
            int(p.get("clube_casa_id", 0)),
            int(p.get("clube_visitante_id", 0)),
            int(pc) if pc is not None else None,
            int(pv) if pv is not None else None,
            str(p.get("partida_data", "") or ""),
            str(p.get("partida_status", "") or ""),
            json.dumps(p, ensure_ascii=False),
        ))

    from pyspark.sql.functions import to_timestamp, col
    df = spark.createDataFrame(rows, schema=schema)
    df = df.withColumn("data_partida", to_timestamp(col("data_partida"), "yyyy-MM-dd HH:mm:ss"))
    write_jdbc(df, "bronze.partidas_raw", mode="append")
    return len(rows)


def ingest_clubes(spark):
    logger.info("Ingestao de clubes...")
    data = fetch_json("clubes")
    rows = []
    for clube_id, clube in data.items():
        rows.append(Row(
            clube_id=int(clube_id),
            nome=str(clube.get("nome", "")),
            abreviacao=str(clube.get("abreviacao", "")),
            escudo_url=str(clube.get("escudos", {}).get("60x60", "")),
            raw_payload=json.dumps(clube, ensure_ascii=False),
        ))

    df = spark.createDataFrame(rows)
    write_jdbc(df, "bronze.clubes_info_raw", mode="overwrite")
    return len(rows)


def ingest_atletas(spark):
    logger.info("Ingestao de atletas (status)...")
    data = fetch_json("atletas/mercado")
    atletas = data.get("atletas", [])
    rows = []
    for a in atletas:
        rows.append(Row(
            atleta_id=int(a.get("atleta_id", 0)),
            nome=str(a.get("apelido", "")),
            clube_id=int(a.get("clube_id", 0)),
            posicao_id=int(a.get("posicao_id", 0)),
            status_id=int(a.get("status_id", 0)),
            raw_payload=json.dumps(a, ensure_ascii=False),
        ))

    df = spark.createDataFrame(rows)
    write_jdbc(df, "bronze.jogadores_status_raw", mode="overwrite")
    return len(rows)


def ingest_pontuacoes(spark, rodada: int):
    logger.info(f"Ingestao de pontuacoes da rodada {rodada}...")
    try:
        data = fetch_json(f"atletas/pontuados?rodada={rodada}")
    except Exception:
        logger.warning(f"Sem pontuacoes para rodada {rodada}.")
        return 0
    if not isinstance(data, dict):
        logger.warning(f"Resposta invalida para pontuacoes rodada {rodada}.")
        return 0
    atletas = data.get("atletas", {})
    rows = []
    for atleta_id, a in atletas.items():
        rows.append(Row(
            rodada=rodada,
            atleta_id=int(atleta_id),
            clube_id=int(a.get("clube_id", 0)),
            pontos=float(a.get("pontuacao", 0.0)),
            raw_payload=json.dumps(a, ensure_ascii=False),
        ))

    if rows:
        df = spark.createDataFrame(rows)
        write_jdbc(df, "bronze.pontuacoes_historico_raw", mode="append")
    return len(rows)


def main():
    spark = get_spark("01_bronze_ingestao")
    total_records = 0
    stage = "bronze"

    try:
        # 1. Partidas
        n = ingest_partidas(spark)
        total_records += n
        log_dq_check("bronze_partidas_count", "bronze.partidas_raw",
                     "success" if n > 0 else "warning", checked=n)

        # 2. Clubes
        n = ingest_clubes(spark)
        total_records += n
        log_dq_check("bronze_clubes_count", "bronze.clubes_info_raw",
                     "success" if n > 0 else "warning", checked=n)

        # 3. Atletas
        n = ingest_atletas(spark)
        total_records += n
        log_dq_check("bronze_atletas_count", "bronze.jogadores_status_raw",
                     "success" if n > 0 else "warning", checked=n)

        # 4. Pontuacoes da rodada atual
        status_data = fetch_json("mercado/status")
        rodada_atual = status_data.get("rodada", {}).get("rodada_atual", 1)
        n = ingest_pontuacoes(spark, rodada_atual)
        total_records += n

        log_pipeline_execution(stage, "success", records=total_records)
        logger.info(f"[BRONZE] Concluido. {total_records} registros ingeridos.")

    except Exception as e:
        logger.error(f"[BRONZE] Falha: {e}", exc_info=True)
        log_pipeline_execution(stage, "failed", error=str(e))
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
