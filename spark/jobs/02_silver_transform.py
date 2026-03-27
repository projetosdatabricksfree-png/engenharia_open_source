"""
Stage: SILVER
Limpa e normaliza os dados brutos da camada Bronze.
Le de bronze.* e escreve em silver.partidas, silver.clubes,
silver.estatisticas_jogador_partida.
"""
import sys
sys.path.insert(0, "/opt/spark-jobs")
from commons import get_spark, read_jdbc, write_jdbc, log_pipeline_execution, log_dq_check, logger

from pyspark.sql import functions as F


def transform_partidas(spark):
    logger.info("Transformando partidas bronze -> silver...")
    df = read_jdbc(spark, "bronze.partidas_raw")

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
        .select(
            "rodada",
            "clube_casa_id",
            "clube_vis_id",
            "placar_casa",
            "placar_vis",
            "resultado",
            "data_partida",
        )
        .dropDuplicates(["rodada", "clube_casa_id", "clube_vis_id"])
    )

    n = df_silver.count()
    if n == 0:
        logger.warning("Nenhuma partida com resultado encontrada — silver.partidas preservada.")
        return 0
    # append + dedup: não apaga dados históricos inseridos manualmente
    write_jdbc(df_silver, "silver.partidas", mode="append")
    # remove duplicatas que possam ter sido inseridas
    import psycopg2, os
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"), port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "brasileirao"),
        user=os.getenv("POSTGRES_USER", "postgres"), password=os.getenv("POSTGRES_PASSWORD", "postgres")
    )
    cur = conn.cursor()
    cur.execute("""
        DELETE FROM silver.partidas a USING silver.partidas b
        WHERE a.ctid < b.ctid
          AND a.rodada = b.rodada
          AND a.clube_casa_id = b.clube_casa_id
          AND a.clube_vis_id  = b.clube_vis_id
    """)
    conn.commit(); conn.close()
    return n


def transform_clubes(spark):
    logger.info("Transformando clubes bronze -> silver...")
    df = read_jdbc(spark, "bronze.clubes_info_raw")

    df_silver = (
        df.select("clube_id", "nome", "abreviacao", "escudo_url")
          .dropDuplicates(["clube_id"])
    )

    n = df_silver.count()
    if n == 0:
        logger.warning("Nenhum clube encontrado — silver.clubes preservada.")
        return 0
    write_jdbc(df_silver, "silver.clubes", mode="append")
    import psycopg2, os
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"), port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "brasileirao"),
        user=os.getenv("POSTGRES_USER", "postgres"), password=os.getenv("POSTGRES_PASSWORD", "postgres")
    )
    cur = conn.cursor()
    cur.execute("""
        DELETE FROM silver.clubes a USING silver.clubes b
        WHERE a.ctid < b.ctid AND a.clube_id = b.clube_id
    """)
    conn.commit(); conn.close()
    return n


def transform_estatisticas(spark):
    logger.info("Transformando estatisticas bronze -> silver...")
    pontuacoes = read_jdbc(spark, "bronze.pontuacoes_historico_raw")
    atletas    = read_jdbc(spark, "bronze.jogadores_status_raw")

    df_silver = (
        pontuacoes
        .join(atletas.select("atleta_id", "posicao_id"), "atleta_id", "left")
        .select(
            pontuacoes["rodada"],
            pontuacoes["atleta_id"],
            pontuacoes["clube_id"],
            F.coalesce(atletas["posicao_id"], F.lit(0)).alias("posicao_id"),
            pontuacoes["pontos"],
        )
        .dropDuplicates(["rodada", "atleta_id"])
    )

    write_jdbc(df_silver, "silver.estatisticas_jogador_partida", mode="overwrite")
    return df_silver.count()


def main():
    spark = get_spark("02_silver_transform")
    stage = "silver"
    total = 0

    try:
        n = transform_partidas(spark)
        total += n
        log_dq_check("silver_partidas_count", "silver.partidas",
                     "success" if n > 0 else "warning", checked=n)

        n = transform_clubes(spark)
        total += n
        log_dq_check("silver_clubes_count", "silver.clubes",
                     "success" if n > 0 else "warning", checked=n)

        n = transform_estatisticas(spark)
        total += n
        log_dq_check("silver_estatisticas_count", "silver.estatisticas_jogador_partida",
                     "success" if n > 0 else "warning", checked=n)

        log_pipeline_execution(stage, "success", records=total)
        logger.info(f"[SILVER] Concluido. {total} registros processados.")

    except Exception as e:
        logger.error(f"[SILVER] Falha: {e}", exc_info=True)
        log_pipeline_execution(stage, "failed", error=str(e))
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
