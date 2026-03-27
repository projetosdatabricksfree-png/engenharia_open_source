"""
Stage: GOLD
Engenharia de features a partir da camada Silver.
Le de silver.partidas e escreve em:
  - gold.feature_store          (stats cumulativos por clube/rodada)
  - gold.feature_store_enhanced (ELO, medias moveis, momentum)
"""
import sys
import pandas as pd
sys.path.insert(0, "/opt/spark-jobs")
from commons import get_spark, read_jdbc, write_jdbc, log_pipeline_execution, log_dq_check, logger

from pyspark.sql import functions as F, Window
from pyspark.sql.types import IntegerType, DoubleType

K_FACTOR   = 32
ELO_INICIAL = 1500


# ==============================================================================
# gold.feature_store — estatisticas acumuladas por clube e rodada
# ==============================================================================
def calcular_feature_store(spark):
    logger.info("Calculando feature_store (stats cumulativos)...")
    df = read_jdbc(spark, "silver.partidas")

    if df.count() == 0:
        logger.warning("[GOLD] silver.partidas vazio — nenhuma partida com resultado disponivel. Pulando feature_store.")
        return 0

    # Uma linha por time por partida
    mandante = df.select(
        F.col("rodada"),
        F.col("clube_casa_id").alias("clube_id"),
        F.col("placar_casa").alias("gols_marcados"),
        F.col("placar_vis").alias("gols_sofridos"),
        F.when(F.col("resultado") == "casa",   F.lit(3))
         .when(F.col("resultado") == "empate", F.lit(1))
         .otherwise(F.lit(0)).alias("pontos"),
        F.when(F.col("resultado") == "casa",      F.lit(1)).otherwise(F.lit(0)).alias("vitoria"),
        F.when(F.col("resultado") == "empate",    F.lit(1)).otherwise(F.lit(0)).alias("empate"),
        F.when(F.col("resultado") == "visitante", F.lit(1)).otherwise(F.lit(0)).alias("derrota"),
    )
    visitante = df.select(
        F.col("rodada"),
        F.col("clube_vis_id").alias("clube_id"),
        F.col("placar_vis").alias("gols_marcados"),
        F.col("placar_casa").alias("gols_sofridos"),
        F.when(F.col("resultado") == "visitante", F.lit(3))
         .when(F.col("resultado") == "empate",    F.lit(1))
         .otherwise(F.lit(0)).alias("pontos"),
        F.when(F.col("resultado") == "visitante", F.lit(1)).otherwise(F.lit(0)).alias("vitoria"),
        F.when(F.col("resultado") == "empate",    F.lit(1)).otherwise(F.lit(0)).alias("empate"),
        F.when(F.col("resultado") == "casa",      F.lit(1)).otherwise(F.lit(0)).alias("derrota"),
    )
    times = mandante.union(visitante)

    # Acumulados até cada rodada (inclusive)
    win = Window.partitionBy("clube_id").orderBy("rodada").rowsBetween(Window.unboundedPreceding, 0)
    df_ac = (
        times
        .withColumn("pontos_acumulados", F.sum("pontos").over(win))
        .withColumn("vitorias",          F.sum("vitoria").over(win))
        .withColumn("empates",           F.sum("empate").over(win))
        .withColumn("derrotas",          F.sum("derrota").over(win))
        .withColumn("gols_marcados",     F.sum("gols_marcados").over(win))
        .withColumn("gols_sofridos",     F.sum("gols_sofridos").over(win))
        .withColumn("saldo_gols",        F.sum("gols_marcados").over(win) - F.sum("gols_sofridos").over(win))
        .withColumn("jogos",             F.count("*").over(win))
        .withColumn("aproveitamento_pct",
                    (F.sum("pontos").over(win) / (F.count("*").over(win) * 3)) * 100)
    )

    df_final = df_ac.select(
        "rodada", "clube_id",
        "pontos_acumulados", "vitorias", "empates", "derrotas",
        "gols_marcados", "gols_sofridos", "saldo_gols",
        F.col("aproveitamento_pct").cast(DoubleType()),
    )

    write_jdbc(df_final, "gold.feature_store", mode="overwrite")
    return df_final.count()


# ==============================================================================
# gold.feature_store_enhanced — ELO + medias moveis + momentum
# ==============================================================================
def calcular_feature_store_enhanced(spark):
    logger.info("Calculando feature_store_enhanced (ELO + medias moveis)...")
    df = read_jdbc(spark, "silver.partidas")

    if df.count() == 0:
        logger.warning("[GOLD] silver.partidas vazio — nenhuma partida com resultado disponivel. Pulando feature_store_enhanced.")
        return 0

    partidas_pd = df.orderBy("data_partida").toPandas()

    # ---- Rating ELO ----
    elos = {}
    elo_m_pre, elo_v_pre = [], []

    for _, row in partidas_pd.iterrows():
        mid = row["clube_casa_id"]
        vid = row["clube_vis_id"]
        res = row["resultado"]

        elo_m = elos.get(mid, ELO_INICIAL)
        elo_v = elos.get(vid, ELO_INICIAL)
        elo_m_pre.append(elo_m)
        elo_v_pre.append(elo_v)

        prob_m  = 1 / (1 + 10 ** ((elo_v - elo_m) / 400))
        score_m = 1.0 if res == "casa" else (0.5 if res == "empate" else 0.0)

        elos[mid] = elo_m + K_FACTOR * (score_m - prob_m)
        elos[vid] = elo_v + K_FACTOR * ((1 - score_m) - (1 - prob_m))

    partidas_pd["elo_m_pre"] = elo_m_pre
    partidas_pd["elo_v_pre"] = elo_v_pre

    # ---- Desnormalizar: uma linha por time por partida ----
    mandante_pd = partidas_pd[[
        "rodada", "clube_casa_id", "data_partida",
        "placar_casa", "placar_vis", "resultado", "elo_m_pre"
    ]].rename(columns={
        "clube_casa_id": "clube_id",
        "placar_casa": "gols_marc",
        "placar_vis":  "gols_sofr",
        "elo_m_pre":   "elo_rating",
    })
    mandante_pd["is_casa"] = 1
    mandante_pd["vitoria"]  = (mandante_pd["resultado"] == "casa").astype(int)
    mandante_pd["pontos"]   = mandante_pd["resultado"].map({"casa": 3, "empate": 1, "visitante": 0})

    visitante_pd = partidas_pd[[
        "rodada", "clube_vis_id", "data_partida",
        "placar_vis", "placar_casa", "resultado", "elo_v_pre"
    ]].rename(columns={
        "clube_vis_id": "clube_id",
        "placar_vis":   "gols_marc",
        "placar_casa":  "gols_sofr",
        "elo_v_pre":    "elo_rating",
    })
    visitante_pd["is_casa"] = 0
    visitante_pd["vitoria"]  = (visitante_pd["resultado"] == "visitante").astype(int)
    visitante_pd["pontos"]   = visitante_pd["resultado"].map({"visitante": 3, "empate": 1, "casa": 0})

    times = (
        pd.concat([mandante_pd, visitante_pd], ignore_index=True)
          .sort_values(["clube_id", "data_partida"])
    )

    # ---- Medias moveis (ultimos 5 jogos, excluindo o atual) ----
    grp = times.groupby("clube_id")
    times["media_pontos_5j"]    = grp["pontos"].transform(lambda x: x.shift(1).rolling(5, min_periods=1).mean())
    times["media_gols_marc_5j"] = grp["gols_marc"].transform(lambda x: x.shift(1).rolling(5, min_periods=1).mean())
    times["media_gols_sofr_5j"] = grp["gols_sofr"].transform(lambda x: x.shift(1).rolling(5, min_periods=1).mean())

    # ---- Aproveitamento casa / fora ----
    times["vit_casa"] = ((times["is_casa"] == 1) & (times["vitoria"] == 1)).astype(int)
    times["vit_fora"] = ((times["is_casa"] == 0) & (times["vitoria"] == 1)).astype(int)
    times["jog_casa"] = (times["is_casa"] == 1).astype(int)
    times["jog_fora"] = (times["is_casa"] == 0).astype(int)

    grp = times.groupby("clube_id")
    times["aproveitamento_casa"] = (
        grp["vit_casa"].transform(lambda x: x.shift(1).expanding().sum()) /
        grp["jog_casa"].transform(lambda x: x.shift(1).expanding().sum().clip(lower=1))
    )
    times["aproveitamento_fora"] = (
        grp["vit_fora"].transform(lambda x: x.shift(1).expanding().sum()) /
        grp["jog_fora"].transform(lambda x: x.shift(1).expanding().sum().clip(lower=1))
    )

    # ---- Momentum (soma de pontos dos ultimos 3 jogos) ----
    times["momentum"] = times.groupby("clube_id")["pontos"].transform(
        lambda x: x.shift(1).rolling(3, min_periods=1).sum()
    )

    times = times.fillna(0)

    cols = [
        "rodada", "clube_id", "elo_rating",
        "media_pontos_5j", "media_gols_marc_5j", "media_gols_sofr_5j",
        "aproveitamento_casa", "aproveitamento_fora", "momentum",
    ]
    df_final = (
        spark.createDataFrame(times[cols].astype(float))
             .withColumn("rodada",   F.col("rodada").cast(IntegerType()))
             .withColumn("clube_id", F.col("clube_id").cast(IntegerType()))
    )

    write_jdbc(df_final, "gold.feature_store_enhanced", mode="overwrite")
    return df_final.count()


# ==============================================================================
# Main
# ==============================================================================
def main():
    spark = get_spark("03_gold_features")
    stage = "gold"
    total = 0

    try:
        n = calcular_feature_store(spark)
        total += n
        log_dq_check("gold_feature_store_count", "gold.feature_store",
                     "success" if n > 0 else "warning", checked=n)

        n = calcular_feature_store_enhanced(spark)
        total += n
        log_dq_check("gold_enhanced_count", "gold.feature_store_enhanced",
                     "success" if n > 0 else "warning", checked=n)

        log_pipeline_execution(stage, "success", records=total)
        logger.info(f"[GOLD] Concluido. {total} registros processados.")

    except Exception as e:
        logger.error(f"[GOLD] Falha: {e}", exc_info=True)
        log_pipeline_execution(stage, "failed", error=str(e))
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
