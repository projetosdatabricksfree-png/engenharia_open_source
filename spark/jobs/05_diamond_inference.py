"""
Stage: DIAMOND - Inference Pipeline
Carrega modelo treinado, gera previsoes das proximas partidas
e atualiza analise de risco de rebaixamento.
"""
import sys
import pickle
import logging
import numpy as np
import psycopg2

sys.path.insert(0, "/opt/spark-jobs")
from commons import get_spark, read_jdbc, write_jdbc, log_pipeline_execution, log_dq_check, logger
from commons import PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASS

MODEL_PATH = "/tmp/model/previsao_brasileirao.pkl"
LABEL_PATH = "/tmp/model/label_encoder.pkl"

FEATURES = [
    "elo_casa", "elo_vis",
    "media_pontos_5j_casa", "media_pontos_5j_vis",
    "media_gols_marc_5j_casa", "media_gols_marc_5j_vis",
    "media_gols_sofr_5j_casa", "media_gols_sofr_5j_vis",
    "aproveitamento_casa_casa", "aproveitamento_fora_vis",
    "momentum_casa", "momentum_vis",
    "saldo_gols_casa", "saldo_gols_vis",
]


def load_model():
    with open(MODEL_PATH, "rb") as f:
        pipeline = pickle.load(f)
    with open(LABEL_PATH, "rb") as f:
        le = pickle.load(f)
    return pipeline, le


def get_model_versao():
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS
    )
    cur = conn.cursor()
    cur.execute("SELECT versao FROM diamond.modelos_registry WHERE ativo = TRUE ORDER BY created_at DESC LIMIT 1")
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row[0] if row else "unknown"


def generate_predictions(spark, pipeline, le, versao):
    """Gera previsoes para partidas sem resultado (proximas)."""
    partidas    = read_jdbc(spark, "silver.partidas")
    clubes      = read_jdbc(spark, "silver.clubes")
    features    = read_jdbc(spark, "gold.feature_store_enhanced")
    base        = read_jdbc(spark, "gold.feature_store")

    # Apenas partidas sem resultado (proximas)
    proximas = partidas.filter(partidas.resultado.isNull())
    if proximas.count() == 0:
        logger.warning("Nenhuma partida proxima encontrada.")
        return 0

    # Enriquecer com nomes
    proximas = (
        proximas
        .join(clubes.withColumnRenamed("clube_id","c_casa_id")
                    .withColumnRenamed("nome","nome_casa"),
              proximas.clube_casa_id == clubes.clube_id, "left")
        .join(clubes.withColumnRenamed("clube_id","c_vis_id")
                    .withColumnRenamed("nome","nome_vis"),
              proximas.clube_vis_id == clubes.clube_id, "left")
    )

    proximas_pd = proximas.toPandas()
    features_pd = features.toPandas()
    base_pd     = base.toPandas()

    rows = []
    for _, row in proximas_pd.iterrows():
        rodada    = row["rodada"]
        casa_id   = row["clube_casa_id"]
        vis_id    = row["clube_vis_id"]
        nome_casa = row.get("nome_casa", str(casa_id))
        nome_vis  = row.get("nome_vis",  str(vis_id))

        # Features casa
        fc = features_pd[(features_pd.rodada == rodada) & (features_pd.clube_id == casa_id)]
        bc = base_pd[(base_pd.rodada == rodada) & (base_pd.clube_id == casa_id)]
        # Features visitante
        fv = features_pd[(features_pd.rodada == rodada) & (features_pd.clube_id == vis_id)]
        bv = base_pd[(base_pd.rodada == rodada) & (base_pd.clube_id == vis_id)]

        def safe(df, col, default=0.0):
            return float(df[col].values[0]) if len(df) > 0 and col in df.columns else default

        X = [[
            safe(fc, "elo_rating"),         safe(fv, "elo_rating"),
            safe(fc, "media_pontos_5j"),     safe(fv, "media_pontos_5j"),
            safe(fc, "media_gols_marc_5j"),  safe(fv, "media_gols_marc_5j"),
            safe(fc, "media_gols_sofr_5j"),  safe(fv, "media_gols_sofr_5j"),
            safe(fc, "aproveitamento_casa"), safe(fv, "aproveitamento_fora"),
            safe(fc, "momentum"),            safe(fv, "momentum"),
            safe(bc, "saldo_gols"),          safe(bv, "saldo_gols"),
        ]]

        proba     = pipeline.predict_proba(X)[0]
        pred_idx  = int(np.argmax(proba))
        classes   = list(le.classes_)
        previsao  = classes[pred_idx]
        confianca = float(proba[pred_idx])

        # Mapeia probabilidades para casa/empate/visitante
        prob_map = {classes[i]: float(proba[i]) for i in range(len(classes))}

        rows.append({
            "rodada":         int(rodada),
            "clube_casa_id":  int(casa_id),
            "clube_vis_id":   int(vis_id),
            "nome_casa":      str(nome_casa),
            "nome_vis":       str(nome_vis),
            "prob_casa":      prob_map.get("casa",      0.0),
            "prob_empate":    prob_map.get("empate",    0.0),
            "prob_visitante": prob_map.get("visitante", 0.0),
            "previsao":       previsao,
            "confianca":      confianca,
            "modelo_versao":  versao,
        })

    import pandas as pd
    df_out = spark.createDataFrame(pd.DataFrame(rows))
    write_jdbc(df_out, "diamond.previsoes_proximas_partidas", mode="overwrite")
    return len(rows)


def validate_past_predictions(spark):
    """Valida previsoes anteriores contra resultados reais."""
    from pyspark.sql.functions import col, when

    previsoes = read_jdbc(spark, "diamond.previsoes_proximas_partidas")
    partidas  = read_jdbc(spark, "silver.partidas").filter(col("resultado").isNotNull())

    validadas = (
        previsoes.alias("p")
        .join(partidas.alias("r"),
              (previsoes.rodada == partidas.rodada) &
              (previsoes.clube_casa_id == partidas.clube_casa_id) &
              (previsoes.clube_vis_id  == partidas.clube_vis_id))
        .select(
            previsoes.rodada,
            previsoes.clube_casa_id,
            previsoes.clube_vis_id,
            previsoes.nome_casa,
            previsoes.nome_vis,
            previsoes.previsao,
            partidas.resultado.alias("resultado_real"),
            when(previsoes.previsao == partidas.resultado, True).otherwise(False).alias("acerto"),
            previsoes.prob_casa,
            previsoes.prob_empate,
            previsoes.prob_visitante,
        )
    )

    count = validadas.count()
    if count > 0:
        write_jdbc(validadas, "diamond.previsoes_validadas", mode="overwrite")
        logger.info(f"[OK] {count} previsoes validadas.")
    return count


def compute_relegation_risk(spark):
    """Calcula risco de rebaixamento com base em posicao e pontos."""
    from pyspark.sql import functions as F

    tabela = read_jdbc(spark, "gold.feature_store")
    clubes = read_jdbc(spark, "silver.clubes")

    # Ultima rodada disponivel
    ultima_rodada = tabela.agg(F.max("rodada")).collect()[0][0]
    if ultima_rodada is None:
        logger.warning("Sem dados na gold.feature_store")
        return 0

    tabela_atual = (
        tabela.filter(tabela.rodada == ultima_rodada)
        .join(clubes, tabela.clube_id == clubes.clube_id, "left")
        .withColumn("jogos", tabela.vitorias + tabela.empates + tabela.derrotas)
        .orderBy(F.desc("pontos_acumulados"), F.desc("vitorias"), F.desc("saldo_gols"))
    )

    tabela_pd = tabela_atual.toPandas()
    tabela_pd = tabela_pd.reset_index(drop=True)
    tabela_pd["posicao"] = tabela_pd.index + 1

    total = len(tabela_pd)
    rows = []
    for _, row in tabela_pd.iterrows():
        posicao = int(row["posicao"])
        # Prob rebaixamento: funcao sigmoide da posicao relativa
        pos_rel = posicao / total
        prob = float(1 / (1 + np.exp(-10 * (pos_rel - 0.85))))

        rows.append({
            "clube_id":          int(row["clube_id"]),
            "nome_clube":        str(row.get("nome", str(row["clube_id"]))),
            "posicao":           posicao,
            "pontos":            int(row.get("pontos_acumulados", 0)),
            "jogos":             int(row.get("jogos", 0)),
            "saldo_gols":        int(row.get("saldo_gols", 0)),
            "prob_rebaixamento": round(prob, 4),
            "zona_rebaixamento": posicao > total - 4,  # ultimos 4 na zona
        })

    import pandas as pd
    df_out = spark.createDataFrame(pd.DataFrame(rows))
    write_jdbc(df_out, "diamond.analise_rebaixamento", mode="overwrite")
    logger.info(f"[OK] Analise de rebaixamento para {len(rows)} clubes")
    return len(rows)


def main():
    spark = get_spark("05_diamond_inference")
    stage = "diamond_inference"
    total = 0

    try:
        pipeline, le = load_model()
        versao = get_model_versao()
        logger.info(f"[INFERENCE] Modelo carregado: {versao}")

        n = generate_predictions(spark, pipeline, le, versao)
        total += n
        log_dq_check("inferencia_previsoes", "diamond.previsoes_proximas_partidas",
                     "success" if n >= 0 else "warning", checked=n)

        n = validate_past_predictions(spark)
        total += n

        n = compute_relegation_risk(spark)
        total += n

        log_pipeline_execution(stage, "success", records=total)
        logger.info(f"[INFERENCE] Concluido. {total} registros gerados.")

    except Exception as e:
        logger.error(f"[INFERENCE] Falha: {e}", exc_info=True)
        log_pipeline_execution(stage, "failed", error=str(e))
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
