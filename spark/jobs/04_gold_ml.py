"""
Stage: GOLD - ML Training + Inference
Le features da camada gold, treina modelo RandomForest, registra metricas,
gera previsoes das proximas partidas e calcula risco de rebaixamento.
Todos os outputs vao para o schema gold.
"""
import sys
import os
import json
import pickle
import logging
import numpy as np
import psycopg2
import pandas as pd
from datetime import datetime

sys.path.insert(0, "/opt/spark-jobs")
from commons import get_spark, read_jdbc, write_jdbc, log_pipeline_execution, log_dq_check, logger
from commons import PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASS

from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.metrics import accuracy_score, f1_score, log_loss
from sklearn.pipeline import Pipeline

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
TARGET = "resultado"


# ─────────────────────────────────────────────
# TRAINING
# ─────────────────────────────────────────────

def build_training_dataset(spark):
    partidas = read_jdbc(spark, "silver.partidas")
    features = read_jdbc(spark, "gold.feature_store_enhanced")
    base     = read_jdbc(spark, "gold.feature_store")

    partidas = partidas.filter(partidas.resultado.isNotNull())

    casa = features.join(
        base,
        (features.rodada == base.rodada) & (features.clube_id == base.clube_id)
    ).select(
        features.rodada,
        features.clube_id.alias("clube_casa_id"),
        features.elo_rating.alias("elo_casa"),
        features.media_pontos_5j.alias("media_pontos_5j_casa"),
        features.media_gols_marc_5j.alias("media_gols_marc_5j_casa"),
        features.media_gols_sofr_5j.alias("media_gols_sofr_5j_casa"),
        features.aproveitamento_casa.alias("aproveitamento_casa_casa"),
        features.momentum.alias("momentum_casa"),
        base.saldo_gols.alias("saldo_gols_casa"),
    )

    vis = features.join(
        base,
        (features.rodada == base.rodada) & (features.clube_id == base.clube_id)
    ).select(
        features.rodada,
        features.clube_id.alias("clube_vis_id"),
        features.elo_rating.alias("elo_vis"),
        features.media_pontos_5j.alias("media_pontos_5j_vis"),
        features.media_gols_marc_5j.alias("media_gols_marc_5j_vis"),
        features.media_gols_sofr_5j.alias("media_gols_sofr_5j_vis"),
        features.aproveitamento_fora.alias("aproveitamento_fora_vis"),
        features.momentum.alias("momentum_vis"),
        base.saldo_gols.alias("saldo_gols_vis"),
    )

    dataset = (
        partidas
        .join(casa, (partidas.rodada == casa.rodada) & (partidas.clube_casa_id == casa.clube_casa_id), "left")
        .join(vis,  (partidas.rodada == vis.rodada)  & (partidas.clube_vis_id  == vis.clube_vis_id),  "left")
        .select(partidas.rodada, partidas.resultado, *[
            casa[c] if c in [f for f in FEATURES if "casa" in f] else
            vis[c]  if c in [f for f in FEATURES if "vis"  in f] else
            partidas[c]
            for c in FEATURES
        ])
    )

    return dataset.toPandas().dropna()


def train(df_pd):
    df_pd  = df_pd.sort_values("rodada")
    split  = int(len(df_pd) * 0.8)
    train_df = df_pd.iloc[:split]
    test_df  = df_pd.iloc[split:]

    X_train = train_df[FEATURES].fillna(0)
    y_train = train_df[TARGET]
    X_test  = test_df[FEATURES].fillna(0)
    y_test  = test_df[TARGET]

    le = LabelEncoder()
    y_train_enc = le.fit_transform(y_train)
    y_test_enc  = le.transform(y_test)

    pipeline = Pipeline([
        ("scaler", StandardScaler()),
        ("clf", RandomForestClassifier(
            n_estimators=200,
            max_depth=8,
            min_samples_split=10,
            class_weight="balanced",
            random_state=42,
            n_jobs=-1,
        )),
    ])

    pipeline.fit(X_train, y_train_enc)
    y_pred     = pipeline.predict(X_test)
    y_pred_prb = pipeline.predict_proba(X_test)

    acc = accuracy_score(y_test_enc, y_pred)
    f1  = f1_score(y_test_enc, y_pred, average="weighted")
    ll  = log_loss(y_test_enc, y_pred_prb)

    rodada_treino = int(train_df["rodada"].max())
    rodada_teste  = int(test_df["rodada"].max())

    logger.info(f"Acuracia: {acc:.4f} | F1: {f1:.4f} | LogLoss: {ll:.4f}")
    return pipeline, le, acc, f1, ll, rodada_treino, rodada_teste


def save_model(pipeline, le):
    os.makedirs("/tmp/model", exist_ok=True)
    with open(MODEL_PATH, "wb") as f:
        pickle.dump(pipeline, f)
    with open(LABEL_PATH, "wb") as f:
        pickle.dump(le, f)
    logger.info(f"Modelo salvo em {MODEL_PATH}")


def register_model(acc, f1, ll, rodada_treino, rodada_teste, params):
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS
    )
    cur = conn.cursor()
    cur.execute("UPDATE gold.modelos_registry SET ativo = FALSE")
    cur.execute("""
        INSERT INTO gold.modelos_registry
            (modelo_nome, versao, algoritmo, acuracia, f1_score, log_loss,
             rodada_treino, rodada_teste, parametros, ativo)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE)
    """, (
        "previsao_resultados",
        datetime.now().strftime("%Y%m%d_%H%M"),
        "RandomForestClassifier",
        round(acc, 6), round(f1, 6), round(ll, 6),
        rodada_treino, rodada_teste,
        json.dumps(params),
    ))
    conn.commit()
    cur.close()
    conn.close()
    logger.info("[OK] Modelo registrado em gold.modelos_registry")


# ─────────────────────────────────────────────
# INFERENCE
# ─────────────────────────────────────────────

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
    cur.execute(
        "SELECT versao FROM gold.modelos_registry WHERE ativo = TRUE "
        "ORDER BY processed_at DESC LIMIT 1"
    )
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row[0] if row else "unknown"


def generate_predictions(spark, pipeline, le, versao):
    partidas = read_jdbc(spark, "silver.partidas")
    clubes   = read_jdbc(spark, "silver.clubes")
    features = read_jdbc(spark, "gold.feature_store_enhanced")
    base     = read_jdbc(spark, "gold.feature_store")

    proximas = partidas.filter(partidas.resultado.isNull())
    if proximas.count() == 0:
        logger.warning("Nenhuma partida proxima encontrada.")
        return 0

    proximas = (
        proximas
        .join(clubes.withColumnRenamed("clube_id", "c_casa_id")
                    .withColumnRenamed("nome", "nome_casa"),
              proximas.clube_casa_id == clubes.clube_id, "left")
        .join(clubes.withColumnRenamed("clube_id", "c_vis_id")
                    .withColumnRenamed("nome", "nome_vis"),
              proximas.clube_vis_id == clubes.clube_id, "left")
    )

    proximas_pd = proximas.toPandas()
    features_pd = features.toPandas()
    base_pd     = base.toPandas()

    def safe(df, col, default=0.0):
        return float(df[col].values[0]) if len(df) > 0 and col in df.columns else default

    rows = []
    for _, row in proximas_pd.iterrows():
        rodada    = row["rodada"]
        casa_id   = row["clube_casa_id"]
        vis_id    = row["clube_vis_id"]
        nome_casa = row.get("nome_casa", str(casa_id))
        nome_vis  = row.get("nome_vis",  str(vis_id))

        fc = features_pd[(features_pd.rodada == rodada) & (features_pd.clube_id == casa_id)]
        bc = base_pd[(base_pd.rodada == rodada) & (base_pd.clube_id == casa_id)]
        fv = features_pd[(features_pd.rodada == rodada) & (features_pd.clube_id == vis_id)]
        bv = base_pd[(base_pd.rodada == rodada) & (base_pd.clube_id == vis_id)]

        X = [[
            safe(fc, "elo_rating"),         safe(fv, "elo_rating"),
            safe(fc, "media_pontos_5j"),     safe(fv, "media_pontos_5j"),
            safe(fc, "media_gols_marc_5j"),  safe(fv, "media_gols_marc_5j"),
            safe(fc, "media_gols_sofr_5j"),  safe(fv, "media_gols_sofr_5j"),
            safe(fc, "aproveitamento_casa"), safe(fv, "aproveitamento_fora"),
            safe(fc, "momentum"),            safe(fv, "momentum"),
            safe(bc, "saldo_gols"),          safe(bv, "saldo_gols"),
        ]]

        proba    = pipeline.predict_proba(X)[0]
        pred_idx = int(np.argmax(proba))
        classes  = list(le.classes_)
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
            "previsao":       classes[pred_idx],
            "confianca":      float(proba[pred_idx]),
            "modelo_versao":  versao,
        })

    df_out = spark.createDataFrame(pd.DataFrame(rows))
    write_jdbc(df_out, "gold.previsoes_proximas_partidas", mode="overwrite")
    return len(rows)


def validate_past_predictions(spark):
    from pyspark.sql.functions import col, when

    previsoes = read_jdbc(spark, "gold.previsoes_proximas_partidas")
    partidas  = read_jdbc(spark, "silver.partidas").filter(col("resultado").isNotNull())

    validadas = (
        previsoes
        .join(partidas,
              (previsoes.rodada       == partidas.rodada) &
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
        write_jdbc(validadas, "gold.previsoes_validadas", mode="overwrite")
        logger.info(f"[OK] {count} previsoes validadas.")
    return count


def compute_relegation_risk(spark):
    from pyspark.sql import functions as F

    tabela = read_jdbc(spark, "gold.feature_store")
    clubes = read_jdbc(spark, "silver.clubes")

    ultima_rodada = tabela.agg(F.max("rodada")).collect()[0][0]
    if ultima_rodada is None:
        logger.warning("Sem dados na gold.feature_store")
        return 0

    tabela_atual = (
        tabela.filter(tabela.rodada == ultima_rodada)
        .join(clubes.select("clube_id", "nome"), "clube_id", "left")
        .withColumn("jogos", tabela.vitorias + tabela.empates + tabela.derrotas)
        .orderBy(F.desc("pontos_acumulados"), F.desc("vitorias"), F.desc("saldo_gols"))
    )

    tabela_pd = tabela_atual.toPandas().reset_index(drop=True)
    tabela_pd["posicao"] = tabela_pd.index + 1
    total = len(tabela_pd)

    rows = []
    for _, row in tabela_pd.iterrows():
        posicao = int(row["posicao"])
        pos_rel = posicao / total
        prob    = float(1 / (1 + np.exp(-10 * (pos_rel - 0.85))))
        rows.append({
            "clube_id":          int(row["clube_id"]),
            "nome_clube":        str(row.get("nome", str(row["clube_id"]))),
            "posicao":           posicao,
            "pontos":            int(row.get("pontos_acumulados", 0)),
            "jogos":             int(row.get("jogos", 0)),
            "saldo_gols":        int(row.get("saldo_gols", 0)),
            "prob_rebaixamento": round(prob, 4),
            "zona_rebaixamento": posicao > total - 4,
        })

    df_out = spark.createDataFrame(pd.DataFrame(rows))
    write_jdbc(df_out, "gold.analise_rebaixamento", mode="overwrite")
    logger.info(f"[OK] Analise de rebaixamento para {total} clubes")
    return total


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    spark = get_spark("04_gold_ml")

    # ── TRAINING ──────────────────────────────
    stage = "gold_ml_training"
    try:
        logger.info("[TRAINING] Construindo dataset de treino...")
        df_pd = build_training_dataset(spark)
        logger.info(f"  {len(df_pd)} amostras disponiveis para treino")

        if len(df_pd) < 10:
            logger.warning("Dados insuficientes para treino. Pulando.")
            log_pipeline_execution(stage, "skipped", records=0, error="Dados insuficientes")
        else:
            pipeline, le, acc, f1, ll, r_treino, r_teste = train(df_pd)
            save_model(pipeline, le)
            register_model(acc, f1, ll, r_treino, r_teste,
                           pipeline.named_steps["clf"].get_params())
            log_pipeline_execution(stage, "success", records=len(df_pd))
            log_dq_check("modelo_acuracia_minima", "gold.modelos_registry",
                         "success" if acc >= 0.45 else "warning",
                         checked=1, failed=0 if acc >= 0.45 else 1,
                         detalhe=f"acuracia={acc:.4f}")
            logger.info(f"[TRAINING] Concluido. Acuracia: {acc:.4f}")
    except Exception as e:
        logger.error(f"[TRAINING] Falha: {e}", exc_info=True)
        log_pipeline_execution(stage, "failed", error=str(e))
        raise

    # ── INFERENCE ─────────────────────────────
    stage = "gold_ml_inference"
    total = 0
    try:
        if not os.path.exists(MODEL_PATH):
            logger.warning("[INFERENCE] Modelo nao encontrado. Pulando inference.")
            log_pipeline_execution(stage, "skipped", records=0,
                                   error="Modelo nao encontrado")
            return

        pipeline, le = load_model()
        versao = get_model_versao()
        logger.info(f"[INFERENCE] Modelo carregado: {versao}")

        n = generate_predictions(spark, pipeline, le, versao)
        total += n
        log_dq_check("inferencia_previsoes", "gold.previsoes_proximas_partidas",
                     "success" if n >= 0 else "warning", checked=n)

        total += validate_past_predictions(spark)
        total += compute_relegation_risk(spark)

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
