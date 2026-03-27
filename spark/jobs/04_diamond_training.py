"""
Stage: DIAMOND - Model Training
Le features da camada gold, treina modelo de previsao (Random Forest),
registra metricas e salva artefato do modelo em /tmp/model/.
"""
import sys
import json
import pickle
import logging
import numpy as np
from datetime import datetime

sys.path.insert(0, "/opt/spark-jobs")
from commons import get_spark, read_jdbc, log_pipeline_execution, log_dq_check, logger
from commons import PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASS

import psycopg2
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import accuracy_score, f1_score, log_loss
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

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


def build_training_dataset(spark):
    """
    Junta partidas silver com features gold para criar dataset de treino.
    Usa split temporal: rodadas anteriores para treino, ultimas para teste.
    """
    partidas = read_jdbc(spark, "silver.partidas")
    features = read_jdbc(spark, "gold.feature_store_enhanced")
    base     = read_jdbc(spark, "gold.feature_store")

    # Filtra apenas partidas com resultado definido
    partidas = partidas.filter(partidas.resultado.isNotNull())

    # Join com features do clube da casa
    casa = features.alias("f_casa").join(
        base.alias("b_casa"),
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

    # Join com features do clube visitante
    vis = features.alias("f_vis").join(
        base.alias("b_vis"),
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

    # Dataset final
    dataset = (
        partidas
        .join(casa, (partidas.rodada == casa.rodada) & (partidas.clube_casa_id == casa.clube_casa_id), "left")
        .join(vis,  (partidas.rodada == vis.rodada)  & (partidas.clube_vis_id  == vis.clube_vis_id),  "left")
        .select(partidas.rodada, partidas.resultado, *[partidas[c] if c in partidas.columns else
                 casa[c] if c in [f for f in FEATURES if "casa" in f] else vis[c]
                 for c in FEATURES])
    )

    return dataset.toPandas().dropna()


def train(df_pd):
    """Treina com split temporal: 80% rodadas mais antigas, 20% mais recentes."""
    df_pd = df_pd.sort_values("rodada")
    split = int(len(df_pd) * 0.8)
    train_df = df_pd.iloc[:split]
    test_df  = df_pd.iloc[split:]

    X_train = train_df[FEATURES].fillna(0)
    y_train = train_df[TARGET]
    X_test  = test_df[FEATURES].fillna(0)
    y_test  = test_df[TARGET]

    rodada_treino = int(train_df["rodada"].max())
    rodada_teste  = int(test_df["rodada"].max())

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

    logger.info(f"Acuracia: {acc:.4f} | F1: {f1:.4f} | LogLoss: {ll:.4f}")
    return pipeline, le, acc, f1, ll, rodada_treino, rodada_teste


def save_model(pipeline, le):
    import os
    os.makedirs("/tmp/model", exist_ok=True)
    with open(MODEL_PATH, "wb") as f:
        pickle.dump(pipeline, f)
    with open(LABEL_PATH, "wb") as f:
        pickle.dump(le, f)
    logger.info(f"Modelo salvo em {MODEL_PATH}")


def register_model(acc, f1, ll, rodada_treino, rodada_teste, params):
    """Registra o modelo no catalogo diamond.modelos_registry."""
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS
    )
    cur = conn.cursor()

    # Desativa modelos anteriores
    cur.execute("UPDATE diamond.modelos_registry SET ativo = FALSE")

    # Insere novo modelo como ativo
    cur.execute("""
        INSERT INTO diamond.modelos_registry
            (modelo_nome, versao, algoritmo, acuracia, f1_score, log_loss,
             rodada_treino, rodada_teste, parametros, ativo)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE)
    """, (
        "previsao_resultados",
        datetime.now().strftime("%Y%m%d_%H%M"),
        "RandomForestClassifier",
        round(acc, 6),
        round(f1,  6),
        round(ll,  6),
        rodada_treino,
        rodada_teste,
        json.dumps(params),
    ))

    conn.commit()
    cur.close()
    conn.close()
    logger.info("[OK] Modelo registrado em diamond.modelos_registry")


def main():
    spark = get_spark("04_diamond_training")
    stage = "diamond_training"

    try:
        logger.info("[TRAINING] Construindo dataset de treino...")
        df_pd = build_training_dataset(spark)
        logger.info(f"  {len(df_pd)} amostras disponíveis para treino")

        if len(df_pd) < 10:
            logger.warning("Dados insuficientes para treino. Usando modelo mock.")
            log_pipeline_execution(stage, "skipped", records=0,
                                   error="Dados insuficientes")
            return

        logger.info("[TRAINING] Treinando modelo...")
        pipeline, le, acc, f1, ll, r_treino, r_teste = train(df_pd)

        save_model(pipeline, le)

        params = pipeline.named_steps["clf"].get_params()
        register_model(acc, f1, ll, r_treino, r_teste, params)

        log_pipeline_execution(stage, "success", records=len(df_pd))
        log_dq_check("modelo_acuracia_minima", "diamond.modelos_registry",
                     "success" if acc >= 0.45 else "warning",
                     checked=1, failed=0 if acc >= 0.45 else 1,
                     detalhe=f"acuracia={acc:.4f}")

        logger.info(f"[TRAINING] Concluido. Acuracia: {acc:.4f}")

    except Exception as e:
        logger.error(f"[TRAINING] Falha: {e}", exc_info=True)
        log_pipeline_execution(stage, "failed", error=str(e))
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
