"""
BrasileirãoPRO - Backend API
FastAPI server exposing PostgreSQL data for the mobile app.
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
import psycopg2.extras
import os
from typing import Any

app = FastAPI(title="BrasileirãoPRO API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5433")),
    "dbname": os.getenv("POSTGRES_DB", "brasileirao"),
    "user": os.getenv("POSTGRES_USER", "admin"),
    "password": os.getenv("POSTGRES_PASSWORD", "admin"),
}


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def query(sql: str, params=None) -> list[dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            return [dict(r) for r in rows]


@app.get("/health")
def health():
    try:
        query("SELECT 1")
        return {"status": "ok", "db": "connected"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))


@app.get("/api/previsoes")
def previsoes():
    """Próximas partidas com probabilidades e métricas."""
    rows = query("""
        SELECT
            id,
            rodada,
            nome_casa,
            nome_visitante,
            abrev_casa,
            abrev_visitante,
            escudo_casa,
            escudo_visitante,
            prob_casa_pct,
            prob_empate_pct,
            prob_visitante_pct,
            previsao,
            confianca_pct,
            modelo_versao,
            elo_casa,
            elo_visitante,
            pontos_casa,
            pontos_visitante,
            aprov_casa_pct,
            aprov_vis_pct,
            processed_at
        FROM gold.mart_previsoes_proximas
        ORDER BY rodada, prob_casa_pct DESC
    """)
    return {"data": rows, "total": len(rows)}


@app.get("/api/classificacao")
def classificacao():
    """Tabela de classificação completa."""
    rows = query("""
        SELECT
            posicao,
            clube,
            abreviacao,
            escudo_url,
            jogos,
            pontos,
            v, e, d,
            gm, gs, sg,
            aproveitamento,
            prob_rebaixamento_pct,
            zona_rebaixamento,
            situacao,
            cor_situacao
        FROM gold.mart_tabela_classificacao
        ORDER BY posicao
    """)
    return {"data": rows, "total": len(rows)}


@app.get("/api/elo")
def elo_ranking():
    """Ranking ELO atual."""
    rows = query("""
        SELECT *
        FROM gold.vw_elo_ranking
        LIMIT 30
    """)
    return {"data": rows, "total": len(rows)}


@app.get("/api/desempenho")
def desempenho():
    """Métricas de desempenho do modelo por rodada."""
    rows = query("""
        SELECT
            rodada,
            total_jogos,
            acertos,
            acuracia_pct,
            acuracia_casa_pct,
            acuracia_empate_pct,
            acuracia_visitante_pct,
            conf_media_acerto,
            conf_media_erro,
            acuracia_media_5r,
            total_jogos_acumulado,
            acertos_acumulado,
            acuracia_geral_pct
        FROM gold.mart_desempenho_modelo
        ORDER BY rodada
    """)
    return {"data": rows, "total": len(rows)}


@app.get("/api/modelos")
def modelos():
    """Registro dos modelos ML treinados."""
    rows = query("""
        SELECT *
        FROM gold.modelos_registry
        ORDER BY treinado_em DESC
        LIMIT 10
    """)
    return {"data": rows, "total": len(rows)}


@app.get("/api/pipeline")
def pipeline_status():
    """Status das últimas execuções do pipeline."""
    rows = query("""
        SELECT *
        FROM gold.pipeline_executions
        ORDER BY started_at DESC
        LIMIT 20
    """)
    return {"data": rows, "total": len(rows)}


@app.get("/api/rebaixamento")
def rebaixamento():
    """Análise de risco de rebaixamento."""
    rows = query("""
        SELECT
            posicao,
            clube,
            abreviacao,
            escudo_url,
            pontos,
            jogos,
            prob_rebaixamento_pct,
            zona_rebaixamento,
            cor_situacao
        FROM gold.mart_tabela_classificacao
        ORDER BY prob_rebaixamento_pct DESC NULLS LAST
    """)
    return {"data": rows, "total": len(rows)}


@app.get("/api/resumo")
def resumo():
    """Dashboard summary — key KPIs."""
    try:
        desempenho_rows = query("""
            SELECT acuracia_geral_pct, total_jogos_acumulado, acertos_acumulado
            FROM gold.mart_desempenho_modelo
            ORDER BY rodada DESC LIMIT 1
        """)
        previsoes_count = query("SELECT COUNT(*) as total FROM gold.mart_previsoes_proximas")
        modelos_rows = query("""
            SELECT modelo_nome, acuracia_validacao
            FROM gold.modelos_registry
            ORDER BY treinado_em DESC LIMIT 1
        """)

        return {
            "acuracia_geral": desempenho_rows[0]["acuracia_geral_pct"] if desempenho_rows else None,
            "total_previsoes_historico": desempenho_rows[0]["total_jogos_acumulado"] if desempenho_rows else None,
            "acertos_historico": desempenho_rows[0]["acertos_acumulado"] if desempenho_rows else None,
            "proximas_partidas": previsoes_count[0]["total"] if previsoes_count else 0,
            "melhor_modelo": modelos_rows[0]["modelo_nome"] if modelos_rows else None,
            "acuracia_modelo": modelos_rows[0]["acuracia_validacao"] if modelos_rows else None,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
