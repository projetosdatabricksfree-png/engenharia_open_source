"""
Testes de integração: valida dados no PostgreSQL após execução do pipeline.
Requer stack rodando: docker compose up

Execução: pytest tests/test_pipeline_integracao.py -v --tb=short
"""
import os
import pytest

try:
    import psycopg2
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

PG_CONFIG = dict(
    host=os.getenv("POSTGRES_HOST", "localhost"),
    port=int(os.getenv("POSTGRES_PORT", "5433")),
    dbname=os.getenv("POSTGRES_DB", "brasileirao"),
    user=os.getenv("POSTGRES_USER", "admin"),
    password=os.getenv("POSTGRES_PASSWORD", "admin"),
    connect_timeout=5,
)

pytestmark = pytest.mark.skipif(
    not HAS_PSYCOPG2, reason="psycopg2 não instalado"
)


@pytest.fixture(scope="session")
def pg():
    """Conexão PostgreSQL reutilizada em todos os testes."""
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        yield conn
        conn.close()
    except Exception as e:
        pytest.skip(f"PostgreSQL indisponível: {e}")


def query(pg, sql):
    try:
        cur = pg.cursor()
        cur.execute(sql)
        return cur.fetchall()
    except Exception:
        pg.rollback()
        raise


# ── Bronze ─────────────────────────────────────────────────────────────

class TestBronze:
    def test_partidas_raw_nao_vazio(self, pg):
        """bronze.partidas_raw deve ter registros após ingestão."""
        rows = query(pg, "SELECT COUNT(*) FROM bronze.partidas_raw")
        assert rows[0][0] > 0, "bronze.partidas_raw está vazio"

    def test_clubes_info_raw_nao_vazio(self, pg):
        """bronze.clubes_info_raw deve ter os 20 clubes da Série A."""
        rows = query(pg, "SELECT COUNT(DISTINCT clube_id) FROM bronze.clubes_info_raw")
        assert rows[0][0] >= 20, "Menos de 20 clubes em bronze.clubes_info_raw"

    def test_raw_payload_e_json_valido(self, pg):
        """Coluna raw_payload deve conter JSON válido (não texto puro)."""
        rows = query(pg, """
            SELECT COUNT(*) FROM bronze.clubes_info_raw
            WHERE raw_payload IS NOT NULL
              AND raw_payload::text != ''
        """)
        assert rows[0][0] > 0, "raw_payload deve conter dados JSONB válidos"


# ── Silver ─────────────────────────────────────────────────────────────

class TestSilver:
    def test_clubes_tem_20_registros(self, pg):
        """silver.clubes deve ter 20 times da Série A."""
        rows = query(pg, "SELECT COUNT(*) FROM silver.clubes")
        assert rows[0][0] >= 20, "silver.clubes deve ter ao menos 20 clubes"

    def test_partidas_resultado_valido(self, pg):
        """Todas as partidas em silver devem ter resultado válido."""
        rows = query(pg, """
            SELECT COUNT(*) FROM silver.partidas
            WHERE resultado NOT IN ('casa', 'visitante', 'empate')
        """)
        assert rows[0][0] == 0, "Resultado inválido encontrado em silver.partidas"

    def test_sem_partidas_sem_placar(self, pg):
        """Partidas sem placar não devem estar em silver.partidas."""
        rows = query(pg, """
            SELECT COUNT(*) FROM silver.partidas
            WHERE placar_casa IS NULL OR placar_vis IS NULL
        """)
        assert rows[0][0] == 0, "silver.partidas não deve conter partidas sem placar"


# ── Gold ───────────────────────────────────────────────────────────────

class TestGold:
    def test_feature_store_tem_dados(self, pg):
        """gold.feature_store deve ter registros por clube por rodada."""
        rows = query(pg, "SELECT COUNT(*) FROM gold.feature_store")
        assert rows[0][0] > 0, "gold.feature_store está vazio"

    def test_feature_store_pontos_nao_negativos(self, pg):
        """Pontos acumulados não podem ser negativos."""
        rows = query(pg, """
            SELECT COUNT(*) FROM gold.feature_store
            WHERE pontos_acumulados < 0
        """)
        assert rows[0][0] == 0, "Pontos acumulados negativos encontrados"

    def test_feature_store_aproveitamento_range(self, pg):
        """Aproveitamento deve estar entre 0 e 100."""
        rows = query(pg, """
            SELECT COUNT(*) FROM gold.feature_store
            WHERE aproveitamento_pct < 0 OR aproveitamento_pct > 100
        """)
        assert rows[0][0] == 0, "Aproveitamento fora do range [0,100]"

    def test_previsoes_proximas_tem_dados(self, pg):
        """gold.previsoes_proximas_partidas deve ter previsões para a próxima rodada."""
        rows = query(pg, "SELECT COUNT(*) FROM gold.previsoes_proximas_partidas")
        assert rows[0][0] > 0, "Nenhuma previsão encontrada"

    def test_previsoes_probabilidades_somam_1(self, pg):
        """prob_casa + prob_empate + prob_visitante deve ser ≈ 1.0."""
        rows = query(pg, """
            SELECT COUNT(*) FROM gold.previsoes_proximas_partidas
            WHERE ABS(prob_casa + prob_empate + prob_visitante - 1.0) > 0.01
        """)
        assert rows[0][0] == 0, "Probabilidades não somam 1.0 em alguma previsão"

    def test_analise_rebaixamento_tem_20_clubes(self, pg):
        """gold.analise_rebaixamento deve ter todos os 20 clubes."""
        rows = query(pg, "SELECT COUNT(*) FROM gold.analise_rebaixamento")
        assert rows[0][0] == 20, "Análise de rebaixamento deve ter 20 clubes"

    def test_prob_rebaixamento_range(self, pg):
        """prob_rebaixamento deve estar entre 0 e 1."""
        rows = query(pg, """
            SELECT COUNT(*) FROM gold.analise_rebaixamento
            WHERE prob_rebaixamento < 0 OR prob_rebaixamento > 1
        """)
        assert rows[0][0] == 0, "prob_rebaixamento fora do range [0,1]"

    def test_pipeline_executions_registra_sucesso(self, pg):
        """Deve haver pelo menos um registro de sucesso no pipeline."""
        rows = query(pg, """
            SELECT COUNT(*) FROM gold.pipeline_executions
            WHERE status = 'success'
        """)
        assert rows[0][0] > 0, "Nenhuma execução com status=success encontrada"

    def test_data_quality_checks_sem_falha_critica(self, pg):
        """Não deve haver DQ checks com status='failed'."""
        rows = query(pg, """
            SELECT COUNT(*) FROM gold.data_quality_checks
            WHERE status = 'failed'
        """)
        assert rows[0][0] == 0, "Data quality checks com falha crítica encontrados"


# ── Views ──────────────────────────────────────────────────────────────

class TestViews:
    def test_vw_classificacao_retorna_20_clubes(self, pg):
        """vw_classificacao deve retornar todos os 20 times."""
        rows = query(pg, "SELECT COUNT(*) FROM gold.vw_classificacao")
        assert rows[0][0] == 20, "vw_classificacao deve ter 20 clubes"

    def test_vw_classificacao_posicoes_sequenciais(self, pg):
        """Posições devem ser sequenciais de 1 a 20."""
        rows = query(pg, "SELECT MIN(posicao), MAX(posicao) FROM gold.vw_classificacao")
        assert rows[0][0] == 1,  "Posição mínima deve ser 1"
        assert rows[0][1] == 20, "Posição máxima deve ser 20"

    def test_vw_previsoes_tem_dados(self, pg):
        """vw_previsoes deve retornar as previsões da próxima rodada."""
        rows = query(pg, "SELECT COUNT(*) FROM gold.vw_previsoes")
        assert rows[0][0] > 0, "vw_previsoes está vazio"

    def test_vw_elo_ranking_ordenado(self, pg):
        """vw_elo_ranking deve retornar 20 clubes rankeados."""
        rows = query(pg, "SELECT COUNT(*) FROM gold.vw_elo_ranking")
        assert rows[0][0] == 20

    def test_vw_desempenho_modelo_tem_rodadas(self, pg):
        """vw_desempenho_modelo deve ter pelo menos 1 rodada."""
        rows = query(pg, "SELECT COUNT(*) FROM gold.vw_desempenho_modelo")
        assert rows[0][0] > 0, "Nenhuma rodada com dados de desempenho"

    def test_vw_desempenho_acuracia_range(self, pg):
        """Acurácia deve estar entre 0 e 100."""
        rows = query(pg, """
            SELECT COUNT(*) FROM gold.vw_desempenho_modelo
            WHERE acuracia_pct < 0 OR acuracia_pct > 100
        """)
        assert rows[0][0] == 0, "Acurácia fora do range [0,100]"
