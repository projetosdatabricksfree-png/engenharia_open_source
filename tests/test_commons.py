"""
Testes unitários para spark/jobs/commons.py
Execução: pytest tests/test_commons.py -v

PySpark é mockado pois não está instalado no host —
os jobs rodam dentro do container Spark.
"""
import sys
import os
import pytest
from unittest.mock import MagicMock, patch, call

# ── Mock PySpark antes de importar commons ─────────────────────────────
# PySpark só existe dentro do container Spark; nos testes unitários
# do host substituímos por mocks leves.
_pyspark_mock = MagicMock()
sys.modules.setdefault("pyspark",             _pyspark_mock)
sys.modules.setdefault("pyspark.sql",         _pyspark_mock)
sys.modules.setdefault("pyspark.sql.session", _pyspark_mock)
sys.modules["pyspark.sql"].SparkSession = MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "spark", "jobs"))


# ── Fixtures ──────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def mock_env(monkeypatch):
    """Garante variáveis de ambiente consistentes para todos os testes."""
    monkeypatch.setenv("POSTGRES_HOST", "localhost")
    monkeypatch.setenv("POSTGRES_PORT", "5432")
    monkeypatch.setenv("POSTGRES_DB",   "brasileirao")
    monkeypatch.setenv("POSTGRES_USER", "admin")
    monkeypatch.setenv("POSTGRES_PASSWORD", "admin")


# ── Testes de write_jdbc ───────────────────────────────────────────────

class TestWriteJdbc:
    """
    write_jdbc deve usar truncate=true quando mode='overwrite'
    para não destruir views e foreign keys dependentes.
    """

    def _make_df_mock(self):
        """DataFrame Spark mockado."""
        df = MagicMock()
        df.count.return_value = 42
        writer = MagicMock()
        writer.format.return_value = writer
        writer.option.return_value = writer
        writer.options.return_value = writer
        writer.mode.return_value = writer
        df.write = writer
        return df, writer

    def test_overwrite_usa_truncate_true(self):
        """mode='overwrite' deve chamar .option('truncate','true')."""
        import commons
        df, writer = self._make_df_mock()

        commons.write_jdbc(df, "gold.feature_store", mode="overwrite")

        # Verifica que truncate=true foi setado
        calls = [str(c) for c in writer.option.call_args_list]
        assert any("truncate" in c and "true" in c for c in calls), \
            "truncate=true deve ser setado em mode=overwrite"

    def test_append_nao_usa_truncate(self):
        """mode='append' não deve usar truncate."""
        import commons
        df, writer = self._make_df_mock()

        commons.write_jdbc(df, "bronze.partidas_raw", mode="append")

        calls = [str(c) for c in writer.option.call_args_list]
        assert not any("truncate" in c for c in calls), \
            "truncate não deve ser setado em mode=append"

    def test_stringtype_unspecified_sempre_presente(self):
        """stringtype=unspecified deve estar em JDBC_PROPS para cast VARCHAR->JSONB."""
        import commons
        assert "stringtype" in commons.JDBC_PROPS, \
            "stringtype deve estar em JDBC_PROPS"
        assert commons.JDBC_PROPS["stringtype"] == "unspecified", \
            "stringtype deve ser 'unspecified'"

    def test_write_jdbc_chama_save(self):
        """write_jdbc deve chamar .save() ao final."""
        import commons
        df, writer = self._make_df_mock()

        commons.write_jdbc(df, "silver.clubes", mode="overwrite")

        writer.save.assert_called_once()

    def test_write_jdbc_tabela_correta(self):
        """write_jdbc deve passar a tabela correta para .option('dbtable', ...)."""
        import commons
        df, writer = self._make_df_mock()
        tabela = "gold.previsoes_proximas_partidas"

        commons.write_jdbc(df, tabela, mode="overwrite")

        calls = [str(c) for c in writer.option.call_args_list]
        assert any("dbtable" in c and tabela in c for c in calls), \
            f"dbtable={tabela} deve ser passado ao writer"


# ── Testes de log_pipeline_execution ──────────────────────────────────

class TestLogPipelineExecution:
    """log_pipeline_execution deve gravar em gold.pipeline_executions."""

    @patch("commons.psycopg2.connect")
    def test_insere_em_gold(self, mock_connect):
        """Deve inserir em gold.pipeline_executions (não diamond)."""
        import commons

        mock_conn = MagicMock()
        mock_cur  = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cur

        commons.log_pipeline_execution("silver", "success", records=100)

        sql = mock_cur.execute.call_args[0][0]
        assert "gold.pipeline_executions" in sql, \
            "Deve inserir em gold.pipeline_executions, não em diamond.*"

    @patch("commons.psycopg2.connect")
    def test_nao_lanca_excecao_em_falha_de_conexao(self, mock_connect):
        """Falha de conexão não deve propagar exceção (log opcional)."""
        import commons
        mock_connect.side_effect = Exception("connection refused")

        # Não deve levantar exceção
        commons.log_pipeline_execution("silver", "failed", error="test error")


# ── Testes de log_dq_check ─────────────────────────────────────────────

class TestLogDqCheck:
    """log_dq_check deve gravar em gold.data_quality_checks."""

    @patch("commons.psycopg2.connect")
    def test_insere_em_gold(self, mock_connect):
        """Deve inserir em gold.data_quality_checks (não diamond)."""
        import commons

        mock_conn = MagicMock()
        mock_cur  = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cur

        commons.log_dq_check("check_partidas", "silver.partidas", "success", checked=50)

        sql = mock_cur.execute.call_args[0][0]
        assert "gold.data_quality_checks" in sql, \
            "Deve inserir em gold.data_quality_checks, não em diamond.*"

    @patch("commons.psycopg2.connect")
    def test_nao_lanca_excecao_em_falha(self, mock_connect):
        """Falha silenciosa — não deve propagar exceção."""
        import commons
        mock_connect.side_effect = Exception("timeout")

        commons.log_dq_check("check_x", "tabela_x", "warning")


# ── Testes de JDBC_PROPS ───────────────────────────────────────────────

class TestJdbcProps:
    """Valida estrutura das propriedades JDBC."""

    def test_campos_obrigatorios(self):
        """JDBC_PROPS deve ter user, password, driver e stringtype."""
        import commons
        for campo in ("user", "password", "driver", "stringtype"):
            assert campo in commons.JDBC_PROPS, f"Campo '{campo}' ausente em JDBC_PROPS"

    def test_driver_postgresql(self):
        """Driver deve ser o PostgreSQL JDBC."""
        import commons
        assert "postgresql" in commons.JDBC_PROPS["driver"].lower()

    def test_jdbc_url_formato(self):
        """JDBC_URL deve começar com jdbc:postgresql://."""
        import commons
        assert commons.JDBC_URL.startswith("jdbc:postgresql://"), \
            f"JDBC_URL inválida: {commons.JDBC_URL}"
