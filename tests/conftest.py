"""
Configuração global do pytest.
Define marcadores e fixtures compartilhadas.
"""
import pytest


def pytest_configure(config):
    config.addinivalue_line("markers", "integracao: testes que requerem stack Docker rodando")
    config.addinivalue_line("markers", "unitario: testes unitários sem dependência externa")
