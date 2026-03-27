import os

SECRET_KEY = os.getenv("SUPERSET_SECRET_KEY", "admin")

SQLALCHEMY_DATABASE_URI = (
    f"postgresql+psycopg2://"
    f"{os.getenv('POSTGRES_USER','admin')}:"
    f"{os.getenv('POSTGRES_PASSWORD','admin')}@"
    f"{os.getenv('POSTGRES_HOST','postgres')}:5432/superset"
)

FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "DASHBOARD_CROSS_FILTERS":    True,
    "DRILL_TO_DETAIL":            True,
    "EMBEDDABLE_CHARTS":          True,
}

BABEL_DEFAULT_LOCALE   = "pt_BR"
BABEL_DEFAULT_TIMEZONE = "America/Sao_Paulo"
SUPERSET_WEBSERVER_TIMEOUT = 300

# Desabilita execução assíncrona (Celery) — queries rodam de forma síncrona
# sem necessidade de um worker separado
GLOBAL_ASYNC_QUERIES = False
SQL_MAX_ROW = 100000

# Sem Celery configurado: desabilita resultados em cache de queries assíncronas
class CeleryConfig:
    broker_url        = "sqla+sqlite:////app/celerydb.sqlite"
    result_backend    = "db+sqlite:////app/celerydb.sqlite"
    worker_concurrency = 0

CELERY_CONFIG = CeleryConfig
