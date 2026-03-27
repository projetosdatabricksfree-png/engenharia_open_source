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
