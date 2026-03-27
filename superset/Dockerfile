FROM apache/superset:3.1.3

USER root

# Driver PostgreSQL (ja incluido na imagem, mas garantimos)
RUN pip install --no-cache-dir psycopg2-binary==2.9.9

COPY superset_config.py /app/pythonpath/superset_config.py
COPY init/init_superset.sh /app/init_superset.sh
RUN chmod +x /app/init_superset.sh

USER superset
