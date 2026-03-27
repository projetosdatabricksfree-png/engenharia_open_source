FROM apache/spark:3.5.3

USER root

# Dependencias Python para os jobs
RUN pip install --no-cache-dir \
    requests \
    pandas \
    numpy \
    scikit-learn \
    psycopg2-binary \
    python-dotenv

# PostgreSQL JDBC driver
RUN curl -fsSL \
    https://jdbc.postgresql.org/download/postgresql-42.7.3.jar \
    -o /opt/spark/jars/postgresql-42.7.3.jar

USER spark
