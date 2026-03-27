FROM apache/airflow:2.9.3-python3.11

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    docker.io \
    libpq-dev \
    gcc \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

RUN usermod -aG docker airflow 2>/dev/null || true

USER airflow

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
