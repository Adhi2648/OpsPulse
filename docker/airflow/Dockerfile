FROM apache/airflow:2.10.2-python3.11

USER root
WORKDIR /opt/airflow

COPY pyproject.toml README.md ./
COPY src ./src

RUN pip install --no-cache-dir \
    pandas \
    pydantic-settings \
    psycopg[binary] \
    python-dotenv && \
    pip install --no-cache-dir -e . --no-deps

USER airflow
