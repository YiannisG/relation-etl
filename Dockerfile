FROM apache/airflow:3.3.1-python3.12

WORKDIR /opt/airflow

COPY pyproject.toml uv.lock ./
COPY dags/relation_etl ./dags/relation_etl
COPY --from=ghcr.io/astral-sh/uv:0.5.27 /uv /usr/local/bin/uv

RUN uv export --frozen --no-hashes --format requirements-txt -o requirements.txt \
    && pip install --no-cache-dir -r requirements.txt

