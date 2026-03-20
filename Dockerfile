FROM apache/airflow:2.7.1

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    build-essential \
    libpq-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow

WORKDIR /opt/airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.7.1/constraints-3.10.txt"

COPY --chown=airflow:root ./dags ./dags
COPY --chown=airflow:root ./models ./dbt/models
COPY --chown=airflow:root ./dbt_project.yml ./dbt/dbt_project.yml
COPY --chown=airflow:root ./scripts ./scripts
COPY --chown=airflow:root ./macros ./dbt/macros
# COPY --chown=airflow:root ./seeds ./dbt/seeds