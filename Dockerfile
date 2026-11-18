FROM apache/airflow:3.1.5

USER airflow

RUN pip install --no-cache-dir \
    "psycopg[binary]>=3,<4"