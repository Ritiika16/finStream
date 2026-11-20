FROM apache/airflow:3.1.3

USER airflow

RUN pip install --no-cache-dir \
    "apache-airflow-providers-postgres" \
    "psycopg[binary]==3.3.9"
