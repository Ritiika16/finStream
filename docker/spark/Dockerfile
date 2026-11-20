FROM apache/spark:3.5.9

USER root

RUN python3 -m pip install --no-cache-dir \
    "psycopg[binary]==3.3.9" \
    "python-dotenv"

WORKDIR /opt/finstream
