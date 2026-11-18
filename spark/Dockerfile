
# Python 3.12
FROM python:3.12-slim-bookworm AS python-runtime

# Java 21
FROM eclipse-temurin:21-jdk-jammy

# Copiar Python 3.12 desde la primera imagen
COPY --from=python-runtime /usr/local /usr/local

# Dependencias necesarias para Python y Spark
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       ca-certificates \
       libexpat1 \
       libsqlite3-0 \
       libbz2-1.0 \
       libffi8 \
       liblzma5 \
       libreadline8 \
       libncursesw6 \
       zlib1g \
       libssl3 \
       procps \
       curl \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1
ENV PYSPARK_PYTHON=python
ENV SPARK_LOCAL_IP=127.0.0.1

WORKDIR /opt/spark-etl

COPY requirements.txt /tmp/requirements.txt

RUN python -m pip install --no-cache-dir \
    -r /tmp/requirements.txt

CMD ["sleep", "infinity"]
