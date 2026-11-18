FROM python:3.12-slim

WORKDIR /opt/dbt

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir \
    -r /tmp/requirements.txt

ENV DBT_PROFILES_DIR=/opt/dbt/profiles

CMD ["sleep", "infinity"]