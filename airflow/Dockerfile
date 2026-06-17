FROM apache/airflow:2.10.5

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends docker-compose-plugin \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
