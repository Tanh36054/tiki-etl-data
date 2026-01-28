FROM apache/airflow:2.8.1-python3.11

USER root

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-17-jre-headless \
         procps \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-google \
    pyspark==3.5.0 \
    google-cloud-storage \
    google-cloud-bigquery \
    pandas \
    faker