FROM apache/airflow:2.7.2
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-11-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

RUN pip install --no-cache-dir apache-airflow apache-airflow-providers-apache-spark pyspark confluent-kafka[avro,json,protobuf]>=1.4.2 certifi requests
