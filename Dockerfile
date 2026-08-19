FROM apache/airflow:3.1.8

USER root

# Install Java + curl
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Install Spark
ENV SPARK_VERSION=4.1.2
ENV SPARK_HOME=/opt/spark

RUN curl -L \
    https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    -o /tmp/spark.tgz \
    && tar -xzf /tmp/spark.tgz -C /opt \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 ${SPARK_HOME} \
    && rm /tmp/spark.tgz

# Put spark-submit on PATH
ENV PATH="${SPARK_HOME}/bin:${PATH}"

USER airflow

# Install Airflow's Spark integration
RUN pip install --no-cache-dir \
    "apache-airflow==3.1.8" \
    apache-airflow-providers-apache-spark