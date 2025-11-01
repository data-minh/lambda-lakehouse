FROM apache/airflow:2.11.0-python3.10

# Chạy với quyền root để cài đặt hệ thống package
USER root

# Cập nhật hệ thống và cài đặt các package cần thiết
RUN apt-get update && apt-get install -y wget gnupg openjdk-17-jdk && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Thiết lập biến môi trường JAVA_HOME cho OpenJDK 17
# ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
# ENV PATH="${JAVA_HOME}/bin:${PATH}"

################################################
################# ADD SPARK ####################
ENV SPARK_HOME=/opt/spark \
    HADOOP_HOME=/opt/hadoop \
    SPARK_LOGS=/opt/spark/spark-events \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$JAVA_HOME/bin:$PATH

RUN mkdir -p $HADOOP_HOME $SPARK_HOME $SPARK_LOGS

# ---- Install Apache Spark -------------------------------------------------
ARG SPARK_VERSION=3.5.1
ENV SPARK_VERSION=${SPARK_VERSION}

RUN set -eux; \
    curl -fSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz -o /tmp/spark.tgz; \
    tar -xzf /tmp/spark.tgz -C /opt/spark --strip-components=1; \
    rm -f /tmp/spark.tgz

# ---- Pre-bundle Spark JARs (Nessie + Iceberg + S3/MinIO) -----------------
# Versions (tùy chỉnh khi cần)
ARG SCALA_BINARY_VERSION=2.12
ARG NESSIE_VERSION=0.103.2
ARG ICEBERG_VERSION=1.8.1
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.262

# Tải JAR về và đặt trong $SPARK_HOME/jars để spark tự nạp classpath khi chạy
RUN set -eux; mkdir -p $SPARK_HOME/jars; \
    curl -fSL https://repo1.maven.org/maven2/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_${SCALA_BINARY_VERSION}/${NESSIE_VERSION}/nessie-spark-extensions-3.5_${SCALA_BINARY_VERSION}-${NESSIE_VERSION}.jar \
      -o $SPARK_HOME/jars/nessie-spark-extensions-3.5_${SCALA_BINARY_VERSION}-${NESSIE_VERSION}.jar; \
    curl -fSL https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_${SCALA_BINARY_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_${SCALA_BINARY_VERSION}-${ICEBERG_VERSION}.jar \
      -o $SPARK_HOME/jars/iceberg-spark-runtime-3.5_${SCALA_BINARY_VERSION}-${ICEBERG_VERSION}.jar; \
    curl -fSL https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
      -o $SPARK_HOME/jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar; \
    curl -fSL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
      -o $SPARK_HOME/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar; \
    curl -fSL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar \
      -o $SPARK_HOME/jars/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar

# Make Spark scripts executable
RUN chmod +x $SPARK_HOME/sbin/* $SPARK_HOME/bin/* && \
    chown -R airflow: $SPARK_HOME $HADOOP_HOME $SPARK_LOGS

# PySpark runtime envs
ENV PYSPARK_PYTHON=python \
    PYSPARK_DRIVER_PYTHON=python \
    PYTHONPATH="$SPARK_HOME/python:$PYTHONPATH"
################################################

# Chuyển về user airflow để cài đặt Python packages
USER airflow

# ---- Python dependencies --------------------------------------------------
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt
# Cài đặt các package Apache Spark cho Airflow
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark findspark

# Nhắc nhở build image
# Build lệnh: docker build -t airflow-spark-jars airflow\image-airflow-spark