# Base: Python 3.10 + Debian Bullseye
FROM python:3.10-bullseye AS spark-base

# ---- System deps ----------------------------------------------------------
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    vim \
    unzip \
    rsync \
    openjdk-17-jdk \
    build-essential \
    software-properties-common \
    ssh \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# ---- Environment ----------------------------------------------------------
ENV SPARK_HOME=/opt/spark \
    HADOOP_HOME=/opt/hadoop \
    SPARK_LOGS=/opt/spark/spark-events \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$JAVA_HOME/bin:$PATH

RUN mkdir -p $HADOOP_HOME $SPARK_HOME $SPARK_LOGS
WORKDIR $SPARK_HOME

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

# ---- Python dependencies --------------------------------------------------
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# ---- Spark configuration --------------------------------------------------
# Nếu có file conf, copy vào (sẽ thất bại nếu file không tồn tại trong context)
COPY --chown=root:root conf/spark-defaults.conf "$SPARK_HOME/conf"

# Make Spark scripts executable
RUN chmod +x $SPARK_HOME/sbin/* $SPARK_HOME/bin/*

# PySpark runtime envs
ENV PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3
ENV PYTHONPATH=""
ENV PYTHONPATH="$SPARK_HOME/python/:${PYTHONPATH}"

# ---- Non-root user (recommended) -----------------------------------------
RUN groupadd -r spark && useradd -m -r -g spark -s /bin/bash spark \
 && chown -R spark:spark /opt/spark

USER spark
WORKDIR $SPARK_HOME

# ---- Entrypoint -----------------------------------------------------------
COPY --chown=spark:spark entrypoint.sh .
RUN chmod +x /opt/spark/entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]
CMD ["bash"]

# ---- (Optional) Ports -----------------------------------------------------
EXPOSE 7077 8080 8081 4040

# Nhắc nhở build image
# Build lệnh: docker build -t spark-image .
